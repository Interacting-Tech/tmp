package bridge_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Interacting-Tech/tmp/pkg/protocol"
	"github.com/gorilla/websocket"
)

// simulatedMachineServer faithfully emulates the legacy arm-wrestling server described in PROTOCOL.md:
//   - Listens on ws://<bindIP>:<port>/ws?role=machine
//   - Pairs exactly 2 machines (3rd accepted on socket and ignored)
//   - Runs a 1ms main loop reading newest message per slot and running the match state machine
//   - Broadcasts opponent telemetry every 10ms with is_connected, client_id, last_message_timestamp,
//     and the section 4.3/4.4 opponent_torque sign flip (including -0.0 when remaining hand is "left")
type simulatedMachineServer struct {
	bindIP            net.IP
	port              int
	httpSrv           *http.Server
	postMatchDuration time.Duration
	startTime         time.Time

	mu                sync.Mutex
	slots             [2]*simSlot
	allConns          map[*simConn]struct{}
	matchState        string
	postMatchEntered  time.Time
	nextClientID      atomic.Int32
	stopLoop          chan struct{}
	loopDone          chan struct{}
}

type simConn struct {
	ws      *websocket.Conn
	writeMu sync.Mutex
}

func (sc *simConn) sendText(msg string) {
	sc.writeMu.Lock()
	defer sc.writeMu.Unlock()
	_ = sc.ws.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
	_ = sc.ws.WriteMessage(websocket.TextMessage, []byte(msg))
}

type simSlot struct {
	conn         *simConn
	clientID     int
	stored       map[string]json.RawMessage
	latestUnread atomic.Pointer[[]byte]
}

func startSimulatedMachineServer(bindIP net.IP, postMatchDuration time.Duration) (*simulatedMachineServer, error) {
	if postMatchDuration <= 0 {
		postMatchDuration = 2 * time.Second
	}

	ln, err := net.Listen("tcp", net.JoinHostPort(bindIP.String(), "0"))
	if err != nil {
		return nil, err
	}
	port := ln.Addr().(*net.TCPAddr).Port

	s := &simulatedMachineServer{
		bindIP:            bindIP,
		port:              port,
		postMatchDuration: postMatchDuration,
		startTime:         time.Now(),
		slots:             [2]*simSlot{{}, {}},
		allConns:          make(map[*simConn]struct{}),
		matchState:        protocol.ServerIdle,
		stopLoop:          make(chan struct{}),
		loopDone:          make(chan struct{}),
	}

	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("role") != "machine" {
			http.Error(w, "missing role=machine", http.StatusBadRequest)
			return
		}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		s.handleSocket(ws)
	})

	s.httpSrv = &http.Server{Handler: mux}

	go func() {
		_ = s.httpSrv.Serve(ln)
	}()
	go s.mainLoop()

	return s, nil
}

func (s *simulatedMachineServer) EndpointURL() string {
	return fmt.Sprintf("ws://%s:%d/ws?role=machine", s.bindIP.String(), s.port)
}

func (s *simulatedMachineServer) Port() int {
	return s.port
}

func (s *simulatedMachineServer) MatchState() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.matchState
}

func (s *simulatedMachineServer) SendOperatorInput(text string) {
	var payload string
	if text == "k" || text == "K" {
		s.mu.Lock()
		s.matchState = protocol.ServerIdle
		s.mu.Unlock()
		payload = `{"cmd":"STOP"}`
	} else {
		b, _ := json.Marshal(map[string]string{"cmd": text})
		payload = string(b)
	}
	s.broadcastAll(payload)
}

func (s *simulatedMachineServer) Close() {
	close(s.stopLoop)
	<-s.loopDone

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = s.httpSrv.Shutdown(ctx)

	s.mu.Lock()
	conns := make([]*simConn, 0, len(s.allConns))
	for c := range s.allConns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		_ = c.ws.Close()
	}
}

func (s *simulatedMachineServer) handleSocket(ws *websocket.Conn) {
	sc := &simConn{ws: ws}
	cid := int(s.nextClientID.Add(1))

	var assigned *simSlot
	s.mu.Lock()
	s.allConns[sc] = struct{}{}
	if s.slots[0].conn == nil {
		assigned = s.slots[0]
	} else if s.slots[1].conn == nil {
		assigned = s.slots[1]
	}
	if assigned != nil {
		assigned.conn = sc
		assigned.clientID = cid
		assigned.stored = nil
		assigned.latestUnread.Store(nil)
	}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.allConns, sc)
		if assigned != nil && assigned.conn == sc {
			assigned.conn = nil
			assigned.stored = nil
			assigned.latestUnread.Store(nil)
			if s.matchState != protocol.ServerIdle {
				s.matchState = protocol.ServerIdle
			}
		}
		s.mu.Unlock()
		_ = ws.Close()
	}()

	for {
		msgType, raw, err := ws.ReadMessage()
		if err != nil {
			return
		}
		if assigned != nil && msgType == websocket.TextMessage && len(raw) > 0 {
			cp := make([]byte, len(raw))
			copy(cp, raw)
			assigned.latestUnread.Store(&cp)
		}
	}
}

func (s *simulatedMachineServer) mainLoop() {
	defer close(s.loopDone)

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	tick := 0
	for {
		select {
		case <-s.stopLoop:
			return
		case <-ticker.C:
			tick++
			nowMs := time.Since(s.startTime).Milliseconds()

			var (
				broadcastCmd string
				targetConn   *simConn
				targetCmd    string
			)

			s.mu.Lock()
			s.consumeNewest(s.slots[0], nowMs)
			s.consumeNewest(s.slots[1], nowMs)

			st1 := slotStringField(s.slots[0], "multiplayer_state")
			st2 := slotStringField(s.slots[1], "multiplayer_state")

			switch s.matchState {
			case protocol.ServerIdle:
				if st1 == protocol.StateReady || st2 == protocol.StateReady {
					s.matchState = protocol.ServerWaiting
				}
			case protocol.ServerWaiting:
				if st1 == protocol.StateReady && st2 == protocol.StateReady {
					s.matchState = protocol.ServerMidMatch
					broadcastCmd = `{"cmd":{"START":1}}`
				}
			case protocol.ServerMidMatch:
				if st1 == protocol.StateGameOverWin {
					s.matchState = protocol.ServerPostMatch
					s.postMatchEntered = time.Now()
					targetConn = s.slots[1].conn
					targetCmd = `{"cmd":{"YOU_LOSE":1}}`
				} else if st1 == protocol.StateGameOverLose {
					s.matchState = protocol.ServerPostMatch
					s.postMatchEntered = time.Now()
					targetConn = s.slots[1].conn
					targetCmd = `{"cmd":{"YOU_WIN":0}}`
				} else if st2 == protocol.StateGameOverWin {
					s.matchState = protocol.ServerPostMatch
					s.postMatchEntered = time.Now()
					targetConn = s.slots[0].conn
					targetCmd = `{"cmd":{"YOU_LOSE":0}}`
				} else if st2 == protocol.StateGameOverLose {
					s.matchState = protocol.ServerPostMatch
					s.postMatchEntered = time.Now()
					targetConn = s.slots[0].conn
					targetCmd = `{"cmd":{"YOU_WIN":0}}`
				}
			case protocol.ServerPostMatch:
				if time.Since(s.postMatchEntered) >= s.postMatchDuration {
					s.matchState = protocol.ServerIdle
				}
			}
			s.mu.Unlock()

			if broadcastCmd != "" {
				s.broadcastAll(broadcastCmd)
			}
			if targetConn != nil && targetCmd != "" {
				targetConn.sendText(targetCmd)
			}

			if tick%10 == 0 {
				var c1, c2 *simConn
				var m1, m2 string

				s.mu.Lock()
				h1 := slotStringField(s.slots[0], "hand")
				h2 := slotStringField(s.slots[1], "hand")
				flip := protocol.DoesServerFlipOpponentTorque(h1, h2)

				c1 = s.slots[0].conn
				c2 = s.slots[1].conn
				if c1 != nil {
					m1 = buildRelayedJSON(s.slots[1], flip)
				}
				if c2 != nil {
					m2 = buildRelayedJSON(s.slots[0], flip)
				}
				s.mu.Unlock()

				if c1 != nil {
					c1.sendText(m1)
				}
				if c2 != nil {
					c2.sendText(m2)
				}
			}
		}
	}
}

func (s *simulatedMachineServer) consumeNewest(slot *simSlot, nowMs int64) {
	if slot.conn == nil {
		return
	}
	rawPtr := slot.latestUnread.Swap(nil)
	if rawPtr == nil {
		return
	}

	var top map[string]json.RawMessage
	if err := json.Unmarshal(*rawPtr, &top); err != nil || len(top) == 0 {
		return
	}

	keys := make([]string, 0, len(top))
	for k := range top {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var inner map[string]json.RawMessage
	if err := json.Unmarshal(top[keys[0]], &inner); err != nil || inner == nil {
		return
	}

	inner[protocol.FieldIsConnected] = json.RawMessage(`true`)
	inner[protocol.FieldClientID] = json.RawMessage(fmt.Sprintf("%d", slot.clientID))
	inner[protocol.FieldLastMessageTimestamp] = json.RawMessage(fmt.Sprintf("%d", nowMs))
	slot.stored = inner
}

func buildRelayedJSON(source *simSlot, flip bool) string {
	if source.conn == nil || source.stored == nil {
		if flip {
			return `{"data":{"opponent_torque":-0.0}}`
		}
		return `{"data":{}}`
	}

	cp := make(map[string]json.RawMessage, len(source.stored))
	for k, v := range source.stored {
		cp[k] = v
	}

	if flip {
		oppTorque := 0.0
		if raw, ok := cp["opponent_torque"]; ok {
			_ = json.Unmarshal(raw, &oppTorque)
		}
		if oppTorque == 0 {
			cp["opponent_torque"] = json.RawMessage(`-0.0`)
		} else {
			b, _ := json.Marshal(-oppTorque)
			cp["opponent_torque"] = b
		}
	}

	env := map[string]any{"data": cp}
	out, _ := json.Marshal(env)
	return string(out)
}

func slotStringField(slot *simSlot, key string) string {
	if slot.stored == nil {
		return ""
	}
	raw, ok := slot.stored[key]
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return s
}

func (s *simulatedMachineServer) broadcastAll(msg string) {
	s.mu.Lock()
	targets := make([]*simConn, 0, len(s.allConns))
	for c := range s.allConns {
		targets = append(targets, c)
	}
	s.mu.Unlock()

	for _, c := range targets {
		c.sendText(msg)
	}
}
