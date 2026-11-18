// Package battleserver is our central server. Machine bridges connect to it over WebSocket; it
// pairs machines into battles and relays each machine's PROTOCOL.md telemetry to its rival.
//
// The battle itself (ready, start, win/lose) is run by each machine's local PROTOCOL.md server,
// where the bridge stands in for the rival. This server only decides who fights whom and carries
// the telemetry between them.
package battleserver

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Interacting-Tech/tmp/pkg/protocol"
	"github.com/gorilla/websocket"
)

// MachineIDHeader is the handshake header a bridge uses to identify its machine.
const MachineIDHeader = "X-Machine-ID"

const maxMachineIDLen = 128

// rivalGone is what a bridge receives when its machine has no rival (PROTOCOL.md section 6.1).
var rivalGone = []byte(`{"data":{}}`)

// Options configures a Server.
type Options struct {
	// AuthToken, when set, must be presented by every bridge as "Authorization: Bearer <token>".
	// When empty, any client may connect.
	AuthToken string

	// IdleTimeout drops a bridge that sends neither telemetry nor WebSocket pings for this long
	// (default: 30s; bridges ping every 10s).
	IdleTimeout time.Duration

	// WriteTimeout bounds each WebSocket write to a bridge (default: 2s).
	WriteTimeout time.Duration

	// MaxInboundBytes caps the size of a frame from a bridge (default: 64 KiB).
	MaxInboundBytes int64

	// Logf receives connection and battle events (default: discarded).
	Logf func(format string, args ...any)
}

func (o Options) withDefaults() Options {
	out := o
	if out.IdleTimeout <= 0 {
		out.IdleTimeout = 30 * time.Second
	}
	if out.WriteTimeout <= 0 {
		out.WriteTimeout = 2 * time.Second
	}
	if out.MaxInboundBytes <= 0 {
		out.MaxInboundBytes = 64 * 1024
	}
	if out.Logf == nil {
		out.Logf = func(string, ...any) {}
	}
	return out
}

// Server is an http.Handler for the WebSocket endpoint that machine bridges connect to.
type Server struct {
	opts     Options
	upgrader websocket.Upgrader

	mu        sync.Mutex
	closed    bool
	machines  map[string]*machine
	waiting   []*machine
	anonymous uint64
	sessions  sync.WaitGroup
}

type machine struct {
	id   string
	ws   *websocket.Conn
	out  outbox
	done chan struct{}

	// Guarded by Server.mu.
	rival *machine
	gone  bool

	// Owned by the machine's read goroutine.
	lastState string
	scratch   []byte
}

// New creates a Server.
func New(opts Options) *Server {
	return &Server{
		opts:     opts.withDefaults(),
		upgrader: websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096},
		machines: make(map[string]*machine),
	}
}

// ServeHTTP upgrades a bridge's connection and serves it until the bridge disconnects.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	id := strings.TrimSpace(r.Header.Get(MachineIDHeader))
	if len(id) > maxMachineIDLen {
		http.Error(w, MachineIDHeader+" is too long", http.StatusBadRequest)
		return
	}

	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	m := &machine{id: id, ws: ws, done: make(chan struct{})}
	m.out.wake = make(chan struct{}, 1)

	if !s.join(m) {
		_ = ws.Close()
		return
	}
	defer s.sessions.Done()

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		m.writeLoop(s.opts.WriteTimeout)
	}()

	s.readLoop(m)

	s.leave(m)
	close(m.done)
	_ = ws.Close()
	<-writerDone
}

// Close disconnects every bridge and rejects new ones.
func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	conns := make([]*websocket.Conn, 0, len(s.machines))
	for _, m := range s.machines {
		conns = append(conns, m.ws)
	}
	s.mu.Unlock()

	for _, ws := range conns {
		_ = ws.Close()
	}
	s.sessions.Wait()
}

func (s *Server) authorized(r *http.Request) bool {
	if s.opts.AuthToken == "" {
		return true
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return ok && subtle.ConstantTimeCompare([]byte(strings.TrimSpace(token)), []byte(s.opts.AuthToken)) == 1
}

func (s *Server) join(m *machine) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}

	if m.id == "" {
		s.anonymous++
		m.id = fmt.Sprintf("anonymous-%d", s.anonymous)
	}
	// A bridge that reconnects before its old connection is known to be dead takes its place.
	if old := s.machines[m.id]; old != nil {
		s.opts.Logf("machine %q reconnected; closing its previous connection", m.id)
		s.detachLocked(old)
		_ = old.ws.Close()
	}

	s.machines[m.id] = m
	s.waiting = append(s.waiting, m)
	s.sessions.Add(1)
	s.opts.Logf("machine %q connected (%d connected)", m.id, len(s.machines))
	s.pairWaitingLocked()
	return true
}

func (s *Server) leave(m *machine) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m.gone {
		return
	}
	s.detachLocked(m)
	s.opts.Logf("machine %q disconnected (%d connected)", m.id, len(s.machines))
}

// detachLocked removes m from the server. Its rival, if any, is told the rival is gone and goes
// back to waiting.
func (s *Server) detachLocked(m *machine) {
	if m.gone {
		return
	}
	m.gone = true
	if s.machines[m.id] == m {
		delete(s.machines, m.id)
	}

	if rival := m.rival; rival != nil {
		m.rival, rival.rival = nil, nil
		rival.out.putPriority(rivalGone)
		s.waiting = append(s.waiting, rival)
		s.opts.Logf("battle ended: %q left, %q is waiting for a rival", m.id, rival.id)
	} else {
		for i, w := range s.waiting {
			if w == m {
				s.waiting = append(s.waiting[:i], s.waiting[i+1:]...)
				break
			}
		}
	}
	s.pairWaitingLocked()
}

// pairWaitingLocked decides who fights whom. Stage 1: machines without a rival are paired in the
// order they became free, and a pair stays together until one of them disconnects.
func (s *Server) pairWaitingLocked() {
	for len(s.waiting) >= 2 {
		a, b := s.waiting[0], s.waiting[1]
		s.waiting = s.waiting[2:]
		a.rival, b.rival = b, a
		s.opts.Logf("battle ready: %q vs %q", a.id, b.id)
	}
}

func (s *Server) readLoop(m *machine) {
	ws := m.ws
	ws.SetReadLimit(s.opts.MaxInboundBytes)
	_ = ws.SetReadDeadline(time.Now().Add(s.opts.IdleTimeout))
	ws.SetPingHandler(func(appData string) error {
		_ = ws.SetReadDeadline(time.Now().Add(s.opts.IdleTimeout))
		err := ws.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(s.opts.WriteTimeout))
		var netErr net.Error
		if errors.Is(err, websocket.ErrCloseSent) || (errors.As(err, &netErr) && netErr.Timeout()) {
			return nil
		}
		return err
	})

	for {
		msgType, raw, err := ws.ReadMessage()
		if err != nil {
			return
		}
		_ = ws.SetReadDeadline(time.Now().Add(s.opts.IdleTimeout))
		if msgType == websocket.TextMessage {
			s.handleFrame(m, raw)
		}
	}
}

// handleFrame relays one frame from m's bridge to m's rival. Only telemetry and "my machine is
// gone" frames are relayed, re-serialized, so a bridge can never make this server send its rival
// a command or anything this server did not understand.
func (s *Server) handleFrame(m *machine, raw []byte) {
	if telemetry, ok := protocol.ParseRivalEnvelope(raw); ok && telemetry != nil {
		// A win/lose result must reach the rival even if it is slow to read; everything else is
		// superseded by the next frame 10 ms later.
		mustDeliver := protocol.IsTerminalMatchState(telemetry.MultiplayerState) &&
			telemetry.MultiplayerState != m.lastState
		m.lastState = telemetry.MultiplayerState
		m.scratch = protocol.AppendMachineEnvelope(m.scratch[:0], telemetry, true, "")

		s.mu.Lock()
		if rival := m.rival; rival != nil {
			if mustDeliver {
				rival.out.putPriority(m.scratch)
			} else {
				rival.out.putLatest(m.scratch)
			}
		}
		s.mu.Unlock()
		return
	}

	// {"data":{}} or "is_connected": false: the bridge is connected but its machine is not.
	parsed, ok := protocol.ParseServerMessage(raw, "", 0, false, "", false)
	if !ok || parsed.Kind != protocol.InboundTelemetry || parsed.Telemetry == nil || parsed.Telemetry.HasMachineTelemetry {
		return
	}
	m.lastState = ""

	s.mu.Lock()
	if rival := m.rival; rival != nil {
		rival.out.putPriority(rivalGone)
	}
	s.mu.Unlock()
}

func (m *machine) writeLoop(writeTimeout time.Duration) {
	var scratch []byte
	for {
		select {
		case <-m.done:
			return
		case <-m.out.wake:
		}

		for {
			frame, ok := m.out.next(&scratch)
			if !ok {
				break
			}
			_ = m.ws.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := m.ws.WriteMessage(websocket.TextMessage, frame); err != nil {
				_ = m.ws.Close()
				return
			}
		}
	}
}

// outbox holds the frames waiting to be written to one bridge. Telemetry is latest-wins, so a slow
// or stalled bridge never builds a backlog of stale rival positions. Priority frames (a win/lose
// result, the rival leaving) are queued in order and are never replaced by telemetry.
type outbox struct {
	mu        sync.Mutex
	latest    []byte
	hasLatest bool
	priority  [][]byte
	wake      chan struct{}
}

const maxPriorityFrames = 16

func (o *outbox) putLatest(frame []byte) {
	o.mu.Lock()
	o.latest = append(o.latest[:0], frame...)
	o.hasLatest = true
	o.mu.Unlock()
	o.signal()
}

// putPriority queues frame and drops any unsent telemetry, which is older than frame.
func (o *outbox) putPriority(frame []byte) {
	cp := append([]byte(nil), frame...)
	o.mu.Lock()
	if len(o.priority) >= maxPriorityFrames {
		o.priority = o.priority[1:]
	}
	o.priority = append(o.priority, cp)
	o.hasLatest = false
	o.mu.Unlock()
	o.signal()
}

// next returns the next frame to write. The result is only valid until the following call with
// the same scratch buffer.
func (o *outbox) next(scratch *[]byte) ([]byte, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.priority) > 0 {
		frame := o.priority[0]
		o.priority = o.priority[1:]
		return frame, true
	}
	if o.hasLatest {
		o.hasLatest = false
		*scratch, o.latest = o.latest, (*scratch)[:0]
		return *scratch, true
	}
	return nil, false
}

func (o *outbox) signal() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}
