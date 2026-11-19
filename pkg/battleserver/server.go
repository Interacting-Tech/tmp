// Package battleserver is our central server. It pairs machines into battles and relays each
// machine's PROTOCOL.md telemetry to its rival. A machine reaches it in one of two ways:
//
//   - Directly (MachineHandler): the machine connects to this server as it would to a PROTOCOL.md
//     server. This server sends it the rival's telemetry every 10 ms and the START, YOU_WIN and
//     YOU_LOSE commands.
//   - Through a machine bridge (the Server itself as a handler): the machine stays connected to
//     its local PROTOCOL.md server, where the bridge stands in for the rival, and that local
//     server sends the commands.
//
// Either way this server follows the PROTOCOL.md section 5 match states for each pair and decides
// a battle itself when a handle reaches one of its position limits.
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

	"dev.azure.com/InteractingTechnologies/Glenfiddich/_git/Arm-Wrestling/pkg/protocol"
	"github.com/gorilla/websocket"
)

// MachineIDHeader is the handshake header a bridge uses to identify its machine.
const MachineIDHeader = "X-Machine-ID"

const maxMachineIDLen = 128

// rivalGone is what a machine or bridge receives while it has no rival (PROTOCOL.md section 6.1).
var rivalGone = []byte(`{"data":{}}`)

// The PROTOCOL.md section 6.2 commands sent to machines that are connected directly. The numbers
// carry no meaning; they are ones the document shows for each command.
var (
	cmdStart   = []byte(`{"cmd":{"START":1}}`)
	cmdYouWin  = []byte(`{"cmd":{"YOU_WIN":0}}`)
	cmdYouLose = []byte(`{"cmd":{"YOU_LOSE":1}}`)
)

// Options configures a Server.
type Options struct {
	// AuthToken, when set, must be presented by every bridge as "Authorization: Bearer <token>".
	// When empty, any client may connect.
	AuthToken string

	// IdleTimeout drops a connection that sends neither telemetry nor WebSocket pings for this
	// long (default: 30s; bridges ping every 10s).
	IdleTimeout time.Duration

	// SendInterval is how often a directly connected machine is sent its rival's telemetry
	// (default: 10ms, as in PROTOCOL.md section 6.1).
	SendInterval time.Duration

	// RivalTimeout is how long a directly connected machine may stay silent before its rival is
	// shown no rival and their battle is abandoned, so a handle never keeps reflecting a frozen
	// rival (default: 1s; negative disables, which is what PROTOCOL.md section 4.4 describes).
	// For machines behind bridges, the bridge's own rival timeout does this.
	RivalTimeout time.Duration

	// WriteTimeout bounds each WebSocket write to a bridge (default: 2s).
	WriteTimeout time.Duration

	// MaxInboundBytes caps the size of a frame from a bridge (default: 64 KiB).
	MaxInboundBytes int64

	// PostMatchDuration is how long a decided battle stays decided before the pair can start
	// another (default: 2s, as in PROTOCOL.md section 5).
	PostMatchDuration time.Duration

	// WinAtOppositeLimit reverses which position limit means victory. By default a machine wins
	// when its handle reaches the limit on the side of its player's hand (right hand: right_limit,
	// left hand: left_limit) and loses when it reaches the other limit.
	WinAtOppositeLimit bool

	// Logf receives connection and battle events (default: discarded).
	Logf func(format string, args ...any)
}

func (o Options) withDefaults() Options {
	out := o
	if out.IdleTimeout <= 0 {
		out.IdleTimeout = 30 * time.Second
	}
	if out.SendInterval <= 0 {
		out.SendInterval = 10 * time.Millisecond
	}
	if out.RivalTimeout == 0 {
		out.RivalTimeout = time.Second
	}
	if out.WriteTimeout <= 0 {
		out.WriteTimeout = 2 * time.Second
	}
	if out.MaxInboundBytes <= 0 {
		out.MaxInboundBytes = 64 * 1024
	}
	if out.PostMatchDuration <= 0 {
		out.PostMatchDuration = 2 * time.Second
	}
	if out.Logf == nil {
		out.Logf = func(string, ...any) {}
	}
	return out
}

// Server is an http.Handler for the WebSocket endpoint that machine bridges connect to. Its
// MachineHandler is the endpoint for machines that connect directly.
type Server struct {
	opts     Options
	upgrader websocket.Upgrader
	now      func() time.Time
	started  time.Time

	mu          sync.Mutex
	closed      bool
	machines    map[machineKey]*machine
	waiting     []*machine
	anonymous   uint64
	connections int
	scratch     []byte
	sessions    sync.WaitGroup
}

type machineKey struct {
	direct bool
	id     string
}

type machine struct {
	id string
	// direct is true for a machine connected to this server itself, false for a machine bridge.
	direct bool
	// clientID is this server's connection number, sent to a direct rival as "client_id".
	clientID int
	ws       *websocket.Conn
	out      outbox
	done     chan struct{}

	// Guarded by Server.mu.
	gone       bool
	identified bool
	rival      *machine
	battle     *battle
	// last is the machine's newest telemetry in its current battle, nil while it has not sent
	// any or its machine is gone. lastAt is when it arrived.
	last   *protocol.MachineTelemetry
	lastAt time.Time
	// declared is the result this server decided for the machine. While set it replaces
	// multiplayer_state in what the rival receives.
	declared string
	// relayedState is the multiplayer_state the rival was last sent.
	relayedState string
}

// battle is the PROTOCOL.md section 5 match state of one pair of machines.
type battle struct {
	state          string
	postMatchUntil time.Time
}

// New creates a Server.
func New(opts Options) *Server {
	return &Server{
		opts:     opts.withDefaults(),
		upgrader: websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096},
		now:      time.Now,
		started:  time.Now(),
		machines: make(map[machineKey]*machine),
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
	s.serve(w, r, &machine{id: id})
}

// MachineHandler is the endpoint for machines that connect to this server directly, the way
// PROTOCOL.md section 2 describes: ws://<server>:<port>/ws?role=machine. As in PROTOCOL.md there
// is no authentication, so Options.AuthToken does not apply to it.
func (s *Server) MachineHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("role") != "machine" {
			http.Error(w, "connect with ?role=machine", http.StatusBadRequest)
			return
		}
		s.serve(w, r, &machine{direct: true})
	})
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request, m *machine) {
	upgrader := s.upgrader
	if m.direct {
		// Machine firmware is not a browser; some WebSocket client libraries send an Origin
		// header that does not match the server, which the default check would refuse.
		upgrader.CheckOrigin = func(*http.Request) bool { return true }
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	m.ws = ws
	m.done = make(chan struct{})
	m.out.wake = make(chan struct{}, 1)

	if !s.join(m, remoteHost(r)) {
		_ = ws.Close()
		return
	}
	defer s.sessions.Done()

	var loops sync.WaitGroup
	loops.Add(1)
	go func() {
		defer loops.Done()
		m.writeLoop(s.opts.WriteTimeout)
	}()
	if m.direct {
		loops.Add(1)
		go func() {
			defer loops.Done()
			s.feedLoop(m)
		}()
	}

	s.readLoop(m)

	s.leave(m)
	close(m.done)
	_ = ws.Close()
	loops.Wait()
}

func remoteHost(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// Close disconnects every machine and bridge and rejects new ones.
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

func (s *Server) join(m *machine, remoteHost string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}

	s.connections++
	m.clientID = s.connections
	switch {
	case m.direct:
		// PROTOCOL.md machines do not identify themselves when they connect.
		m.id = fmt.Sprintf("machine-%d", m.clientID)
	case m.id == "":
		s.anonymous++
		m.id = fmt.Sprintf("anonymous-%d", s.anonymous)
	}
	// A bridge that reconnects before its old connection is known to be dead takes its place.
	if old := s.machines[m.key()]; old != nil {
		s.opts.Logf("machine %q reconnected; closing its previous connection", m.id)
		s.detachLocked(old)
		_ = old.ws.Close()
	}

	s.machines[m.key()] = m
	s.waiting = append(s.waiting, m)
	s.sessions.Add(1)
	if m.direct {
		s.opts.Logf("machine %q connected directly from %s (%d connected)", m.id, remoteHost, len(s.machines))
	} else {
		s.opts.Logf("machine %q connected (%d connected)", m.id, len(s.machines))
	}
	s.pairWaitingLocked()
	return true
}

func (m *machine) key() machineKey {
	return machineKey{direct: m.direct, id: m.id}
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
	if s.machines[m.key()] == m {
		delete(s.machines, m.key())
	}

	if rival := m.rival; rival != nil {
		m.leaveBattle()
		rival.leaveBattle()
		rival.out.putPriority(rivalGone)
		s.waiting = append(s.waiting, rival)
		s.opts.Logf("%q left; %q is waiting for a rival", m.id, rival.id)
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
	for {
		a, b := s.takePairLocked()
		if a == nil {
			return
		}
		a.rival, b.rival = b, a
		bt := &battle{state: protocol.ServerIdle}
		a.battle, b.battle = bt, bt
		s.opts.Logf("paired %q with %q", a.id, b.id)
	}
}

// takePairLocked removes and returns the two longest-waiting machines that can battle each other.
// A directly connected machine and one behind a bridge cannot: the bridge's local server applies
// its own torque sign rule and sends its own commands.
func (s *Server) takePairLocked() (a, b *machine) {
	for i, first := range s.waiting {
		for j := i + 1; j < len(s.waiting); j++ {
			if second := s.waiting[j]; second.direct == first.direct {
				s.waiting = append(s.waiting[:j], s.waiting[j+1:]...)
				s.waiting = append(s.waiting[:i], s.waiting[i+1:]...)
				return first, second
			}
		}
	}
	return nil, nil
}

func (m *machine) leaveBattle() {
	m.rival, m.battle, m.last = nil, nil, nil
	m.declared, m.relayedState = "", ""
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

// handleFrame takes one frame from m. Only telemetry and "my machine is gone" frames are used,
// and telemetry is re-serialized for the rival, so a connection can never make this server send
// its rival a command or anything this server did not understand.
func (s *Server) handleFrame(m *machine, raw []byte) {
	if telemetry, ok := protocol.ParseRivalEnvelope(raw); ok && telemetry != nil {
		s.mu.Lock()
		if m.direct && !m.identified {
			m.identified = true
			s.opts.Logf("machine %q is MAC_address %q, player %q, %s hand",
				m.id, telemetry.MACAddress, telemetry.PlayerID, handName(telemetry.Hand))
		}
		if m.rival != nil {
			previousState := ""
			if m.last != nil {
				previousState = m.last.MultiplayerState
			}
			m.last, m.lastAt = telemetry, s.now()
			s.advanceBattleLocked(m, previousState)
			// A directly connected rival is sent m's newest telemetry by its own feedLoop.
			if !m.direct {
				s.relayLocked(m)
			}
		}
		s.mu.Unlock()
		return
	}

	// {"data":{}} or "is_connected": false: the bridge is connected but its machine is not.
	parsed, ok := protocol.ParseServerMessage(raw, "", 0, false)
	if !ok || parsed.Kind != protocol.InboundTelemetry || parsed.Telemetry == nil || parsed.Telemetry.HasMachineTelemetry {
		return
	}

	s.mu.Lock()
	s.machineGoneLocked(m, "is gone")
	s.mu.Unlock()
}

// machineGoneLocked records that m's machine is gone while m's connection stays: its rival is
// shown no rival and their battle is abandoned. The rival reappears with m's next telemetry.
func (s *Server) machineGoneLocked(m *machine, why string) {
	rival := m.rival
	if rival == nil {
		return
	}
	m.last, m.relayedState = nil, ""
	if m.battle.state != protocol.ServerIdle {
		s.opts.Logf("battle between %q and %q abandoned: machine %q %s", m.id, rival.id, m.id, why)
	}
	m.battle.state = protocol.ServerIdle
	m.declared, rival.declared = "", ""
	rival.out.putPriority(rivalGone)
}

// advanceBattleLocked runs the PROTOCOL.md section 5 match states for m's battle on m's newest
// telemetry, and decides the battle when m's handle reaches a position limit.
func (s *Server) advanceBattleLocked(m *machine, previousState string) {
	b, rival := m.battle, m.rival
	now := s.now()

	if b.state == protocol.ServerPostMatch && !now.Before(b.postMatchUntil) {
		b.state = protocol.ServerIdle
		m.declared, rival.declared = "", ""
	}

	mine, theirs := m.last.MultiplayerState, ""
	if rival.last != nil {
		theirs = rival.last.MultiplayerState
	}

	// A machine that becomes ready again is asking for a new battle, so the one in progress ended
	// without a result (for example, an operator stopped it on the machine's local server).
	if b.state == protocol.ServerMidMatch && mine == protocol.StateReady && previousState != protocol.StateReady {
		s.opts.Logf("battle between %q and %q abandoned: %q is ready for a new one", m.id, rival.id, m.id)
		b.state = protocol.ServerWaiting
	}

	switch b.state {
	case protocol.ServerIdle, protocol.ServerWaiting:
		if mine == protocol.StateReady || theirs == protocol.StateReady {
			b.state = protocol.ServerWaiting
		}
		if b.state == protocol.ServerWaiting && mine == protocol.StateReady && theirs == protocol.StateReady {
			b.state = protocol.ServerMidMatch
			s.opts.Logf("battle started: %q vs %q", m.id, rival.id)
			if m.direct {
				m.out.putPriority(cmdStart)
				rival.out.putPriority(cmdStart)
			}
		}

	case protocol.ServerMidMatch:
		if protocol.IsTerminalMatchState(mine) {
			b.state = protocol.ServerPostMatch
			b.postMatchUntil = now.Add(s.opts.PostMatchDuration)
			s.opts.Logf("battle decided: %q reported %s", m.id, mine)
			// Only the rival of the reporting machine is told (PROTOCOL.md section 6.2). A rival
			// behind a bridge is told by its local server, from the relayed state.
			if m.direct {
				if mine == protocol.StateGameOverWin {
					rival.out.putPriority(cmdYouLose)
				} else {
					rival.out.putPriority(cmdYouWin)
				}
			}
			return
		}
		// The handle may be anywhere before the machines are told to start, so a limit only
		// counts while the machine reports that the battle is running.
		if mine != protocol.StateRun {
			return
		}
		won, limit := limitResult(m.last, s.opts.WinAtOppositeLimit)
		if limit == "" {
			return
		}
		winner, loser := m, rival
		if !won {
			winner, loser = rival, m
		}
		b.state = protocol.ServerPostMatch
		b.postMatchUntil = now.Add(s.opts.PostMatchDuration)
		s.opts.Logf("battle decided: %q (%s hand) reached its %s at position %v: %q wins, %q loses",
			m.id, handName(m.last.Hand), limit, m.last.Position, winner.id, loser.id)

		if m.direct {
			winner.out.putPriority(cmdYouWin)
			loser.out.putPriority(cmdYouLose)
			return
		}
		// Bridges: each local server tells its machine the result when it sees the rival report
		// the opposite one.
		winner.declared, loser.declared = protocol.StateGameOverWin, protocol.StateGameOverLose
		// m's bridge learns the result from the rival's telemetry; do not wait for its next frame.
		if rival.last != nil {
			s.relayLocked(rival)
		}
	}
}

// limitResult reports which position limit t's handle has reached ("" for none) and whether
// reaching it means t's player won.
func limitResult(t *protocol.MachineTelemetry, winAtOppositeLimit bool) (won bool, limit string) {
	if !(t.LeftLimit < t.RightLimit) {
		return false, "" // the machine reported no usable limits
	}
	atOwnSide := false
	switch {
	case t.Position <= t.LeftLimit:
		limit = "left_limit"
		atOwnSide = protocol.NormalizeHand(t.Hand) == protocol.HandLeft
	case t.Position >= t.RightLimit:
		limit = "right_limit"
		atOwnSide = protocol.NormalizeHand(t.Hand) != protocol.HandLeft
	default:
		return false, ""
	}
	return atOwnSide != winAtOppositeLimit, limit
}

func handName(hand string) string {
	if protocol.NormalizeHand(hand) == protocol.HandLeft {
		return protocol.HandLeft
	}
	return protocol.HandRight
}

// forRival returns t as the rival receives it: a left-handed player's torque has its sign reversed.
func forRival(t *protocol.MachineTelemetry) protocol.MachineTelemetry {
	out := *t
	if protocol.NormalizeHand(out.Hand) == protocol.HandLeft {
		out.MachineTorque = protocol.SanitizeFloat(-out.MachineTorque)
	}
	return out
}

// feedLoop sends a directly connected machine its rival's telemetry every SendInterval, whether
// or not the rival has sent anything new (PROTOCOL.md section 6.1).
func (s *Server) feedLoop(m *machine) {
	ticker := time.NewTicker(s.opts.SendInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			s.mu.Lock()
			s.feedLocked(m)
			s.mu.Unlock()
		}
	}
}

func (s *Server) feedLocked(m *machine) {
	rival := m.rival
	if rival != nil && rival.last != nil && s.opts.RivalTimeout > 0 && s.now().Sub(rival.lastAt) > s.opts.RivalTimeout {
		s.opts.Logf("machine %q has sent nothing for %v; %q is shown no rival until it does", rival.id, s.opts.RivalTimeout, m.id)
		s.machineGoneLocked(rival, "stopped sending")
		return
	}
	if rival == nil || rival.last == nil {
		m.out.putLatest(rivalGone)
		return
	}
	out := forRival(rival.last)
	s.scratch = protocol.AppendOpponentEnvelope(s.scratch[:0], &out, rival.clientID, rival.lastAt.Sub(s.started).Milliseconds())
	m.out.putLatest(s.scratch)
}

// relayLocked sends m's newest telemetry to its rival's bridge.
func (s *Server) relayLocked(m *machine) {
	out := forRival(m.last)
	if m.declared != "" {
		out.MultiplayerState = m.declared
	} else if s.heldReadyLocked(m) {
		out.MultiplayerState = protocol.StateReady
	}

	// A win/lose result must reach the rival even if it is slow to read; everything else is
	// superseded by the next frame 10 ms later.
	mustDeliver := protocol.IsTerminalMatchState(out.MultiplayerState) && out.MultiplayerState != m.relayedState
	m.relayedState = out.MultiplayerState

	s.scratch = protocol.AppendMachineEnvelope(s.scratch[:0], &out, true, "")
	if mustDeliver {
		m.rival.out.putPriority(s.scratch)
	} else {
		m.rival.out.putLatest(s.scratch)
	}
}

// heldReadyLocked reports whether m must still be shown to its rival as ready. A machine leaves
// MULTIPLAYER_SM_READY as soon as its own local server starts it, but the rival's local server
// starts the rival only while it sees both machines ready (PROTOCOL.md section 5). So once the
// battle has started, m stays ready in the rival's eyes until the rival has started too.
func (s *Server) heldReadyLocked(m *machine) bool {
	return m.battle.state == protocol.ServerMidMatch &&
		m.rival.last != nil && m.rival.last.MultiplayerState == protocol.StateReady
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

// outbox holds the frames waiting to be written to one connection. Telemetry is latest-wins, so a
// slow or stalled connection never builds a backlog of stale rival positions. Priority frames (a
// command, a win/lose result, the rival leaving) are queued in order and are never replaced by
// telemetry.
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
