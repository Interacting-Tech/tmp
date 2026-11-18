package machineclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Interacting-Tech/tmp/pkg/netutil"
	"github.com/Interacting-Tech/tmp/pkg/protocol"
	"github.com/gorilla/websocket"
)

var (
	wsWritePool sync.Pool
	readBufPool = sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, 1024))
		},
	}
	marshalBufPool = sync.Pool{
		New: func() any {
			b := make([]byte, 0, 512)
			return &b
		},
	}
)

// Client connects to the local arm-wrestling server running on the machine
// (ws://<machine-ip>:8080/ws?role=machine), reads the physical machine's data and commands,
// and transfers the rival's data to the machine according to PROTOCOL.md.
type Client struct {
	opts      Options
	machineIP net.IP
	serverURL string
	dialer    websocket.Dialer

	lifecycleMu sync.Mutex
	cancelRun   context.CancelFunc
	runDone     chan struct{}

	rivalMu                  sync.Mutex
	latestRival              *protocol.MachineTelemetry
	cachedRivalServerPID     string
	pendingTerminals         []*protocol.MachineTelemetry
	hasUnsentRivalUpdate     bool
	activeConn               *websocket.Conn
	activeSessionCancel      context.CancelFunc
	intentionalResetPending  bool
	slotResetWaiters         []chan struct{}
	writeMu                  sync.Mutex
	sendWake                 chan struct{}
	hasActiveRival           atomic.Bool
	serverConnected          atomic.Bool
	localMachineConnected    atomic.Bool
	latestLocalData          atomic.Pointer[protocol.ReceivedMachineData]
	latestCommand            atomic.Pointer[protocol.ServerCommand]
}

// New creates a new local machine client, resolving the IPv4 address of the host machine
// it runs on and building the PROTOCOL.md WebSocket endpoint URL.
func New(opts Options) (*Client, error) {
	cfg := opts.withDefaults()

	ip, err := netutil.ResolveMachineIP(netutil.IPResolveOptions{
		MachineIPOverride:  cfg.MachineIPOverride,
		PreferredInterface: cfg.PreferredInterface,
		AllowLoopback:      cfg.AllowLoopbackFallback,
	})
	if err != nil {
		return nil, err
	}

	wsURL, err := netutil.BuildMachineWSURL(ip, cfg.Port, cfg.PathAndQuery)
	if err != nil {
		return nil, err
	}

	netDialer := &net.Dialer{
		Timeout:   cfg.DialTimeout,
		KeepAlive: 15 * time.Second,
	}

	c := &Client{
		opts:      cfg,
		machineIP: ip,
		serverURL: wsURL,
		sendWake:  make(chan struct{}, 1),
		dialer: websocket.Dialer{
			HandshakeTimeout: cfg.DialTimeout,
			ReadBufferSize:   4096,
			WriteBufferSize:  4096,
			WriteBufferPool:  &wsWritePool,
			NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := netDialer.DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				if tcp, ok := conn.(*net.TCPConn); ok {
					// Disable Nagle's algorithm so 10ms telemetry frames never suffer 40ms coalescing delay.
					_ = tcp.SetNoDelay(true)
				}
				return conn, nil
			},
		},
	}
	return c, nil
}

// MachineIP returns the resolved IP address of the host machine this process runs on.
func (c *Client) MachineIP() net.IP {
	cp := make(net.IP, len(c.machineIP))
	copy(cp, c.machineIP)
	return cp
}

// ServerURL returns the full local WebSocket endpoint (e.g. ws://192.168.1.50:8080/ws?role=machine).
func (c *Client) ServerURL() string {
	return c.serverURL
}

// IsConnectedToServer reports whether the WebSocket connection to the local server is currently open.
func (c *Client) IsConnectedToServer() bool {
	return c.serverConnected.Load()
}

// IsLocalMachineConnected reports whether the physical machine in the other slot is currently connected.
func (c *Client) IsLocalMachineConnected() bool {
	return c.localMachineConnected.Load()
}

// LatestLocalData returns the most recent snapshot received from the local server (lock-free).
func (c *Client) LatestLocalData() *protocol.ReceivedMachineData {
	return c.latestLocalData.Load()
}

// LatestCommand returns the most recent command received from the local server (lock-free).
func (c *Client) LatestCommand() *protocol.ServerCommand {
	return c.latestCommand.Load()
}

// LatestLocalDataProtocolJSON formats the local machine's latest telemetry as a PROTOCOL.md
// section 3.1 JSON envelope ({"data":{...}}), or {"data":{}} if disconnected.
func (c *Client) LatestLocalDataProtocolJSON(includeServerPlayerID bool) []byte {
	snap := c.latestLocalData.Load()
	if snap == nil || !snap.HasMachineTelemetry || snap.NormalizedTelemetry == nil {
		return []byte(`{"data":{}}`)
	}
	return protocol.MarshalMachineEnvelope(
		snap.NormalizedTelemetry,
		includeServerPlayerID,
		snap.EffectiveServerPlayerID,
	)
}

// Start launches the non-blocking connection supervisor, receive loop, and rival send loop.
func (c *Client) Start(ctx context.Context) error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()

	if c.runDone != nil {
		select {
		case <-c.runDone:
		default:
			return nil
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	c.cancelRun = cancel
	c.runDone = done

	go func() {
		defer close(done)
		c.supervisorLoop(runCtx)
	}()

	return nil
}

// Stop gracefully closes the WebSocket connection and waits for background goroutines to exit.
func (c *Client) Stop(ctx context.Context) error {
	c.lifecycleMu.Lock()
	cancel := c.cancelRun
	done := c.runDone
	c.cancelRun = nil
	c.lifecycleMu.Unlock()

	if cancel == nil || done == nil {
		return nil
	}

	cancel()

	c.rivalMu.Lock()
	conn := c.activeConn
	c.rivalMu.Unlock()
	if conn != nil {
		_ = c.sendCloseControl(conn, "stopping client")
		_ = conn.Close()
	}

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ProvideRivalTelemetry queues the rival's telemetry for transmission to the local machine
// using PROTOCOL.md latest-message-wins semantics (section 4.1), while guaranteeing that
// terminal win/lose states are never overwritten before hitting the wire.
func (c *Client) ProvideRivalTelemetry(rival *protocol.MachineTelemetry) {
	if rival == nil {
		return
	}

	snapshot := rival.Clone()

	c.rivalMu.Lock()
	if snapshot.HasServerPlayerID && snapshot.ServerPlayerID != "" {
		c.cachedRivalServerPID = snapshot.ServerPlayerID
	}

	prevState := ""
	if c.latestRival != nil {
		prevState = c.latestRival.MultiplayerState
	}

	if protocol.IsTerminalMatchState(snapshot.MultiplayerState) &&
		snapshot.MultiplayerState != prevState &&
		len(c.pendingTerminals) < 4 {
		c.pendingTerminals = append(c.pendingTerminals, snapshot.Clone())
	}

	c.latestRival = snapshot
	c.hasUnsentRivalUpdate = true
	c.hasActiveRival.Store(true)
	c.rivalMu.Unlock()

	c.wakeSendLoop()
}

// ProvideRivalJSON parses a PROTOCOL.md JSON message containing rival telemetry and transfers
// it to the local machine. Returns false if raw is empty, a disconnect frame, or invalid JSON.
func (c *Client) ProvideRivalJSON(raw []byte) bool {
	telemetry, ok := protocol.ParseRivalEnvelope(raw)
	if !ok || telemetry == nil {
		return false
	}
	c.ProvideRivalTelemetry(telemetry)
	return true
}

// ClearRival clears active rival telemetry and cleanly cycles our WebSocket slot on the local
// server so stale rival data is cleared to {} (section 4.4) and any stuck MULTIPLAYER_WAITING
// or MULTIPLAYER_MID_MATCH state returns to MULTIPLAYER_IDLE (section 5).
func (c *Client) ClearRival(ctx context.Context) error {
	c.rivalMu.Lock()
	c.latestRival = nil
	c.cachedRivalServerPID = ""
	c.pendingTerminals = nil
	c.hasUnsentRivalUpdate = false
	c.hasActiveRival.Store(false)

	conn := c.activeConn
	sessCancel := c.activeSessionCancel
	var waitCh chan struct{}
	if conn != nil && sessCancel != nil {
		c.intentionalResetPending = true
		waitCh = make(chan struct{})
		c.slotResetWaiters = append(c.slotResetWaiters, waitCh)
	}
	c.rivalMu.Unlock()

	if conn == nil {
		return nil
	}

	// Send an RFC 6455 Close frame so the server runs its disconnect cleanup before echoing Close.
	if err := c.sendCloseControl(conn, "clearing rival slot"); err != nil && sessCancel != nil {
		sessCancel()
		_ = conn.Close()
	}

	fallbackTimer := time.NewTimer(300 * time.Millisecond)
	defer fallbackTimer.Stop()

	select {
	case <-waitCh:
		return nil
	case <-fallbackTimer.C:
		if sessCancel != nil {
			sessCancel()
		}
		_ = conn.Close()
		select {
		case <-waitCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) supervisorLoop(ctx context.Context) {
	backoff := c.opts.ReconnectInitialDelay

	for {
		if ctx.Err() != nil {
			return
		}

		wasIntentionalReset, connectedOnce := c.runSingleSession(ctx)
		if ctx.Err() != nil {
			return
		}

		if connectedOnce {
			backoff = c.opts.ReconnectInitialDelay
		}

		if wasIntentionalReset {
			// Reconnect immediately after ClearRival so local machine reception resumes without gap.
			continue
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		backoff *= 2
		if backoff > c.opts.ReconnectMaxDelay {
			backoff = c.opts.ReconnectMaxDelay
		}
	}
}

func (c *Client) runSingleSession(parentCtx context.Context) (wasIntentionalReset bool, connectedOnce bool) {
	sessCtx, sessCancel := context.WithCancel(parentCtx)
	defer sessCancel()

	dialCtx, dialCancel := context.WithTimeout(sessCtx, c.opts.DialTimeout)
	conn, resp, err := c.dialer.DialContext(dialCtx, c.serverURL, nil)
	dialCancel()
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		if parentCtx.Err() == nil {
			c.logDiag(fmt.Sprintf("dial %s failed", c.serverURL), err)
		}
		return false, false
	}

	connectedOnce = true
	conn.SetReadLimit(c.opts.MaxInboundMessageBytes)

	c.rivalMu.Lock()
	c.activeConn = conn
	c.activeSessionCancel = sessCancel
	c.intentionalResetPending = false
	if c.latestRival != nil {
		c.hasUnsentRivalUpdate = true
		c.wakeSendLoop()
	}
	c.rivalMu.Unlock()

	c.setServerConnected(true)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		c.receiveLoop(sessCtx, conn)
		sessCancel()
	}()

	go func() {
		defer wg.Done()
		c.sendLoop(sessCtx, conn)
		sessCancel()
	}()

	<-sessCtx.Done()
	_ = conn.Close()
	wg.Wait()

	c.rivalMu.Lock()
	wasIntentionalReset = c.intentionalResetPending && parentCtx.Err() == nil
	c.intentionalResetPending = false
	c.activeConn = nil
	c.activeSessionCancel = nil
	waiters := c.slotResetWaiters
	c.slotResetWaiters = nil
	c.rivalMu.Unlock()

	c.setLocalMachineConnected(false)
	c.setServerConnected(false)

	for _, ch := range waiters {
		close(ch)
	}

	return wasIntentionalReset, connectedOnce
}

func (c *Client) receiveLoop(ctx context.Context, conn *websocket.Conn) {
	var (
		lastLocalServerPID string
		lastMsgTS          int64
		hasLastMsgTS       bool
	)

	for {
		if ctx.Err() != nil {
			return
		}

		msgType, reader, err := conn.NextReader()
		if err != nil {
			if ctx.Err() == nil && !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				c.logDiag("local server read closed", err)
			}
			return
		}

		// PROTOCOL.md section 3.1: binary frames are discarded without dropping connection.
		if msgType != websocket.TextMessage {
			_, _ = io.Copy(io.Discard, reader)
			continue
		}

		buf := readBufPool.Get().(*bytes.Buffer)
		buf.Reset()
		_, copyErr := buf.ReadFrom(io.LimitReader(reader, c.opts.MaxInboundMessageBytes+1))
		if copyErr != nil {
			readBufPool.Put(buf)
			if errors.Is(copyErr, websocket.ErrReadLimit) {
				c.logDiag("inbound message exceeded size limit", copyErr)
				return
			}
			return
		}

		if int64(buf.Len()) > c.opts.MaxInboundMessageBytes || buf.Len() == 0 {
			readBufPool.Put(buf)
			continue
		}

		parsed, ok := protocol.ParseServerMessage(
			buf.Bytes(),
			lastLocalServerPID,
			lastMsgTS,
			hasLastMsgTS,
		)
		readBufPool.Put(buf)

		if !ok || parsed == nil {
			// PROTOCOL.md section 4.1: discard unparseable JSON and keep previous telemetry.
			c.logDiag("discarded invalid JSON frame from local server", nil)
			continue
		}

		switch parsed.Kind {
		case protocol.InboundCommand:
			if parsed.Command != nil {
				cmdCopy := *parsed.Command
				c.latestCommand.Store(&cmdCopy)
				c.invokeCommand(cmdCopy)
			}

		case protocol.InboundTelemetry:
			if parsed.Telemetry != nil {
				td := parsed.Telemetry
				if td.HasMachineTelemetry {
					lastLocalServerPID = td.EffectiveServerPlayerID
					if td.HasLastMessageTimestamp {
						lastMsgTS = td.LastMessageTimestamp
						hasLastMsgTS = true
					}
					c.setLocalMachineConnected(true)
				} else {
					lastLocalServerPID = ""
					lastMsgTS = 0
					hasLastMsgTS = false
					c.setLocalMachineConnected(false)
				}

				c.latestLocalData.Store(td)
				c.invokeLocalData(td)
			}
		}
	}
}

func (c *Client) sendLoop(ctx context.Context, conn *websocket.Conn) {
	var (
		outboundCount uint64
		lastSendTime  time.Time
	)

	keepSending := *c.opts.KeepSendingLatestRival
	cadence := uint64(c.opts.ServerPlayerIDCadence)
	if cadence == 0 {
		cadence = 1
	}

	ticker := time.NewTicker(c.opts.RivalSendInterval)
	defer ticker.Stop()

	for {
		if c.hasActiveRival.Load() && keepSending {
			select {
			case <-ctx.Done():
				return
			case <-c.sendWake:
			case <-ticker.C:
			}
		} else {
			select {
			case <-ctx.Done():
				return
			case <-c.sendWake:
			}
		}

		if c.opts.MinFrameSpacing > 0 && !lastSendTime.IsZero() {
			if rem := c.opts.MinFrameSpacing - time.Since(lastSendTime); rem > 0 {
				timer := time.NewTimer(rem)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}

		for {
			if ctx.Err() != nil {
				return
			}

			var (
				toSend             *protocol.MachineTelemetry
				fallbackServerPID  string
				isPriorityTerminal bool
			)

			c.rivalMu.Lock()
			if len(c.pendingTerminals) > 0 {
				toSend = c.pendingTerminals[0]
				c.pendingTerminals = c.pendingTerminals[1:]
				fallbackServerPID = c.cachedRivalServerPID
				isPriorityTerminal = true
				if len(c.pendingTerminals) == 0 &&
					c.latestRival != nil &&
					c.latestRival.MultiplayerState == toSend.MultiplayerState {
					c.hasUnsentRivalUpdate = false
				}
			} else if c.latestRival != nil && (c.hasUnsentRivalUpdate || keepSending) {
				toSend = c.latestRival
				fallbackServerPID = c.cachedRivalServerPID
				c.hasUnsentRivalUpdate = false
			}
			c.rivalMu.Unlock()

			if toSend == nil {
				break
			}

			outboundCount++
			includeSPID := ((outboundCount - 1) % cadence) == 0

			bufPtr := marshalBufPool.Get().(*[]byte)
			payload := protocol.AppendMachineEnvelope((*bufPtr)[:0], toSend, includeSPID, fallbackServerPID)

			err := c.writeTextFrame(conn, payload)
			*bufPtr = payload[:0]
			marshalBufPool.Put(bufPtr)

			if err != nil {
				if ctx.Err() == nil {
					c.logDiag("local server write failed", err)
				}
				return
			}

			lastSendTime = time.Now()

			if !isPriorityTerminal {
				break
			}
		}
	}
}

func (c *Client) writeTextFrame(conn *websocket.Conn, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	_ = conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
	return conn.WriteMessage(websocket.TextMessage, payload)
}

func (c *Client) sendCloseControl(conn *websocket.Conn, reason string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	msg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, reason)
	return conn.WriteControl(websocket.CloseMessage, msg, time.Now().Add(300*time.Millisecond))
}

func (c *Client) wakeSendLoop() {
	select {
	case c.sendWake <- struct{}{}:
	default:
	}
}

func (c *Client) setServerConnected(connected bool) {
	if c.serverConnected.Swap(connected) == connected {
		return
	}
	if fn := c.opts.OnServerConnectionChange; fn != nil {
		c.safeCall(func() { fn(connected) })
	}
}

func (c *Client) setLocalMachineConnected(connected bool) {
	if c.localMachineConnected.Swap(connected) == connected {
		return
	}
	if fn := c.opts.OnLocalMachineConnectionChange; fn != nil {
		c.safeCall(func() { fn(connected) })
	}
}

func (c *Client) invokeLocalData(data *protocol.ReceivedMachineData) {
	if fn := c.opts.OnLocalData; fn != nil {
		c.safeCall(func() { fn(data) })
	}
}

func (c *Client) invokeCommand(cmd protocol.ServerCommand) {
	if fn := c.opts.OnCommand; fn != nil {
		c.safeCall(func() { fn(cmd) })
	}
}

func (c *Client) logDiag(msg string, err error) {
	if fn := c.opts.OnDiagnostic; fn != nil {
		c.safeCall(func() { fn(msg, err) })
	}
}

func (c *Client) safeCall(fn func()) {
	defer func() {
		if r := recover(); r != nil && c.opts.OnDiagnostic != nil {
			func() {
				defer func() { _ = recover() }()
				c.opts.OnDiagnostic(fmt.Sprintf("recovered panic in callback: %v", r), nil)
			}()
		}
	}()
	fn()
}
