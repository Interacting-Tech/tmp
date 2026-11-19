package bridge

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"dev.azure.com/InteractingTechnologies/Glenfiddich/_git/Arm-Wrestling/pkg/protocol"
	"github.com/gorilla/websocket"
)

var (
	upstreamWritePool sync.Pool
	upstreamReadPool  = sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, 1024))
		},
	}
)

// ServerTransport abstracts the upstream connection from the machine bridge to our central server.
type ServerTransport interface {
	Start(ctx context.Context, handlers UpstreamHandlers) error
	Stop(ctx context.Context) error
	SendMachineData(payload []byte, isTerminal bool)
	SendMachineDisconnected()
	IsConnected() bool
}

// UpstreamHandlers receives rival events arriving from our server to be transferred to the machine.
type UpstreamHandlers struct {
	OnRivalTelemetry    func(*protocol.MachineTelemetry)
	OnRivalDisconnected func()
	OnServerStopCommand func()
}

// UpstreamOptions configures the WebSocket connection from the machine bridge to our server.
type UpstreamOptions struct {
	// ServerURL is the WebSocket URL of our server (ws://... or wss://...).
	ServerURL string

	// Headers holds optional HTTP headers sent during the WebSocket handshake (e.g. Authorization, X-Machine-ID).
	Headers http.Header

	// DialTimeout bounds connection setup to our server (default: 5s).
	DialTimeout time.Duration

	// WriteTimeout bounds each WebSocket write to our server (default: 2s).
	WriteTimeout time.Duration

	// PingInterval sends RFC 6455 WebSocket Ping control frames to our server to keep NAT/LB mappings
	// alive and detect silent WAN drops quickly (default: 10s).
	PingInterval time.Duration

	// PongTimeout closes and reconnects the upstream link if our server stops answering pings (default: 25s).
	PongTimeout time.Duration

	// ReconnectInitialDelay is the initial backoff when the connection to our server fails (default: 200ms).
	ReconnectInitialDelay time.Duration

	// ReconnectMaxDelay caps reconnect backoff to our server (default: 5s).
	ReconnectMaxDelay time.Duration

	// MaxInboundBytes caps incoming frame size from our server (default: 64 KiB).
	MaxInboundBytes int64

	// RivalTimeout clears the rival on the machine when our server relays no rival data for this
	// long while a rival is active (default: 1s; negative disables). The local server keeps relaying
	// the last rival frame indefinitely, so without it a stalled link would leave the handle
	// reflecting a frozen rival until PongTimeout expires.
	RivalTimeout time.Duration

	// Callbacks
	OnConnectionChange func(connected bool)
	OnDiagnostic       func(msg string, err error)
}

func (o UpstreamOptions) withDefaults() UpstreamOptions {
	out := o
	if out.DialTimeout <= 0 {
		out.DialTimeout = 5 * time.Second
	}
	if out.WriteTimeout <= 0 {
		out.WriteTimeout = 2 * time.Second
	}
	if out.PingInterval <= 0 {
		out.PingInterval = 10 * time.Second
	}
	if out.PongTimeout <= out.PingInterval {
		out.PongTimeout = 25 * time.Second
	}
	if out.ReconnectInitialDelay <= 0 {
		out.ReconnectInitialDelay = 200 * time.Millisecond
	}
	if out.ReconnectMaxDelay < out.ReconnectInitialDelay {
		out.ReconnectMaxDelay = 5 * time.Second
	}
	if out.MaxInboundBytes <= 0 {
		out.MaxInboundBytes = 64 * 1024
	}
	if out.RivalTimeout == 0 {
		out.RivalTimeout = time.Second
	}
	return out
}

// WebSocketServerClient maintains a fast, auto-reconnecting outbound WebSocket link to our server,
// streaming the local machine's PROTOCOL.md JSON data upstream (latest-message-wins, zero queue lag)
// and receiving rival PROTOCOL.md JSON data from our server.
type WebSocketServerClient struct {
	opts   UpstreamOptions
	dialer websocket.Dialer

	lifecycleMu sync.Mutex
	cancelRun   context.CancelFunc
	runDone     chan struct{}

	outMu            sync.Mutex
	latestPayload    []byte
	hasUnsentLatest  bool
	priorityPayloads [][]byte
	sendWake         chan struct{}
	activeConn       *websocket.Conn
	writeMu          sync.Mutex
	connected        atomic.Bool
}

// NewWebSocketServerClient creates an upstream transport to our server.
func NewWebSocketServerClient(opts UpstreamOptions) (*WebSocketServerClient, error) {
	cfg := opts.withDefaults()
	if cfg.ServerURL == "" {
		return nil, fmt.Errorf("upstream ServerURL is required")
	}

	netDialer := &net.Dialer{
		Timeout:   cfg.DialTimeout,
		KeepAlive: 15 * time.Second,
	}

	return &WebSocketServerClient{
		opts:     cfg,
		sendWake: make(chan struct{}, 1),
		dialer: websocket.Dialer{
			HandshakeTimeout: cfg.DialTimeout,
			ReadBufferSize:   4096,
			WriteBufferSize:  4096,
			WriteBufferPool:  &upstreamWritePool,
			NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := netDialer.DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				if tcp, ok := conn.(*net.TCPConn); ok {
					_ = tcp.SetNoDelay(true)
				}
				return conn, nil
			},
		},
	}, nil
}

// IsConnected reports whether the WebSocket connection to our server is currently open.
func (u *WebSocketServerClient) IsConnected() bool {
	return u.connected.Load()
}

// Start launches the upstream supervisor loop.
func (u *WebSocketServerClient) Start(ctx context.Context, handlers UpstreamHandlers) error {
	u.lifecycleMu.Lock()
	defer u.lifecycleMu.Unlock()

	if u.runDone != nil {
		select {
		case <-u.runDone:
		default:
			return nil
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	u.cancelRun = cancel
	u.runDone = done

	go func() {
		defer close(done)
		u.supervisorLoop(runCtx, handlers)
	}()

	return nil
}

// Stop closes the connection to our server and waits for background goroutines to finish.
func (u *WebSocketServerClient) Stop(ctx context.Context) error {
	u.lifecycleMu.Lock()
	cancel := u.cancelRun
	done := u.runDone
	u.cancelRun = nil
	u.lifecycleMu.Unlock()

	if cancel == nil || done == nil {
		return nil
	}

	cancel()

	u.outMu.Lock()
	conn := u.activeConn
	u.outMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SendMachineData queues outbound local machine JSON for our server.
// Regular telemetry uses latest-message-wins coalescing so WAN jitter never creates a backlog;
// terminal win/lose transitions are queued in a non-droppable priority FIFO.
func (u *WebSocketServerClient) SendMachineData(payload []byte, isTerminal bool) {
	if len(payload) == 0 {
		return
	}

	u.outMu.Lock()
	if isTerminal && len(u.priorityPayloads) < 8 {
		cp := make([]byte, len(payload))
		copy(cp, payload)
		u.priorityPayloads = append(u.priorityPayloads, cp)
	} else {
		u.latestPayload = append(u.latestPayload[:0], payload...)
		u.hasUnsentLatest = true
	}
	u.outMu.Unlock()

	u.wakeSender()
}

// SendMachineDisconnected notifies our server that the local physical machine disconnected ({"data":{}}).
func (u *WebSocketServerClient) SendMachineDisconnected() {
	u.outMu.Lock()
	u.hasUnsentLatest = false
	if len(u.priorityPayloads) < 8 {
		u.priorityPayloads = append(u.priorityPayloads, []byte(`{"data":{}}`))
	}
	u.outMu.Unlock()

	u.wakeSender()
}

func (u *WebSocketServerClient) supervisorLoop(ctx context.Context, handlers UpstreamHandlers) {
	backoff := u.opts.ReconnectInitialDelay

	for {
		if ctx.Err() != nil {
			return
		}

		connectedOnce := u.runSession(ctx, handlers)
		if ctx.Err() != nil {
			return
		}

		if connectedOnce {
			backoff = u.opts.ReconnectInitialDelay
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		backoff *= 2
		if backoff > u.opts.ReconnectMaxDelay {
			backoff = u.opts.ReconnectMaxDelay
		}
	}
}

func (u *WebSocketServerClient) runSession(parentCtx context.Context, handlers UpstreamHandlers) bool {
	sessCtx, sessCancel := context.WithCancel(parentCtx)
	defer sessCancel()

	dialCtx, dialCancel := context.WithTimeout(sessCtx, u.opts.DialTimeout)
	conn, resp, err := u.dialer.DialContext(dialCtx, u.opts.ServerURL, u.opts.Headers)
	dialCancel()
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		if parentCtx.Err() == nil && u.opts.OnDiagnostic != nil {
			u.opts.OnDiagnostic(fmt.Sprintf("upstream dial %s failed", u.opts.ServerURL), err)
		}
		return false
	}

	conn.SetReadLimit(u.opts.MaxInboundBytes)
	_ = conn.SetReadDeadline(time.Now().Add(u.opts.PongTimeout))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(u.opts.PongTimeout))
	})

	u.outMu.Lock()
	u.activeConn = conn
	if len(u.latestPayload) > 0 {
		u.hasUnsentLatest = true
		u.wakeSender()
	}
	u.outMu.Unlock()

	u.setConnected(true)

	var (
		wg sync.WaitGroup
		// lastRivalAt is the monoNow time of the latest rival frame while a rival is active, else 0.
		lastRivalAt atomic.Int64
	)
	wg.Add(3)

	go func() {
		defer wg.Done()
		u.readLoop(sessCtx, conn, handlers, &lastRivalAt)
		sessCancel()
	}()

	go func() {
		defer wg.Done()
		u.writeLoop(sessCtx, conn)
		sessCancel()
	}()

	go func() {
		defer wg.Done()
		u.pingLoop(sessCtx, conn)
		sessCancel()
	}()

	if u.opts.RivalTimeout > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u.rivalWatchdog(sessCtx, handlers, &lastRivalAt)
		}()
	}

	<-sessCtx.Done()
	_ = conn.Close()
	wg.Wait()

	u.outMu.Lock()
	u.activeConn = nil
	u.outMu.Unlock()

	u.setConnected(false)

	// Safety: if our server disconnects while a rival was active, immediately clear rival torque on the machine!
	if lastRivalAt.Load() != 0 && handlers.OnRivalDisconnected != nil {
		handlers.OnRivalDisconnected()
	}

	return true
}

func (u *WebSocketServerClient) rivalWatchdog(
	ctx context.Context,
	handlers UpstreamHandlers,
	lastRivalAt *atomic.Int64,
) {
	timeout := int64(u.opts.RivalTimeout)
	ticker := time.NewTicker(max(u.opts.RivalTimeout/10, 5*time.Millisecond))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		last := lastRivalAt.Load()
		if last == 0 || monoNow()-last < timeout || !lastRivalAt.CompareAndSwap(last, 0) {
			continue
		}
		if u.opts.OnDiagnostic != nil {
			u.opts.OnDiagnostic(fmt.Sprintf("no rival data from our server for %v; clearing rival", u.opts.RivalTimeout), nil)
		}
		if handlers.OnRivalDisconnected != nil {
			handlers.OnRivalDisconnected()
		}
	}
}

var monoEpoch = time.Now()

// monoNow returns a positive monotonic timestamp in nanoseconds, immune to wall-clock steps.
func monoNow() int64 {
	return int64(time.Since(monoEpoch)) + 1
}

func (u *WebSocketServerClient) readLoop(
	ctx context.Context,
	conn *websocket.Conn,
	handlers UpstreamHandlers,
	lastRivalAt *atomic.Int64,
) {
	for {
		if ctx.Err() != nil {
			return
		}

		msgType, reader, err := conn.NextReader()
		if err != nil {
			if ctx.Err() == nil && u.opts.OnDiagnostic != nil &&
				!websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				u.opts.OnDiagnostic("upstream read error", err)
			}
			return
		}

		// Any valid incoming frame refreshes the dead-link watchdog deadline.
		_ = conn.SetReadDeadline(time.Now().Add(u.opts.PongTimeout))

		if msgType != websocket.TextMessage {
			_, _ = io.Copy(io.Discard, reader)
			continue
		}

		buf := upstreamReadPool.Get().(*bytes.Buffer)
		buf.Reset()
		_, copyErr := buf.ReadFrom(io.LimitReader(reader, u.opts.MaxInboundBytes+1))
		if copyErr != nil || int64(buf.Len()) > u.opts.MaxInboundBytes || buf.Len() == 0 {
			upstreamReadPool.Put(buf)
			if copyErr != nil {
				return
			}
			continue
		}

		raw := buf.Bytes()

		// 1. Check if our server sentrival telemetry in PROTOCOL.md format
		if rivalTelemetry, ok := protocol.ParseRivalEnvelope(raw); ok && rivalTelemetry != nil {
			lastRivalAt.Store(monoNow())
			upstreamReadPool.Put(buf)
			if handlers.OnRivalTelemetry != nil {
				handlers.OnRivalTelemetry(rivalTelemetry)
			}
			continue
		}

		// 2. Otherwise inspect via ParseServerMessage for {"data":{}} disconnect or {"cmd":"STOP"}
		parsed, ok := protocol.ParseServerMessage(raw, "", 0, false)
		upstreamReadPool.Put(buf)
		if !ok || parsed == nil {
			continue
		}

		switch parsed.Kind {
		case protocol.InboundTelemetry:
			if parsed.Telemetry != nil && !parsed.Telemetry.HasMachineTelemetry && lastRivalAt.Swap(0) != 0 {
				if handlers.OnRivalDisconnected != nil {
					handlers.OnRivalDisconnected()
				}
			}
		case protocol.InboundCommand:
			if parsed.Command != nil && parsed.Command.Kind == protocol.CommandStop {
				lastRivalAt.Store(0)
				if handlers.OnServerStopCommand != nil {
					handlers.OnServerStopCommand()
				} else if handlers.OnRivalDisconnected != nil {
					handlers.OnRivalDisconnected()
				}
			}
		}
	}
}

func (u *WebSocketServerClient) writeLoop(ctx context.Context, conn *websocket.Conn) {
	var sendScratch []byte

	for {
		select {
		case <-ctx.Done():
			return
		case <-u.sendWake:
		}

		for {
			if ctx.Err() != nil {
				return
			}

			var priorityFrame []byte
			hasLatest := false

			u.outMu.Lock()
			if len(u.priorityPayloads) > 0 {
				priorityFrame = u.priorityPayloads[0]
				u.priorityPayloads = u.priorityPayloads[1:]
			} else if u.hasUnsentLatest && len(u.latestPayload) > 0 {
				sendScratch = append(sendScratch[:0], u.latestPayload...)
				u.hasUnsentLatest = false
				hasLatest = true
			}
			u.outMu.Unlock()

			var toWrite []byte
			if priorityFrame != nil {
				toWrite = priorityFrame
			} else if hasLatest {
				toWrite = sendScratch
			} else {
				break
			}

			u.writeMu.Lock()
			_ = conn.SetWriteDeadline(time.Now().Add(u.opts.WriteTimeout))
			err := conn.WriteMessage(websocket.TextMessage, toWrite)
			u.writeMu.Unlock()

			if err != nil {
				if ctx.Err() == nil && u.opts.OnDiagnostic != nil {
					u.opts.OnDiagnostic("upstream write failed", err)
				}
				return
			}

			if priorityFrame == nil {
				break
			}
		}
	}
}

func (u *WebSocketServerClient) pingLoop(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(u.opts.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			u.writeMu.Lock()
			err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(u.opts.WriteTimeout))
			u.writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

func (u *WebSocketServerClient) wakeSender() {
	select {
	case u.sendWake <- struct{}{}:
	default:
	}
}

func (u *WebSocketServerClient) setConnected(connected bool) {
	if u.connected.Swap(connected) == connected {
		return
	}
	if u.opts.OnConnectionChange != nil {
		u.opts.OnConnectionChange(connected)
	}
}
