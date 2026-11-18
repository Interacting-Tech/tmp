package battleserver_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Interacting-Tech/tmp/pkg/battleserver"
	"github.com/gorilla/websocket"
)

// bridgeFrame is a frame as current machine firmware reports it, including the three fields a
// PROTOCOL.md server adds. Bridges strip those before sending, but the server must cope with them.
const bridgeFrame = `{
  "data": {
    "MAC_address": "11:22:33:44:55:66",
    "player_id": "player-2",
    "hand": "left",
    "multiplayer_state": "MULTIPLAYER_SM_RUN",
    "single_player_state": "",
    "machine_torque": -12.5,
    "position": -3.1,
    "velocity": 0.4,
    "left_limit": -45.0,
    "right_limit": 45.0,
    "peak_force": 28.0,
    "timeStamp": 123456790,
    "is_connected": true,
    "client_id": 2,
    "last_message_timestamp": 86400123
  }
}`

const bridgeFrameRelayed = `{"data":{"MAC_address":"11:22:33:44:55:66","player_id":"player-2","hand":"left","multiplayer_state":"MULTIPLAYER_SM_RUN","single_player_state":"","machine_torque":-12.5,"position":-3.1,"velocity":0.4,"left_limit":-45,"right_limit":45,"peak_force":28,"timeStamp":123456790}}`

const rivalGone = `{"data":{}}`

func TestRelay_MachineFramesReachTheRivalInBothDirections(t *testing.T) {
	url := startServer(t, battleserver.Options{})
	a := connect(t, url, "cabinet-A", "")
	b := connect(t, url, "cabinet-B", "")
	waitPaired(t, a, b)

	a.send(t, bridgeFrame)
	if got := b.next(t); got != bridgeFrameRelayed {
		t.Fatalf("B received\n got: %s\nwant: %s", got, bridgeFrameRelayed)
	}

	b.send(t, playerFrame("from-B"))
	if got := a.next(t); got != playerFrameRelayed("from-B") {
		t.Fatalf("A received\n got: %s\nwant: %s", got, playerFrameRelayed("from-B"))
	}
}

func TestRelay_OnlyTelemetryIsRelayed(t *testing.T) {
	url := startServer(t, battleserver.Options{})
	a := connect(t, url, "cabinet-A", "")
	b := connect(t, url, "cabinet-B", "")
	waitPaired(t, a, b)

	a.send(t, `{"cmd":"STOP"}`)
	a.send(t, `{"cmd":{"YOU_LOSE":1}}`)
	a.send(t, `not json`)
	a.send(t, `{"data":{"is_connected":true,"client_id":7,"last_message_timestamp":1}}`)
	if err := a.ws.WriteMessage(websocket.BinaryMessage, []byte{1, 2, 3}); err != nil {
		t.Fatalf("write binary: %v", err)
	}
	a.send(t, playerFrame("after"))

	if got := b.next(t); got != playerFrameRelayed("after") {
		t.Fatalf("B received a frame that is not machine telemetry: %s", got)
	}
}

func TestRelay_MachineGoneFramesClearTheRival(t *testing.T) {
	url := startServer(t, battleserver.Options{})
	a := connect(t, url, "cabinet-A", "")
	b := connect(t, url, "cabinet-B", "")
	waitPaired(t, a, b)

	for _, gone := range []string{
		`{"data":{}}`,
		strings.Replace(bridgeFrame, `"is_connected": true`, `"is_connected": false`, 1),
	} {
		a.send(t, gone)
		if got := b.next(t); got != rivalGone {
			t.Fatalf("after %s B received %s, want %s", gone, got, rivalGone)
		}
		// The bridge stays paired, so the rival reappears when the machine does.
		a.send(t, playerFrame("back"))
		if got := b.next(t); got != playerFrameRelayed("back") {
			t.Fatalf("B received %s after the machine came back", got)
		}
	}
}

func TestRivalLeaves_RemainingMachineIsToldAndBattlesTheNextMachine(t *testing.T) {
	url := startServer(t, battleserver.Options{})
	a := connect(t, url, "cabinet-A", "")
	b := connect(t, url, "cabinet-B", "")
	waitPaired(t, a, b)

	_ = a.ws.Close()
	if got := b.next(t); got != rivalGone {
		t.Fatalf("B received %s after A left, want %s", got, rivalGone)
	}

	a2 := connect(t, url, "cabinet-A", "")
	waitPaired(t, a2, b)
	waitPaired(t, b, a2)
}

func TestThirdMachine_WaitsUntilAMachineIsFree(t *testing.T) {
	url := startServer(t, battleserver.Options{})
	a := connect(t, url, "cabinet-A", "")
	b := connect(t, url, "cabinet-B", "")
	waitPaired(t, a, b)
	c := connect(t, url, "cabinet-C", "")

	stopFeed := c.feed(playerFrame("from-C"))
	defer stopFeed()
	a.expectNothing(t, 150*time.Millisecond)
	b.expectNothing(t, 150*time.Millisecond)

	// B must be told its rival is gone before it hears from the next one.
	_ = a.ws.Close()
	if got := b.next(t); got != rivalGone {
		t.Fatalf("B received %s after A left, want %s first", got, rivalGone)
	}
	if got := b.next(t); got != playerFrameRelayed("from-C") {
		t.Fatalf("B received %s, want C's telemetry", got)
	}
	b.send(t, playerFrame("from-B"))
	if got := c.next(t); got != playerFrameRelayed("from-B") {
		t.Fatalf("C received %s, want B's telemetry", got)
	}
}

func TestReconnect_SameMachineIDReplacesTheOldConnection(t *testing.T) {
	url := startServer(t, battleserver.Options{})
	stale := connect(t, url, "cabinet-A", "")
	b := connect(t, url, "cabinet-B", "")
	waitPaired(t, stale, b)

	fresh := connect(t, url, "cabinet-A", "")
	if got := b.next(t); got != rivalGone {
		t.Fatalf("B received %s when cabinet-A reconnected, want %s", got, rivalGone)
	}
	select {
	case msg, ok := <-stale.frames:
		if ok {
			t.Fatalf("stale connection received %s instead of being closed", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("stale connection of cabinet-A was not closed")
	}
	waitPaired(t, fresh, b)
	waitPaired(t, b, fresh)
}

func TestAuthToken_RequiredWhenConfigured(t *testing.T) {
	url := startServer(t, battleserver.Options{AuthToken: "s3cret"})

	for _, header := range []string{"", "Bearer wrong", "s3cret", "Basic s3cret"} {
		h := http.Header{}
		if header != "" {
			h.Set("Authorization", header)
		}
		ws, resp, err := websocket.DefaultDialer.Dial(url, h)
		if err == nil {
			_ = ws.Close()
			t.Fatalf("connection with Authorization %q was accepted", header)
		}
		if resp == nil || resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("Authorization %q: got response %v, want 401", header, resp)
		}
	}

	a := connect(t, url, "cabinet-A", "s3cret")
	b := connect(t, url, "cabinet-B", "s3cret")
	waitPaired(t, a, b)
}

func TestIdleTimeout_SilentBridgeIsDroppedAndPingsKeepABridgeAlive(t *testing.T) {
	const idle = 300 * time.Millisecond
	url := startServer(t, battleserver.Options{IdleTimeout: idle})
	a := connect(t, url, "cabinet-A", "")
	b := connect(t, url, "cabinet-B", "")
	waitPaired(t, a, b)

	// B sends nothing but pings, the way a bridge without a connected machine does.
	stopPings := make(chan struct{})
	defer close(stopPings)
	go func() {
		ticker := time.NewTicker(idle / 6)
		defer ticker.Stop()
		for {
			select {
			case <-stopPings:
				return
			case <-ticker.C:
				_ = b.ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second))
			}
		}
	}()

	stopFeed := a.feed(playerFrame("from-A"))
	time.Sleep(3 * idle)
	b.drain()
	if got := b.next(t); got != playerFrameRelayed("from-A") {
		t.Fatalf("B received %s after only pinging for 3 idle timeouts, want A's telemetry", got)
	}

	// A stays connected but goes silent.
	stopFeed()
	silentSince := time.Now()
	for {
		if got := b.next(t); got == rivalGone {
			break
		}
	}
	if silent := time.Since(silentSince); silent < idle/2 {
		t.Fatalf("A was dropped %v after going silent, well before the %v idle timeout", silent, idle)
	}
}

func TestIdleTimeout_BridgeThatNeverSendsAnythingIsDropped(t *testing.T) {
	const idle = 200 * time.Millisecond
	url := startServer(t, battleserver.Options{IdleTimeout: idle})
	connectedAt := time.Now()
	silent := connect(t, url, "cabinet-A", "")

	select {
	case msg, ok := <-silent.frames:
		if ok {
			t.Fatalf("silent bridge received %s instead of being dropped", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("a bridge that never sent anything was still connected after 3s")
	}
	if lasted := time.Since(connectedAt); lasted < idle/2 {
		t.Fatalf("silent bridge was dropped after %v, well before the %v idle timeout", lasted, idle)
	}
}

func startServer(t *testing.T, opts battleserver.Options) string {
	t.Helper()
	srv := battleserver.New(opts)
	hs := httptest.NewServer(srv)
	t.Cleanup(func() {
		srv.Close()
		hs.Close()
	})
	return "ws" + strings.TrimPrefix(hs.URL, "http")
}

type testBridge struct {
	ws     *websocket.Conn
	frames chan string
}

func connect(t *testing.T, url, machineID, token string) *testBridge {
	t.Helper()
	h := http.Header{}
	if machineID != "" {
		h.Set(battleserver.MachineIDHeader, machineID)
	}
	if token != "" {
		h.Set("Authorization", "Bearer "+token)
	}
	ws, _, err := websocket.DefaultDialer.Dial(url, h)
	if err != nil {
		t.Fatalf("connect %s: %v", machineID, err)
	}
	t.Cleanup(func() { _ = ws.Close() })

	b := &testBridge{ws: ws, frames: make(chan string, 4096)}
	go func() {
		defer close(b.frames)
		for {
			mt, msg, err := ws.ReadMessage()
			if err != nil {
				return
			}
			if mt == websocket.TextMessage {
				b.frames <- string(msg)
			}
		}
	}()
	return b
}

func (b *testBridge) send(t *testing.T, frame string) {
	t.Helper()
	if err := b.ws.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
		t.Fatalf("send frame: %v", err)
	}
}

// feed sends frame every 10 ms, like a bridge with a connected machine, until the returned stop
// function is called.
func (b *testBridge) feed(frame string) (stop func()) {
	quit := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-quit:
				return
			case <-ticker.C:
				if err := b.ws.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
					return
				}
			}
		}
	}()
	return func() {
		select {
		case <-quit:
		default:
			close(quit)
		}
		<-done
	}
}

// next returns the next frame the bridge received.
func (b *testBridge) next(t *testing.T) string {
	t.Helper()
	select {
	case msg, ok := <-b.frames:
		if !ok {
			t.Fatalf("connection closed while waiting for a frame")
		}
		return msg
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for a frame")
	}
	return ""
}

func (b *testBridge) expectNothing(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case msg, ok := <-b.frames:
		if ok {
			t.Fatalf("received unexpected frame: %s", msg)
		}
		t.Fatalf("connection closed unexpectedly")
	case <-time.After(d):
	}
}

func (b *testBridge) drain() {
	for {
		select {
		case _, ok := <-b.frames:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

// waitPaired returns once frames from one bridge reach the other, with nothing older still on its
// way. A bridge's connection is established slightly before the server pairs it.
func waitPaired(t *testing.T, from, to *testBridge) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for i := 0; ; i++ {
		marker := fmt.Sprintf("sync-%d-%d", time.Now().UnixNano(), i)
		from.send(t, playerFrame(marker))
		timeout := time.After(20 * time.Millisecond)
	wait:
		for {
			select {
			case msg, ok := <-to.frames:
				if !ok {
					t.Fatalf("connection closed while waiting to be paired")
				}
				if msg == playerFrameRelayed(marker) {
					return
				}
			case <-timeout:
				break wait
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("bridges were not paired within 3s")
		}
	}
}

func playerFrame(playerID string) string {
	return fmt.Sprintf(`{"data":{"player_id":%q,"machine_torque":1.5}}`, playerID)
}

func playerFrameRelayed(playerID string) string {
	return fmt.Sprintf(`{"data":{"MAC_address":"","player_id":%q,"hand":"","multiplayer_state":"","single_player_state":"","machine_torque":1.5,"position":0,"velocity":0,"left_limit":0,"right_limit":0,"peak_force":0,"timeStamp":0}}`, playerID)
}
