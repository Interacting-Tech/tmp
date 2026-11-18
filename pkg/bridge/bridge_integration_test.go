package bridge_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Interacting-Tech/tmp/pkg/bridge"
	"github.com/Interacting-Tech/tmp/pkg/machineclient"
	"github.com/Interacting-Tech/tmp/pkg/netutil"
	"github.com/Interacting-Tech/tmp/pkg/protocol"
	"github.com/gorilla/websocket"
)

// testCentralRelayServer simulates "our server" in the cloud, pairing two remote MachineBridge
// connections over WebSockets and relaying each machine's PROTOCOL.md JSON to its rival.
type testCentralRelayServer struct {
	httpSrv *http.Server
	url     string

	mu    sync.Mutex
	peers [2]*centralPeer
}

type centralPeer struct {
	ws      *websocket.Conn
	writeMu sync.Mutex
}

func (p *centralPeer) sendText(msg []byte) {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	_ = p.ws.SetWriteDeadline(time.Now().Add(time.Second))
	_ = p.ws.WriteMessage(websocket.TextMessage, msg)
}

func startTestCentralRelayServer(t *testing.T) *testCentralRelayServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen central server: %v", err)
	}

	srv := &testCentralRelayServer{
		url: "ws://" + ln.Addr().String() + "/bridge",
	}

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux := http.NewServeMux()
	mux.HandleFunc("/bridge", func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		peer := &centralPeer{ws: ws}

		srv.mu.Lock()
		idx := -1
		if srv.peers[0] == nil {
			idx = 0
			srv.peers[0] = peer
		} else if srv.peers[1] == nil {
			idx = 1
			srv.peers[1] = peer
		}
		srv.mu.Unlock()

		if idx == -1 {
			_ = ws.Close()
			return
		}

		otherIdx := 1 - idx
		defer func() {
			srv.mu.Lock()
			if srv.peers[idx] == peer {
				srv.peers[idx] = nil
			}
			other := srv.peers[otherIdx]
			srv.mu.Unlock()
			if other != nil {
				other.sendText([]byte(`{"data":{}}`))
			}
			_ = ws.Close()
		}()

		for {
			mt, msg, err := ws.ReadMessage()
			if err != nil {
				return
			}
			if mt != websocket.TextMessage {
				continue
			}
			srv.mu.Lock()
			other := srv.peers[otherIdx]
			srv.mu.Unlock()
			if other != nil {
				other.sendText(msg)
			}
		}
	})

	srv.httpSrv = &http.Server{Handler: mux}
	go func() { _ = srv.httpSrv.Serve(ln) }()
	return srv
}

func (s *testCentralRelayServer) BroadcastStop() {
	s.mu.Lock()
	p0, p1 := s.peers[0], s.peers[1]
	s.mu.Unlock()
	if p0 != nil {
		p0.sendText([]byte(`{"cmd":"STOP"}`))
	}
	if p1 != nil {
		p1.sendText([]byte(`{"cmd":"STOP"}`))
	}
}

func (s *testCentralRelayServer) DropPeer(idx int) {
	s.mu.Lock()
	p := s.peers[idx]
	s.mu.Unlock()
	if p != nil {
		_ = p.ws.Close()
	}
}

func (s *testCentralRelayServer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = s.httpSrv.Shutdown(ctx)
	s.mu.Lock()
	for _, p := range s.peers {
		if p != nil {
			_ = p.ws.Close()
		}
	}
	s.mu.Unlock()
}

func TestEndToEnd_TwoMachinesViaOurServer_FullMatchLifecycleAndTorqueRule(t *testing.T) {
	localIP, err := netutil.ResolveMachineIP(netutil.IPResolveOptions{AllowLoopback: true})
	if err != nil {
		t.Fatalf("ResolveMachineIP: %v", err)
	}

	// Start two independent local machine servers (Cabinet A & Cabinet B) bound to the machine's IP.
	cabinetServerA, err := startSimulatedMachineServer(localIP, 150*time.Millisecond)
	if err != nil {
		t.Fatalf("start cabinetServerA: %v", err)
	}
	defer cabinetServerA.Close()

	cabinetServerB, err := startSimulatedMachineServer(localIP, 150*time.Millisecond)
	if err != nil {
		t.Fatalf("start cabinetServerB: %v", err)
	}
	defer cabinetServerB.Close()

	// Start "our server" (central relay server).
	ourServer := startTestCentralRelayServer(t)
	defer ourServer.Close()

	// Connect Physical Machine A to Cabinet Server A, and Physical Machine B to Cabinet Server B.
	physA := dialTestMachine(t, cabinetServerA.EndpointURL())
	defer physA.Close()
	framesAtA := pumpFrames(physA)

	physB := dialTestMachine(t, cabinetServerB.EndpointURL())
	defer physB.Close()
	framesAtB := pumpFrames(physB)

	// Start MachineBridge A (no MachineIPOverride — auto-reads localIP!) and MachineBridge B.
	upA, err := bridge.NewWebSocketServerClient(bridge.UpstreamOptions{ServerURL: ourServer.url})
	if err != nil {
		t.Fatalf("upA: %v", err)
	}
	bridgeA, err := bridge.New(bridge.Config{
		MachineOptions: machineclient.Options{
			Port:              cabinetServerA.Port(),
			RivalSendInterval: 5 * time.Millisecond,
			MinFrameSpacing:   -1,
		},
	}, upA)
	if err != nil {
		t.Fatalf("bridgeA: %v", err)
	}

	if !bridgeA.LocalClient().MachineIP().Equal(localIP) {
		t.Fatalf("bridgeA resolved IP %s, want %s", bridgeA.LocalClient().MachineIP(), localIP)
	}

	upB, err := bridge.NewWebSocketServerClient(bridge.UpstreamOptions{ServerURL: ourServer.url})
	if err != nil {
		t.Fatalf("upB: %v", err)
	}
	bridgeB, err := bridge.New(bridge.Config{
		MachineOptions: machineclient.Options{
			Port:              cabinetServerB.Port(),
			RivalSendInterval: 5 * time.Millisecond,
			MinFrameSpacing:   -1,
		},
	}, upB)
	if err != nil {
		t.Fatalf("bridgeB: %v", err)
	}

	ctx := context.Background()
	if err := bridgeA.Start(ctx); err != nil {
		t.Fatalf("bridgeA.Start: %v", err)
	}
	defer func() { _ = bridgeA.Stop(ctx) }()

	if err := bridgeB.Start(ctx); err != nil {
		t.Fatalf("bridgeB.Start: %v", err)
	}
	defer func() { _ = bridgeB.Stop(ctx) }()

	waitUntil(t, 3*time.Second, func() bool {
		return bridgeA.LocalClient().IsConnectedToServer() &&
			bridgeB.LocalClient().IsConnectedToServer() &&
			upA.IsConnected() &&
			upB.IsConnected()
	})

	// 1. Both physical machines report MULTIPLAYER_SM_READY -> both receive {"cmd":{"START":1}}.
	sendMachineFrame(t, physA, &protocol.MachineTelemetry{
		MACAddress:        "AA:00:00:00:00:01",
		PlayerID:          "fighter-A",
		ServerPlayerID:    "spid-A",
		HasServerPlayerID: true,
		Hand:              "right",
		MultiplayerState:  protocol.StateReady,
	})
	sendMachineFrame(t, physB, &protocol.MachineTelemetry{
		MACAddress:        "BB:00:00:00:00:02",
		PlayerID:          "fighter-B",
		ServerPlayerID:    "spid-B",
		HasServerPlayerID: true,
		Hand:              "left",
		MultiplayerState:  protocol.StateReady,
	})

	waitForFrame(t, framesAtA, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"START"`)
	})
	waitForFrame(t, framesAtB, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"START"`)
	})

	// 2. Active run with opposite hands ("right" vs "left"): verify single torque sign flip (no double flip!).
	sendMachineFrame(t, physA, &protocol.MachineTelemetry{
		PlayerID:         "fighter-A",
		Hand:             "right",
		MultiplayerState: protocol.StateRun,
		Torque:           28.0,
		OpponentTorque:   21.5,
		Position:         5.5,
		TimeStamp:        1001,
	})
	sendMachineFrame(t, physB, &protocol.MachineTelemetry{
		PlayerID:         "fighter-B",
		Hand:             "left",
		MultiplayerState: protocol.StateRun,
		Torque:           21.5,
		OpponentTorque:   28.0,
		Position:         -5.5,
		TimeStamp:        1002,
	})

	runAtB := waitForFrame(t, framesAtB, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"fighter-A"`) && strings.Contains(s, `"MULTIPLAYER_SM_RUN"`)
	})
	var parsedAtB struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal([]byte(runAtB), &parsedAtB); err != nil {
		t.Fatalf("unmarshal runAtB: %v", err)
	}
	if got := parsedAtB.Data["opponent_torque"]; got != -21.5 {
		t.Fatalf("Physical Machine B opponent_torque = %v, want -21.5 (single flip)", got)
	}

	// 3. Machine A wins (MULTIPLAYER_SM_GAMEOVER_WIN) -> Machine B receives {"cmd":{"YOU_LOSE":...}}.
	sendMachineFrame(t, physA, &protocol.MachineTelemetry{
		PlayerID:         "fighter-A",
		Hand:             "right",
		MultiplayerState: protocol.StateGameOverWin,
		Torque:           35.0,
		OpponentTorque:   12.0,
	})
	waitForFrame(t, framesAtB, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"YOU_LOSE"`)
	})
}

func TestMachineBridge_SafetyResetOnUpstreamDropAndStuckWaitingClear(t *testing.T) {
	localIP, err := netutil.ResolveMachineIP(netutil.IPResolveOptions{AllowLoopback: true})
	if err != nil {
		t.Fatalf("ResolveMachineIP: %v", err)
	}

	cabinetSrv, err := startSimulatedMachineServer(localIP, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("start cabinetSrv: %v", err)
	}
	defer cabinetSrv.Close()

	ourServer := startTestCentralRelayServer(t)
	defer ourServer.Close()

	physMachine := dialTestMachine(t, cabinetSrv.EndpointURL())
	defer physMachine.Close()
	frames := pumpFrames(physMachine)

	// Track operator commands + test panic recovery in user callback!
	var gotStop atomic.Bool
	up, err := bridge.NewWebSocketServerClient(bridge.UpstreamOptions{ServerURL: ourServer.url})
	if err != nil {
		t.Fatalf("up: %v", err)
	}

	var firstCallback atomic.Bool
	mb, err := bridge.New(bridge.Config{
		MachineOptions: machineclient.Options{
			Port:                  cabinetSrv.Port(),
			RivalSendInterval:     4 * time.Millisecond,
			MinFrameSpacing:       -1,
			ServerPlayerIDCadence: 3,
			OnCommand: func(cmd protocol.ServerCommand) {
				if cmd.Kind == protocol.CommandStop && cmd.IsOperatorCommand {
					gotStop.Store(true)
				}
			},
			OnLocalData: func(*protocol.ReceivedMachineData) {
				if firstCallback.CompareAndSwap(false, true) {
					panic("simulated buggy callback must not crash bridge")
				}
			},
		},
	}, up)
	if err != nil {
		t.Fatalf("bridge.New: %v", err)
	}

	ctx := context.Background()
	if err := mb.Start(ctx); err != nil {
		t.Fatalf("mb.Start: %v", err)
	}
	defer func() { _ = mb.Stop(ctx) }()

	// Connect a simulated remote peer to ourServer representing the rival.
	rivalPeer := dialTestMachine(t, ourServer.url)
	defer rivalPeer.Close()

	waitUntil(t, 3*time.Second, func() bool {
		return mb.LocalClient().IsConnectedToServer() && up.IsConnected()
	})

	// Send binary + invalid JSON from physical machine -> bridge survives and accepts valid frame.
	_ = physMachine.WriteMessage(websocket.BinaryMessage, []byte{1, 2, 3, 4})
	_ = physMachine.WriteMessage(websocket.TextMessage, []byte(`{broken`))
	sendMachineFrame(t, physMachine, &protocol.MachineTelemetry{
		PlayerID: "local-1",
		Hand:     "right",
		Torque:   10.0,
	})
	waitUntil(t, 3*time.Second, func() bool {
		d := mb.LocalClient().LatestLocalData()
		return d != nil && d.HasMachineTelemetry && d.NormalizedTelemetry.PlayerID == "local-1"
	})

	// Operator 'k' -> STOP command received cleanly.
	cabinetSrv.SendOperatorInput("k")
	waitUntil(t, 3*time.Second, gotStop.Load)

	// Rival reports READY -> cabinetSrv enters MULTIPLAYER_WAITING, and verify 3rd-message server_player_id cadence!
	sendMachineFrame(t, rivalPeer, &protocol.MachineTelemetry{
		PlayerID:          "rival-waiting",
		ServerPlayerID:    "spid-cadence",
		HasServerPlayerID: true,
		Hand:              "right",
		MultiplayerState:  protocol.StateReady,
		Torque:            17.0,
	})

	waitUntil(t, 3*time.Second, func() bool {
		return cabinetSrv.MatchState() == protocol.ServerWaiting
	})

	// Verify frame 1 contained server_player_id and a subsequent intermediate frame omitted it.
	waitForFrame(t, frames, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"rival-waiting"`) && strings.Contains(s, `"spid-cadence"`)
	})
	waitForFrame(t, frames, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"rival-waiting"`) && !strings.Contains(s, `"server_player_id"`)
	})

	// Drop the upstream connection abruptly mid-waiting -> safety ClearRival resets cabinetSrv to MULTIPLAYER_IDLE
	// and clears stale rival torque back to {"data":{}}!
	ourServer.DropPeer(0)

	waitUntil(t, 3*time.Second, func() bool {
		return cabinetSrv.MatchState() == protocol.ServerIdle
	})

	waitForFrame(t, frames, 3*time.Second, func(s string) bool {
		return s == `{"data":{}}`
	})
}

func dialTestMachine(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	return ws
}

func sendMachineFrame(t *testing.T, ws *websocket.Conn, tel *protocol.MachineTelemetry) {
	t.Helper()
	raw := protocol.MarshalMachineEnvelope(tel, true, "")
	if err := ws.WriteMessage(websocket.TextMessage, raw); err != nil {
		t.Fatalf("write machine frame: %v", err)
	}
}

func pumpFrames(ws *websocket.Conn) <-chan string {
	ch := make(chan string, 256)
	go func() {
		defer close(ch)
		for {
			mt, b, err := ws.ReadMessage()
			if err != nil {
				return
			}
			if mt != websocket.TextMessage {
				continue
			}
			select {
			case ch <- string(b):
			default:
				<-ch
				ch <- string(b)
			}
		}
	}()
	return ch
}

func waitForFrame(t *testing.T, ch <-chan string, timeout time.Duration, pred func(string) bool) string {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				t.Fatalf("frame channel closed before matching message")
			}
			if pred(msg) {
				return msg
			}
		case <-deadline.C:
			t.Fatalf("timed out waiting for expected WebSocket frame")
		}
	}
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not satisfied within %s", timeout)
	}
}
