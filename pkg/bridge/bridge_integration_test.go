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

	mu       sync.Mutex
	peers    [2]*centralPeer
	received map[string]chan string
}

// FramesFrom returns the frames received from peers that connected with the given X-Machine-ID.
func (s *testCentralRelayServer) FramesFrom(machineID string) <-chan string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.receivedLocked(machineID)
}

func (s *testCentralRelayServer) receivedLocked(machineID string) chan string {
	ch, ok := s.received[machineID]
	if !ok {
		ch = make(chan string, 1024)
		s.received[machineID] = ch
	}
	return ch
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
		url:      "ws://" + ln.Addr().String() + "/bridge",
		received: make(map[string]chan string),
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
		received := srv.receivedLocked(r.Header.Get("X-Machine-ID"))
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
			select {
			case received <- string(msg):
			default:
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

// Frames as current machine firmware sends them to its local server, before the server adds
// is_connected, client_id and last_message_timestamp.
const (
	firmwareFrameA = `{"data":{"MAC_address":"11:22:33:44:55:66","player_id":"player-2","hand":"left","multiplayer_state":"MULTIPLAYER_SM_RUN","single_player_state":"","machine_torque":-12.5,"position":-3.1,"velocity":0.4,"left_limit":-45.0,"right_limit":45.0,"peak_force":28.0,"timeStamp":123456790}}`
	firmwareFrameB = `{"data":{"MAC_address":"AA:BB:CC:DD:EE:01","player_id":"player-1","hand":"right","multiplayer_state":"MULTIPLAYER_SM_RUN","single_player_state":"","machine_torque":9.75,"position":2.5,"velocity":-0.2,"left_limit":-40.0,"right_limit":40.0,"peak_force":31.5,"timeStamp":555000111}}`
)

// ourServerRivalFrame is a rival frame as our server relays it, including the fields a server adds.
const ourServerRivalFrame = `{
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

func TestEndToEnd_MachineTorqueFramesCrossOurServerUnchanged(t *testing.T) {
	cabinetA := startTestCabinet(t)
	cabinetB := startTestCabinet(t)
	ourServer := startTestCentralRelayServer(t)
	t.Cleanup(ourServer.Close)

	physA := dialTestMachine(t, cabinetA.EndpointURL())
	t.Cleanup(func() { _ = physA.Close() })
	framesAtA := pumpFrames(physA)
	physB := dialTestMachine(t, cabinetB.EndpointURL())
	t.Cleanup(func() { _ = physB.Close() })
	framesAtB := pumpFrames(physB)

	bridgeA, upA := startTestBridge(t, cabinetA, ourServer.url, "cabinet-A", bridge.Config{}, bridge.UpstreamOptions{})
	bridgeB, upB := startTestBridge(t, cabinetB, ourServer.url, "cabinet-B", bridge.Config{}, bridge.UpstreamOptions{})
	waitUntil(t, 3*time.Second, func() bool {
		return bridgeA.LocalClient().IsConnectedToServer() && bridgeB.LocalClient().IsConnectedToServer() &&
			upA.IsConnected() && upB.IsConnected()
	})

	writeText(t, physA, firmwareFrameA)
	writeText(t, physB, firmwareFrameB)

	// Machine -> our server. The hands differ, so the local servers also add opponent_torque -0.0
	// to what they relay to the bridges.
	for _, tc := range []struct{ machineID, playerID, want string }{
		{"cabinet-A", "player-2", `{"data":{"MAC_address":"11:22:33:44:55:66","player_id":"player-2","hand":"left","multiplayer_state":"MULTIPLAYER_SM_RUN","single_player_state":"","machine_torque":-12.5,"position":-3.1,"velocity":0.4,"left_limit":-45,"right_limit":45,"peak_force":28,"timeStamp":123456790}}`},
		{"cabinet-B", "player-1", `{"data":{"MAC_address":"AA:BB:CC:DD:EE:01","player_id":"player-1","hand":"right","multiplayer_state":"MULTIPLAYER_SM_RUN","single_player_state":"","machine_torque":9.75,"position":2.5,"velocity":-0.2,"left_limit":-40,"right_limit":40,"peak_force":31.5,"timeStamp":555000111}}`},
	} {
		got := waitForFrame(t, ourServer.FramesFrom(tc.machineID), 3*time.Second, func(s string) bool {
			return strings.Contains(s, `"`+tc.playerID+`"`)
		})
		if got != tc.want {
			t.Errorf("our server received from %s\n got: %s\nwant: %s", tc.machineID, got, tc.want)
		}
	}

	// Our server -> machine.
	assertMachineReceivedRival(t, waitForFrame(t, framesAtB, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"player-2"`)
	}), firmwareFrameA)
	assertMachineReceivedRival(t, waitForFrame(t, framesAtA, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"player-1"`)
	}), firmwareFrameB)
}

func TestMachineBridge_RivalFromOurServerReachesMachineAndClearsWhenOurServerGoesSilent(t *testing.T) {
	cabinet := startTestCabinet(t)
	ourServer := startTestCentralRelayServer(t)
	t.Cleanup(ourServer.Close)
	phys := dialTestMachine(t, cabinet.EndpointURL())
	t.Cleanup(func() { _ = phys.Close() })
	frames := pumpFrames(phys)

	const rivalTimeout = 300 * time.Millisecond
	mb, up := startTestBridge(t, cabinet, ourServer.url, "cabinet-A", bridge.Config{},
		bridge.UpstreamOptions{RivalTimeout: rivalTimeout})
	rivalSide := dialTestMachine(t, ourServer.url)
	t.Cleanup(func() { _ = rivalSide.Close() })
	_ = pumpFrames(rivalSide)
	waitUntil(t, 3*time.Second, func() bool {
		return mb.LocalClient().IsConnectedToServer() && up.IsConnected()
	})
	writeText(t, phys, firmwareFrameB)

	feed := startOurServerFeed(rivalSide, strings.Replace(ourServerRivalFrame, protocol.StateRun, protocol.StateReady, 1))
	waitUntil(t, 3*time.Second, func() bool {
		return cabinet.MatchState() == protocol.ServerWaiting
	})
	feed.Set(ourServerRivalFrame)
	assertMachineReceivedRival(t, waitForFrame(t, frames, 3*time.Second, func(s string) bool {
		return strings.Contains(s, protocol.StateRun)
	}), ourServerRivalFrame)

	expectNoFrame(t, frames, 3*rivalTimeout, func(s string) bool {
		return s == `{"data":{}}`
	})

	lastSent := feed.Stop()
	waitForFrame(t, frames, 3*time.Second, func(s string) bool {
		return s == `{"data":{}}`
	})
	if silent := time.Since(lastSent); silent < rivalTimeout {
		t.Fatalf("rival cleared %v after our server's last frame, before the %v timeout", silent, rivalTimeout)
	}
	waitUntil(t, 3*time.Second, func() bool {
		return cabinet.MatchState() == protocol.ServerIdle
	})
}

func TestMachineBridge_RivalNotConnectedFrameFromOurServerClearsRival(t *testing.T) {
	cabinet := startTestCabinet(t)
	ourServer := startTestCentralRelayServer(t)
	t.Cleanup(ourServer.Close)
	phys := dialTestMachine(t, cabinet.EndpointURL())
	t.Cleanup(func() { _ = phys.Close() })
	frames := pumpFrames(phys)

	mb, up := startTestBridge(t, cabinet, ourServer.url, "cabinet-A", bridge.Config{},
		bridge.UpstreamOptions{RivalTimeout: -1})
	rivalSide := dialTestMachine(t, ourServer.url)
	t.Cleanup(func() { _ = rivalSide.Close() })
	_ = pumpFrames(rivalSide)
	waitUntil(t, 3*time.Second, func() bool {
		return mb.LocalClient().IsConnectedToServer() && up.IsConnected()
	})
	writeText(t, phys, firmwareFrameB)

	feed := startOurServerFeed(rivalSide, strings.Replace(ourServerRivalFrame, protocol.StateRun, protocol.StateReady, 1))
	defer feed.Stop()
	waitUntil(t, 3*time.Second, func() bool {
		return cabinet.MatchState() == protocol.ServerWaiting
	})
	waitForFrame(t, frames, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"player-2"`)
	})

	feed.Set(strings.Replace(ourServerRivalFrame, `"is_connected": true`, `"is_connected": false`, 1))
	waitForFrame(t, frames, 3*time.Second, func(s string) bool {
		return s == `{"data":{}}`
	})
	waitUntil(t, 3*time.Second, func() bool {
		return cabinet.MatchState() == protocol.ServerIdle
	})
}

func TestMachineBridge_OnlyNewSamplesStillRefreshesOurServerWhileMachineIsIdle(t *testing.T) {
	cabinet := startTestCabinet(t)
	ourServer := startTestCentralRelayServer(t)
	t.Cleanup(ourServer.Close)
	phys := dialTestMachine(t, cabinet.EndpointURL())
	t.Cleanup(func() { _ = phys.Close() })
	_ = pumpFrames(phys)

	mb, up := startTestBridge(t, cabinet, ourServer.url, "cabinet-A",
		bridge.Config{OnlyForwardNewSamples: true}, bridge.UpstreamOptions{})
	waitUntil(t, 3*time.Second, func() bool {
		return mb.LocalClient().IsConnectedToServer() && up.IsConnected()
	})

	// One sample, which the local server then repeats every 10 ms.
	writeText(t, phys, firmwareFrameA)
	upstream := ourServer.FramesFrom("cabinet-A")
	waitForFrame(t, upstream, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"player-2"`)
	})

	// Repeats every 10 ms would deliver ~110 frames; no refresh would deliver none.
	if n := countFrames(upstream, 1100*time.Millisecond); n < 2 || n > 6 {
		t.Fatalf("our server received %d frames in 1.1s from an idle machine, want a refresh about every 250ms", n)
	}
}

func startTestCabinet(t *testing.T) *simulatedMachineServer {
	t.Helper()
	localIP, err := netutil.ResolveMachineIP(netutil.IPResolveOptions{AllowLoopback: true})
	if err != nil {
		t.Fatalf("ResolveMachineIP: %v", err)
	}
	cabinet, err := startSimulatedMachineServer(localIP, 150*time.Millisecond)
	if err != nil {
		t.Fatalf("start simulated machine server: %v", err)
	}
	t.Cleanup(cabinet.Close)
	return cabinet
}

// startTestBridge starts a MachineBridge between a simulated local machine server and our server,
// identifying itself to our server with the X-Machine-ID header.
func startTestBridge(
	t *testing.T,
	cabinet *simulatedMachineServer,
	serverURL string,
	machineID string,
	cfg bridge.Config,
	upOpts bridge.UpstreamOptions,
) (*bridge.MachineBridge, *bridge.WebSocketServerClient) {
	t.Helper()
	upOpts.ServerURL = serverURL
	upOpts.Headers = upOpts.Headers.Clone()
	if upOpts.Headers == nil {
		upOpts.Headers = make(http.Header)
	}
	upOpts.Headers.Set("X-Machine-ID", machineID)
	up, err := bridge.NewWebSocketServerClient(upOpts)
	if err != nil {
		t.Fatalf("NewWebSocketServerClient: %v", err)
	}

	cfg.MachineOptions.Port = cabinet.Port()
	cfg.MachineOptions.RivalSendInterval = 5 * time.Millisecond
	cfg.MachineOptions.MinFrameSpacing = -1
	mb, err := bridge.New(cfg, up)
	if err != nil {
		t.Fatalf("bridge.New: %v", err)
	}
	if err := mb.Start(context.Background()); err != nil {
		t.Fatalf("bridge Start: %v", err)
	}
	t.Cleanup(func() { _ = mb.Stop(context.Background()) })
	return mb, up
}

// ourServerFeed streams a frame every 10 ms over a connection to our server, the way our server
// streams rival data while the rival is connected.
type ourServerFeed struct {
	frame    atomic.Value
	stop     chan struct{}
	lastSent chan time.Time
}

func startOurServerFeed(ws *websocket.Conn, frame string) *ourServerFeed {
	f := &ourServerFeed{stop: make(chan struct{}), lastSent: make(chan time.Time, 1)}
	f.frame.Store(frame)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		var last time.Time
		defer func() { f.lastSent <- last }()
		for {
			select {
			case <-f.stop:
				return
			case <-ticker.C:
				if err := ws.WriteMessage(websocket.TextMessage, []byte(f.frame.Load().(string))); err != nil {
					return
				}
				last = time.Now()
			}
		}
	}()
	return f
}

func (f *ourServerFeed) Set(frame string) {
	f.frame.Store(frame)
}

// Stop ends the feed and returns when its last frame was sent. Call it once.
func (f *ourServerFeed) Stop() time.Time {
	close(f.stop)
	return <-f.lastSent
}

// assertMachineReceivedRival checks that a frame the local server relayed to a physical machine
// carries every field of the rival's frame unchanged, apart from the fields servers add.
func assertMachineReceivedRival(t *testing.T, relayed, rivalFrame string) {
	t.Helper()
	var got, sent struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal([]byte(relayed), &got); err != nil {
		t.Fatalf("unmarshal relayed frame %s: %v", relayed, err)
	}
	if err := json.Unmarshal([]byte(rivalFrame), &sent); err != nil {
		t.Fatalf("unmarshal rival frame: %v", err)
	}
	for k, v := range sent.Data {
		switch k {
		case protocol.FieldIsConnected, protocol.FieldClientID, protocol.FieldLastMessageTimestamp:
			continue
		}
		if got.Data[k] != v {
			t.Errorf("machine received %s = %v, want %v\nframe: %s", k, got.Data[k], v, relayed)
		}
	}
	if v, ok := got.Data["torque"]; ok {
		t.Errorf("machine received torque %v that the rival never sent\nframe: %s", v, relayed)
	}
	// The local server's section 4.3 sign rule adds opponent_torque -0.0 when the hands differ.
	if v, ok := got.Data["opponent_torque"]; ok && v != 0.0 {
		t.Errorf("machine received opponent_torque %v that the rival never sent\nframe: %s", v, relayed)
	}
}

func writeText(t *testing.T, ws *websocket.Conn, msg string) {
	t.Helper()
	if err := ws.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

// expectNoFrame fails if a frame matching pred arrives within d.
func expectNoFrame(t *testing.T, ch <-chan string, d time.Duration, pred func(string) bool) {
	t.Helper()
	deadline := time.NewTimer(d)
	defer deadline.Stop()
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				t.Fatalf("frame channel closed")
			}
			if pred(msg) {
				t.Fatalf("unexpected frame: %s", msg)
			}
		case <-deadline.C:
			return
		}
	}
}

func countFrames(ch <-chan string, d time.Duration) int {
	deadline := time.NewTimer(d)
	defer deadline.Stop()
	n := 0
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return n
			}
			n++
		case <-deadline.C:
			return n
		}
	}
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
