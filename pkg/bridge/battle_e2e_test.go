package bridge_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Interacting-Tech/tmp/pkg/battleserver"
	"github.com/Interacting-Tech/tmp/pkg/bridge"
	"github.com/Interacting-Tech/tmp/pkg/protocol"
	"github.com/gorilla/websocket"
)

// battleRig is two machines, each with its own PROTOCOL.md server and bridge, connected through
// the real battle server. Machine A's player uses the left hand, machine B's the right.
type battleRig struct {
	t                    *testing.T
	serverURL            string
	auth                 http.Header
	cabinetA, cabinetB   *simulatedMachineServer
	physA, physB         *websocket.Conn
	framesAtA, framesAtB <-chan string
	bridgeA              *bridge.MachineBridge
}

func startBattleRig(t *testing.T) *battleRig {
	t.Helper()
	const token = "test-token"
	battles := battleserver.New(battleserver.Options{
		AuthToken:         token,
		PostMatchDuration: 150 * time.Millisecond,
		Logf:              t.Logf,
	})
	httpSrv := httptest.NewServer(battles)
	t.Cleanup(func() {
		battles.Close()
		httpSrv.Close()
	})

	r := &battleRig{
		t:         t,
		serverURL: "ws" + strings.TrimPrefix(httpSrv.URL, "http"),
		auth:      http.Header{"Authorization": {"Bearer " + token}},
		cabinetA:  startTestCabinet(t),
		cabinetB:  startTestCabinet(t),
	}

	r.physA = dialTestMachine(t, r.cabinetA.EndpointURL())
	t.Cleanup(func() { _ = r.physA.Close() })
	r.framesAtA = pumpFrames(r.physA)
	r.physB = dialTestMachine(t, r.cabinetB.EndpointURL())
	t.Cleanup(func() { _ = r.physB.Close() })
	r.framesAtB = pumpFrames(r.physB)

	var upA *bridge.WebSocketServerClient
	r.bridgeA, upA = r.startBridge(r.cabinetA, "cabinet-A")
	bridgeB, upB := r.startBridge(r.cabinetB, "cabinet-B")
	waitUntil(t, 3*time.Second, func() bool {
		return r.bridgeA.LocalClient().IsConnectedToServer() && bridgeB.LocalClient().IsConnectedToServer() &&
			upA.IsConnected() && upB.IsConnected()
	})
	return r
}

func (r *battleRig) startBridge(cabinet *simulatedMachineServer, machineID string) (*bridge.MachineBridge, *bridge.WebSocketServerClient) {
	return startTestBridge(r.t, cabinet, r.serverURL, machineID, bridge.Config{}, bridge.UpstreamOptions{Headers: r.auth})
}

func inState(frame, state string) string {
	return strings.Replace(frame, protocol.StateRun, state, 1)
}

func withField(frame, old, replacement string) string {
	if !strings.Contains(frame, old) {
		panic("frame has no " + old)
	}
	return strings.Replace(frame, old, replacement, 1)
}

func contains(substr string) func(string) bool {
	return func(s string) bool { return strings.Contains(s, substr) }
}

// bothReadyThenStarted has both players report ready and waits for each machine to be told to start.
func (r *battleRig) bothReadyThenStarted() {
	r.t.Helper()
	writeText(r.t, r.physA, inState(firmwareFrameA, protocol.StateReady))
	writeText(r.t, r.physB, inState(firmwareFrameB, protocol.StateReady))
	waitForFrame(r.t, r.framesAtA, 3*time.Second, contains(`"START"`))
	waitForFrame(r.t, r.framesAtB, 3*time.Second, contains(`"START"`))
}

func (r *battleRig) bothIdle() {
	r.t.Helper()
	waitUntil(r.t, 3*time.Second, func() bool {
		return r.cabinetA.MatchState() == protocol.ServerIdle && r.cabinetB.MatchState() == protocol.ServerIdle
	})
}

// Ready, start, torque exchange, a result reported by a machine, rematch, and one side going
// offline and returning.
func TestEndToEnd_BattleBetweenTwoMachinesThroughBattleServer(t *testing.T) {
	r := startBattleRig(t)

	r.bothReadyThenStarted()

	// During the battle each machine receives the other's handle data. Machine A's player
	// uses the left hand, so machine B receives A's machine_torque negated; machine A
	// receives right-handed B's as B sent it.
	writeText(t, r.physA, firmwareFrameA)
	writeText(t, r.physB, firmwareFrameB)
	assertMachineReceivedRival(t, waitForFrame(t, r.framesAtB, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"player-2"`) && strings.Contains(s, protocol.StateRun)
	}), withField(firmwareFrameA, `"machine_torque":-12.5`, `"machine_torque":12.5`))
	assertMachineReceivedRival(t, waitForFrame(t, r.framesAtA, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"player-1"`) && strings.Contains(s, protocol.StateRun)
	}), firmwareFrameB)

	// Machine A reports a win: machine B is told it lost, and both local matches finish.
	writeText(t, r.physA, inState(firmwareFrameA, protocol.StateGameOverWin))
	waitForFrame(t, r.framesAtB, 3*time.Second, contains(`"YOU_LOSE"`))
	r.bothIdle()

	// The same two machines can battle again.
	r.bothReadyThenStarted()

	// Machine A's bridge goes offline mid-battle: machine B loses its rival and its battle is
	// abandoned. Machine B still reports ready, so its local server goes back to waiting.
	if r.cabinetB.MatchState() != protocol.ServerMidMatch {
		t.Fatalf("cabinet B is in %s before bridge A goes offline, want %s", r.cabinetB.MatchState(), protocol.ServerMidMatch)
	}
	if err := r.bridgeA.Stop(context.Background()); err != nil {
		t.Fatalf("stop bridge A: %v", err)
	}
	waitForFrame(t, r.framesAtB, 3*time.Second, func(s string) bool {
		return s == `{"data":{}}`
	})
	waitUntil(t, 3*time.Second, func() bool {
		return r.cabinetB.MatchState() == protocol.ServerWaiting
	})

	// Machine A's bridge comes back: machine B sees machine A again.
	r.startBridge(r.cabinetA, "cabinet-A")
	returned := withField(firmwareFrameA, `"timeStamp":123456790`, `"timeStamp":424242`)
	writeText(t, r.physA, returned)
	assertMachineReceivedRival(t, waitForFrame(t, r.framesAtB, 3*time.Second, contains(`"timeStamp":424242`)),
		withField(returned, `"machine_torque":-12.5`, `"machine_torque":12.5`))
}

// The battle server decides the battle when a handle reaches one of its position limits, and each
// machine is told the result by its own local server, as PROTOCOL.md section 6.2 describes.
func TestEndToEnd_HandleReachingItsLimitDecidesTheBattle(t *testing.T) {
	r := startBattleRig(t)

	// Before the battle starts a handle at its limit decides nothing.
	writeText(t, r.physA, withField(firmwareFrameA, `"position":-3.1`, `"position":-45.0`))
	writeText(t, r.physB, firmwareFrameB)
	waitForFrame(t, r.framesAtB, 3*time.Second, contains(`"position":-45`))
	expectNoFrame(t, r.framesAtA, 100*time.Millisecond, contains(`"cmd"`))

	// Left-handed machine A reaches its left limit: A wins.
	r.bothReadyThenStarted()
	writeText(t, r.physA, firmwareFrameA)
	writeText(t, r.physB, firmwareFrameB)
	writeText(t, r.physA, withField(firmwareFrameA, `"position":-3.1`, `"position":-45.0`))
	waitForFrame(t, r.framesAtA, 3*time.Second, contains(`"YOU_WIN"`))
	waitForFrame(t, r.framesAtB, 3*time.Second, contains(`"YOU_LOSE"`))
	r.bothIdle()

	// Rematch. Right-handed machine B reaches its right limit: B wins.
	r.bothReadyThenStarted()
	writeText(t, r.physA, firmwareFrameA)
	writeText(t, r.physB, firmwareFrameB)
	writeText(t, r.physB, withField(firmwareFrameB, `"position":2.5`, `"position":40.0`))
	waitForFrame(t, r.framesAtB, 3*time.Second, contains(`"YOU_WIN"`))
	waitForFrame(t, r.framesAtA, 3*time.Second, contains(`"YOU_LOSE"`))
	r.bothIdle()

	// Rematch. Right-handed machine B is pushed to its left limit: B loses.
	r.bothReadyThenStarted()
	writeText(t, r.physA, firmwareFrameA)
	writeText(t, r.physB, firmwareFrameB)
	writeText(t, r.physB, withField(firmwareFrameB, `"position":2.5`, `"position":-40.0`))
	waitForFrame(t, r.framesAtB, 3*time.Second, contains(`"YOU_LOSE"`))
	waitForFrame(t, r.framesAtA, 3*time.Second, contains(`"YOU_WIN"`))
}

// Machine firmware leaves MULTIPLAYER_SM_READY as soon as its local server tells it to start, which
// for the second player to get ready is about a millisecond after reporting it: less than the
// 10 ms between the frames its local server relays. The first player's machine must start anyway.
func TestEndToEnd_BattleStartsWhenTheSecondMachineIsReadyOnlyForAnInstant(t *testing.T) {
	r := startBattleRig(t)

	// Both machines are connected and sending before anyone is ready.
	writeText(t, r.physA, inState(firmwareFrameA, "MULTIPLAYER_SM_IDLE"))
	writeText(t, r.physB, inState(firmwareFrameB, "MULTIPLAYER_SM_IDLE"))
	waitForFrame(t, r.framesAtA, 3*time.Second, contains(`"player-1"`))
	waitForFrame(t, r.framesAtB, 3*time.Second, contains(`"player-2"`))

	writeText(t, r.physA, inState(firmwareFrameA, protocol.StateReady))
	waitUntil(t, 3*time.Second, func() bool { return r.cabinetB.MatchState() == protocol.ServerWaiting })

	writeText(t, r.physB, inState(firmwareFrameB, protocol.StateReady))
	waitForFrame(t, r.framesAtB, 3*time.Second, contains(`"START"`))
	writeText(t, r.physB, firmwareFrameB)

	waitForFrame(t, r.framesAtA, 3*time.Second, contains(`"START"`))
	writeText(t, r.physA, firmwareFrameA)

	// The battle server knows the battle is running: a handle reaching its limit decides it.
	writeText(t, r.physB, withField(firmwareFrameB, `"position":2.5`, `"position":40.0`))
	waitForFrame(t, r.framesAtB, 3*time.Second, contains(`"YOU_WIN"`))
	waitForFrame(t, r.framesAtA, 3*time.Second, contains(`"YOU_LOSE"`))
}
