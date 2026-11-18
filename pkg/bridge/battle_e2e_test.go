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
)

// Two machines, each with its own PROTOCOL.md server and bridge, battle through the real battle
// server: ready, start, torque exchange, result, rematch, and one side going offline and returning.
func TestEndToEnd_BattleBetweenTwoMachinesThroughBattleServer(t *testing.T) {
	const token = "test-token"
	battles := battleserver.New(battleserver.Options{AuthToken: token, Logf: t.Logf})
	httpSrv := httptest.NewServer(battles)
	t.Cleanup(func() {
		battles.Close()
		httpSrv.Close()
	})
	serverURL := "ws" + strings.TrimPrefix(httpSrv.URL, "http")
	auth := http.Header{"Authorization": {"Bearer " + token}}

	cabinetA := startTestCabinet(t)
	cabinetB := startTestCabinet(t)
	physA := dialTestMachine(t, cabinetA.EndpointURL())
	t.Cleanup(func() { _ = physA.Close() })
	framesAtA := pumpFrames(physA)
	physB := dialTestMachine(t, cabinetB.EndpointURL())
	t.Cleanup(func() { _ = physB.Close() })
	framesAtB := pumpFrames(physB)

	bridgeA, upA := startTestBridge(t, cabinetA, serverURL, "cabinet-A", bridge.Config{}, bridge.UpstreamOptions{Headers: auth})
	bridgeB, upB := startTestBridge(t, cabinetB, serverURL, "cabinet-B", bridge.Config{}, bridge.UpstreamOptions{Headers: auth})
	waitUntil(t, 3*time.Second, func() bool {
		return bridgeA.LocalClient().IsConnectedToServer() && bridgeB.LocalClient().IsConnectedToServer() &&
			upA.IsConnected() && upB.IsConnected()
	})

	inState := func(frame, state string) string {
		return strings.Replace(frame, protocol.StateRun, state, 1)
	}
	isStart := func(s string) bool { return strings.Contains(s, `"START"`) }
	bothReadyThenStarted := func() {
		t.Helper()
		writeText(t, physA, inState(firmwareFrameA, protocol.StateReady))
		writeText(t, physB, inState(firmwareFrameB, protocol.StateReady))
		waitForFrame(t, framesAtA, 3*time.Second, isStart)
		waitForFrame(t, framesAtB, 3*time.Second, isStart)
	}

	// Both players ready: each machine is told to start.
	bothReadyThenStarted()

	// During the battle each machine receives the other's handle data unchanged.
	writeText(t, physA, firmwareFrameA)
	writeText(t, physB, firmwareFrameB)
	assertMachineReceivedRival(t, waitForFrame(t, framesAtB, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"player-2"`) && strings.Contains(s, protocol.StateRun)
	}), firmwareFrameA)
	assertMachineReceivedRival(t, waitForFrame(t, framesAtA, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"player-1"`) && strings.Contains(s, protocol.StateRun)
	}), firmwareFrameB)

	// Machine A reports a win: machine B is told it lost, and both local matches finish.
	writeText(t, physA, inState(firmwareFrameA, protocol.StateGameOverWin))
	waitForFrame(t, framesAtB, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"YOU_LOSE"`)
	})
	waitUntil(t, 3*time.Second, func() bool {
		return cabinetA.MatchState() == protocol.ServerIdle && cabinetB.MatchState() == protocol.ServerIdle
	})

	// The same two machines can battle again.
	bothReadyThenStarted()

	// Machine A's bridge goes offline mid-battle: machine B loses its rival and its battle is
	// abandoned. Machine B still reports ready, so its local server goes back to waiting for a rival.
	if cabinetB.MatchState() != protocol.ServerMidMatch {
		t.Fatalf("cabinet B is in %s before bridge A goes offline, want %s", cabinetB.MatchState(), protocol.ServerMidMatch)
	}
	if err := bridgeA.Stop(context.Background()); err != nil {
		t.Fatalf("stop bridge A: %v", err)
	}
	waitForFrame(t, framesAtB, 3*time.Second, func(s string) bool {
		return s == `{"data":{}}`
	})
	waitUntil(t, 3*time.Second, func() bool {
		return cabinetB.MatchState() == protocol.ServerWaiting
	})

	// Machine A's bridge comes back: machine B sees machine A again.
	startTestBridge(t, cabinetA, serverURL, "cabinet-A", bridge.Config{}, bridge.UpstreamOptions{Headers: auth})
	returned := strings.Replace(firmwareFrameA, `"timeStamp":123456790`, `"timeStamp":424242`, 1)
	writeText(t, physA, returned)
	assertMachineReceivedRival(t, waitForFrame(t, framesAtB, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"timeStamp":424242`)
	}), returned)
}
