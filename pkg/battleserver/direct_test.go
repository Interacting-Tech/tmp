package battleserver_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"dev.azure.com/InteractingTechnologies/Glenfiddich/_git/Arm-Wrestling/pkg/battleserver"
	"github.com/gorilla/websocket"
)

// startDirectServer serves machines that connect directly at /ws and bridges at /ws/machine.
func startDirectServer(t *testing.T, opts battleserver.Options) (machineURL, bridgeURL string) {
	t.Helper()
	srv := battleserver.New(opts)
	mux := http.NewServeMux()
	mux.Handle("/ws", srv.MachineHandler())
	mux.Handle("/ws/machine", srv)
	hs := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.Close()
		hs.Close()
	})
	base := "ws" + strings.TrimPrefix(hs.URL, "http")
	return base + "/ws?role=machine", base + "/ws/machine"
}

// testMachine plays machine firmware connected directly to the battle server.
type testMachine struct {
	name     string
	ws       *websocket.Conn
	data     chan string
	commands chan string
	closed   chan struct{}

	writeMu sync.Mutex
	mu      sync.Mutex
	frame   string
	sending bool
}

func (m *testMachine) write(messageType int, frame []byte) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return m.ws.WriteMessage(messageType, frame)
}

func connectMachine(t *testing.T, url, name string) *testMachine {
	t.Helper()
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("connect %s: %v", name, err)
	}
	m := &testMachine{name: name, ws: ws, data: make(chan string, 8192), commands: make(chan string, 64), closed: make(chan struct{})}
	t.Cleanup(func() { _ = ws.Close() })

	go func() {
		defer close(m.closed)
		for {
			_, msg, err := ws.ReadMessage()
			if err != nil {
				return
			}
			if strings.HasPrefix(string(msg), `{"cmd"`) {
				m.commands <- string(msg)
				continue
			}
			select {
			case m.data <- string(msg):
			default:
			}
		}
	}()
	return m
}

// report makes the machine send frame every 10 ms from now on.
func (m *testMachine) report(frame string) {
	m.mu.Lock()
	m.frame = frame
	start := !m.sending
	m.sending = true
	m.mu.Unlock()
	if !start {
		return
	}
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			m.mu.Lock()
			frame := m.frame
			m.mu.Unlock()
			if frame == "" {
				continue
			}
			if m.write(websocket.TextMessage, []byte(frame)) != nil {
				return
			}
		}
	}()
}

// goSilent stops the machine sending without disconnecting it.
func (m *testMachine) goSilent() {
	m.mu.Lock()
	m.frame = ""
	m.mu.Unlock()
}

// waitData returns the first telemetry message from now on that satisfies want.
func (m *testMachine) waitData(t *testing.T, what string, want func(msg string) bool) string {
	t.Helper()
	timeout := time.After(3 * time.Second)
	last := ""
	for {
		select {
		case msg := <-m.data:
			if want(msg) {
				return msg
			}
			last = msg
		case <-m.closed:
			t.Fatalf("%s was disconnected while waiting for %s", m.name, what)
		case <-timeout:
			t.Fatalf("%s did not receive %s within 3s; last received: %s", m.name, what, last)
		}
	}
}

func (m *testMachine) dropReceived() {
	for {
		select {
		case <-m.data:
		default:
			return
		}
	}
}

func (m *testMachine) nextCommand(t *testing.T) string {
	t.Helper()
	select {
	case cmd := <-m.commands:
		return cmd
	case <-time.After(3 * time.Second):
		t.Fatalf("%s received no command within 3s", m.name)
	}
	return ""
}

func (m *testMachine) expectCommand(t *testing.T, want string) {
	t.Helper()
	if got := m.nextCommand(t); got != want {
		t.Fatalf("%s received command %s, want %s", m.name, got, want)
	}
}

func (m *testMachine) expectNoCommand(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case cmd := <-m.commands:
		t.Fatalf("%s received unexpected command %s", m.name, cmd)
	case <-time.After(d):
	}
}

func machineFrame(player, hand, state string, position, torque float64) string {
	return fmt.Sprintf(`{"data":{"MAC_address":"mac-%s","player_id":%q,"hand":%q,"multiplayer_state":%q,"single_player_state":"","machine_torque":%v,"position":%v,"velocity":0.5,"left_limit":-45,"right_limit":45,"peak_force":30,"timeStamp":77}}`,
		player, player, hand, state, torque, position)
}

func fields(t *testing.T, msg string) map[string]any {
	t.Helper()
	var env struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal([]byte(msg), &env); err != nil || env.Data == nil {
		t.Fatalf("not a telemetry message: %s", msg)
	}
	return env.Data
}

func from(player string) func(string) bool {
	return func(msg string) bool { return strings.Contains(msg, fmt.Sprintf(`"player_id":%q`, player)) }
}

func isNoRival(msg string) bool { return msg == rivalGone }

const (
	cmdStart   = `{"cmd":{"START":1}}`
	cmdYouWin  = `{"cmd":{"YOU_WIN":0}}`
	cmdYouLose = `{"cmd":{"YOU_LOSE":1}}`
)

// startDirectBattle connects two machines and takes them through ready into a running battle.
func startDirectBattle(t *testing.T, opts battleserver.Options, handA, handB string) (a, b *testMachine) {
	t.Helper()
	url, _ := startDirectServer(t, opts)
	a = connectMachine(t, url, "A")
	b = connectMachine(t, url, "B")
	a.report(machineFrame("A", handA, "MULTIPLAYER_SM_READY", 0, 0))
	b.report(machineFrame("B", handB, "MULTIPLAYER_SM_READY", 0, 0))
	a.expectCommand(t, cmdStart)
	b.expectCommand(t, cmdStart)
	a.report(machineFrame("A", handA, "MULTIPLAYER_SM_RUN", 0, 5))
	b.report(machineFrame("B", handB, "MULTIPLAYER_SM_RUN", 0, 5))
	return a, b
}

func TestDirect_ConnectionRequiresRoleMachine(t *testing.T) {
	url, _ := startDirectServer(t, battleserver.Options{})
	for _, bad := range []string{strings.TrimSuffix(url, "?role=machine"), strings.Replace(url, "role=machine", "role=debug", 1)} {
		ws, resp, err := websocket.DefaultDialer.Dial(bad, nil)
		if err == nil {
			_ = ws.Close()
			t.Fatalf("connected to %s; only ?role=machine may connect", bad)
		}
		if resp == nil || resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("connecting to %s: %v (response %v), want 400", bad, err, resp)
		}
	}
}

// Some firmware WebSocket libraries send an Origin header that is not the server's address.
func TestDirect_AcceptsAnyOriginHeader(t *testing.T) {
	url, _ := startDirectServer(t, battleserver.Options{})
	ws, _, err := websocket.DefaultDialer.Dial(url, http.Header{"Origin": {"file://"}})
	if err != nil {
		t.Fatalf("machine sending Origin: file:// was refused: %v", err)
	}
	_ = ws.Close()
}

func TestDirect_MachineWithoutRivalReceivesEmptyDataEvery10ms(t *testing.T) {
	url, _ := startDirectServer(t, battleserver.Options{})
	a := connectMachine(t, url, "A")
	a.report(machineFrame("A", "right", "MULTIPLAYER_SM_READY", 0, 0))

	a.waitData(t, "a first message", isNoRival)
	a.dropReceived()
	time.Sleep(500 * time.Millisecond)
	got := 0
	for len(a.data) > 0 {
		if msg := <-a.data; msg != rivalGone {
			t.Fatalf("machine without a rival received %s, want %s", msg, rivalGone)
		}
		got++
	}
	// 50 expected; the lower bound leaves room for a busy test machine.
	if got < 25 || got > 55 {
		t.Fatalf("received %d messages in 500 ms, want about 50 (one every 10 ms)", got)
	}
	a.expectNoCommand(t, 0)
}

func TestDirect_MachineReceivesItsRivalsTelemetryWithTheServerFields(t *testing.T) {
	url, _ := startDirectServer(t, battleserver.Options{})
	a := connectMachine(t, url, "A")
	a.waitData(t, "a first message", isNoRival)
	b := connectMachine(t, url, "B")

	a.report(`{"data":{"MAC_address":"AA:BB:CC:DD:EE:FF","player_id":"A","server_player_id":"abc123","hand":"left","multiplayer_state":"MULTIPLAYER_SM_RUN","single_player_state":"sp","machine_torque":-12.5,"position":-3.1,"velocity":0.4,"left_limit":-45,"right_limit":45,"peak_force":28,"timeStamp":123456790,"firmware":"1.2"}}`)
	b.report(machineFrame("B", "right", "MULTIPLAYER_SM_RUN", 7.25, 9.75))

	got := fields(t, b.waitData(t, "A's telemetry", from("A")))
	want := map[string]any{
		"MAC_address": "AA:BB:CC:DD:EE:FF", "player_id": "A", "server_player_id": "abc123", "hand": "left",
		"multiplayer_state": "MULTIPLAYER_SM_RUN", "single_player_state": "sp",
		"machine_torque": 12.5, // left hand: multiplied by -1
		"position":       -3.1, "velocity": 0.4, "left_limit": -45.0, "right_limit": 45.0, "peak_force": 28.0,
		"timeStamp": 123456790.0, "firmware": "1.2",
		"is_connected": true, "client_id": 1.0,
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("B received %s = %v, want %v", key, got[key], value)
		}
	}
	if ts, ok := got["last_message_timestamp"].(float64); !ok || ts < 0 {
		t.Errorf("B received last_message_timestamp = %v, want the server's time in ms", got["last_message_timestamp"])
	}
	if len(got) != len(want)+1 {
		t.Errorf("B received %d fields, want %d: %v", len(got), len(want)+1, got)
	}

	got = fields(t, a.waitData(t, "B's telemetry", from("B")))
	if got["machine_torque"] != 9.75 || got["position"] != 7.25 || got["client_id"] != 2.0 {
		t.Errorf("A received machine_torque %v, position %v, client_id %v; want 9.75 (right hand: unchanged), 7.25, 2",
			got["machine_torque"], got["position"], got["client_id"])
	}
	if _, has := got["server_player_id"]; has {
		t.Errorf("A received server_player_id although B did not send one")
	}
}

func TestDirect_SilentRivalIsRepeatedThenShownAsGoneUntilItSendsAgain(t *testing.T) {
	const rivalTimeout = 400 * time.Millisecond
	url, _ := startDirectServer(t, battleserver.Options{RivalTimeout: rivalTimeout})
	a := connectMachine(t, url, "A")
	b := connectMachine(t, url, "B")
	a.report(machineFrame("A", "right", "MULTIPLAYER_SM_RUN", 1, 2))
	b.report(machineFrame("B", "right", "MULTIPLAYER_SM_RUN", 1, 2))
	b.waitData(t, "A's telemetry", from("A"))

	a.goSilent()
	silentSince := time.Now()
	time.Sleep(50 * time.Millisecond)
	b.dropReceived()

	// The last telemetry keeps arriving, unchanged, although A sends nothing new.
	first := b.waitData(t, "A's last telemetry again", from("A"))
	if second := b.waitData(t, "A's last telemetry again", from("A")); second != first {
		t.Fatalf("B received different telemetry while A was silent:\n%s\n%s", first, second)
	}

	b.waitData(t, "no rival after A stayed silent", isNoRival)
	if waited := time.Since(silentSince); waited < rivalTimeout-50*time.Millisecond {
		t.Fatalf("A was shown as gone after %v of silence, before the %v rival timeout", waited, rivalTimeout)
	}
	b.dropReceived()
	if msg := b.waitData(t, "a message", func(string) bool { return true }); msg != rivalGone {
		t.Fatalf("B received %s while A was still silent, want %s", msg, rivalGone)
	}

	a.report(machineFrame("A", "right", "MULTIPLAYER_SM_RUN", 3, 4))
	b.waitData(t, "A's telemetry once A sends again", from("A"))
}

func TestDirect_SilentRivalIsRepeatedForeverWhenRivalTimeoutIsDisabled(t *testing.T) {
	url, _ := startDirectServer(t, battleserver.Options{RivalTimeout: -1})
	a := connectMachine(t, url, "A")
	b := connectMachine(t, url, "B")
	a.report(machineFrame("A", "right", "MULTIPLAYER_SM_RUN", 1, 2))
	b.report(machineFrame("B", "right", "MULTIPLAYER_SM_RUN", 1, 2))
	b.waitData(t, "A's telemetry", from("A"))

	a.goSilent()
	time.Sleep(1500 * time.Millisecond)
	b.dropReceived()
	if msg := b.waitData(t, "a message", func(string) bool { return true }); !from("A")(msg) {
		t.Fatalf("B received %s, want A's last telemetry", msg)
	}
}

func TestDirect_BattleStartsWhenBothAreReadyAndIsDecidedAtAPositionLimit(t *testing.T) {
	for _, tc := range []struct {
		name               string
		winAtOppositeLimit bool
		position           float64
		wantA, wantB       string
	}{
		// A is left-handed; its handle is the one that reaches a limit.
		{"own side wins", false, -45, cmdYouWin, cmdYouLose},
		{"other side loses", false, 45, cmdYouLose, cmdYouWin},
		{"reversed rule: own side loses", true, -45, cmdYouLose, cmdYouWin},
		{"past the limit", false, -47.5, cmdYouWin, cmdYouLose},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url, _ := startDirectServer(t, battleserver.Options{WinAtOppositeLimit: tc.winAtOppositeLimit})
			a := connectMachine(t, url, "A")
			b := connectMachine(t, url, "B")

			a.report(machineFrame("A", "left", "MULTIPLAYER_SM_READY", 0, 0))
			b.report(machineFrame("B", "right", "", 0, 0))
			a.expectNoCommand(t, 150*time.Millisecond)
			b.expectNoCommand(t, 0)

			b.report(machineFrame("B", "right", "MULTIPLAYER_SM_READY", 0, 0))
			a.expectCommand(t, cmdStart)
			b.expectCommand(t, cmdStart)

			// Machines keep reporting ready for a moment after START is sent; that must not start again.
			time.Sleep(50 * time.Millisecond)
			a.report(machineFrame("A", "left", "MULTIPLAYER_SM_RUN", -44.9, 5))
			b.report(machineFrame("B", "right", "MULTIPLAYER_SM_RUN", 0, 5))
			a.expectNoCommand(t, 150*time.Millisecond)
			b.expectNoCommand(t, 0)

			a.report(machineFrame("A", "left", "MULTIPLAYER_SM_RUN", tc.position, 5))
			a.expectCommand(t, tc.wantA)
			b.expectCommand(t, tc.wantB)

			// The handle stays at the limit; the result is sent once.
			a.expectNoCommand(t, 150*time.Millisecond)
			b.expectNoCommand(t, 0)
		})
	}
}

func TestDirect_PositionLimitDoesNotCountBeforeTheBattleRuns(t *testing.T) {
	url, _ := startDirectServer(t, battleserver.Options{})
	a := connectMachine(t, url, "A")
	b := connectMachine(t, url, "B")

	a.report(machineFrame("A", "right", "MULTIPLAYER_SM_READY", 45, 0))
	b.report(machineFrame("B", "right", "MULTIPLAYER_SM_READY", 0, 0))
	a.expectCommand(t, cmdStart)
	b.expectCommand(t, cmdStart)
	a.report(machineFrame("A", "right", "MULTIPLAYER_SM_COUNT_DOWN", 45, 0))
	a.expectNoCommand(t, 150*time.Millisecond)
	b.expectNoCommand(t, 0)

	a.report(machineFrame("A", "right", "MULTIPLAYER_SM_RUN", 45, 0))
	a.expectCommand(t, cmdYouWin)
	b.expectCommand(t, cmdYouLose)
}

func TestDirect_ResultReportedByAMachineIsSentToItsRivalOnly(t *testing.T) {
	for _, tc := range []struct {
		reported, wantRival string
	}{
		{"MULTIPLAYER_SM_GAMEOVER_WIN", cmdYouLose},
		{"MULTIPLAYER_SM_GAMEOVER_LOSE", cmdYouWin},
	} {
		t.Run(tc.reported, func(t *testing.T) {
			a, b := startDirectBattle(t, battleserver.Options{}, "right", "right")
			a.report(machineFrame("A", "right", tc.reported, 10, 5))
			b.expectCommand(t, tc.wantRival)
			a.expectNoCommand(t, 150*time.Millisecond)
			b.expectNoCommand(t, 0)
		})
	}
}

func TestDirect_RematchStartsOnlyAfterThePostMatchPeriod(t *testing.T) {
	const postMatch = 500 * time.Millisecond
	a, b := startDirectBattle(t, battleserver.Options{PostMatchDuration: postMatch}, "right", "left")
	a.report(machineFrame("A", "right", "MULTIPLAYER_SM_RUN", 45, 5))
	a.expectCommand(t, cmdYouWin)
	b.expectCommand(t, cmdYouLose)
	decided := time.Now()

	a.report(machineFrame("A", "right", "MULTIPLAYER_SM_READY", 0, 0))
	b.report(machineFrame("B", "left", "MULTIPLAYER_SM_READY", 0, 0))
	a.expectCommand(t, cmdStart)
	b.expectCommand(t, cmdStart)
	if waited := time.Since(decided); waited < postMatch-50*time.Millisecond {
		t.Fatalf("the rematch started %v after the result, before the %v post-match period ended", waited, postMatch)
	}

	// The second battle is decided like the first.
	a.report(machineFrame("A", "right", "MULTIPLAYER_SM_RUN", 0, 5))
	b.report(machineFrame("B", "left", "MULTIPLAYER_SM_RUN", -45, 5))
	b.expectCommand(t, cmdYouWin)
	a.expectCommand(t, cmdYouLose)
}

func TestDirect_RivalDisconnects_RemainingMachineSeesNoRivalThenTheNextMachine(t *testing.T) {
	url, _ := startDirectServer(t, battleserver.Options{})
	a := connectMachine(t, url, "A")
	b := connectMachine(t, url, "B")
	a.report(machineFrame("A", "right", "MULTIPLAYER_SM_RUN", 1, 2))
	b.report(machineFrame("B", "right", "MULTIPLAYER_SM_RUN", 1, 2))
	b.waitData(t, "A's telemetry", from("A"))

	// A third machine waits: it sees no rival, and neither of the pair sees it.
	c := connectMachine(t, url, "C")
	c.report(machineFrame("C", "right", "MULTIPLAYER_SM_RUN", 1, 2))
	c.waitData(t, "a first message", isNoRival)
	time.Sleep(100 * time.Millisecond)
	for len(c.data) > 0 {
		if msg := <-c.data; msg != rivalGone {
			t.Fatalf("waiting machine C received %s", msg)
		}
	}

	_ = a.ws.Close()
	b.waitData(t, "no rival after A disconnected", func(msg string) bool {
		if from("C")(msg) {
			t.Fatalf("B received C's telemetry before being told A is gone")
		}
		return isNoRival(msg)
	})
	b.waitData(t, "C's telemetry", from("C"))
	c.waitData(t, "B's telemetry", from("B"))
	b.expectNoCommand(t, 0)
}

func TestDirect_AbandonedBattleCanBeFollowedByANewOne(t *testing.T) {
	url, _ := startDirectServer(t, battleserver.Options{})
	a := connectMachine(t, url, "A")
	b := connectMachine(t, url, "B")
	a.report(machineFrame("A", "right", "MULTIPLAYER_SM_READY", 0, 0))
	b.report(machineFrame("B", "right", "MULTIPLAYER_SM_READY", 0, 0))
	a.expectCommand(t, cmdStart)
	b.expectCommand(t, cmdStart)
	b.report(machineFrame("B", "right", "MULTIPLAYER_SM_RUN", 0, 5))

	_ = a.ws.Close()
	b.waitData(t, "no rival after A disconnected", isNoRival)

	// B is still at a limit from the abandoned battle when the next machine arrives.
	b.report(machineFrame("B", "right", "MULTIPLAYER_SM_RUN", 45, 5))
	c := connectMachine(t, url, "C")
	c.report(machineFrame("C", "right", "MULTIPLAYER_SM_READY", 0, 0))
	c.waitData(t, "B's telemetry", from("B"))
	b.expectNoCommand(t, 150*time.Millisecond)
	c.expectNoCommand(t, 0)

	b.report(machineFrame("B", "right", "MULTIPLAYER_SM_READY", 0, 0))
	b.expectCommand(t, cmdStart)
	c.expectCommand(t, cmdStart)
}

func TestDirect_OnlyTelemetryFromAMachineIsUsed(t *testing.T) {
	a, b := startDirectBattle(t, battleserver.Options{}, "right", "right")
	for _, frame := range []string{`{"cmd":"STOP"}`, `{"cmd":{"YOU_LOSE":1}}`, `not json`, `ping`} {
		if err := a.write(websocket.TextMessage, []byte(frame)); err != nil {
			t.Fatalf("send %s: %v", frame, err)
		}
	}
	if err := a.write(websocket.BinaryMessage, []byte{1, 2, 3}); err != nil {
		t.Fatalf("send binary: %v", err)
	}
	b.expectNoCommand(t, 150*time.Millisecond)
	b.dropReceived()
	for i := 0; i < 5; i++ {
		if msg := b.waitData(t, "a message", func(string) bool { return true }); !from("A")(msg) {
			t.Fatalf("B received %s, want A's telemetry", msg)
		}
	}
}

func TestDirect_MachineIsNotPairedWithAMachineBehindABridge(t *testing.T) {
	machineURL, bridgeURL := startDirectServer(t, battleserver.Options{})
	bridge := connect(t, bridgeURL, "cabinet-A", "")
	stop := bridge.feed(playerFrame("behind-bridge"))
	defer stop()

	a := connectMachine(t, machineURL, "A")
	a.report(machineFrame("A", "right", "MULTIPLAYER_SM_RUN", 1, 2))
	a.waitData(t, "a first message", isNoRival)
	time.Sleep(150 * time.Millisecond)
	for len(a.data) > 0 {
		if msg := <-a.data; msg != rivalGone {
			t.Fatalf("directly connected machine received %s from a machine behind a bridge", msg)
		}
	}
	bridge.expectNothing(t, 50*time.Millisecond)

	b := connectMachine(t, machineURL, "B")
	b.report(machineFrame("B", "right", "MULTIPLAYER_SM_RUN", 1, 2))
	a.waitData(t, "B's telemetry", from("B"))
	b.waitData(t, "A's telemetry", from("A"))
	bridge.expectNothing(t, 50*time.Millisecond)
}

func TestDirect_MachineThatSendsNothingIsDropped(t *testing.T) {
	const idle = 300 * time.Millisecond
	url, _ := startDirectServer(t, battleserver.Options{IdleTimeout: idle})
	a := connectMachine(t, url, "A")
	connected := time.Now()
	select {
	case <-a.closed:
	case <-time.After(3 * time.Second):
		t.Fatalf("a machine that never sent anything was still connected after 3s (idle timeout %v)", idle)
	}
	if lasted := time.Since(connected); lasted < idle-50*time.Millisecond {
		t.Fatalf("machine was dropped after %v, before the %v idle timeout", lasted, idle)
	}
}

func TestDirect_AuthTokenDoesNotApplyToMachines(t *testing.T) {
	machineURL, bridgeURL := startDirectServer(t, battleserver.Options{AuthToken: "secret"})
	if ws, _, err := websocket.DefaultDialer.Dial(bridgeURL, nil); err == nil {
		_ = ws.Close()
		t.Fatalf("a bridge without the token was accepted")
	}
	a := connectMachine(t, machineURL, "A")
	a.waitData(t, "a first message", isNoRival)
}
