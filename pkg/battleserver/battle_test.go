package battleserver

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Interacting-Tech/tmp/pkg/protocol"
)

// handle describes one machine's telemetry frame. Limits are -45 and 45 unless noLimits is set.
type handle struct {
	hand     string
	state    string
	position float64
	torque   float64
	noLimits bool
}

func (h handle) frame() string {
	left, right := -45.0, 45.0
	if h.noLimits {
		left, right = 0, 0
	}
	return fmt.Sprintf(`{"data":{"hand":%q,"multiplayer_state":%q,"machine_torque":%v,"position":%v,"left_limit":%v,"right_limit":%v}}`,
		h.hand, h.state, h.torque, h.position, left, right)
}

// sentTo drains what m's bridge would be sent and returns each frame's multiplayer_state without
// its common prefix, or the raw frame when it is not telemetry.
func sentTo(t *testing.T, m *machine) []string {
	t.Helper()
	frames := pendingFor(m)
	out := make([]string, len(frames))
	for i, f := range frames {
		rival, ok := protocol.ParseRivalEnvelope([]byte(f))
		if !ok {
			out[i] = f
			continue
		}
		out[i] = strings.TrimPrefix(rival.MultiplayerState, "MULTIPLAYER_SM_")
	}
	return out
}

func expectSent(t *testing.T, what string, m *machine, want ...string) {
	t.Helper()
	got := sentTo(t, m)
	if len(got) != len(want) || strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("%s: %s's bridge would be sent %v, want %v", what, m.id, got, want)
	}
}

type testBattle struct {
	t     *testing.T
	s     *Server
	a, b  *machine
	clock time.Time
}

func newTestBattle(t *testing.T, opts Options) *testBattle {
	t.Helper()
	tb := &testBattle{t: t, clock: time.Unix(1_000_000, 0)}
	_, tb.a, tb.b = newPair()
	tb.s = New(opts)
	tb.s.now = func() time.Time { return tb.clock }
	return tb
}

func (tb *testBattle) send(m *machine, h handle) {
	tb.s.handleFrame(m, []byte(h.frame()))
}

// start takes both machines through ready into a running battle with centred handles.
func (tb *testBattle) start(handA, handB string) {
	tb.t.Helper()
	tb.send(tb.a, handle{hand: handA, state: protocol.StateReady})
	tb.send(tb.b, handle{hand: handB, state: protocol.StateReady})
	tb.send(tb.b, handle{hand: handB, state: protocol.StateRun})
	tb.send(tb.a, handle{hand: handA, state: protocol.StateRun})
	tb.send(tb.b, handle{hand: handB, state: protocol.StateRun})
	if got := tb.a.battle.state; got != protocol.ServerMidMatch {
		tb.t.Fatalf("battle is in %s after both machines reported ready, want %s", got, protocol.ServerMidMatch)
	}
	expectSent(tb.t, "running battle", tb.a, "RUN")
	expectSent(tb.t, "running battle", tb.b, "RUN")
}

func TestBattle_HandleReachingAPositionLimitDecidesTheBattle(t *testing.T) {
	for _, tc := range []struct {
		name               string
		hand               string
		position           float64
		winAtOppositeLimit bool
		aWins              bool
	}{
		{name: "right hand reaches right limit: wins", hand: "right", position: 45, aWins: true},
		{name: "right hand reaches left limit: loses", hand: "right", position: -45, aWins: false},
		{name: "left hand reaches left limit: wins", hand: "left", position: -45, aWins: true},
		{name: "left hand reaches right limit: loses", hand: "left", position: 45, aWins: false},
		{name: "past the limit counts as reaching it", hand: "right", position: 47.5, aWins: true},
		{name: "hand not reported counts as right", hand: "", position: 45, aWins: true},
		{name: "reversed: right hand reaches right limit: loses", hand: "right", position: 45, winAtOppositeLimit: true, aWins: false},
		{name: "reversed: left hand reaches right limit: wins", hand: "left", position: 45, winAtOppositeLimit: true, aWins: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tb := newTestBattle(t, Options{WinAtOppositeLimit: tc.winAtOppositeLimit})
			tb.start(tc.hand, "right")

			tb.send(tb.a, handle{hand: tc.hand, state: protocol.StateRun, position: math.Copysign(44.9, tc.position)})
			expectSent(t, "just short of the limit", tb.b, "RUN")
			expectSent(t, "just short of the limit", tb.a)

			aResult, bResult := "GAMEOVER_LOSE", "GAMEOVER_WIN"
			if tc.aWins {
				aResult, bResult = bResult, aResult
			}

			// Both bridges learn the result at once: each is sent its rival's telemetry with the
			// rival's result as the state, which makes the local server tell its machine the opposite.
			tb.send(tb.a, handle{hand: tc.hand, state: protocol.StateRun, position: tc.position})
			expectSent(t, "at the limit", tb.b, aResult)
			expectSent(t, "at the limit", tb.a, bResult)

			// The result holds whatever the machines report, for the post-match time.
			tb.clock = tb.clock.Add(1900 * time.Millisecond)
			tb.send(tb.a, handle{hand: tc.hand, state: protocol.StateRun, position: -tc.position})
			tb.send(tb.b, handle{hand: "right", state: protocol.StateReady})
			expectSent(t, "during post-match", tb.b, aResult)
			expectSent(t, "during post-match", tb.a, bResult)

			// After it the machines' own states are relayed again and they can start another battle.
			tb.clock = tb.clock.Add(200 * time.Millisecond)
			tb.send(tb.a, handle{hand: tc.hand, state: protocol.StateReady})
			tb.send(tb.b, handle{hand: "right", state: protocol.StateReady})
			expectSent(t, "after post-match", tb.b, "READY")
			expectSent(t, "after post-match", tb.a, "READY")
			tb.send(tb.a, handle{hand: tc.hand, state: protocol.StateRun, position: tc.position})
			expectSent(t, "second battle decided", tb.b, aResult)
			expectSent(t, "second battle decided", tb.a, bResult)
		})
	}
}

func TestBattle_PositionLimitOnlyCountsWhileTheBattleIsRunning(t *testing.T) {
	atLimit := handle{hand: "right", state: protocol.StateRun, position: 45}

	t.Run("no battle started", func(t *testing.T) {
		tb := newTestBattle(t, Options{})
		tb.send(tb.b, handle{hand: "right", state: protocol.StateRun})
		tb.send(tb.a, atLimit)
		expectSent(t, "idle", tb.b, "RUN")
		expectSent(t, "idle", tb.a, "RUN")
	})

	t.Run("only one machine ready", func(t *testing.T) {
		tb := newTestBattle(t, Options{})
		tb.send(tb.b, handle{hand: "right", state: protocol.StateReady})
		tb.send(tb.a, atLimit)
		expectSent(t, "waiting", tb.b, "RUN")
		expectSent(t, "waiting", tb.a, "READY")
	})

	t.Run("before the machine reports running", func(t *testing.T) {
		tb := newTestBattle(t, Options{})
		tb.send(tb.a, handle{hand: "right", state: protocol.StateReady})
		tb.send(tb.b, handle{hand: "right", state: protocol.StateReady})
		tb.send(tb.b, handle{hand: "right", state: protocol.StateCountDown})
		pendingFor(tb.a)
		for _, state := range []string{protocol.StateReady, protocol.StateCountDown, ""} {
			tb.send(tb.a, handle{hand: "right", state: state, position: 45})
			expectSent(t, "state "+state, tb.b, strings.TrimPrefix(state, "MULTIPLAYER_SM_"))
			expectSent(t, "state "+state, tb.a)
		}
	})

	t.Run("machine reports no limits", func(t *testing.T) {
		tb := newTestBattle(t, Options{})
		tb.start("right", "right")
		for _, position := range []float64{0, 60, -60} {
			tb.send(tb.a, handle{hand: "right", state: protocol.StateRun, position: position, noLimits: true})
			expectSent(t, fmt.Sprintf("position %v", position), tb.b, "RUN")
			expectSent(t, fmt.Sprintf("position %v", position), tb.a)
		}
	})

	t.Run("already decided", func(t *testing.T) {
		tb := newTestBattle(t, Options{})
		tb.start("right", "right")
		tb.send(tb.a, atLimit)
		expectSent(t, "A wins", tb.b, "GAMEOVER_WIN")
		expectSent(t, "A wins", tb.a, "GAMEOVER_LOSE")
		tb.send(tb.b, atLimit)
		expectSent(t, "B at its winning limit afterwards", tb.a, "GAMEOVER_LOSE")
		expectSent(t, "B at its winning limit afterwards", tb.b)
	})

	t.Run("post-match over and nobody ready again", func(t *testing.T) {
		tb := newTestBattle(t, Options{})
		tb.start("right", "right")
		tb.send(tb.a, atLimit)
		pendingFor(tb.a)
		pendingFor(tb.b)
		tb.clock = tb.clock.Add(3 * time.Second)
		tb.send(tb.a, atLimit)
		expectSent(t, "idle again", tb.b, "RUN")
		expectSent(t, "idle again", tb.a)
	})
}

func TestBattle_ResultReportedByAMachineIsRelayedAsReported(t *testing.T) {
	tb := newTestBattle(t, Options{})
	tb.start("right", "left")

	tb.send(tb.a, handle{hand: "right", state: protocol.StateGameOverLose})
	expectSent(t, "A reports it lost", tb.b, "GAMEOVER_LOSE")
	expectSent(t, "A reports it lost", tb.a)

	// The battle is decided, so a handle reaching a limit afterwards changes nothing.
	tb.send(tb.b, handle{hand: "left", state: protocol.StateRun, position: 45})
	expectSent(t, "B at its losing limit afterwards", tb.a, "RUN")
	expectSent(t, "B at its losing limit afterwards", tb.b)
}

func TestBattle_AbandonedWhenAMachineIsGone(t *testing.T) {
	tb := newTestBattle(t, Options{})
	tb.start("right", "right")

	tb.s.handleFrame(tb.a, []byte(`{"data":{}}`))
	expectSent(t, "A's machine gone", tb.b, `{"data":{}}`)

	tb.send(tb.a, handle{hand: "right", state: protocol.StateRun, position: 45})
	expectSent(t, "A's machine back at a limit", tb.b, "RUN")
	expectSent(t, "A's machine back at a limit", tb.a)
}

// A machine leaves READY as soon as its local server starts it. Its rival's local server starts
// the rival only while it sees both machines ready, and frames that arrive together over the
// network are reduced to the newest one, so the rival might never see that READY.
func TestBattle_StartedMachineStaysReadyForItsRivalUntilTheRivalHasStartedToo(t *testing.T) {
	tb := newTestBattle(t, Options{})
	tb.send(tb.a, handle{hand: "right", state: protocol.StateReady})
	expectSent(t, "A ready", tb.b, "READY")

	// B is ready and started by its local server at once; both frames reach A's bridge together.
	tb.send(tb.b, handle{hand: "right", state: protocol.StateReady})
	tb.send(tb.b, handle{hand: "right", state: protocol.StateCountDown, torque: 3})
	got := pendingFor(tb.a)
	if len(got) != 1 || !strings.Contains(got[0], protocol.StateReady) || !strings.Contains(got[0], `"machine_torque":3,`) {
		t.Fatalf("A's bridge would be sent %v, want B's newest telemetry shown as ready", got)
	}
	tb.send(tb.a, handle{hand: "right", state: protocol.StateReady})
	tb.send(tb.b, handle{hand: "right", state: protocol.StateRun})
	expectSent(t, "A not started yet", tb.a, "READY")

	// A has started: B is shown as it reports itself.
	tb.send(tb.a, handle{hand: "right", state: protocol.StateCountDown})
	expectSent(t, "A started", tb.b, "COUNT_DOWN")
	tb.send(tb.b, handle{hand: "right", state: protocol.StateRun})
	expectSent(t, "A started", tb.a, "RUN")
}

func TestBattle_ResultIsNeverHiddenFromARivalThatIsStillReady(t *testing.T) {
	tb := newTestBattle(t, Options{})
	tb.send(tb.a, handle{hand: "right", state: protocol.StateReady})
	tb.send(tb.b, handle{hand: "right", state: protocol.StateReady})
	sentTo(t, tb.a)
	sentTo(t, tb.b)

	tb.send(tb.b, handle{hand: "right", state: protocol.StateRun, position: 45})
	expectSent(t, "B at its winning limit while A still reports ready", tb.a, "GAMEOVER_WIN")
	expectSent(t, "B at its winning limit while A still reports ready", tb.b, "GAMEOVER_LOSE")

	tb.clock = tb.clock.Add(time.Hour)
	tb.send(tb.a, handle{hand: "right", state: protocol.StateReady})
	tb.send(tb.b, handle{hand: "right", state: protocol.StateGameOverWin})
	expectSent(t, "B reports a result outside a battle", tb.a, "GAMEOVER_WIN")
}

// A battle can end without a result, for example when an operator stops it on a machine's local
// server. The next time a machine reports ready, the battle server must be waiting for both again.
func TestBattle_MachineReadyAgainDuringABattleStartsANewOne(t *testing.T) {
	tb := newTestBattle(t, Options{})
	tb.start("right", "right")
	tb.send(tb.a, handle{hand: "right", state: ""})
	tb.send(tb.b, handle{hand: "right", state: ""})
	sentTo(t, tb.a)
	sentTo(t, tb.b)

	tb.send(tb.a, handle{hand: "right", state: protocol.StateReady})
	tb.send(tb.b, handle{hand: "right", state: "MULTIPLAYER_SM_IDLE", position: 45})
	expectSent(t, "only A ready again", tb.a, "IDLE")
	expectSent(t, "only A ready again", tb.b, "READY")

	tb.send(tb.b, handle{hand: "right", state: protocol.StateReady})
	tb.send(tb.a, handle{hand: "right", state: protocol.StateRun})
	tb.send(tb.b, handle{hand: "right", state: protocol.StateRun, position: 45})
	expectSent(t, "B at its winning limit in the new battle", tb.a, "GAMEOVER_WIN")
	expectSent(t, "B at its winning limit in the new battle", tb.b, "GAMEOVER_LOSE")
}

func TestBattle_ResultIsForgottenWhenAMachineIsGone(t *testing.T) {
	tb := newTestBattle(t, Options{})
	tb.start("right", "right")
	tb.send(tb.a, handle{hand: "right", state: protocol.StateRun, position: 45})
	expectSent(t, "A at its winning limit", tb.b, "GAMEOVER_WIN")
	expectSent(t, "A at its winning limit", tb.a, "GAMEOVER_LOSE")

	tb.s.handleFrame(tb.b, []byte(`{"data":{}}`))
	expectSent(t, "B's machine gone", tb.a, `{"data":{}}`)

	tb.send(tb.b, handle{hand: "right", state: protocol.StateCountDown})
	tb.send(tb.a, handle{hand: "right", state: protocol.StateCountDown})
	expectSent(t, "B's machine back", tb.a, "COUNT_DOWN")
	expectSent(t, "B's machine back", tb.b, "COUNT_DOWN")
}

func TestRelay_LeftHandMachineTorqueIsNegatedForTheRival(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame string
		want  string
	}{
		{"left hand", `{"data":{"hand":"left","machine_torque":-12.5}}`, `"machine_torque":12.5,`},
		{"left hand, positive torque", `{"data":{"hand":"left","machine_torque":7.25}}`, `"machine_torque":-7.25,`},
		{"left hand, zero torque", `{"data":{"hand":"left","machine_torque":0}}`, `"machine_torque":0,`},
		{"left hand, other spelling", `{"data":{"hand":" Left ","machine_torque":3}}`, `"machine_torque":-3,`},
		{"right hand", `{"data":{"hand":"right","machine_torque":-12.5}}`, `"machine_torque":-12.5,`},
		{"hand not reported", `{"data":{"machine_torque":-12.5}}`, `"machine_torque":-12.5,`},
		{"left hand, older torque fields are left alone", `{"data":{"hand":"left","torque":5,"opponent_torque":3}}`, `"torque":5,"opponent_torque":3,`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, a, b := newPair()
			s.handleFrame(a, []byte(tc.frame))
			got := pendingFor(b)
			if len(got) != 1 || !strings.Contains(got[0], tc.want) {
				t.Fatalf("rival would be sent %v, want a frame containing %s", got, tc.want)
			}
		})
	}

	// Each player's own hand decides, whatever hand the rival uses.
	s, a, b := newPair()
	s.handleFrame(a, []byte(`{"data":{"hand":"left","machine_torque":4}}`))
	s.handleFrame(b, []byte(`{"data":{"hand":"left","machine_torque":-6}}`))
	if got := pendingFor(b); len(got) != 1 || !strings.Contains(got[0], `"machine_torque":-4,`) {
		t.Fatalf("left vs left: B would be sent %v, want A's torque negated", got)
	}
	if got := pendingFor(a); len(got) != 1 || !strings.Contains(got[0], `"machine_torque":6,`) {
		t.Fatalf("left vs left: A would be sent %v, want B's torque negated", got)
	}
}
