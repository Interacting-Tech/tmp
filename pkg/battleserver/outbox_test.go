package battleserver

import (
	"fmt"
	"strings"
	"testing"

	"dev.azure.com/InteractingTechnologies/Glenfiddich/_git/Arm-Wrestling/pkg/protocol"
)

// pendingFor returns what a bridge that has not read anything yet would be sent, in order.
func pendingFor(m *machine) []string {
	var (
		scratch []byte
		out     []string
	)
	for {
		frame, ok := m.out.next(&scratch)
		if !ok {
			return out
		}
		out = append(out, string(frame))
	}
}

func newPair() (s *Server, a, b *machine) {
	s = New(Options{})
	a, b = &machine{id: "a"}, &machine{id: "b"}
	for _, m := range []*machine{a, b} {
		m.out.wake = make(chan struct{}, 1)
	}
	a.rival, b.rival = b, a
	bt := &battle{state: protocol.ServerIdle}
	a.battle, b.battle = bt, bt
	return s, a, b
}

func stateFrame(state string, timeStamp int) string {
	return fmt.Sprintf(`{"data":{"multiplayer_state":%q,"machine_torque":2,"timeStamp":%d}}`, state, timeStamp)
}

func summarize(frames []string) []string {
	out := make([]string, len(frames))
	for i, f := range frames {
		rival, ok := protocol.ParseRivalEnvelope([]byte(f))
		if !ok {
			out[i] = f
			continue
		}
		out[i] = fmt.Sprintf("%s@%d", strings.TrimPrefix(rival.MultiplayerState, "MULTIPLAYER_SM_"), rival.TimeStamp)
	}
	return out
}

func TestSlowRival_GetsOnlyTheNewestTelemetryButNeverMissesTheResult(t *testing.T) {
	for _, tc := range []struct {
		name string
		sent []string
		want []string
	}{
		{
			name: "telemetry is latest-wins",
			sent: []string{stateFrame(protocol.StateRun, 1), stateFrame(protocol.StateRun, 2), stateFrame(protocol.StateRun, 3)},
			want: []string{"RUN@3"},
		},
		{
			name: "result survives the telemetry that follows it",
			sent: []string{
				stateFrame(protocol.StateRun, 1),
				stateFrame(protocol.StateGameOverWin, 2),
				stateFrame(protocol.StateGameOverWin, 3),
				stateFrame(protocol.StateReady, 4),
				stateFrame(protocol.StateReady, 5),
			},
			want: []string{"GAMEOVER_WIN@2", "READY@5"},
		},
		{
			name: "machine gone replaces unsent telemetry and stays ahead of what follows",
			sent: []string{stateFrame(protocol.StateRun, 1), `{"data":{}}`, stateFrame(protocol.StateRun, 2), stateFrame(protocol.StateRun, 3)},
			want: []string{`{"data":{}}`, "RUN@3"},
		},
		{
			name: "no stale telemetry follows the machine being gone",
			sent: []string{stateFrame(protocol.StateRun, 1), `{"data":{}}`},
			want: []string{`{"data":{}}`},
		},
		{
			name: "no stale telemetry follows the result",
			sent: []string{stateFrame(protocol.StateRun, 1), stateFrame(protocol.StateGameOverLose, 2)},
			want: []string{"GAMEOVER_LOSE@2"},
		},
		{
			name: "win then lose are both delivered in order",
			sent: []string{stateFrame(protocol.StateGameOverWin, 1), stateFrame(protocol.StateGameOverLose, 2)},
			want: []string{"GAMEOVER_WIN@1", "GAMEOVER_LOSE@2"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, a, b := newPair()
			for _, frame := range tc.sent {
				s.handleFrame(a, []byte(frame))
			}
			got := summarize(pendingFor(b))
			if strings.Join(got, " ") != strings.Join(tc.want, " ") {
				t.Fatalf("rival would be sent %v, want %v", got, tc.want)
			}
			if echoed := pendingFor(a); len(echoed) != 0 {
				t.Fatalf("sender was sent its own frames back: %v", echoed)
			}
		})
	}
}

func TestOutbox_BoundsUndeliveredPriorityFrames(t *testing.T) {
	_, _, b := newPair()
	for i := 0; i < maxPriorityFrames+5; i++ {
		b.out.putPriority([]byte(fmt.Sprintf("p%d", i)))
	}
	got := pendingFor(b)
	if len(got) != maxPriorityFrames || got[0] != "p5" || got[len(got)-1] != fmt.Sprintf("p%d", maxPriorityFrames+4) {
		t.Fatalf("kept %d priority frames from %s to %s, want the newest %d", len(got), got[0], got[len(got)-1], maxPriorityFrames)
	}
}
