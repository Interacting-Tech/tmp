// battle-demo runs battles between two simulated arm-wrestling machines and prints what happens.
//
// Each simulated machine has its own simulated PROTOCOL.md server and a real machine bridge, and
// the two bridges meet at a battle server: one this program starts, or yours with --server-url.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Interacting-Tech/tmp/pkg/battleserver"
	"github.com/Interacting-Tech/tmp/pkg/bridge"
	"github.com/Interacting-Tech/tmp/pkg/machineclient"
	"github.com/Interacting-Tech/tmp/pkg/machinesim"
	"github.com/Interacting-Tech/tmp/pkg/protocol"
)

type config struct {
	// serverURL is the battle server the bridges connect to. Empty: the demo starts its own.
	serverURL string
	authToken string
	// winAtOppositeLimit configures the battle server the demo starts.
	winAtOppositeLimit bool
	// postMatch is how long the simulated local servers, and the battle server the demo starts,
	// keep a decided battle before the next can begin (PROTOCOL.md: 2s).
	postMatch time.Duration
	// connectTimeout bounds the wait for the two machines to see each other.
	connectTimeout time.Duration
}

func main() {
	cfg := config{postMatch: 2 * time.Second, connectTimeout: 15 * time.Second}
	flag.StringVar(&cfg.serverURL, "server-url", "", "Battle server to use, e.g. ws://host:9000/ws/machine (default: start one inside this program). Only use a server no real machine is connected to: the demo's machines are paired with whichever machine is waiting")
	flag.StringVar(&cfg.authToken, "auth-token", os.Getenv("OUR_SERVER_TOKEN"), "Bearer token for --server-url")
	flag.BoolVar(&cfg.winAtOppositeLimit, "win-at-opposite-limit", false, "Start the built-in battle server with --win-at-opposite-limit")
	flag.Parse()

	if err := run(cfg, os.Stdout); err != nil {
		fmt.Fprintf(os.Stdout, "\nFAIL: %v\n", err)
		os.Exit(1)
	}
}

type demo struct {
	cfg     config
	started time.Time
	outMu   sync.Mutex
	out     io.Writer
	a, b    *player
	// finished silences the battle server's log while the demo disconnects everything.
	finished atomic.Bool
}

// player is one simulated machine with its local server and bridge.
type player struct {
	name        string
	playerID    string
	hand        string
	left, right float64
	local       *machinesim.LocalServer
	machine     *machinesim.Machine
	bridge      *bridge.MachineBridge

	diagMu   sync.Mutex
	lastDiag string
}

func (d *demo) logf(format string, args ...any) {
	d.outMu.Lock()
	defer d.outMu.Unlock()
	fmt.Fprintf(d.out, "%7.3fs  %s\n", time.Since(d.started).Seconds(), fmt.Sprintf(format, args...))
}

func (d *demo) say(format string, args ...any) {
	d.outMu.Lock()
	defer d.outMu.Unlock()
	fmt.Fprintf(d.out, format+"\n", args...)
}

func run(cfg config, out io.Writer) error {
	d := &demo{cfg: cfg, started: time.Now(), out: out}
	defer d.finished.Store(true)
	builtIn := cfg.serverURL == ""

	serverURL := cfg.serverURL
	if builtIn {
		url, stop, err := d.startBattleServer()
		if err != nil {
			return err
		}
		defer stop()
		serverURL = url
		d.say("Battle server: %s (started by this demo)", serverURL)
	} else {
		d.say("Battle server: %s", serverURL)
	}
	d.say("Machine A: left hand, limits -45 to 45. Machine B: right hand, limits -40 to 40.")
	d.say("Each simulated machine sends a frame every 10 ms to its own simulated PROTOCOL.md server;")
	d.say("a machine bridge connects each of those servers to the battle server.\n")

	d.a = &player{name: "machine A", playerID: "demo-player-A", hand: protocol.HandLeft, left: -45, right: 45}
	d.b = &player{name: "machine B", playerID: "demo-player-B", hand: protocol.HandRight, left: -40, right: 40}
	for i, p := range []*player{d.a, d.b} {
		letter := string(rune('A' + i))
		stop, err := d.startPlayer(p, serverURL, "02:00:00:00:00:0"+letter, "battle-demo-"+letter)
		if err != nil {
			return err
		}
		defer stop()
	}

	if err := d.waitUntilPaired(); err != nil {
		return err
	}
	d.logf("each machine is receiving the other's frames")

	rounds := []round{
		{
			title:      "Round 1: both handles move right",
			firstReady: d.a,
			speedA:     40, torqueA: -12.5,
			speedB: 40, torqueB: 9.75,
		},
		{
			title:      "Round 2 (rematch): both handles move left",
			firstReady: d.b,
			speedA:     -50, torqueA: -14,
			speedB: -40, torqueB: 8.5,
		},
	}
	for i, r := range rounds {
		if i > 0 {
			time.Sleep(cfg.postMatch + 500*time.Millisecond)
		}
		if err := d.play(r, builtIn); err != nil {
			return fmt.Errorf("%s: %w", r.title, err)
		}
	}

	sentA, gotA := d.a.machine.Counts()
	sentB, gotB := d.b.machine.Counts()
	d.finished.Store(true)
	d.say("\nMachine A sent %d frames and received machine B's data %d times.", sentA, gotA)
	d.say("Machine B sent %d frames and received machine A's data %d times.", sentB, gotB)
	d.say("PASS: %d battles completed", len(rounds))
	return nil
}

func (d *demo) startBattleServer() (url string, stop func(), err error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, fmt.Errorf("start battle server: %w", err)
	}
	battles := battleserver.New(battleserver.Options{
		WinAtOppositeLimit: d.cfg.winAtOppositeLimit,
		PostMatchDuration:  d.cfg.postMatch,
		Logf: func(format string, args ...any) {
			if !d.finished.Load() {
				d.logf("battle server: "+format, args...)
			}
		},
	})
	mux := http.NewServeMux()
	mux.Handle("/ws/machine", battles)
	httpSrv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = httpSrv.Serve(ln) }()

	return "ws://" + ln.Addr().String() + "/ws/machine", func() {
		_ = httpSrv.Close()
		battles.Close()
	}, nil
}

func (d *demo) startPlayer(p *player, serverURL, macAddress, machineID string) (stop func(), err error) {
	loopback := net.IPv4(127, 0, 0, 1)
	p.local, err = machinesim.StartLocalServer(loopback, d.cfg.postMatch)
	if err != nil {
		return nil, fmt.Errorf("start %s's local server: %w", p.name, err)
	}
	p.machine, err = machinesim.ConnectMachine(p.local.EndpointURL(), machinesim.MachineConfig{
		MACAddress: macAddress,
		PlayerID:   p.playerID,
		Hand:       p.hand,
		LeftLimit:  p.left,
		RightLimit: p.right,
	})
	if err != nil {
		p.local.Close()
		return nil, err
	}

	headers := http.Header{battleserver.MachineIDHeader: {machineID}}
	if token := strings.TrimSpace(d.cfg.authToken); token != "" {
		headers.Set("Authorization", "Bearer "+token)
	}
	upstream, err := bridge.NewWebSocketServerClient(bridge.UpstreamOptions{
		ServerURL: serverURL,
		Headers:   headers,
		OnDiagnostic: func(msg string, err error) {
			p.diagMu.Lock()
			p.lastDiag = msg
			if err != nil {
				p.lastDiag = fmt.Sprintf("%s: %v", msg, err)
			}
			p.diagMu.Unlock()
		},
	})
	if err == nil {
		p.bridge, err = bridge.New(bridge.Config{MachineOptions: machineclient.Options{
			Port:              p.local.Port(),
			MachineIPOverride: loopback.String(),
		}}, upstream)
	}
	if err == nil {
		err = p.bridge.Start(context.Background())
	}
	if err != nil {
		p.machine.Close()
		p.local.Close()
		return nil, fmt.Errorf("start %s's bridge: %w", p.name, err)
	}

	return func() {
		p.machine.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = p.bridge.Stop(ctx)
		p.local.Close()
	}, nil
}

// waitUntilPaired waits until each machine receives the other's frames, and fails if the battle
// server paired a demo machine with some other machine.
func (d *demo) waitUntilPaired() error {
	deadline := time.Now().Add(d.cfg.connectTimeout)
	for {
		seen := 0
		for _, pair := range [][2]*player{{d.a, d.b}, {d.b, d.a}} {
			me, other := pair[0], pair[1]
			frame, ok := parseFrame(me.machine.LastRivalFrame())
			if !ok {
				continue
			}
			if got := frame.str("player_id"); got != other.playerID {
				return fmt.Errorf("%s was paired with another machine (player %q): the battle server already had a machine waiting. Run the demo against a server no other machine is connected to", me.name, got)
			}
			seen++
		}
		if seen == 2 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the two machines did not see each other through the battle server within %v%s", d.cfg.connectTimeout, d.bridgeProblems())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (d *demo) bridgeProblems() string {
	var sb strings.Builder
	for _, p := range []*player{d.a, d.b} {
		p.diagMu.Lock()
		if p.lastDiag != "" {
			fmt.Fprintf(&sb, "\n  %s's bridge: %s", p.name, p.lastDiag)
		}
		p.diagMu.Unlock()
	}
	if strings.Contains(sb.String(), "bad handshake") {
		sb.WriteString("\n  (\"bad handshake\" usually means a wrong --auth-token or URL path)")
	}
	return sb.String()
}

type round struct {
	title      string
	firstReady *player
	// Handle speed in position units per second and reported torque, per machine.
	speedA, torqueA float64
	speedB, torqueB float64
}

func (d *demo) play(r round, checkWinner bool) error {
	d.say("\n--- %s ---", r.title)
	d.a.machine.Pull(r.speedA, r.torqueA)
	d.b.machine.Pull(r.speedB, r.torqueB)

	second := d.b
	if r.firstReady == d.b {
		second = d.a
	}
	r.firstReady.machine.GetReady()
	d.logf("%s is ready", r.firstReady.name)
	time.Sleep(300 * time.Millisecond)
	second.machine.GetReady()
	d.logf("%s is ready", second.name)

	for _, p := range []*player{d.a, d.b} {
		if _, err := d.expect(p, protocol.CmdStart); err != nil {
			return err
		}
	}

	time.Sleep(300 * time.Millisecond)
	if err := d.showExchange(d.a, d.b); err != nil {
		return err
	}
	if err := d.showExchange(d.b, d.a); err != nil {
		return err
	}

	resultA, err := d.expect(d.a, protocol.CmdYouWin, protocol.CmdYouLose)
	if err != nil {
		return err
	}
	resultB, err := d.expect(d.b, protocol.CmdYouWin, protocol.CmdYouLose)
	if err != nil {
		return err
	}
	if resultA == resultB {
		return fmt.Errorf("both machines received %s", resultA)
	}
	winner := d.a
	if resultB == protocol.CmdYouWin {
		winner = d.b
	}
	d.logf("%s won", winner.name)

	if checkWinner {
		if want := d.expectedWinner(r); winner != want {
			return fmt.Errorf("%s won, but by the battle rules %s should have", winner.name, want.name)
		}
	}
	return nil
}

// expectedWinner applies the battle server's limit rule to the round's handle movements.
func (d *demo) expectedWinner(r round) *player {
	timeToLimit := func(p *player, speed float64) float64 {
		if speed > 0 {
			return p.right / speed
		}
		return p.left / speed
	}
	first, other, speed := d.a, d.b, r.speedA
	if timeToLimit(d.b, r.speedB) < timeToLimit(d.a, r.speedA) {
		first, other, speed = d.b, d.a, r.speedB
	}
	atOwnSide := (first.hand == protocol.HandLeft) == (speed < 0)
	if atOwnSide != d.cfg.winAtOppositeLimit {
		return first
	}
	return other
}

// expect waits for one of the named commands to reach p's machine and returns it.
func (d *demo) expect(p *player, commands ...string) (string, error) {
	timeout := time.After(10 * time.Second)
	for {
		select {
		case got := <-p.machine.Commands():
			for _, want := range commands {
				if got == want {
					d.logf("%s received %s (its handle is at %v)", p.name, got, p.machine.Position())
					return got, nil
				}
			}
		case <-timeout:
			return "", fmt.Errorf("%s did not receive %s within 10s", p.name, strings.Join(commands, " or "))
		}
	}
}

// showExchange prints the newest frame of from's that reached to's machine next to the frame as
// from sent it, and checks the values that must match.
func (d *demo) showExchange(from, to *player) error {
	receivedRaw := to.machine.LastRivalFrame()
	received, ok := parseFrame(receivedRaw)
	if !ok {
		return fmt.Errorf("%s is not receiving %s's frames", to.name, from.name)
	}
	sentRaw, ok := from.machine.SentFrame(uint64(received.num("timeStamp")))
	if !ok {
		return fmt.Errorf("%s received a frame %s did not send: %s", to.name, from.name, receivedRaw)
	}
	sent, _ := parseFrame(sentRaw)

	d.logf("%s sent:      %s", from.name, sentRaw)
	d.logf("%s received:  %s", to.name, receivedRaw)

	wantTorque := sent.num("machine_torque")
	rule := "right hand: machine_torque unchanged"
	if from.hand == protocol.HandLeft {
		wantTorque = -wantTorque
		rule = "left hand: machine_torque multiplied by -1"
	}
	d.logf("          (%s uses the %s)", from.name, rule)

	if got := received.num("machine_torque"); math.Abs(got-wantTorque) > 1e-9 {
		return fmt.Errorf("%s received machine_torque %v, want %v", to.name, got, wantTorque)
	}
	if got, want := received.num("position"), sent.num("position"); got != want {
		return fmt.Errorf("%s received position %v, want %v", to.name, got, want)
	}
	for _, field := range []string{"torque", "opponent_torque"} {
		if _, exists := received[field]; exists {
			return fmt.Errorf("%s received a %q field; machine_torque is the only torque value", to.name, field)
		}
	}
	return nil
}

type frame map[string]any

func parseFrame(raw string) (frame, bool) {
	if raw == "" {
		return nil, false
	}
	var msg struct {
		Data frame `json:"data"`
	}
	if err := json.Unmarshal([]byte(raw), &msg); err != nil || msg.Data == nil {
		return nil, false
	}
	return msg.Data, true
}

func (f frame) num(key string) float64 {
	v, _ := f[key].(float64)
	return v
}

func (f frame) str(key string) string {
	v, _ := f[key].(string)
	return v
}
