package main

import (
	"bytes"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Interacting-Tech/tmp/pkg/battleserver"
	"github.com/gorilla/websocket"
)

func testConfig() config {
	return config{postMatch: 150 * time.Millisecond, connectTimeout: 5 * time.Second}
}

// winners returns who the demo reported as the winner of each round.
func winners(output string) []string {
	var out []string
	for _, line := range strings.Split(output, "\n") {
		if name, ok := strings.CutSuffix(strings.TrimSpace(line), " won"); ok {
			out = append(out, name[len(name)-len("machine A"):])
		}
	}
	return out
}

func TestDemo_TwoBattlesOnItsOwnBattleServer(t *testing.T) {
	for _, tc := range []struct {
		name               string
		winAtOppositeLimit bool
		want               string
	}{
		// Round 1: right-handed B reaches its right limit. Round 2: left-handed A reaches its left limit.
		{"default rule", false, "machine B, machine A"},
		{"win at opposite limit", true, "machine A, machine B"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.winAtOppositeLimit = tc.winAtOppositeLimit
			var out bytes.Buffer
			if err := run(cfg, &out); err != nil {
				t.Fatalf("demo failed: %v\n%s", err, out.String())
			}
			if got := strings.Join(winners(out.String()), ", "); got != tc.want {
				t.Fatalf("winners = %s, want %s\n%s", got, tc.want, out.String())
			}
			if !strings.Contains(out.String(), "PASS: 2 battles completed") {
				t.Fatalf("demo did not report success:\n%s", out.String())
			}
		})
	}
}

func startBattleServer(t *testing.T, token string) string {
	t.Helper()
	battles := battleserver.New(battleserver.Options{AuthToken: token, PostMatchDuration: 150 * time.Millisecond})
	httpSrv := httptest.NewServer(battles)
	t.Cleanup(func() {
		battles.Close()
		httpSrv.Close()
	})
	return "ws" + strings.TrimPrefix(httpSrv.URL, "http")
}

func TestDemo_AgainstASeparateBattleServer(t *testing.T) {
	cfg := testConfig()
	cfg.serverURL = startBattleServer(t, "secret")
	cfg.authToken = "secret"
	var out bytes.Buffer
	if err := run(cfg, &out); err != nil {
		t.Fatalf("demo failed: %v\n%s", err, out.String())
	}
	if got := strings.Join(winners(out.String()), ", "); got != "machine B, machine A" {
		t.Fatalf("winners = %s\n%s", got, out.String())
	}
}

func TestDemo_WrongTokenIsReported(t *testing.T) {
	cfg := testConfig()
	cfg.serverURL = startBattleServer(t, "secret")
	cfg.authToken = "wrong"
	cfg.connectTimeout = time.Second
	err := run(cfg, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "--auth-token") {
		t.Fatalf("run with a wrong token returned %v, want an error pointing at --auth-token", err)
	}
}

// A machine already waiting on the server would be paired with a demo machine. The demo must stop
// rather than run a battle against it.
func TestDemo_StopsWhenPairedWithAnotherMachine(t *testing.T) {
	cfg := testConfig()
	cfg.serverURL = startBattleServer(t, "")

	other, _, err := websocket.DefaultDialer.Dial(cfg.serverURL, nil)
	if err != nil {
		t.Fatalf("connect the other machine: %v", err)
	}
	defer other.Close()
	received := make(chan string, 1024)
	go func() {
		defer close(received)
		for {
			_, msg, err := other.ReadMessage()
			if err != nil {
				return
			}
			select {
			case received <- string(msg):
			default:
			}
		}
	}()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
				_ = other.WriteMessage(websocket.TextMessage, []byte(`{"data":{"player_id":"someone-else","hand":"right","multiplayer_state":"MULTIPLAYER_SM_READY","left_limit":-45,"right_limit":45}}`))
			}
		}
	}()

	err = run(cfg, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "paired with another machine") {
		t.Fatalf("run returned %v, want an error saying a demo machine was paired with another machine", err)
	}
	for {
		select {
		case msg, ok := <-received:
			if !ok {
				return
			}
			if strings.Contains(msg, "MULTIPLAYER_SM_READY") || strings.Contains(msg, "MULTIPLAYER_SM_RUN") {
				t.Fatalf("the demo machine got ready against another machine: %s", msg)
			}
		default:
			return
		}
	}
}
