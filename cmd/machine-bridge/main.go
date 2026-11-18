package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Interacting-Tech/tmp/pkg/bridge"
	"github.com/Interacting-Tech/tmp/pkg/machineclient"
	"github.com/Interacting-Tech/tmp/pkg/protocol"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	var (
		serverURL     = flag.String("server-url", os.Getenv("OUR_SERVER_WS_URL"), "Upstream WebSocket URL of our server (ws://... or wss://...)")
		authToken     = flag.String("auth-token", os.Getenv("OUR_SERVER_TOKEN"), "Optional Bearer token sent to our server")
		machineID     = flag.String("machine-id", os.Getenv("MACHINE_ID"), "Optional X-Machine-ID header sent to our server")
		machineIP     = flag.String("machine-ip", os.Getenv("MACHINE_IP"), "Optional override for local machine IP (auto-detected when empty)")
		machineNIC    = flag.String("interface", os.Getenv("MACHINE_NIC"), "Optional preferred network interface name (e.g. eth0, wlan0)")
		machinePort   = flag.Int("machine-port", envInt("MACHINE_PORT", 8080), "Local machine WebSocket port (PROTOCOL.md default: 8080)")
		allowLoopback = flag.Bool("allow-loopback", true, "Fall back to 127.0.0.1 if no active LAN IPv4 interface is up")
		onlyNew       = flag.Bool("only-new-samples", false, "Only forward local frames to our server when last_message_timestamp changes")
		verbose       = flag.Bool("verbose", os.Getenv("VERBOSE") == "1", "Log every telemetry frame in standalone console mode")
	)
	flag.Parse()

	mcOpts := machineclient.Options{
		Port:                  *machinePort,
		MachineIPOverride:     *machineIP,
		PreferredInterface:    *machineNIC,
		AllowLoopbackFallback: *allowLoopback,
		OnServerConnectionChange: func(connected bool) {
			log.Printf("[local-ws] connected=%v", connected)
		},
		OnLocalMachineConnectionChange: func(connected bool) {
			log.Printf("[local-machine] active=%v", connected)
		},
		OnCommand: func(cmd protocol.ServerCommand) {
			log.Printf("[local-cmd] kind=%d text=%q operator=%v", cmd.Kind, cmd.CommandText, cmd.IsOperatorCommand)
		},
		OnDiagnostic: func(msg string, err error) {
			if err != nil {
				log.Printf("[local-diag] %s: %v", msg, err)
			} else {
				log.Printf("[local-diag] %s", msg)
			}
		},
	}

	var upstream bridge.ServerTransport
	if trimmedURL := strings.TrimSpace(*serverURL); trimmedURL != "" {
		headers := make(http.Header)
		if tok := strings.TrimSpace(*authToken); tok != "" {
			headers.Set("Authorization", "Bearer "+tok)
		}
		if id := strings.TrimSpace(*machineID); id != "" {
			headers.Set("X-Machine-ID", id)
		}

		wsUp, err := bridge.NewWebSocketServerClient(bridge.UpstreamOptions{
			ServerURL: trimmedURL,
			Headers:   headers,
			OnConnectionChange: func(connected bool) {
				log.Printf("[upstream-server] connected=%v url=%s", connected, trimmedURL)
			},
			OnDiagnostic: func(msg string, err error) {
				if err != nil {
					log.Printf("[upstream-diag] %s: %v", msg, err)
				} else {
					log.Printf("[upstream-diag] %s", msg)
				}
			},
		})
		if err != nil {
			log.Fatalf("invalid upstream configuration: %v", err)
		}
		upstream = wsUp
	} else {
		log.Printf("[mode] --server-url not provided; running in interactive local diagnostic mode (stdin JSON -> rival)")
		upstream = newConsoleTransport(*verbose)
	}

	mb, err := bridge.New(bridge.Config{
		MachineOptions:        mcOpts,
		OnlyForwardNewSamples: *onlyNew,
	}, upstream)
	if err != nil {
		log.Fatalf("failed to initialize machine bridge: %v", err)
	}

	log.Printf("[machine-bridge] resolved local machine IP : %s", mb.LocalClient().MachineIP())
	log.Printf("[machine-bridge] local machine endpoint    : %s", mb.LocalClient().ServerURL())

	runCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	if err := mb.Start(runCtx); err != nil {
		log.Fatalf("failed to start machine bridge: %v", err)
	}

	<-runCtx.Done()
	log.Printf("[machine-bridge] shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := mb.Stop(shutdownCtx); err != nil {
		log.Printf("[machine-bridge] shutdown warning: %v", err)
	}
}

func envInt(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// consoleTransport allows engineers on a cabinet without --server-url to inspect outbound machine
// JSON and type/paste rival JSON lines or "clear" on stdin.
type consoleTransport struct {
	verbose   bool
	cancel    context.CancelFunc
	lastState string
}

func newConsoleTransport(verbose bool) *consoleTransport {
	return &consoleTransport{verbose: verbose}
}

func (c *consoleTransport) IsConnected() bool { return true }

func (c *consoleTransport) Start(ctx context.Context, handlers bridge.UpstreamHandlers) error {
	subCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			select {
			case <-subCtx.Done():
				return
			default:
			}
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			if strings.EqualFold(line, "clear") {
				if handlers.OnRivalDisconnected != nil {
					handlers.OnRivalDisconnected()
				}
				continue
			}
			if t, ok := protocol.ParseRivalEnvelope([]byte(line)); ok && t != nil {
				if handlers.OnRivalTelemetry != nil {
					handlers.OnRivalTelemetry(t)
				}
			} else {
				log.Printf("[stdin] ignored line: expected single-key PROTOCOL.md JSON or 'clear'")
			}
		}
	}()
	return nil
}

func (c *consoleTransport) Stop(_ context.Context) error {
	if c.cancel != nil {
		c.cancel()
	}
	return nil
}

func (c *consoleTransport) SendMachineData(payload []byte, isTerminal bool) {
	if c.verbose || isTerminal {
		fmt.Printf("[machine -> server] %s\n", payload)
		return
	}
	if t, ok := protocol.ParseRivalEnvelope(payload); ok && t != nil && t.MultiplayerState != c.lastState {
		c.lastState = t.MultiplayerState
		fmt.Printf("[machine -> server] %s\n", payload)
	}
}

func (c *consoleTransport) SendMachineDisconnected() {
	c.lastState = ""
	fmt.Println(`[machine -> server] {"data":{}}`)
}
