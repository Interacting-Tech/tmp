package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"dev.azure.com/InteractingTechnologies/Glenfiddich/_git/Arm-Wrestling/pkg/battleserver"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	var (
		machineListen = flag.String("machine-listen", os.Getenv("BATTLE_SERVER_MACHINE_LISTEN"), "Address to listen on for machines that connect directly, the way PROTOCOL.md describes, at ws://<this server>:<port>"+machinePath+"?role=machine (e.g. :8080; empty: machines cannot connect directly)")
		listen        = flag.String("listen", envOr("BATTLE_SERVER_LISTEN", ":9000"), "Address to listen on for machine bridges (empty: bridges cannot connect)")
		path          = flag.String("path", "/ws/machine", "URL path of the WebSocket endpoint that machine bridges connect to")
		authToken     = flag.String("auth-token", os.Getenv("OUR_SERVER_TOKEN"), "Bearer token every machine bridge must present (empty: anyone may connect). Machines that connect directly have no way to present one")
		oppositeLimit = flag.Bool("win-at-opposite-limit", false, "A player wins when the handle reaches the position limit opposite the hand in use (default: right hand wins at right_limit, left hand at left_limit)")
		rivalTimeout  = flag.Duration("rival-timeout", time.Second, "Show a directly connected machine no rival when its rival has sent nothing for this long (0 disables)")
	)
	flag.Parse()

	if *listen == "" && *machineListen == "" {
		log.Fatalf("[battle-server] nothing to listen on: set --machine-listen, --listen or both")
	}
	if *listen == *machineListen && *path == machinePath {
		log.Fatalf("[battle-server] --path %s on %s is the endpoint for machines that connect directly; choose another --path or --listen", *path, *listen)
	}

	opts := battleserver.Options{
		AuthToken:          strings.TrimSpace(*authToken),
		WinAtOppositeLimit: *oppositeLimit,
		RivalTimeout:       *rivalTimeout,
		Logf: func(format string, args ...any) {
			log.Printf("[battle-server] "+format, args...)
		},
	}
	if opts.RivalTimeout <= 0 {
		opts.RivalTimeout = -1
	}
	battles := battleserver.New(opts)

	// One HTTP server per address; both endpoints may share an address.
	muxes := make(map[string]*http.ServeMux)
	var addresses []string
	muxFor := func(address string) *http.ServeMux {
		if muxes[address] == nil {
			mux := http.NewServeMux()
			mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte("ok\n"))
			})
			muxes[address] = mux
			addresses = append(addresses, address)
		}
		return muxes[address]
	}
	if *machineListen != "" {
		muxFor(*machineListen).Handle(machinePath, battles.MachineHandler())
	}
	if *listen != "" {
		muxFor(*listen).Handle(*path, battles)
	}

	serveErr := make(chan error, len(addresses))
	var servers []*http.Server
	for _, address := range addresses {
		ln, err := net.Listen("tcp", address)
		if err != nil {
			log.Fatalf("[battle-server] cannot listen on %s: %v", address, err)
		}
		if address == *machineListen {
			log.Printf("[battle-server] machines connect directly to ws://%s%s?role=machine", ln.Addr(), machinePath)
			log.Printf("[battle-server] WARNING: PROTOCOL.md has no authentication, so anyone who can reach %s can join a battle as a machine; restrict who can reach it with a firewall", ln.Addr())
		}
		if address == *listen {
			log.Printf("[battle-server] machine bridges connect to ws://%s%s", ln.Addr(), *path)
			if opts.AuthToken == "" {
				log.Printf("[battle-server] WARNING: no --auth-token set; any machine bridge that can reach %s can join a battle", ln.Addr())
			}
		}
		httpSrv := &http.Server{Handler: muxes[address], ReadHeaderTimeout: 5 * time.Second}
		servers = append(servers, httpSrv)
		go func() { serveErr <- httpSrv.Serve(ln) }()
	}

	runCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[battle-server] server failed: %v", err)
		}
	case <-runCtx.Done():
	}

	log.Printf("[battle-server] shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, httpSrv := range servers {
		_ = httpSrv.Shutdown(shutdownCtx)
	}
	battles.Close()
}

// machinePath is where PROTOCOL.md section 2 has machines connect.
const machinePath = "/ws"

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
