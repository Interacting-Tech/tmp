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

	"github.com/Interacting-Tech/tmp/pkg/battleserver"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	var (
		listen    = flag.String("listen", envOr("BATTLE_SERVER_LISTEN", ":9000"), "Address to listen on for machine bridges")
		path      = flag.String("path", "/ws/machine", "URL path of the WebSocket endpoint that machine bridges connect to")
		authToken = flag.String("auth-token", os.Getenv("OUR_SERVER_TOKEN"), "Bearer token every machine bridge must present (empty: anyone may connect)")
	)
	flag.Parse()

	token := strings.TrimSpace(*authToken)
	if token == "" {
		log.Printf("[battle-server] WARNING: no --auth-token set; any client that can reach this server can join a battle")
	}

	battles := battleserver.New(battleserver.Options{
		AuthToken: token,
		Logf: func(format string, args ...any) {
			log.Printf("[battle-server] "+format, args...)
		},
	})

	mux := http.NewServeMux()
	mux.Handle(*path, battles)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("[battle-server] cannot listen on %s: %v", *listen, err)
	}
	httpSrv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("[battle-server] machine bridges connect to ws://%s%s", ln.Addr(), *path)

	runCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	serveErr := make(chan error, 1)
	go func() { serveErr <- httpSrv.Serve(ln) }()

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
	_ = httpSrv.Shutdown(shutdownCtx)
	battles.Close()
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
