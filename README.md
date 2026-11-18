# Arm-Wrestling Machine Bridge (Go)

Fast, low-jitter, zero-CGO Go daemon (`machine-bridge`) that runs on each physical arm-wrestling machine, connects to the machine's local WebSocket server (`ws://<machine-ip>:8080/ws?role=machine`) per `PROTOCOL.md`, streams the machine's data to our central server, and transfers the rival's data from our server back into the machine.

## Architecture

```text
[Physical Machine]
        ▲ │  ws://<machine-ip>:8080/ws?role=machine  (PROTOCOL.md)
        │ ▼
[Local Machine Server (:8080)]
        ▲ │  ws://<machine-ip>:8080/ws?role=machine  (TCP_NODELAY, 10 ms telemetry + cmd)
        │ ▼
[machine-bridge (Go)]
        ▲ │  ws(s)://<our-server>/...                (TCP_NODELAY, WS ping/pong watchdog)
        │ ▼
[Our Central Server]
```

### Packages

- **`pkg/netutil`** — Resolves the active IPv4 address of the host machine (`ResolveMachineIP`) via OS routing table lookup with active physical NIC fallback (handling isolated LANs without internet gateways) and constructs `ws://<machine-ip>:8080/ws?role=machine`.
- **`pkg/protocol`** — Zero-reflection outbound wire serializer (`AppendMachineEnvelope`: ~550 ns/op, 0 allocs/op) and strict `PROTOCOL.md` parser (`ParseServerMessage`, `ParseRivalEnvelope`):
  - Single-key `"data"` envelope output; alphabetically-first top-level key selection on input.
  - All 14 Section 3.2 fields with exact defaults, `NaN`/`Inf`/`-0.0` float sanitization, extra-field pass-through, and server-field (`is_connected`, `client_id`, `last_message_timestamp`) stripping.
  - Caches `server_player_id` across the 49 frames between every 50th message (`keeps last value`).
  - Undoes the local server's Section 4.3 `opponent_torque` left/right sign flip (`UndoLocalServerFlip`) on outbound telemetry so two bridged machines never double-negate torque.
  - Parses both Section 6.2 match command objects (`START`, `YOU_WIN`, `YOU_LOSE`) and Section 6.3 operator command strings (`STOP`, custom text), and detects `{"data":{}}` and Section 4.4 `{"data":{"opponent_torque":-0.0}}` disconnect frames.
- **`pkg/machineclient`** — Real-time local WebSocket client (`TCP_NODELAY`, pooled read/write buffers, panic-safe callbacks, write deadlines, exponential-backoff reconnect, latest-message-wins coalescing, non-droppable terminal `GAMEOVER_WIN`/`GAMEOVER_LOSE` delivery, and `ClearRival` slot cycling to clear stale rival torque and reset stuck `MULTIPLAYER_WAITING` / `MULTIPLAYER_MID_MATCH` to `MULTIPLAYER_IDLE`).
- **`pkg/bridge`** — Bidirectional `MachineBridge` + `WebSocketServerClient` connecting the local machine to our central server with latest-message-wins WAN forwarding, RFC 6455 ping/pong dead-link detection, and automatic local `ClearRival` safety reset if the rival leaves or the upstream connection drops mid-match.
- **`cmd/machine-bridge`** — Standalone CLI/service binary (~6.8 MB static executable).

## Build & Cross-Compile

```bash
# Native build
CGO_ENABLED=0 go build -o bin/machine-bridge ./cmd/machine-bridge

# Linux ARM64 (e.g. Raspberry Pi / embedded cabinet)
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bin/machine-bridge-linux-arm64 ./cmd/machine-bridge

# Linux x86_64
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/machine-bridge-linux-amd64 ./cmd/machine-bridge

# Windows x86_64
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o bin/machine-bridge-windows-amd64.exe ./cmd/machine-bridge
```

## Run on the Machine

```bash
./bin/machine-bridge \
  --server-url wss://our-server.example.com/ws/machine \
  --machine-id cabinet-01 \
  --auth-token "$OUR_SERVER_TOKEN"
```

Environment variables (`OUR_SERVER_WS_URL`, `OUR_SERVER_TOKEN`, `MACHINE_ID`, `MACHINE_IP`, `MACHINE_NIC`, `MACHINE_PORT`) are also supported. Running without `--server-url` starts local interactive diagnostic mode.

## Test & Benchmark

```bash
go test -race -v ./...
go test -bench=. -benchmem ./pkg/protocol
```
