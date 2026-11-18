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
  - All 14 Section 3.2 fields plus the current firmware's `machine_torque` with exact defaults, `NaN`/`Inf`/`-0.0` float sanitization, extra-field pass-through, and server-field (`is_connected`, `client_id`, `last_message_timestamp`) stripping.
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

`--rival-timeout` (default `1s`, `0` disables) clears the rival on the machine when our server sends no rival data for that long, so a stalled internet link never leaves the handle reflecting a frozen rival. A stall longer than the timeout also returns the local match to `MULTIPLAYER_IDLE`.

## Message Format

The local server relays the machine's frames to the bridge like this (current firmware sends `machine_torque` instead of `torque` / `opponent_torque`):

```json
{"data":{"MAC_address":"11:22:33:44:55:66","player_id":"player-2","hand":"left","multiplayer_state":"MULTIPLAYER_SM_RUN","single_player_state":"","machine_torque":-12.5,"position":-3.1,"velocity":0.4,"left_limit":-45.0,"right_limit":45.0,"peak_force":28.0,"timeStamp":123456790,"is_connected":true,"client_id":2,"last_message_timestamp":86400123}}
```

**Machine → our server.** The bridge sends the same fields and values in a single `"data"` envelope, plus `server_player_id` when the machine has reported one:

```json
{"data":{"MAC_address":"11:22:33:44:55:66","player_id":"player-2","hand":"left","multiplayer_state":"MULTIPLAYER_SM_RUN","single_player_state":"","machine_torque":-12.5,"position":-3.1,"velocity":0.4,"left_limit":-45,"right_limit":45,"peak_force":28,"timeStamp":123456790}}
```

- `is_connected`, `client_id` and `last_message_timestamp` are dropped: they describe the local server's connection, and each server adds its own.
- `torque` / `opponent_torque` are only sent if the machine sent them. The `"opponent_torque": -0.0` the local server writes into relayed frames when the hands differ (`PROTOCOL.md` section 4.3) is dropped.
- Frames in the older format (`torque` + `opponent_torque`) are forwarded exactly as before.
- When the physical machine disconnects, the bridge sends `{"data":{}}`.

**Our server → machine.** Our server sends the rival's frames in the same format, with or without the three server fields. The bridge hands the rival's fields unchanged to the local server, which relays them to the machine every 10 ms, so the handle reflects the rival's `machine_torque`. Neither the bridge nor the local server flips the sign of `machine_torque`; the machine receives the rival's `hand` with it.

The bridge clears the rival from the machine (the local server then sends it an empty `data` object, `PROTOCOL.md` section 4.4, and resets the match to `MULTIPLAYER_IDLE`) when:

- our server sends `{"data":{}}` or a frame with `"is_connected": false`
- our server sends `{"cmd":"STOP"}`
- our server sends no rival data for `--rival-timeout`, or the connection to it drops

## Test & Benchmark

```bash
go test -race -v ./...
go test -bench=. -benchmem ./pkg/protocol
```
