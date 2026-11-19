# Arm-Wrestling Machine Bridge and Battle Server (Go)

Programs that let arm-wrestling machines in different places battle each other:

- **`battle-server`** is our central server. It pairs machines, relays each machine's data to its rival, and decides the battle when a handle reaches its position limit.
- **`machine-bridge`** is only needed for a machine that must stay connected to its own local `PROTOCOL.md` server. It connects that local server (`ws://<machine-ip>:8080/ws?role=machine`) to the battle server.

A machine reaches the battle server in one of two ways.

## Architecture

### Machines connect directly (nothing to install on the machines)

```text
[Physical Machine A] ──────► [battle-server] ◄────── [Physical Machine B]
              ws://<our-server>:8080/ws?role=machine
```

A machine is a WebSocket client that connects to `ws://<server-ip>:8080/ws?role=machine` (`PROTOCOL.md` section 2). Set that server address on each machine to our battle server, and the battle server is the machine's `PROTOCOL.md` server: it sends the rival's telemetry every 10 ms and the `START`, `YOU_WIN` and `YOU_LOSE` commands. This needs the machine's server address to be configurable and the battle server to be reachable from the machine.

### Machines stay on their local servers, with a bridge each

```text
[Physical Machine A]                                  [Physical Machine B]
        ▲ │  PROTOCOL.md                                      ▲ │
        │ ▼                                                   │ ▼
[Local Machine Server A (:8080)]                      [Local Machine Server B (:8080)]
        ▲ │  ws://<machine-ip>:8080/ws?role=machine           ▲ │
        │ ▼                                                   │ ▼
[machine-bridge A] ──────► [battle-server] ◄────── [machine-bridge B]
                    ws(s)://<our-server>/ws/machine
```

Each local machine server pairs its physical machine with the bridge, which stands in for the rival, and sends its machine `START`, `YOU_WIN` and `YOU_LOSE` exactly as `PROTOCOL.md` describes. The battle server decides who fights whom, carries the telemetry between them, follows the same match states, and decides the result (see [Battle rules](#battle-rules)).

### Packages

- **`pkg/netutil`** — Resolves the active IPv4 address of the host machine (`ResolveMachineIP`) via OS routing table lookup with active physical NIC fallback (handling isolated LANs without internet gateways) and constructs `ws://<machine-ip>:8080/ws?role=machine`.
- **`pkg/protocol`** — Zero-reflection outbound wire serializer (`AppendMachineEnvelope`: ~550 ns/op, 0 allocs/op) and strict `PROTOCOL.md` parser (`ParseServerMessage`, `ParseRivalEnvelope`):
  - Single-key `"data"` envelope output; alphabetically-first top-level key selection on input.
  - All Section 3.2 fields with exact defaults, `NaN`/`Inf`/`-0.0` float sanitization, extra-field pass-through, and server-field (`is_connected`, `client_id`, `last_message_timestamp`) stripping.
  - Caches `server_player_id` across the 49 frames between every 50th message (`keeps last value`).
  - Parses both Section 6.2 match command objects (`START`, `YOU_WIN`, `YOU_LOSE`) and Section 6.3 operator command strings (`STOP`, custom text), and detects `{"data":{}}` and Section 4.4 `{"data":{"machine_torque":-0.0}}` disconnect frames.
- **`pkg/machineclient`** — Real-time local WebSocket client (`TCP_NODELAY`, pooled read/write buffers, panic-safe callbacks, write deadlines, exponential-backoff reconnect, latest-message-wins coalescing, non-droppable terminal `GAMEOVER_WIN`/`GAMEOVER_LOSE` delivery, and `ClearRival` slot cycling to clear stale rival torque and reset stuck `MULTIPLAYER_WAITING` / `MULTIPLAYER_MID_MATCH` to `MULTIPLAYER_IDLE`).
- **`pkg/bridge`** — Bidirectional `MachineBridge` + `WebSocketServerClient` connecting the local machine to our central server with latest-message-wins WAN forwarding, RFC 6455 ping/pong dead-link detection, and automatic local `ClearRival` safety reset if the rival leaves or the upstream connection drops mid-match.
- **`cmd/machine-bridge`** — Standalone CLI/service binary (~6.8 MB static executable).
- **`pkg/battleserver`** — Our central server: pairs connected machines (directly connected, or behind bridges), relays telemetry between the two machines of a battle, and runs the battle rules.
- **`cmd/battle-server`** — Standalone battle server binary.
- **`pkg/machinesim`** — A simulated local `PROTOCOL.md` server and a simulated physical machine, used by the tests and the demo.
- **`cmd/battle-demo`** — Runs battles between two simulated machines (see [Try a battle without machines](#try-a-battle-without-machines)).

## Build & Cross-Compile

```bash
# Native build
CGO_ENABLED=0 go build -o bin/machine-bridge ./cmd/machine-bridge
CGO_ENABLED=0 go build -o bin/battle-server ./cmd/battle-server

# Linux ARM64 (e.g. Raspberry Pi / embedded cabinet)
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bin/machine-bridge-linux-arm64 ./cmd/machine-bridge

# Linux x86_64
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/machine-bridge-linux-amd64 ./cmd/machine-bridge

# Windows x86_64
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o bin/machine-bridge-windows-amd64.exe ./cmd/machine-bridge
```

## Try a battle without machines

```bash
go run ./cmd/battle-demo
```

This runs two battles between two simulated machines and prints what happens: when each machine is told to start, a frame each machine sent next to the frame the other received, the battle server's decision, and which machine received `YOU_WIN` and `YOU_LOSE`. It exits with an error if anything is not as the [battle rules](#battle-rules) say.

Each simulated machine sends a frame every 10 ms to its own simulated `PROTOCOL.md` server. The bridges and the battle server are the real ones. The simulated server and machine are built from `PROTOCOL.md`, so the demo shows that our programs follow the document, not that they work with the real machines.

Add `--direct` to connect the simulated machines to the battle server themselves, with no local servers or bridges:

```bash
go run ./cmd/battle-demo --direct
```

To run the same two simulated machines through a battle server that is already running:

```bash
# machines connect directly
go run ./cmd/battle-demo --direct --server-url "ws://our-server.example.com:8080/ws?role=machine"

# through bridges
go run ./cmd/battle-demo --server-url ws://our-server.example.com:9000/ws/machine --auth-token "$OUR_SERVER_TOKEN"
```

Only do this while no real machine is connected to that server. The server pairs machines in the order they connect, so a real machine waiting there would be paired with a simulated one; the demo stops when it sees that, before either simulated machine gets ready.

## Run a Battle Between Two Machines

### Machines connect directly

1. Start the battle server on a host both machines can reach:

```bash
./bin/battle-server --machine-listen :8080
```

2. On each machine, set the server address to that host, so the machine connects to `ws://<our-server>:8080/ws?role=machine`. Nothing is installed on the machine.

3. The server logs each machine as it connects (`machine "machine-1" connected directly from <address>`, then its `MAC_address`, player and hand from its first frame) and `paired "machine-1" with "machine-2"`. When both machines report `MULTIPLAYER_SM_READY` the server sends both `START`; when a handle reaches its position limit it sends `YOU_WIN` to one and `YOU_LOSE` to the other.

What a directly connected machine receives, compared with `PROTOCOL.md`:

- **Telemetry (section 6.1):** the rival's telemetry every 10 ms with `is_connected`, `client_id` and `last_message_timestamp` added, and `{"data":{}}` while there is no rival.
- **Torque sign:** the [left-hand rule](#battle-rules) below, instead of the section 4.3 rule. The no-rival message is always `{"data":{}}`, never `{"data":{"machine_torque":-0.0}}` (section 4.4).
- **Commands (section 6.2):** `{"cmd":{"START":1}}`, `{"cmd":{"YOU_WIN":0}}` and `{"cmd":{"YOU_LOSE":1}}`. When the server decides the battle at a position limit, both machines are told. When a machine reports `MULTIPLAYER_SM_GAMEOVER_WIN` or `MULTIPLAYER_SM_GAMEOVER_LOSE` itself, only its rival is told, as in the document.
- **Silent rival:** a rival that has sent nothing for 1 second (`--rival-timeout`, `0` disables) is shown as no rival and the battle is abandoned, so a handle does not keep reflecting a frozen rival. Section 4.4 has no such timeout.
- **Third machine:** it waits and is paired when a machine becomes free, instead of being ignored.
- **Operator commands (section 6.3):** there is no debug page, so none are sent.

`PROTOCOL.md` has no authentication, so anyone who can reach the `--machine-listen` port can connect as a machine. Restrict who can reach it with a firewall. A directly connected machine is only paired with another directly connected machine, never with one behind a bridge.

### Machines stay on their local servers, with a bridge each

1. Start the battle server on a host both machines can reach:

```bash
./bin/battle-server --listen :9000 --auth-token "$OUR_SERVER_TOKEN"
```

2. Start a bridge on each machine, each with its own `--machine-id`:

```bash
./bin/machine-bridge \
  --server-url ws://our-server.example.com:9000/ws/machine \
  --machine-id cabinet-01 \
  --auth-token "$OUR_SERVER_TOKEN"
```

3. The server logs `paired "cabinet-01" with "cabinet-02"` once both bridges are connected. From then on each machine sees the other as its rival: when both players report `MULTIPLAYER_SM_READY`, each local server sends `START`, and the handles reflect each other until a handle reaches its position limit or a machine reports a result.

### Battle rules

The battle server follows the match states of `PROTOCOL.md` section 5 for each pair, from the `multiplayer_state` each machine reports:

| Server state | Leaves when |
|---|---|
| `MULTIPLAYER_IDLE` | Either machine reports `MULTIPLAYER_SM_READY`. |
| `MULTIPLAYER_WAITING` | Both machines report `MULTIPLAYER_SM_READY`: the battle starts. |
| `MULTIPLAYER_MID_MATCH` | A handle reaches a position limit, or a machine reports `MULTIPLAYER_SM_GAMEOVER_WIN` / `MULTIPLAYER_SM_GAMEOVER_LOSE`. |
| `MULTIPLAYER_POST_MATCH` | 2 seconds have passed. |

- **Victory at a position limit.** During a battle, while a machine reports `MULTIPLAYER_SM_RUN`, its `position` reaching its own `left_limit` (minimum) or `right_limit` (maximum) decides the battle. A player wins at the limit on the side of the hand in use (right hand: `right_limit`; left hand: `left_limit`) and loses at the other limit. A machine that reports no `hand` counts as right-handed. Start the server with `--win-at-opposite-limit` to reverse this. A machine that reports no usable limits (`left_limit` not below `right_limit`) is never decided this way.
- **How the machines learn the result.** A directly connected machine is sent `YOU_WIN` or `YOU_LOSE` by the battle server. For machines behind bridges, the server relays the winner's frames with `multiplayer_state` `MULTIPLAYER_SM_GAMEOVER_WIN` and the loser's with `MULTIPLAYER_SM_GAMEOVER_LOSE`, for the 2 seconds of `MULTIPLAYER_POST_MATCH`; each local server then sends its machine `YOU_WIN` or `YOU_LOSE` (`PROTOCOL.md` section 6.2). The server logs `battle decided: ...`.
- **Left hand.** The `machine_torque` of a machine whose `hand` is `"left"` is multiplied by −1 in the frames its rival receives. A right-handed machine's `machine_torque` is relayed unchanged, and so are `position`, `velocity` and the limits of both.
- **Start (machines behind bridges).** A machine stops reporting `MULTIPLAYER_SM_READY` as soon as its local server starts it, but the rival's local server starts the rival only while it sees both machines ready. So the bridge reports its machine ready when the local server sends `START`, even if no relayed frame showed it, and the server keeps showing a started machine as ready to a rival that has not started yet.
- **Abandoned battle.** A battle ends without a result when a machine or bridge disconnects, when a directly connected machine sends nothing for `--rival-timeout`, or when a machine reports `MULTIPLAYER_SM_READY` again.

### Battle server (stage 1)

- **Pairing:** machines without a rival are paired in the order they became free, directly connected machines with each other and machines behind bridges with each other. With two machines connected, they battle each other. A third machine waits until one is free. A pair stays together, including for rematches, until one of them disconnects.
- **Rival leaves:** the remaining machine is sent `{"data":{}}`, which clears its rival and abandons its battle, before it is paired again.
- **Relay:** only machine telemetry is relayed. Telemetry is latest-wins, so a slow connection never builds up a backlog of old handle positions. A win/lose result is always delivered.
- **Reconnects:** a bridge that reconnects with the same `--machine-id` replaces its previous connection. A connection that sends neither data nor pings for 30 s is dropped. A directly connected machine has no ID to reconnect under: if its connection dies without being closed, the machine's new connection waits until the dead one is dropped.
- **Options:** `--machine-listen` (no default, env `BATTLE_SERVER_MACHINE_LISTEN`) for machines that connect directly, `--rival-timeout` (default `1s`) for those machines; `--listen` (default `:9000`, env `BATTLE_SERVER_LISTEN`, empty to turn off), `--path` (default `/ws/machine`) and `--auth-token` (env `OUR_SERVER_TOKEN`) for bridges; `--win-at-opposite-limit`. `GET /healthz` answers `ok` on each port.

Not in stage 1:

- **Matching:** there is no way to choose which machines battle. Pairing is first come, first served.
- **Encryption:** the server speaks plain `ws://`. For use over the internet, put it behind a TLS-terminating proxy so bridges can connect with `wss://`; otherwise the token and telemetry travel unencrypted. Whether a machine can connect with `wss://` depends on its firmware.
- **Per-machine credentials:** all bridges share one token, and any bridge holding it can connect under any machine ID. Directly connected machines present no credentials at all.

## Run the Bridge on the Machine

```bash
./bin/machine-bridge \
  --server-url wss://our-server.example.com/ws/machine \
  --machine-id cabinet-01 \
  --auth-token "$OUR_SERVER_TOKEN"
```

Environment variables (`OUR_SERVER_WS_URL`, `OUR_SERVER_TOKEN`, `MACHINE_ID`, `MACHINE_IP`, `MACHINE_NIC`, `MACHINE_PORT`) are also supported. Running without `--server-url` starts local interactive diagnostic mode.

`--rival-timeout` (default `1s`, `0` disables) clears the rival on the machine when our server sends no rival data for that long, so a stalled internet link never leaves the handle reflecting a frozen rival. A stall longer than the timeout also returns the local match to `MULTIPLAYER_IDLE`.

## Message Format

`machine_torque` is the only torque value, everywhere: from the machine, between the bridge and our server, and back to the machine. The local server relays the machine's frames to the bridge like this:

```json
{"data":{"MAC_address":"11:22:33:44:55:66","player_id":"player-2","hand":"left","multiplayer_state":"MULTIPLAYER_SM_RUN","single_player_state":"","machine_torque":-12.5,"position":-3.1,"velocity":0.4,"left_limit":-45.0,"right_limit":45.0,"peak_force":28.0,"timeStamp":123456790,"is_connected":true,"client_id":2,"last_message_timestamp":86400123}}
```

**Machine → our server.** The bridge sends the machine's fields and values in a single `"data"` envelope, plus `server_player_id` when the machine has reported one:

```json
{"data":{"MAC_address":"11:22:33:44:55:66","player_id":"player-2","hand":"left","multiplayer_state":"MULTIPLAYER_SM_RUN","single_player_state":"","machine_torque":-12.5,"position":-3.1,"velocity":0.4,"left_limit":-45,"right_limit":45,"peak_force":28,"timeStamp":123456790}}
```

- `is_connected`, `client_id` and `last_message_timestamp` are dropped: they describe the local server's connection, and each server adds its own.
- `machine_torque` is sent as the local server relayed it, which is with its sign reversed when one player uses the left hand and the other the right (`PROTOCOL.md` section 4.3).
- When the physical machine disconnects, the bridge sends `{"data":{}}`.
- When the local server sends `START`, `YOU_LOSE` or `YOU_WIN`, the machine must have reported `MULTIPLAYER_SM_READY`, `MULTIPLAYER_SM_GAMEOVER_WIN` or `MULTIPLAYER_SM_GAMEOVER_LOSE`. If the bridge has not already sent that state, it sends the machine's last frame with it.

**Our server → machine.** Our server sends the rival's frames in the same format, with the changes described in [Battle rules](#battle-rules): a left-handed rival's `machine_torque` is negated, and `multiplayer_state` carries the result when the server decides the battle. The bridge hands the rival's fields unchanged to the local server, which relays them to the machine every 10 ms, so the handle reflects the rival's `machine_torque`.

A frame passes through two local servers, the sender's and the receiver's. Both see the same two hands, so both apply the `PROTOCOL.md` section 4.3 sign rule or neither does, and the two cancel. What the machine receives is therefore the rival machine's `machine_torque` with only our server's left-hand rule applied.

The bridge clears the rival from the machine (the local server then sends it an empty `data` object, `PROTOCOL.md` section 4.4, and resets the match to `MULTIPLAYER_IDLE`) when:

- our server sends `{"data":{}}` or a frame with `"is_connected": false`
- our server sends `{"cmd":"STOP"}`
- our server sends no rival data for `--rival-timeout`, or the connection to it drops

## Test & Benchmark

```bash
go test -race -v ./...
go test -bench=. -benchmem ./pkg/protocol
```
