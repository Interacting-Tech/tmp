# Arm-Wrestling Machine Client

.NET 8 implementation of the machine-side client component specified by `PROTOCOL.md`.

## Overview

Each arm-wrestling machine runs its local WebSocket server at `ws://<machine-ip>:8080/ws?role=machine`. This project provides:

- **`src/ArmWrestling.MachineClient`** — Zero-dependency .NET 8 library that:
  - Reads the IPv4 address of the host machine it runs on (`LocalIpResolver`) and connects to `ws://<machine-ip>:8080/ws?role=machine`.
  - Accepts the local machine's 10 ms telemetry frames (`{"data": { ... }}`), handles empty/disconnected frames (`{"data":{}}` and `{"data":{"opponent_torque":-0.0}}`), caches `server_player_id` across every 50th message, normalizes the server's `opponent_torque` left/right sign flip so bridged servers never double-negate torque, and parses both match command objects (`START`, `YOU_WIN`, `YOU_LOSE`) and operator command strings (`STOP`, custom text).
  - Provides the rival's telemetry to the local machine in the exact single-key `{"data": { ... }}` JSON format and rules defined in `PROTOCOL.md` (latest-message-wins, field defaults, extra-field preservation, 50th-message `server_player_id` cadence, terminal game-over delivery guarantee, and slot reset via `ClearRivalAsync` to clear stale rival data and reset `MULTIPLAYER_WAITING` / `MULTIPLAYER_MID_MATCH` to `MULTIPLAYER_IDLE`).
- **`src/ArmWrestling.MachineAgent`** — Runnable host (`MachineRelayCoordinator` + `IRivalBridge` / `WebSocketRivalBridge` / interactive console bridge).
- **`tests/ArmWrestling.MachineClient.Tests`** — xUnit unit and end-to-end WebSocket integration tests backed by `SimulatedArmWrestlingServer`.

## Build & Test

```bash
dotnet build
dotnet test
```

## Run the Machine Agent

```bash
dotnet run --project src/ArmWrestling.MachineAgent -- --verbose
# Or connect directly to a remote rival/relay WebSocket exchanging PROTOCOL.md JSON:
dotnet run --project src/ArmWrestling.MachineAgent -- --rival-ws ws://<relay-host>:<port>/ws
```
