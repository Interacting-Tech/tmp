# Arm-Wrestling Server: Client Message Protocol

This document describes how the current server exchanges data with the arm-wresteling machines ("clients"): what a machine sends, what the server does with it, and what the server sends back. It describes the behaviour of the existing code as it is today.

## 1. Overview

- The server pairs exactly two machines and relays each machine's data to the other.
- Transport is WebSocket, text frames, JSON.
- The server runs a 1 ms main loop. It reads the newest message from each machine every cycle and sends data back to the machines every 10 ms.
- The server also runs a small match state machine that tells the machines when to start and who won.

## 2. Connecting

A machine connects to:

```
ws://<server-ip>:8080/ws?role=machine
```

- The first machine to connect becomes **machine 1**, the second becomes **machine 2**. A third machine is accepted on the socket but ignored.
- When a machine disconnects, its slot is cleared and the next machine to connect takes it.
- There is no login, handshake message or authentication. The `role=machine` query parameter is the only identification.

## 3. What the machine sends

### 3.1 Envelope

Each message is a JSON object with **one top-level key**, whose value is the telemetry object:

```json
{
  "data": {
    "MAC_address": "AA:BB:CC:DD:EE:FF",
    "player_id": "player-1",
    "server_player_id": "abc123",
    "hand": "right",
    "multiplayer_state": "MULTIPLAYER_SM_RUN",
    "single_player_state": "",
    "machine_torque": 10.2,
    "position": 3.1,
    "velocity": -0.4,
    "left_limit": -45.0,
    "right_limit": 45.0,
    "peak_force": 30.0,
    "timeStamp": 123456789
  }
}
```

- The server does not check the name of the top-level key (`"data"` above is only an example). It takes the value of the first key and treats it as the telemetry object.
- If the object has more than one top-level key, the server uses the one that comes first **alphabetically**, not the one written first. Send exactly one key.
- Messages that are not valid JSON, and binary frames, are discarded.

### 3.2 Telemetry fields

| Field | Type | Default if missing | Notes |
|---|---|---|---|
| `MAC_address` | string | `""` | Stored and relayed. Not used to identify the machine. |
| `player_id` | string | `""` | Stored and relayed. |
| `hand` | string | `""` | `"left"` or `"right"`, lowercase. Used for the torque sign rule in section 4.3. |
| `multiplayer_state` | string | `""` | Drives the match state machine. See section 5. |
| `single_player_state` | string | `""` | Stored and relayed. Not used by the server. |
| `machine_torque` | number | `0.0` | Relayed; sign may be flipped. See section 4.3. |
| `position` | number | `0.0` | Stored and relayed. |
| `velocity` | number | `0.0` | Stored and relayed. |
| `left_limit` | number | `0.0` | Stored and relayed. |
| `right_limit` | number | `0.0` | Stored and relayed. |
| `peak_force` | number | `0.0` | Stored and relayed. |
| `timeStamp` | unsigned integer | `0` | The machine's own clock. Stored and relayed. |

The server does not define or check units, and does no arithmetic on the numbers other than the sign flip on `opponent_torque`. Any extra fields in the telemetry object are kept and relayed unchanged.

`multiplayer_state` values the server recognises:

- `MULTIPLAYER_SM_READY`
- `MULTIPLAYER_SM_GAMEOVER_WIN`
- `MULTIPLAYER_SM_GAMEOVER_LOSE`

Any other value (for example `MULTIPLAYER_SM_COUNT_DOWN` or `MULTIPLAYER_SM_RUN`) is relayed but causes no action on the server.

## 4. What the server does with the data

### 4.1 Receiving

- The server keeps **only the newest message** from each machine. If a machine sends several messages within one 1 ms cycle, the older ones are overwritten and never processed.
- Each cycle, the server reads the newest unread message from machine 1 and machine 2, and replaces that machine's stored telemetry with it.
- If the telemetry cannot be parsed, the server logs the error and keeps the previous telemetry.

### 4.2 Fields the server adds

After storing a machine's telemetry, the server adds three fields to it:

| Field | Type | Meaning |
|---|---|---|
| `is_connected` | boolean | `true` while the machine is connected. |
| `client_id` | integer | The server's internal connection number for this machine. |
| `last_message_timestamp` | integer | Server time, in milliseconds, when the message was processed. This is a monotonic clock, not Unix time. Use it only to compare with other values of the same field. |

### 4.3 Torque sign rule

If one player uses the left hand and the other uses the right hand, the server multiplies `opponent_torque` by −1 in both machines' relayed telemetry. If both players use the same hand, the value is relayed unchanged.

### 4.4 When a machine disconnects

The server clears that machine's stored telemetry to an empty object and frees its slot. The remaining machine then receives an empty `data` object (section 6.1). If the remaining machine's `hand` is `"left"`, the object contains only `"opponent_torque": -0.0`.

There is no timeout for a machine that stays connected but stops sending. The server keeps relaying its last telemetry.

## 5. Match state machine

The server holds one match state, driven by the `multiplayer_state` each machine reports.

| Server state | Leaves when | Action |
|---|---|---|
| `MULTIPLAYER_IDLE` | Either machine reports `MULTIPLAYER_SM_READY` | Go to `MULTIPLAYER_WAITING`. |
| `MULTIPLAYER_WAITING` | Both machines report `MULTIPLAYER_SM_READY` | Send `START` to all clients, go to `MULTIPLAYER_MID_MATCH`. |
| `MULTIPLAYER_MID_MATCH` | Either machine reports `MULTIPLAYER_SM_GAMEOVER_WIN` or `MULTIPLAYER_SM_GAMEOVER_LOSE` | Send the result to the **other** machine, go to `MULTIPLAYER_POST_MATCH`. |
| `MULTIPLAYER_POST_MATCH` | 2 seconds have passed | Go to `MULTIPLAYER_IDLE`. |

In any state other than `MULTIPLAYER_IDLE`, if either machine disconnects the server returns to `MULTIPLAYER_IDLE`.

`MULTIPLAYER_WAITING` has no timeout. If one machine reports ready and the other never does, the server stays in this state.

## 6. What the server sends to the machine

All messages are JSON text frames.

### 6.1 Opponent telemetry — every 10 ms

Each connected machine receives the **other** machine's stored telemetry:

```json
{
  "data": {
    "MAC_address": "11:22:33:44:55:66",
    "player_id": "player-2",
    "hand": "left",
    "multiplayer_state": "MULTIPLAYER_SM_RUN",
    "single_player_state": "",
    "machine_torque": -12.5,
    "position": -3.1,
    "velocity": 0.4,
    "left_limit": -45.0,
    "right_limit": 45.0,
    "peak_force": 28.0,
    "timeStamp": 123456790,
    "is_connected": true,
    "client_id": 2,
    "last_message_timestamp": 86400123
  }
}
```

- The content is the opponent's last telemetry object as described in section 3.2, plus the three server fields from section 4.2, with the sign rule from section 4.3 applied.
- `server_player_id` appears only when the opponent's most recent message contained it.
- The message is sent every 10 ms whether or not the opponent has sent anything new, so the same values can arrive several times.
- If the opponent is not connected, or has not sent anything yet, `data` is an empty object: `{"data":{}}`.

### 6.2 Match commands

| Message | Sent to | When |
|---|---|---|
| `{"cmd":{"START":1}}` | All clients | Both machines report `MULTIPLAYER_SM_READY`. |
| `{"cmd":{"YOU_LOSE":1}}` | Machine 2 | Machine 1 reports `MULTIPLAYER_SM_GAMEOVER_WIN`. |
| `{"cmd":{"YOU_WIN":0}}` | Machine 2 | Machine 1 reports `MULTIPLAYER_SM_GAMEOVER_LOSE`. |
| `{"cmd":{"YOU_LOSE":0}}` | Machine 1 | Machine 2 reports `MULTIPLAYER_SM_GAMEOVER_WIN`. |
| `{"cmd":{"YOU_WIN":0}}` | Machine 1 | Machine 2 reports `MULTIPLAYER_SM_GAMEOVER_LOSE`. |

- The command is the key name (`START`, `YOU_WIN`, `YOU_LOSE`). The number after it has no consistent meaning in the current code and should be ignored.
- Only the opponent of the reporting machine is told the result. The machine that reported the result receives nothing.
- Machine 1's report is checked first. If both machines report a result in the same cycle, only machine 1's report is acted on.

### 6.3 Operator commands

An operator can type a command into the server's debug web page. The server forwards it to **all** clients as:

```json
{"cmd":"<text typed by the operator>"}
```

If the operator types `k` or `K`, the server resets the match state to `MULTIPLAYER_IDLE` and sends:

```json
{"cmd":"STOP"}
```

Note that here `cmd` holds a string, while in the match commands in section 6.2 it holds an object. A machine must handle both forms.

## 7. Timing summary

| Item | Rate |
|---|---|
| Server main loop (reads newest machine messages, runs the match state machine) | every 1 ms |
| Opponent telemetry sent to each machine | every 10 ms |
| Status sent to the debug web page | every 50 ms |
| Delay in `MULTIPLAYER_POST_MATCH` before returning to idle | 2 s |

## 8. Known limitations of the current behaviour

- Machines are identified by connection order only, not by `MAC_address` or `player_id`.
- There is no authentication and no encryption.
- There is no keep-alive for machines. A `ping` message from a machine is treated as telemetry, not answered.
- Stale telemetry is relayed indefinitely if a machine stops sending without disconnecting.
- The values attached to `YOU_WIN` and `YOU_LOSE` are inconsistent.
- Nothing is saved: no match results, players or logs.
