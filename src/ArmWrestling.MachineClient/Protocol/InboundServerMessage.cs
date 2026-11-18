namespace ArmWrestling.MachineClient.Protocol;

public enum InboundMessageKind
{
    Telemetry,
    Command
}

/// <summary>
/// Snapshot of local machine data delivered by the local server every 10 ms (PROTOCOL.md section 6.1).
/// </summary>
public sealed class ReceivedMachineData
{
    /// <summary>
    /// True when the local machine is connected and has sent telemetry; false when the server sent
    /// an empty {"data":{}} or the section 4.4 {"data":{"opponent_torque":-0.0}} disconnect frame.
    /// </summary>
    public required bool HasMachineTelemetry { get; init; }

    /// <summary>
    /// Value of the server-injected "is_connected" field (PROTOCOL.md section 4.2).
    /// </summary>
    public required bool IsConnected { get; init; }

    /// <summary>
    /// Value of the server-injected "client_id" field when present (PROTOCOL.md section 4.2).
    /// </summary>
    public int? ClientId { get; init; }

    /// <summary>
    /// Monotonic server timestamp in ms when the local machine's message was processed (PROTOCOL.md section 4.2).
    /// </summary>
    public long? LastMessageTimestamp { get; init; }

    /// <summary>
    /// True when <see cref="LastMessageTimestamp"/> advanced compared to the previous 10 ms frame;
    /// false when the server re-sent unchanged telemetry (PROTOCOL.md section 6.1).
    /// </summary>
    public bool IsNewSample { get; init; }

    /// <summary>
    /// Cached "server_player_id" across messages, preserving the last value between every 50th frame
    /// (PROTOCOL.md sections 3.2 &amp; 6.1).
    /// </summary>
    public string? EffectiveServerPlayerId { get; init; }

    /// <summary>
    /// True if this specific wire frame explicitly contained "server_player_id".
    /// </summary>
    public bool FrameContainedServerPlayerId { get; init; }

    /// <summary>
    /// Telemetry as received on the wire (before undoing the local server's opponent_torque flip),
    /// or null when <see cref="HasMachineTelemetry"/> is false.
    /// </summary>
    public MachineTelemetry? RawTelemetry { get; init; }

    /// <summary>
    /// Clean PROTOCOL.md section 3.1 machine telemetry ready to be sent as rival data to another
    /// machine/server (server-only fields stripped, opponent_torque sign normalized),
    /// or null when <see cref="HasMachineTelemetry"/> is false.
    /// </summary>
    public MachineTelemetry? NormalizedTelemetry { get; init; }
}

/// <summary>
/// Parsed inbound WebSocket text message from the server.
/// </summary>
public sealed record InboundServerMessage(
    InboundMessageKind Kind,
    ReceivedMachineData? TelemetryData = null,
    ServerCommand? Command = null);
