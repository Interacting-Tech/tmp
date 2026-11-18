namespace ArmWrestling.MachineClient.Configuration;

/// <summary>
/// Configuration options for connecting to the local arm-wrestling machine WebSocket server
/// according to PROTOCOL.md.
/// </summary>
public sealed class MachineClientOptions
{
    /// <summary>
    /// Port of the local arm-wrestling WebSocket server (PROTOCOL.md section 2: 8080).
    /// </summary>
    public int Port { get; set; } = 8080;

    /// <summary>
    /// WebSocket endpoint path and query (PROTOCOL.md section 2: /ws?role=machine).
    /// </summary>
    public string PathAndQuery { get; set; } = "/ws?role=machine";

    /// <summary>
    /// Optional explicit IP address override. When null or empty, the client automatically
    /// reads the IPv4 address of the machine it runs on.
    /// </summary>
    public string? MachineIpOverride { get; set; }

    /// <summary>
    /// Optional network interface name (e.g., "eth0", "wlan0", "Ethernet") to prefer when
    /// reading the local machine IP address.
    /// </summary>
    public string? PreferredNetworkInterfaceName { get; set; }

    /// <summary>
    /// Whether to fall back to 127.0.0.1 if no active non-loopback IPv4 interface is found.
    /// </summary>
    public bool AllowLoopbackFallback { get; set; } = true;

    /// <summary>
    /// Cadence at which active rival telemetry is sent to the local server.
    /// Defaults to 10 ms to match the server's 10 ms relay interval (PROTOCOL.md section 7).
    /// </summary>
    public TimeSpan RivalSendInterval { get; set; } = TimeSpan.FromMilliseconds(10);

    /// <summary>
    /// Minimum spacing between outbound WebSocket frames when coalescing rapid rival updates.
    /// Defaults to 1 ms to match the server's 1 ms main loop (PROTOCOL.md section 4.1).
    /// </summary>
    public TimeSpan MinimumFrameSpacing { get; set; } = TimeSpan.FromMilliseconds(1);

    /// <summary>
    /// Whether to continuously re-send the latest rival telemetry at <see cref="RivalSendInterval"/>
    /// while a rival is active, or only send when new rival updates are provided.
    /// </summary>
    public bool KeepSendingLatestRivalTelemetry { get; set; } = true;

    /// <summary>
    /// Emits "server_player_id" once every N outbound rival messages (PROTOCOL.md section 3.2:
    /// "The machine sends this only in every 50th message"). Set to 1 to include on every message.
    /// </summary>
    public int ServerPlayerIdCadence { get; set; } = 50;

    /// <summary>
    /// When true, undoes the local server's section 4.3 opponent_torque sign flip on received
    /// telemetry so that NormalizedTelemetry reflects the physical machine's true outbound data
    /// (preventing double negation when bridged to a remote PROTOCOL.md server).
    /// </summary>
    public bool NormalizeLocalOpponentTorque { get; set; } = true;

    /// <summary>
    /// Initial delay before reconnecting when the WebSocket connection drops or cannot be established.
    /// </summary>
    public TimeSpan ReconnectInitialDelay { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Maximum backoff delay between WebSocket reconnection attempts.
    /// </summary>
    public TimeSpan ReconnectMaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Maximum inbound message size in bytes (guards against malformed oversized frames).
    /// </summary>
    public int MaxInboundMessageBytes { get; set; } = 64 * 1024;
}
