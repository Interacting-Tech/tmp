using System.Net;
using ArmWrestling.MachineClient.Protocol;

namespace ArmWrestling.MachineClient;

/// <summary>
/// Contract for the machine-side client component that resolves the local machine's IP,
/// connects to ws://&lt;machine-ip&gt;:8080/ws?role=machine, accepts the local machine's telemetry
/// and match commands, and provides the rival's telemetry in PROTOCOL.md format.
/// </summary>
public interface IMachinePartClient : IAsyncDisposable
{
    /// <summary>
    /// The resolved IPv4/IPv6 address of the machine this process runs on.
    /// </summary>
    IPAddress MachineIp { get; }

    /// <summary>
    /// Full WebSocket endpoint URI (e.g., ws://192.168.1.50:8080/ws?role=machine).
    /// </summary>
    Uri ServerUri { get; }

    /// <summary>
    /// True while the WebSocket connection to the local server is open.
    /// </summary>
    bool IsConnectedToServer { get; }

    /// <summary>
    /// True while the local server reports that the physical machine in the other slot is connected.
    /// </summary>
    bool IsLocalMachineConnected { get; }

    /// <summary>
    /// Most recent snapshot received from the local server.
    /// </summary>
    ReceivedMachineData? LatestLocalData { get; }

    /// <summary>
    /// Most recent command received from the local server.
    /// </summary>
    ServerCommand? LatestCommand { get; }

    /// <summary>
    /// Raised on every telemetry frame received from the local server (every 10 ms per PROTOCOL.md section 6.1).
    /// </summary>
    event Action<ReceivedMachineData>? LocalDataReceived;

    /// <summary>
    /// Raised whenever a match or operator command is received from the local server (sections 6.2 &amp; 6.3).
    /// </summary>
    event Action<ServerCommand>? CommandReceived;

    /// <summary>
    /// Raised when the WebSocket connection to the local server connects (true) or disconnects (false).
    /// </summary>
    event Action<bool>? ServerConnectionChanged;

    /// <summary>
    /// Raised when the physical local machine connects (true) or disconnects (false) on the local server.
    /// </summary>
    event Action<bool>? LocalMachineConnectionChanged;

    /// <summary>
    /// Starts the background WebSocket connection, receive loop, and rival telemetry send loop.
    /// </summary>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gracefully stops the WebSocket connection and background loops.
    /// </summary>
    Task StopAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Queues the rival's latest telemetry to be sent to the local machine in PROTOCOL.md format
    /// (latest-message-wins, matching section 4.1).
    /// </summary>
    void ProvideRivalTelemetry(MachineTelemetry rivalTelemetry);

    /// <summary>
    /// Parses a JSON envelope containing the rival's telemetry according to PROTOCOL.md rules
    /// and queues it to be sent to the local machine.
    /// </summary>
    bool TryProvideRivalJson(string rivalEnvelopeJson);

    /// <summary>
    /// Parses UTF-8 JSON bytes containing the rival's telemetry according to PROTOCOL.md rules
    /// and queues it to be sent to the local machine.
    /// </summary>
    bool TryProvideRivalJson(ReadOnlySpan<byte> rivalEnvelopeUtf8);

    /// <summary>
    /// Clears the rival's telemetry and resets our slot on the local server by cycling the
    /// WebSocket connection, clearing stale rival data (section 4.4) and returning any non-idle
    /// server match state back to MULTIPLAYER_IDLE (section 5).
    /// </summary>
    Task ClearRivalAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Formats the local machine's latest accepted data as a PROTOCOL.md section 3.1 JSON envelope
    /// ({"data": { ... }}), or {"data":{}} if the local machine is not connected.
    /// </summary>
    string GetLatestLocalDataAsProtocolJson(bool includeServerPlayerId = true);
}
