using ArmWrestling.MachineClient.Protocol;

namespace ArmWrestling.MachineAgent;

/// <summary>
/// Abstraction for exchanging local machine data and rival machine data (in PROTOCOL.md JSON format)
/// with an external rival source (e.g., central matchmaking server, peer bridge, or test harness).
/// </summary>
public interface IRivalBridge : IAsyncDisposable
{
    event Action<MachineTelemetry>? RivalTelemetryReceived;

    event Action<string>? RivalJsonReceived;

    event Action? RivalDisconnected;

    Task StartAsync(CancellationToken cancellationToken = default);

    Task StopAsync(CancellationToken cancellationToken = default);

    ValueTask PublishLocalMachineDataAsync(
        string protocolJsonEnvelope,
        ReceivedMachineData snapshot,
        CancellationToken cancellationToken = default);

    ValueTask PublishLocalServerCommandAsync(
        ServerCommand command,
        CancellationToken cancellationToken = default);

    ValueTask PublishLocalMachineDisconnectedAsync(
        CancellationToken cancellationToken = default);
}
