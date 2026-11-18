using System.Threading.Channels;
using ArmWrestling.MachineClient;
using ArmWrestling.MachineClient.Protocol;

namespace ArmWrestling.MachineAgent;

/// <summary>
/// Coordinates <see cref="IMachinePartClient"/> (connected to the local machine's server at
/// ws://&lt;machine-ip&gt;:8080/ws?role=machine) with an <see cref="IRivalBridge"/> so that:
/// 1. Local machine telemetry is accepted and published to the rival in PROTOCOL.md JSON format.
/// 2. Rival telemetry in PROTOCOL.md JSON format is fed into the local machine according to protocol rules.
/// </summary>
public sealed class MachineRelayCoordinator : IAsyncDisposable
{
    private readonly IMachinePartClient _machineClient;
    private readonly IRivalBridge _rivalBridge;
    private readonly int _outboundServerPlayerIdCadence;
    private readonly Channel<OutboundEvent> _outboundChannel;

    private CancellationTokenSource? _cts;
    private Task? _pumpTask;
    private long _publishedFrameCount;
    private string? _lastPublishedState;

    public MachineRelayCoordinator(
        IMachinePartClient machineClient,
        IRivalBridge rivalBridge,
        int outboundServerPlayerIdCadence = 50)
    {
        ArgumentNullException.ThrowIfNull(machineClient);
        ArgumentNullException.ThrowIfNull(rivalBridge);

        _machineClient = machineClient;
        _rivalBridge = rivalBridge;
        _outboundServerPlayerIdCadence = outboundServerPlayerIdCadence <= 1 ? 1 : outboundServerPlayerIdCadence;

        // Latest-wins bounded channel for normal telemetry; terminal transitions are pushed separately.
        _outboundChannel = Channel.CreateBounded<OutboundEvent>(new BoundedChannelOptions(16)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false
        });

        _machineClient.LocalDataReceived += OnLocalDataReceived;
        _machineClient.CommandReceived += OnCommandReceived;
        _machineClient.LocalMachineConnectionChanged += OnLocalMachineConnectionChanged;

        _rivalBridge.RivalTelemetryReceived += OnRivalTelemetryReceived;
        _rivalBridge.RivalJsonReceived += OnRivalJsonReceived;
        _rivalBridge.RivalDisconnected += OnRivalDisconnected;
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _cts = new CancellationTokenSource();
        _pumpTask = Task.Run(() => PumpOutboundAsync(_cts.Token), CancellationToken.None);

        await _rivalBridge.StartAsync(cancellationToken).ConfigureAwait(false);
        await _machineClient.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        await _machineClient.StopAsync(cancellationToken).ConfigureAwait(false);
        await _rivalBridge.StopAsync(cancellationToken).ConfigureAwait(false);

        if (_cts is not null)
        {
            await _cts.CancelAsync().ConfigureAwait(false);
            if (_pumpTask is not null)
            {
                try
                {
                    await _pumpTask.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                }
            }

            _cts.Dispose();
            _cts = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        _machineClient.LocalDataReceived -= OnLocalDataReceived;
        _machineClient.CommandReceived -= OnCommandReceived;
        _machineClient.LocalMachineConnectionChanged -= OnLocalMachineConnectionChanged;

        _rivalBridge.RivalTelemetryReceived -= OnRivalTelemetryReceived;
        _rivalBridge.RivalJsonReceived -= OnRivalJsonReceived;
        _rivalBridge.RivalDisconnected -= OnRivalDisconnected;

        await StopAsync().ConfigureAwait(false);
    }

    private void OnLocalDataReceived(ReceivedMachineData snapshot)
    {
        if (!snapshot.HasMachineTelemetry || snapshot.NormalizedTelemetry is null)
        {
            return;
        }

        _outboundChannel.Writer.TryWrite(new OutboundEvent(OutboundEventKind.Telemetry, snapshot, null));
    }

    private void OnCommandReceived(ServerCommand command)
    {
        _outboundChannel.Writer.TryWrite(new OutboundEvent(OutboundEventKind.Command, null, command));
    }

    private void OnLocalMachineConnectionChanged(bool connected)
    {
        if (!connected)
        {
            _outboundChannel.Writer.TryWrite(new OutboundEvent(OutboundEventKind.Disconnected, null, null));
        }
    }

    private void OnRivalTelemetryReceived(MachineTelemetry telemetry)
    {
        _machineClient.ProvideRivalTelemetry(telemetry);
    }

    private void OnRivalJsonReceived(string rivalJson)
    {
        _machineClient.TryProvideRivalJson(rivalJson);
    }

    private void OnRivalDisconnected()
    {
        _ = _machineClient.ClearRivalAsync();
    }

    private async Task PumpOutboundAsync(CancellationToken cancellationToken)
    {
        var reader = _outboundChannel.Reader;
        while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (reader.TryRead(out var item))
            {
                switch (item.Kind)
                {
                    case OutboundEventKind.Telemetry when item.Snapshot?.NormalizedTelemetry is not null:
                    {
                        _publishedFrameCount++;
                        var includeServerPlayerId = ((_publishedFrameCount - 1) % _outboundServerPlayerIdCadence) == 0;
                        var json = ProtocolJsonSerializer.SerializeMachineEnvelope(
                            item.Snapshot.NormalizedTelemetry,
                            includeServerPlayerId,
                            item.Snapshot.EffectiveServerPlayerId);

                        _lastPublishedState = item.Snapshot.NormalizedTelemetry.MultiplayerState;
                        await _rivalBridge.PublishLocalMachineDataAsync(json, item.Snapshot, cancellationToken)
                            .ConfigureAwait(false);
                        break;
                    }

                    case OutboundEventKind.Command when item.Command is not null:
                    {
                        // If the local server sent YOU_LOSE / YOU_WIN to us (1 ms loop), ensure the rival
                        // also receives the inferred terminal multiplayer_state even if the physical machine
                        // cleared its state before the next 10 ms telemetry broadcast.
                        var inferredTerminal = item.Command.InferredLocalMachineGameOverState;
                        if (inferredTerminal is not null &&
                            !string.Equals(_lastPublishedState, inferredTerminal, StringComparison.Ordinal) &&
                            _machineClient.LatestLocalData is { HasMachineTelemetry: true, NormalizedTelemetry: not null } latest)
                        {
                            var synthesized = latest.NormalizedTelemetry.Clone();
                            synthesized.MultiplayerState = inferredTerminal;

                            _publishedFrameCount++;
                            var includeServerPlayerId = ((_publishedFrameCount - 1) % _outboundServerPlayerIdCadence) == 0;
                            var json = ProtocolJsonSerializer.SerializeMachineEnvelope(
                                synthesized,
                                includeServerPlayerId,
                                latest.EffectiveServerPlayerId);

                            _lastPublishedState = inferredTerminal;
                            await _rivalBridge.PublishLocalMachineDataAsync(json, latest, cancellationToken)
                                .ConfigureAwait(false);
                        }

                        await _rivalBridge.PublishLocalServerCommandAsync(item.Command, cancellationToken)
                            .ConfigureAwait(false);
                        break;
                    }

                    case OutboundEventKind.Disconnected:
                    {
                        _publishedFrameCount = 0;
                        _lastPublishedState = null;
                        await _rivalBridge.PublishLocalMachineDisconnectedAsync(cancellationToken)
                            .ConfigureAwait(false);
                        break;
                    }
                }
            }
        }
    }

    private enum OutboundEventKind
    {
        Telemetry,
        Command,
        Disconnected
    }

    private readonly record struct OutboundEvent(
        OutboundEventKind Kind,
        ReceivedMachineData? Snapshot,
        ServerCommand? Command);
}
