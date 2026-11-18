using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.WebSockets;
using ArmWrestling.MachineClient.Configuration;
using ArmWrestling.MachineClient.Networking;
using ArmWrestling.MachineClient.Protocol;

namespace ArmWrestling.MachineClient;

/// <summary>
/// Connects to the arm-wrestling server running on the local machine (ws://&lt;machine-ip&gt;:8080/ws?role=machine),
/// accepts the local machine's telemetry and server commands, and provides the rival's telemetry
/// in the exact JSON format and rules defined in PROTOCOL.md.
/// </summary>
public sealed class ArmWrestlingMachineClient : IMachinePartClient
{
    private readonly MachineClientOptions _options;
    private readonly object _rivalSync = new();
    private readonly Queue<MachineTelemetry> _pendingTerminalStates = new();
    private readonly SemaphoreSlim _sendSignal = new(0, 1);
    private readonly SemaphoreSlim _socketWriteLock = new(1, 1);
    private readonly SemaphoreSlim _lifecycleLock = new(1, 1);

    private CancellationTokenSource? _runCts;
    private CancellationTokenSource? _slotResetCts;
    private ClientWebSocket? _activeWebSocket;
    private volatile bool _intentionalSlotResetPending;
    private TaskCompletionSource<bool>? _slotResetCompletion;
    private Task? _runnerTask;

    private MachineTelemetry? _latestRivalTelemetry;
    private string? _cachedRivalServerPlayerId;
    private bool _hasUnsentRivalUpdate;
    private volatile bool _hasActiveRival;

    private volatile string _lastSentRivalHand = "";
    private string? _lastKnownLocalServerPlayerId;
    private long? _lastLocalMessageTimestamp;

    private volatile bool _isConnectedToServer;
    private volatile bool _isLocalMachineConnected;
    private volatile ReceivedMachineData? _latestLocalData;
    private volatile ServerCommand? _latestCommand;

    public ArmWrestlingMachineClient(MachineClientOptions? options = null)
    {
        _options = options ?? new MachineClientOptions();
        MachineIp = LocalIpResolver.ResolveMachineIp(_options);
        ServerUri = LocalIpResolver.BuildMachineWebSocketUri(MachineIp, _options);
    }

    public IPAddress MachineIp { get; }

    public Uri ServerUri { get; }

    public bool IsConnectedToServer => _isConnectedToServer;

    public bool IsLocalMachineConnected => _isLocalMachineConnected;

    public ReceivedMachineData? LatestLocalData => _latestLocalData;

    public ServerCommand? LatestCommand => _latestCommand;

    public event Action<ReceivedMachineData>? LocalDataReceived;

    public event Action<ServerCommand>? CommandReceived;

    public event Action<bool>? ServerConnectionChanged;

    public event Action<bool>? LocalMachineConnectionChanged;

    /// <summary>
    /// Optional diagnostic callback invoked on recoverable transport or parse issues.
    /// </summary>
    public event Action<string, Exception?>? DiagnosticLogged;

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycleLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_runnerTask is { IsCompleted: false })
            {
                return;
            }

            _runCts = new CancellationTokenSource();
            _runnerTask = Task.Run(() => RunConnectionSupervisorAsync(_runCts.Token), CancellationToken.None);
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        Task? taskToAwait;
        await _lifecycleLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_runCts is null)
            {
                return;
            }

            await _runCts.CancelAsync().ConfigureAwait(false);
            taskToAwait = _runnerTask;
            _runnerTask = null;
        }
        finally
        {
            _lifecycleLock.Release();
        }

        if (taskToAwait is not null)
        {
            try
            {
                await taskToAwait.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
            }
        }

        _runCts?.Dispose();
        _runCts = null;
    }

    public void ProvideRivalTelemetry(MachineTelemetry rivalTelemetry)
    {
        ArgumentNullException.ThrowIfNull(rivalTelemetry);

        var snapshot = rivalTelemetry.Clone();

        lock (_rivalSync)
        {
            if (!string.IsNullOrEmpty(snapshot.ServerPlayerId))
            {
                _cachedRivalServerPlayerId = snapshot.ServerPlayerId;
            }

            var previousState = _latestRivalTelemetry?.MultiplayerState;
            var isNewTerminalTransition =
                ProtocolConstants.IsTerminalMatchState(snapshot.MultiplayerState) &&
                !string.Equals(previousState, snapshot.MultiplayerState, StringComparison.Ordinal);

            if (isNewTerminalTransition && _pendingTerminalStates.Count < 4)
            {
                // Ensure terminal win/lose transitions hit the wire at least once even if idle telemetry follows immediately.
                _pendingTerminalStates.Enqueue(snapshot.Clone());
            }

            _latestRivalTelemetry = snapshot;
            _hasUnsentRivalUpdate = true;
            _hasActiveRival = true;
        }

        SignalSendLoop();
    }

    public bool TryProvideRivalJson(string rivalEnvelopeJson)
    {
        if (!ProtocolJsonSerializer.TryParseRivalEnvelope(rivalEnvelopeJson, out var telemetry) ||
            telemetry is null)
        {
            return false;
        }

        ProvideRivalTelemetry(telemetry);
        return true;
    }

    public bool TryProvideRivalJson(ReadOnlySpan<byte> rivalEnvelopeUtf8)
    {
        if (!ProtocolJsonSerializer.TryParseRivalEnvelope(rivalEnvelopeUtf8, out var telemetry) ||
            telemetry is null)
        {
            return false;
        }

        ProvideRivalTelemetry(telemetry);
        return true;
    }

    public async Task ClearRivalAsync(CancellationToken cancellationToken = default)
    {
        CancellationTokenSource? resetCts;
        ClientWebSocket? socketToClose;
        Task? waitForDisconnect = null;

        lock (_rivalSync)
        {
            _latestRivalTelemetry = null;
            _cachedRivalServerPlayerId = null;
            _pendingTerminalStates.Clear();
            _hasUnsentRivalUpdate = false;
            _hasActiveRival = false;
            resetCts = _slotResetCts;
            socketToClose = _activeWebSocket;

            if (resetCts is not null && !resetCts.IsCancellationRequested)
            {
                _intentionalSlotResetPending = true;
                _slotResetCompletion ??= new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                waitForDisconnect = _slotResetCompletion.Task;
            }
        }

        // Send a clean RFC 6455 Close frame first so the server processes the disconnect (clearing {}
        // per section 4.4 and resetting match state to MULTIPLAYER_IDLE per section 5) before echoing Close.
        if (socketToClose is { State: WebSocketState.Open })
        {
            try
            {
                using var closeCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                closeCts.CancelAfter(TimeSpan.FromMilliseconds(300));
                await _socketWriteLock.WaitAsync(closeCts.Token).ConfigureAwait(false);
                try
                {
                    if (socketToClose.State == WebSocketState.Open)
                    {
                        await socketToClose.CloseOutputAsync(
                            WebSocketCloseStatus.NormalClosure,
                            "Clearing rival slot",
                            closeCts.Token).ConfigureAwait(false);
                    }
                }
                finally
                {
                    _socketWriteLock.Release();
                }
            }
            catch (Exception)
            {
                if (resetCts is not null)
                {
                    await resetCts.CancelAsync().ConfigureAwait(false);
                }
            }
        }
        else if (resetCts is not null)
        {
            await resetCts.CancelAsync().ConfigureAwait(false);
        }

        if (waitForDisconnect is not null)
        {
            using var fallbackCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            fallbackCts.CancelAfter(TimeSpan.FromMilliseconds(500));
            try
            {
                await waitForDisconnect.WaitAsync(fallbackCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                if (resetCts is not null)
                {
                    await resetCts.CancelAsync().ConfigureAwait(false);
                }

                await waitForDisconnect.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public string GetLatestLocalDataAsProtocolJson(bool includeServerPlayerId = true)
    {
        var snapshot = _latestLocalData;
        if (snapshot is not { HasMachineTelemetry: true, NormalizedTelemetry: not null })
        {
            return "{\"data\":{}}";
        }

        return ProtocolJsonSerializer.SerializeMachineEnvelope(
            snapshot.NormalizedTelemetry,
            includeServerPlayerId,
            snapshot.EffectiveServerPlayerId);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _sendSignal.Dispose();
        _socketWriteLock.Dispose();
        _lifecycleLock.Dispose();
    }

    private async Task RunConnectionSupervisorAsync(CancellationToken stoppingToken)
    {
        var reconnectDelay = _options.ReconnectInitialDelay > TimeSpan.Zero
            ? _options.ReconnectInitialDelay
            : TimeSpan.FromMilliseconds(100);
        var maxDelay = _options.ReconnectMaxDelay >= reconnectDelay
            ? _options.ReconnectMaxDelay
            : reconnectDelay;

        while (!stoppingToken.IsCancellationRequested)
        {
            var wasIntentionalSlotReset = false;
            using var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            lock (_rivalSync)
            {
                _slotResetCts = sessionCts;
            }

            using var webSocket = new ClientWebSocket();
            webSocket.Options.KeepAliveInterval = Timeout.InfiniteTimeSpan;

            try
            {
                await webSocket.ConnectAsync(ServerUri, sessionCts.Token).ConfigureAwait(false);

                lock (_rivalSync)
                {
                    _activeWebSocket = webSocket;
                    _intentionalSlotResetPending = false;
                }

                // Reset backoff and per-connection wire tracking on successful connect.
                reconnectDelay = _options.ReconnectInitialDelay > TimeSpan.Zero
                    ? _options.ReconnectInitialDelay
                    : TimeSpan.FromMilliseconds(100);
                _lastSentRivalHand = "";

                if (_hasActiveRival)
                {
                    lock (_rivalSync)
                    {
                        _hasUnsentRivalUpdate = _latestRivalTelemetry is not null;
                    }

                    SignalSendLoop();
                }

                SetServerConnected(true);

                var receiveTask = ReceiveLoopAsync(webSocket, sessionCts.Token);
                var sendTask = SendLoopAsync(webSocket, sessionCts.Token);

                await Task.WhenAny(receiveTask, sendTask).ConfigureAwait(false);
                wasIntentionalSlotReset = _intentionalSlotResetPending ||
                                          (sessionCts.IsCancellationRequested && !stoppingToken.IsCancellationRequested);

                await sessionCts.CancelAsync().ConfigureAwait(false);

                try
                {
                    await Task.WhenAll(receiveTask, sendTask).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }

                if (webSocket.State is WebSocketState.Open or WebSocketState.CloseReceived)
                {
                    using var closeTimeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
                    try
                    {
                        await webSocket.CloseAsync(
                            WebSocketCloseStatus.NormalClosure,
                            "Closing machine session",
                            closeTimeout.Token).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (OperationCanceledException) when (sessionCts.IsCancellationRequested)
            {
                wasIntentionalSlotReset = true;
            }
            catch (Exception ex)
            {
                DiagnosticLogged?.Invoke($"WebSocket error on {ServerUri}: {ex.Message}", ex);
            }
            finally
            {
                TaskCompletionSource<bool>? completedReset = null;
                lock (_rivalSync)
                {
                    if (ReferenceEquals(_slotResetCts, sessionCts))
                    {
                        _slotResetCts = null;
                    }

                    if (ReferenceEquals(_activeWebSocket, webSocket))
                    {
                        _activeWebSocket = null;
                    }

                    _intentionalSlotResetPending = false;
                    completedReset = _slotResetCompletion;
                    _slotResetCompletion = null;
                }

                SetLocalMachineConnected(false);
                SetServerConnected(false);
                completedReset?.TrySetResult(true);
            }

            if (stoppingToken.IsCancellationRequested)
            {
                break;
            }

            if (!wasIntentionalSlotReset)
            {
                try
                {
                    await Task.Delay(reconnectDelay, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                var nextMs = Math.Min(reconnectDelay.TotalMilliseconds * 2.0, maxDelay.TotalMilliseconds);
                reconnectDelay = TimeSpan.FromMilliseconds(nextMs);
            }
        }
    }

    private async Task ReceiveLoopAsync(ClientWebSocket webSocket, CancellationToken cancellationToken)
    {
        var rentBuffer = ArrayPool<byte>.Shared.Rent(8192);
        var messageBuffer = new ArrayBufferWriter<byte>(1024);

        try
        {
            while (!cancellationToken.IsCancellationRequested &&
                   webSocket.State is WebSocketState.Open or WebSocketState.CloseSent)
            {
                messageBuffer.Clear();
                ValueWebSocketReceiveResult result;
                var oversized = false;

                do
                {
                    result = await webSocket.ReceiveAsync(rentBuffer.AsMemory(), cancellationToken)
                        .ConfigureAwait(false);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        return;
                    }

                    if (result.MessageType == WebSocketMessageType.Text && !oversized)
                    {
                        if (messageBuffer.WrittenCount + result.Count > _options.MaxInboundMessageBytes)
                        {
                            oversized = true;
                        }
                        else if (result.Count > 0)
                        {
                            messageBuffer.Write(rentBuffer.AsSpan(0, result.Count));
                        }
                    }
                }
                while (!result.EndOfMessage);

                // PROTOCOL.md section 3.1: binary frames and invalid JSON are discarded.
                if (result.MessageType != WebSocketMessageType.Text || oversized || messageBuffer.WrittenCount == 0)
                {
                    continue;
                }

                if (!ProtocolJsonSerializer.TryParseServerMessage(
                        messageBuffer.WrittenSpan,
                        _lastKnownLocalServerPlayerId,
                        _lastLocalMessageTimestamp,
                        _lastSentRivalHand,
                        _options.NormalizeLocalOpponentTorque,
                        out var parsed) ||
                    parsed is null)
                {
                    DiagnosticLogged?.Invoke("Discarded invalid inbound JSON message from server.", null);
                    continue;
                }

                if (parsed.Kind == InboundMessageKind.Command && parsed.Command is not null)
                {
                    _latestCommand = parsed.Command;
                    InvokeSafely(CommandReceived, parsed.Command);
                }
                else if (parsed.Kind == InboundMessageKind.Telemetry && parsed.TelemetryData is not null)
                {
                    var telemetryData = parsed.TelemetryData;
                    if (telemetryData.HasMachineTelemetry)
                    {
                        _lastKnownLocalServerPlayerId = telemetryData.EffectiveServerPlayerId;
                        _lastLocalMessageTimestamp = telemetryData.LastMessageTimestamp;
                        SetLocalMachineConnected(true);
                    }
                    else
                    {
                        _lastKnownLocalServerPlayerId = null;
                        _lastLocalMessageTimestamp = null;
                        SetLocalMachineConnected(false);
                    }

                    _latestLocalData = telemetryData;
                    InvokeSafely(LocalDataReceived, telemetryData);
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rentBuffer);
        }
    }

    private async Task SendLoopAsync(ClientWebSocket webSocket, CancellationToken cancellationToken)
    {
        long outboundFrameIndex = 0;
        var lastSendTimestamp = 0L;
        var sendInterval = _options.RivalSendInterval > TimeSpan.Zero
            ? _options.RivalSendInterval
            : TimeSpan.FromMilliseconds(10);

        while (!cancellationToken.IsCancellationRequested &&
               webSocket.State == WebSocketState.Open)
        {
            var waitTimeout = _hasActiveRival && _options.KeepSendingLatestRivalTelemetry
                ? sendInterval
                : Timeout.InfiniteTimeSpan;

            try
            {
                await _sendSignal.WaitAsync(waitTimeout, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (_options.MinimumFrameSpacing > TimeSpan.Zero && lastSendTimestamp != 0)
            {
                var elapsed = Stopwatch.GetElapsedTime(lastSendTimestamp);
                var remaining = _options.MinimumFrameSpacing - elapsed;
                if (remaining > TimeSpan.Zero)
                {
                    await Task.Delay(remaining, cancellationToken).ConfigureAwait(false);
                }
            }

            while (!cancellationToken.IsCancellationRequested &&
                   webSocket.State == WebSocketState.Open)
            {
                MachineTelemetry? toSend = null;
                string? fallbackServerPlayerId = null;
                var isPriorityTerminal = false;

                lock (_rivalSync)
                {
                    if (_pendingTerminalStates.Count > 0)
                    {
                        toSend = _pendingTerminalStates.Dequeue();
                        fallbackServerPlayerId = _cachedRivalServerPlayerId;
                        isPriorityTerminal = true;
                        if (_pendingTerminalStates.Count == 0 &&
                            _latestRivalTelemetry is not null &&
                            string.Equals(_latestRivalTelemetry.MultiplayerState, toSend.MultiplayerState, StringComparison.Ordinal))
                        {
                            _hasUnsentRivalUpdate = false;
                        }
                    }
                    else if (_latestRivalTelemetry is not null &&
                             (_hasUnsentRivalUpdate || _options.KeepSendingLatestRivalTelemetry))
                    {
                        toSend = _latestRivalTelemetry;
                        fallbackServerPlayerId = _cachedRivalServerPlayerId;
                        _hasUnsentRivalUpdate = false;
                    }
                }

                if (toSend is null)
                {
                    break;
                }

                outboundFrameIndex++;
                var cadence = _options.ServerPlayerIdCadence <= 1 ? 1 : _options.ServerPlayerIdCadence;
                var includeServerPlayerId = ((outboundFrameIndex - 1) % cadence) == 0;

                var payloadBytes = ProtocolJsonSerializer.SerializeMachineEnvelopeToUtf8Bytes(
                    toSend,
                    includeServerPlayerId,
                    fallbackServerPlayerId);

                _lastSentRivalHand = MachineTelemetry.NormalizeHand(toSend.Hand);

                await _socketWriteLock.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    if (webSocket.State != WebSocketState.Open)
                    {
                        break;
                    }

                    await webSocket.SendAsync(
                        payloadBytes.AsMemory(),
                        WebSocketMessageType.Text,
                        endOfMessage: true,
                        cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    _socketWriteLock.Release();
                }

                lastSendTimestamp = Stopwatch.GetTimestamp();

                if (!isPriorityTerminal)
                {
                    break;
                }
            }
        }
    }

    private void SignalSendLoop()
    {
        try
        {
            if (_sendSignal.CurrentCount == 0)
            {
                _sendSignal.Release();
            }
        }
        catch (SemaphoreFullException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
    }

    private void SetServerConnected(bool connected)
    {
        if (_isConnectedToServer == connected)
        {
            return;
        }

        _isConnectedToServer = connected;
        InvokeSafely(ServerConnectionChanged, connected);
    }

    private void SetLocalMachineConnected(bool connected)
    {
        if (_isLocalMachineConnected == connected)
        {
            return;
        }

        _isLocalMachineConnected = connected;
        InvokeSafely(LocalMachineConnectionChanged, connected);
    }

    private void InvokeSafely<T>(Action<T>? handler, T arg)
    {
        if (handler is null)
        {
            return;
        }

        try
        {
            handler(arg);
        }
        catch (Exception ex)
        {
            DiagnosticLogged?.Invoke($"Subscriber callback threw an exception: {ex.Message}", ex);
        }
    }
}
