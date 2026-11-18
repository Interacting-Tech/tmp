using System.Buffers;
using System.Net.WebSockets;
using System.Text;
using ArmWrestling.MachineClient.Protocol;

namespace ArmWrestling.MachineAgent;

/// <summary>
/// Optional WebSocket rival bridge that connects outbound to a remote relay/matchmaking endpoint,
/// sending local machine data as PROTOCOL.md {"data": {...}} JSON frames and feeding incoming
/// rival {"data": {...}} JSON frames back into the local machine.
/// </summary>
public sealed class WebSocketRivalBridge : IRivalBridge
{
    private readonly Uri _remoteUri;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private CancellationTokenSource? _cts;
    private Task? _loopTask;
    private volatile ClientWebSocket? _activeSocket;

    public WebSocketRivalBridge(Uri remoteUri)
    {
        ArgumentNullException.ThrowIfNull(remoteUri);
        _remoteUri = remoteUri;
    }

    public event Action<MachineTelemetry>? RivalTelemetryReceived;

    public event Action<string>? RivalJsonReceived;

    public event Action? RivalDisconnected;

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        _cts = new CancellationTokenSource();
        _loopTask = Task.Run(() => ConnectionLoopAsync(_cts.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_cts is null)
        {
            return;
        }

        await _cts.CancelAsync().ConfigureAwait(false);
        if (_loopTask is not null)
        {
            try
            {
                await _loopTask.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
            }
        }

        _cts.Dispose();
        _cts = null;
    }

    public async ValueTask PublishLocalMachineDataAsync(
        string protocolJsonEnvelope,
        ReceivedMachineData snapshot,
        CancellationToken cancellationToken = default)
    {
        var socket = _activeSocket;
        if (socket is not { State: WebSocketState.Open })
        {
            return;
        }

        var bytes = Encoding.UTF8.GetBytes(protocolJsonEnvelope);
        await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (socket.State == WebSocketState.Open)
            {
                await socket.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception)
        {
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public ValueTask PublishLocalServerCommandAsync(
        ServerCommand command,
        CancellationToken cancellationToken = default) => ValueTask.CompletedTask;

    public async ValueTask PublishLocalMachineDisconnectedAsync(
        CancellationToken cancellationToken = default)
    {
        var socket = _activeSocket;
        if (socket is not { State: WebSocketState.Open })
        {
            return;
        }

        var emptyEnvelope = "{\"data\":{}}"u8.ToArray();
        await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (socket.State == WebSocketState.Open)
            {
                await socket.SendAsync(emptyEnvelope, WebSocketMessageType.Text, endOfMessage: true, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception)
        {
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _sendLock.Dispose();
    }

    private async Task ConnectionLoopAsync(CancellationToken stoppingToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(8192);
        var messageWriter = new ArrayBufferWriter<byte>(1024);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                using var socket = new ClientWebSocket();
                var hadRivalData = false;

                try
                {
                    await socket.ConnectAsync(_remoteUri, stoppingToken).ConfigureAwait(false);
                    _activeSocket = socket;

                    while (!stoppingToken.IsCancellationRequested &&
                           socket.State == WebSocketState.Open)
                    {
                        messageWriter.Clear();
                        ValueWebSocketReceiveResult result;
                        do
                        {
                            result = await socket.ReceiveAsync(buffer.AsMemory(), stoppingToken)
                                .ConfigureAwait(false);
                            if (result.MessageType == WebSocketMessageType.Close)
                            {
                                break;
                            }

                            if (result.MessageType == WebSocketMessageType.Text && result.Count > 0)
                            {
                                messageWriter.Write(buffer.AsSpan(0, result.Count));
                            }
                        }
                        while (!result.EndOfMessage);

                        if (result.MessageType == WebSocketMessageType.Close)
                        {
                            break;
                        }

                        if (result.MessageType != WebSocketMessageType.Text || messageWriter.WrittenCount == 0)
                        {
                            continue;
                        }

                        var json = Encoding.UTF8.GetString(messageWriter.WrittenSpan);
                        if (ProtocolJsonSerializer.TryParseRivalEnvelope(messageWriter.WrittenSpan, out var parsed) &&
                            parsed is not null)
                        {
                            hadRivalData = true;
                            if (RivalTelemetryReceived is not null)
                            {
                                RivalTelemetryReceived.Invoke(parsed);
                            }
                            else
                            {
                                RivalJsonReceived?.Invoke(json);
                            }
                        }
                        else if (hadRivalData && json.Contains("\"data\":{}", StringComparison.Ordinal))
                        {
                            hadRivalData = false;
                            RivalDisconnected?.Invoke();
                        }
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception)
                {
                }
                finally
                {
                    _activeSocket = null;
                    if (hadRivalData)
                    {
                        RivalDisconnected?.Invoke();
                    }
                }

                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
