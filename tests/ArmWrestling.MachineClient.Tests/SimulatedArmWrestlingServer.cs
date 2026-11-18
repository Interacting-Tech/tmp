using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using ArmWrestling.MachineClient.Protocol;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace ArmWrestling.MachineClient.Tests;

/// <summary>
/// Faithful in-process simulator of the existing arm-wrestling server described in PROTOCOL.md,
/// used for end-to-end integration testing of <see cref="ArmWrestlingMachineClient"/>.
/// </summary>
public sealed class SimulatedArmWrestlingServer : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly TimeSpan _postMatchDuration;
    private readonly object _slotLock = new();
    private readonly SlotState[] _slots = [new SlotState(1), new SlotState(2)];
    private readonly List<WebSocket> _allSockets = [];
    private readonly Stopwatch _monotonicClock = Stopwatch.StartNew();
    private readonly CancellationTokenSource _loopCts = new();
    private readonly Task _mainLoopTask;

    private int _nextClientId;
    private string _matchState = ProtocolConstants.ServerIdle;
    private long _postMatchEnteredAtMs;

    public SimulatedArmWrestlingServer(IPAddress bindAddress, TimeSpan? postMatchDuration = null)
    {
        BindAddress = bindAddress;
        Port = AllocateFreeTcpPort(bindAddress);
        _postMatchDuration = postMatchDuration ?? TimeSpan.FromSeconds(2);

        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Listen(bindAddress, Port);
        });

        _app = builder.Build();
        _app.UseWebSockets();

        _app.Map("/ws", async (HttpContext context) =>
        {
            if (!context.WebSockets.IsWebSocketRequest)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var role = context.Request.Query["role"].ToString();
            if (!string.Equals(role, "machine", StringComparison.Ordinal))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            using var ws = await context.WebSockets.AcceptWebSocketAsync();
            await HandleMachineSocketAsync(ws, context.RequestAborted);
        });

        _mainLoopTask = Task.Run(() => RunServerLoopAsync(_loopCts.Token));
    }

    public IPAddress BindAddress { get; }

    public int Port { get; }

    public string MatchState
    {
        get
        {
            lock (_slotLock)
            {
                return _matchState;
            }
        }
    }

    public Uri EndpointUri => new($"ws://{BindAddress}:{Port}/ws?role=machine");

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        await _app.StartAsync(cancellationToken);
    }

    public async Task SendOperatorCommandAsync(string operatorInput, CancellationToken cancellationToken = default)
    {
        string payload;
        if (string.Equals(operatorInput, "k", StringComparison.Ordinal) ||
            string.Equals(operatorInput, "K", StringComparison.Ordinal))
        {
            lock (_slotLock)
            {
                _matchState = ProtocolConstants.ServerIdle;
            }

            payload = "{\"cmd\":\"STOP\"}";
        }
        else
        {
            payload = JsonSerializer.Serialize(new { cmd = operatorInput });
        }

        await BroadcastToAllAsync(payload, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _loopCts.CancelAsync();
        try
        {
            await _mainLoopTask;
        }
        catch (OperationCanceledException)
        {
        }

        await _app.StopAsync();
        await _app.DisposeAsync();
        _loopCts.Dispose();
    }

    private async Task HandleMachineSocketAsync(WebSocket ws, CancellationToken requestAborted)
    {
        SlotState? assignedSlot = null;
        var assignedClientId = Interlocked.Increment(ref _nextClientId);

        lock (_slotLock)
        {
            _allSockets.Add(ws);
            if (_slots[0].Socket is null)
            {
                assignedSlot = _slots[0];
            }
            else if (_slots[1].Socket is null)
            {
                assignedSlot = _slots[1];
            }

            if (assignedSlot is not null)
            {
                assignedSlot.Socket = ws;
                assignedSlot.ClientId = assignedClientId;
                assignedSlot.StoredTelemetry = null;
                assignedSlot.LatestUnreadJson = null;
                assignedSlot.LastServerPlayerId = null;
            }
        }

        var buffer = ArrayPool<byte>.Shared.Rent(8192);
        var writer = new ArrayBufferWriter<byte>(1024);

        try
        {
            while (!requestAborted.IsCancellationRequested && ws.State == WebSocketState.Open)
            {
                writer.Clear();
                ValueWebSocketReceiveResult res;
                do
                {
                    res = await ws.ReceiveAsync(buffer.AsMemory(), requestAborted);
                    if (res.MessageType == WebSocketMessageType.Close)
                    {
                        return;
                    }

                    if (res.MessageType == WebSocketMessageType.Text && res.Count > 0)
                    {
                        writer.Write(buffer.AsSpan(0, res.Count));
                    }
                }
                while (!res.EndOfMessage);

                // PROTOCOL.md section 3.1 & 2: 3rd machine accepted on socket but ignored; binary frames discarded.
                if (assignedSlot is not null && res.MessageType == WebSocketMessageType.Text && writer.WrittenCount > 0)
                {
                    var text = Encoding.UTF8.GetString(writer.WrittenSpan);
                    Interlocked.Exchange(ref assignedSlot.LatestUnreadJson, text);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (WebSocketException)
        {
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);

            lock (_slotLock)
            {
                _allSockets.Remove(ws);
                if (assignedSlot is not null && ReferenceEquals(assignedSlot.Socket, ws))
                {
                    assignedSlot.Socket = null;
                    assignedSlot.StoredTelemetry = null;
                    assignedSlot.LatestUnreadJson = null;
                    assignedSlot.LastServerPlayerId = null;

                    // PROTOCOL.md section 5: in any state other than MULTIPLAYER_IDLE, disconnect returns to IDLE.
                    if (_matchState != ProtocolConstants.ServerIdle)
                    {
                        _matchState = ProtocolConstants.ServerIdle;
                    }
                }
            }

            if (ws.State is WebSocketState.Open or WebSocketState.CloseReceived)
            {
                try
                {
                    using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
                    await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closed", cts.Token);
                }
                catch
                {
                }
            }
        }
    }

    private async Task RunServerLoopAsync(CancellationToken cancellationToken)
    {
        var tickCounter = 0;
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(1));

        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            tickCounter++;
            var nowMs = _monotonicClock.ElapsedMilliseconds;

            string? broadcastCmd = null;
            (WebSocket Target, string Payload)? targetedCmd = null;

            lock (_slotLock)
            {
                ProcessSlotNewestMessage(_slots[0], nowMs);
                ProcessSlotNewestMessage(_slots[1], nowMs);

                var state1 = GetSlotMultiplayerState(_slots[0]);
                var state2 = GetSlotMultiplayerState(_slots[1]);

                switch (_matchState)
                {
                    case ProtocolConstants.ServerIdle:
                        if (state1 == ProtocolConstants.StateReady || state2 == ProtocolConstants.StateReady)
                        {
                            _matchState = ProtocolConstants.ServerWaiting;
                        }
                        break;

                    case ProtocolConstants.ServerWaiting:
                        if (state1 == ProtocolConstants.StateReady && state2 == ProtocolConstants.StateReady)
                        {
                            _matchState = ProtocolConstants.ServerMidMatch;
                            broadcastCmd = "{\"cmd\":{\"START\":1}}";
                        }
                        break;

                    case ProtocolConstants.ServerMidMatch:
                        // PROTOCOL.md section 6.2: Machine 1's report is checked first.
                        if (state1 == ProtocolConstants.StateGameOverWin)
                        {
                            _matchState = ProtocolConstants.ServerPostMatch;
                            _postMatchEnteredAtMs = nowMs;
                            if (_slots[1].Socket is { State: WebSocketState.Open } m2)
                            {
                                targetedCmd = (m2, "{\"cmd\":{\"YOU_LOSE\":1}}");
                            }
                        }
                        else if (state1 == ProtocolConstants.StateGameOverLose)
                        {
                            _matchState = ProtocolConstants.ServerPostMatch;
                            _postMatchEnteredAtMs = nowMs;
                            if (_slots[1].Socket is { State: WebSocketState.Open } m2)
                            {
                                targetedCmd = (m2, "{\"cmd\":{\"YOU_WIN\":0}}");
                            }
                        }
                        else if (state2 == ProtocolConstants.StateGameOverWin)
                        {
                            _matchState = ProtocolConstants.ServerPostMatch;
                            _postMatchEnteredAtMs = nowMs;
                            if (_slots[0].Socket is { State: WebSocketState.Open } m1)
                            {
                                targetedCmd = (m1, "{\"cmd\":{\"YOU_LOSE\":0}}");
                            }
                        }
                        else if (state2 == ProtocolConstants.StateGameOverLose)
                        {
                            _matchState = ProtocolConstants.ServerPostMatch;
                            _postMatchEnteredAtMs = nowMs;
                            if (_slots[0].Socket is { State: WebSocketState.Open } m1)
                            {
                                targetedCmd = (m1, "{\"cmd\":{\"YOU_WIN\":0}}");
                            }
                        }
                        break;

                    case ProtocolConstants.ServerPostMatch:
                        if (nowMs - _postMatchEnteredAtMs >= (long)_postMatchDuration.TotalMilliseconds)
                        {
                            _matchState = ProtocolConstants.ServerIdle;
                        }
                        break;
                }
            }

            if (broadcastCmd is not null)
            {
                await BroadcastToAllAsync(broadcastCmd, cancellationToken);
            }

            if (targetedCmd is { } cmd)
            {
                await SendTextSafeAsync(cmd.Target, cmd.Payload, cancellationToken);
            }

            if (tickCounter % 10 == 0)
            {
                (WebSocket? Sock1, string Msg1, WebSocket? Sock2, string Msg2) relayBatch;
                lock (_slotLock)
                {
                    var hand1 = GetSlotHand(_slots[0]);
                    var hand2 = GetSlotHand(_slots[1]);
                    var flip = TorqueSignRule.DoesServerFlipOpponentTorque(hand1, hand2);

                    var msgForSlot1 = BuildRelayedDataJson(_slots[1], flip);
                    var msgForSlot2 = BuildRelayedDataJson(_slots[0], flip);

                    relayBatch = (_slots[0].Socket, msgForSlot1, _slots[1].Socket, msgForSlot2);
                }

                if (relayBatch.Sock1 is { State: WebSocketState.Open })
                {
                    await SendTextSafeAsync(relayBatch.Sock1, relayBatch.Msg1, cancellationToken);
                }

                if (relayBatch.Sock2 is { State: WebSocketState.Open })
                {
                    await SendTextSafeAsync(relayBatch.Sock2, relayBatch.Msg2, cancellationToken);
                }
            }
        }
    }

    private static void ProcessSlotNewestMessage(SlotState slot, long nowMs)
    {
        if (slot.Socket is null)
        {
            return;
        }

        var newestJson = Interlocked.Exchange(ref slot.LatestUnreadJson, null);
        if (newestJson is null)
        {
            return;
        }

        try
        {
            var rootNode = JsonNode.Parse(newestJson) as JsonObject;
            if (rootNode is null || rootNode.Count == 0)
            {
                return;
            }

            // PROTOCOL.md section 3.1: alphabetically first top-level key
            var firstKey = rootNode
                .Select(kv => kv.Key)
                .OrderBy(k => k, StringComparer.Ordinal)
                .First();

            if (rootNode[firstKey] is not JsonObject telemetryObj)
            {
                return;
            }

            var stored = new JsonObject();
            foreach (var kv in telemetryObj)
            {
                stored[kv.Key] = kv.Value?.DeepClone();
            }

            if (stored["server_player_id"] is JsonValue spid && spid.TryGetValue<string>(out var spidStr))
            {
                slot.LastServerPlayerId = spidStr;
            }

            stored[ProtocolConstants.FieldIsConnected] = true;
            stored[ProtocolConstants.FieldClientId] = slot.ClientId;
            stored[ProtocolConstants.FieldLastMessageTimestamp] = nowMs;

            slot.StoredTelemetry = stored;
        }
        catch (JsonException)
        {
            // PROTOCOL.md section 4.1: log error and keep previous telemetry.
        }
    }

    private static string BuildRelayedDataJson(SlotState sourceSlot, bool flipTorque)
    {
        if (sourceSlot.Socket is null || sourceSlot.StoredTelemetry is null)
        {
            // PROTOCOL.md section 4.4: if remaining machine's hand is "left", {"data":{"opponent_torque":-0.0}}
            return flipTorque
                ? "{\"data\":{\"opponent_torque\":-0.0}}"
                : "{\"data\":{}}";
        }

        var clone = (JsonObject)sourceSlot.StoredTelemetry.DeepClone();
        var opponentTorque = 0.0;
        if (clone["opponent_torque"] is JsonValue val && val.TryGetValue<double>(out var parsedTorque))
        {
            opponentTorque = parsedTorque;
        }

        if (flipTorque)
        {
            clone["opponent_torque"] = -opponentTorque;
        }

        var envelope = new JsonObject
        {
            ["data"] = clone
        };

        return envelope.ToJsonString();
    }

    private static string GetSlotMultiplayerState(SlotState slot)
    {
        if (slot.StoredTelemetry?["multiplayer_state"] is JsonValue v && v.TryGetValue<string>(out var s))
        {
            return s;
        }

        return "";
    }

    private static string GetSlotHand(SlotState slot)
    {
        if (slot.StoredTelemetry?["hand"] is JsonValue v && v.TryGetValue<string>(out var s))
        {
            return s;
        }

        return "";
    }

    private async Task BroadcastToAllAsync(string json, CancellationToken cancellationToken)
    {
        WebSocket[] targets;
        lock (_slotLock)
        {
            targets = _allSockets.ToArray();
        }

        foreach (var sock in targets)
        {
            if (sock.State == WebSocketState.Open)
            {
                await SendTextSafeAsync(sock, json, cancellationToken);
            }
        }
    }

    private static async Task SendTextSafeAsync(WebSocket socket, string json, CancellationToken cancellationToken)
    {
        try
        {
            var bytes = Encoding.UTF8.GetBytes(json);
            await socket.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, cancellationToken);
        }
        catch
        {
        }
    }

    private static int AllocateFreeTcpPort(IPAddress bindAddress)
    {
        using var listener = new TcpListener(bindAddress, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private sealed class SlotState(int slotNumber)
    {
        public int SlotNumber { get; } = slotNumber;
        public WebSocket? Socket { get; set; }
        public int ClientId { get; set; }
        public JsonObject? StoredTelemetry { get; set; }
        public string? LatestUnreadJson;
        public string? LastServerPlayerId { get; set; }
    }
}
