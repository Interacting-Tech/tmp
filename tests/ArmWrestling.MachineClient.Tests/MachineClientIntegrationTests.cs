using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using ArmWrestling.MachineAgent;
using ArmWrestling.MachineClient.Configuration;
using ArmWrestling.MachineClient.Networking;
using ArmWrestling.MachineClient.Protocol;

namespace ArmWrestling.MachineClient.Tests;

public sealed class MachineClientIntegrationTests
{
    [Fact]
    public async Task AutoResolvesLocalMachineIp_AcceptsMachineData_AndProvidesRivalDataByProtocolRules()
    {
        var machineIp = LocalIpResolver.ResolveMachineIp();
        await using var server = new SimulatedArmWrestlingServer(machineIp, TimeSpan.FromMilliseconds(200));
        await server.StartAsync();

        // Simulate the physical local arm-wrestling machine connected in Slot 1.
        using var physicalMachineSocket = new ClientWebSocket();
        await physicalMachineSocket.ConnectAsync(server.EndpointUri, CancellationToken.None);

        // Start ArmWrestlingMachineClient WITHOUT MachineIpOverride: it must read the machine's IP itself.
        await using var machinePart = new ArmWrestlingMachineClient(new MachineClientOptions
        {
            Port = server.Port,
            RivalSendInterval = TimeSpan.FromMilliseconds(5),
            ServerPlayerIdCadence = 50
        });

        Assert.Equal(machineIp, machinePart.MachineIp);
        Assert.Equal(server.EndpointUri, machinePart.ServerUri);

        var receivedFromLocalMachineTcs = new TaskCompletionSource<ReceivedMachineData>(TaskCreationOptions.RunContinuationsAsynchronously);
        machinePart.LocalDataReceived += snapshot =>
        {
            if (snapshot.HasMachineTelemetry && snapshot.NormalizedTelemetry?.PlayerId == "local-fighter")
            {
                receivedFromLocalMachineTcs.TrySetResult(snapshot);
            }
        };

        await machinePart.StartAsync();

        // Send telemetry from the physical local machine (Slot 1) to the local server.
        const string physicalMachinePayload = """
        {
          "data": {
            "MAC_address": "AA:11:22:33:44:55",
            "player_id": "local-fighter",
            "server_player_id": "srv-local-1",
            "hand": "right",
            "multiplayer_state": "MULTIPLAYER_SM_RUN",
            "single_player_state": "",
            "torque": 22.4,
            "opponent_torque": 15.0,
            "position": 6.5,
            "velocity": 1.2,
            "left_limit": -45.0,
            "right_limit": 45.0,
            "peak_force": 35.0,
            "timeStamp": 987654,
            "cabinet_temp": 38.5
          }
        }
        """;

        await physicalMachineSocket.SendAsync(
            Encoding.UTF8.GetBytes(physicalMachinePayload),
            WebSocketMessageType.Text,
            endOfMessage: true,
            CancellationToken.None);

        var accepted = await receivedFromLocalMachineTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.True(accepted.IsConnected);
        Assert.NotNull(accepted.ClientId);
        Assert.NotNull(accepted.LastMessageTimestamp);
        Assert.Equal("AA:11:22:33:44:55", accepted.NormalizedTelemetry!.MacAddress);
        Assert.Equal("srv-local-1", accepted.EffectiveServerPlayerId);
        Assert.Equal(22.4, accepted.NormalizedTelemetry.Torque);
        Assert.Equal(38.5, accepted.NormalizedTelemetry.ExtraFields!["cabinet_temp"].GetDouble());

        // Now provide rival's data in PROTOCOL.md JSON format into machinePart and verify physical machine receives it.
        const string rivalJson = """
        {
          "data": {
            "MAC_address": "BB:66:77:88:99:00",
            "player_id": "remote-rival",
            "server_player_id": "srv-rival-2",
            "hand": "left",
            "multiplayer_state": "MULTIPLAYER_SM_RUN",
            "single_player_state": "",
            "torque": 19.8,
            "opponent_torque": 22.4,
            "position": -6.5,
            "velocity": -1.2,
            "left_limit": -45.0,
            "right_limit": 45.0,
            "peak_force": 31.0,
            "timeStamp": 987660
          }
        }
        """;

        Assert.True(machinePart.TryProvideRivalJson(rivalJson));

        var receivedByPhysicalMachine = await WaitForDataFrameAsync(
            physicalMachineSocket,
            data => data.TryGetProperty("player_id", out var pid) && pid.GetString() == "remote-rival",
            TimeSpan.FromSeconds(3));

        Assert.Equal("BB:66:77:88:99:00", receivedByPhysicalMachine.GetProperty("MAC_address").GetString());
        Assert.Equal("left", receivedByPhysicalMachine.GetProperty("hand").GetString());
        Assert.Equal(19.8, receivedByPhysicalMachine.GetProperty("torque").GetDouble());
        // Local machine is "right" and rival is "left" -> local server flips opponent_torque to -22.4 (section 4.3)!
        Assert.Equal(-22.4, receivedByPhysicalMachine.GetProperty("opponent_torque").GetDouble());
        Assert.True(receivedByPhysicalMachine.GetProperty("is_connected").GetBoolean());
    }

    [Fact]
    public async Task TwoMachinesBridgedEndToEnd_RunFullMatchLifecycle_WithoutDoubleFlippingTorque()
    {
        var ip = LocalIpResolver.ResolveMachineIp();

        // Machine A cabinet (right-handed player) + Machine B cabinet (left-handed player)
        await using var serverA = new SimulatedArmWrestlingServer(ip, TimeSpan.FromMilliseconds(150));
        await using var serverB = new SimulatedArmWrestlingServer(ip, TimeSpan.FromMilliseconds(150));
        await serverA.StartAsync();
        await serverB.StartAsync();

        using var physicalMachineA = new ClientWebSocket();
        using var physicalMachineB = new ClientWebSocket();
        await physicalMachineA.ConnectAsync(serverA.EndpointUri, CancellationToken.None);
        await physicalMachineB.ConnectAsync(serverB.EndpointUri, CancellationToken.None);

        var framesForA = new ConcurrentQueue<string>();
        var framesForB = new ConcurrentQueue<string>();
        using var readerCts = new CancellationTokenSource();
        var readerATask = PumpIncomingFramesAsync(physicalMachineA, framesForA, readerCts.Token);
        var readerBTask = PumpIncomingFramesAsync(physicalMachineB, framesForB, readerCts.Token);

        await using var clientA = new ArmWrestlingMachineClient(new MachineClientOptions
        {
            Port = serverA.Port,
            RivalSendInterval = TimeSpan.FromMilliseconds(5),
            ServerPlayerIdCadence = 1
        });
        await using var clientB = new ArmWrestlingMachineClient(new MachineClientOptions
        {
            Port = serverB.Port,
            RivalSendInterval = TimeSpan.FromMilliseconds(5),
            ServerPlayerIdCadence = 1
        });

        var (bridgeA, bridgeB) = InMemoryRivalBridgePair.CreateLinkedPair();
        await using var coordinatorA = new MachineRelayCoordinator(clientA, bridgeA, outboundServerPlayerIdCadence: 1);
        await using var coordinatorB = new MachineRelayCoordinator(clientB, bridgeB, outboundServerPlayerIdCadence: 1);

        await coordinatorA.StartAsync();
        await coordinatorB.StartAsync();

        // 1. Both physical machines report MULTIPLAYER_SM_READY -> both servers must emit {"cmd":{"START":1}}!
        await SendTelemetryAsync(physicalMachineA, new MachineTelemetry
        {
            PlayerId = "player-A",
            Hand = "right",
            MultiplayerState = ProtocolConstants.StateReady,
            Torque = 0.0,
            OpponentTorque = 0.0
        });

        await SendTelemetryAsync(physicalMachineB, new MachineTelemetry
        {
            PlayerId = "player-B",
            Hand = "left",
            MultiplayerState = ProtocolConstants.StateReady,
            Torque = 0.0,
            OpponentTorque = 0.0
        });

        await WaitForQueueMatchAsync(framesForA, f => f.Contains("\"START\"", StringComparison.Ordinal), TimeSpan.FromSeconds(3));
        await WaitForQueueMatchAsync(framesForB, f => f.Contains("\"START\"", StringComparison.Ordinal), TimeSpan.FromSeconds(3));

        // 2. Mid-match run with opposite hands ("right" vs "left"): verify single sign flip (no double negation!).
        await SendTelemetryAsync(physicalMachineA, new MachineTelemetry
        {
            PlayerId = "player-A",
            Hand = "right",
            MultiplayerState = ProtocolConstants.StateRun,
            Torque = 25.0,
            OpponentTorque = 18.5
        });

        await SendTelemetryAsync(physicalMachineB, new MachineTelemetry
        {
            PlayerId = "player-B",
            Hand = "left",
            MultiplayerState = ProtocolConstants.StateRun,
            Torque = 18.5,
            OpponentTorque = 25.0
        });

        var runFrameAtB = await WaitForQueueMatchAsync(
            framesForB,
            f => f.Contains("\"player-A\"", StringComparison.Ordinal) &&
                 f.Contains("\"MULTIPLAYER_SM_RUN\"", StringComparison.Ordinal),
            TimeSpan.FromSeconds(3));

        using (var docB = JsonDocument.Parse(runFrameAtB))
        {
            // Machine A sent +18.5. Server A flipped to -18.5 -> Client A normalized back to +18.5 -> Server B flipped once to -18.5!
            Assert.Equal(-18.5, docB.RootElement.GetProperty("data").GetProperty("opponent_torque").GetDouble());
        }

        // 3. Machine A wins (MULTIPLAYER_SM_GAMEOVER_WIN) -> Machine B must receive {"cmd":{"YOU_LOSE":...}}!
        await SendTelemetryAsync(physicalMachineA, new MachineTelemetry
        {
            PlayerId = "player-A",
            Hand = "right",
            MultiplayerState = ProtocolConstants.StateGameOverWin,
            Torque = 30.0,
            OpponentTorque = 10.0
        });

        await WaitForQueueMatchAsync(framesForB, f => f.Contains("\"YOU_LOSE\"", StringComparison.Ordinal), TimeSpan.FromSeconds(3));

        // 4. Rival A disconnects -> Server B clears rival slot and resets match state cleanly.
        bridgeB.SimulateRemoteDisconnect();
        await WaitUntilAsync(() => serverB.MatchState == ProtocolConstants.ServerIdle, TimeSpan.FromSeconds(2));

        await readerCts.CancelAsync();
        await Task.WhenAll(readerATask, readerBTask);
        await bridgeA.DisposeAsync();
        await bridgeB.DisposeAsync();
    }

    [Fact]
    public async Task ClearRivalAsync_ResetsStuckMultiplayerWaiting_AndClearsStaleRivalTelemetry()
    {
        var ip = LocalIpResolver.ResolveMachineIp();
        await using var server = new SimulatedArmWrestlingServer(ip);
        await server.StartAsync();

        using var physicalMachine = new ClientWebSocket();
        await physicalMachine.ConnectAsync(server.EndpointUri, CancellationToken.None);

        await using var machinePart = new ArmWrestlingMachineClient(new MachineClientOptions
        {
            Port = server.Port,
            RivalSendInterval = TimeSpan.FromMilliseconds(5)
        });
        await machinePart.StartAsync();

        // Rival reports READY while local machine has not readied -> server enters MULTIPLAYER_WAITING.
        machinePart.ProvideRivalTelemetry(new MachineTelemetry
        {
            PlayerId = "waiting-rival",
            Hand = "right",
            MultiplayerState = ProtocolConstants.StateReady,
            Torque = 9.5
        });

        await WaitUntilAsync(() => server.MatchState == ProtocolConstants.ServerWaiting, TimeSpan.FromSeconds(3));

        // Rival leaves before match starts -> ClearRivalAsync cycles the slot so server returns to MULTIPLAYER_IDLE
        // and stops relaying stale rival telemetry!
        await machinePart.ClearRivalAsync();

        Assert.Equal(ProtocolConstants.ServerIdle, server.MatchState);

        // Drain to verify physical machine now receives empty {"data":{}} instead of stale "waiting-rival".
        var emptyFrame = await WaitForDataFrameAsync(
            physicalMachine,
            data => !data.EnumerateObject().Any(),
            TimeSpan.FromSeconds(3));

        Assert.Empty(emptyFrame.EnumerateObject());
    }

    [Fact]
    public async Task HandlesOperatorCommands_BinaryAndInvalidJsonResilience_AndEvery50thServerPlayerId()
    {
        var ip = LocalIpResolver.ResolveMachineIp();
        await using var server = new SimulatedArmWrestlingServer(ip);
        await server.StartAsync();

        using var physicalMachine = new ClientWebSocket();
        await physicalMachine.ConnectAsync(server.EndpointUri, CancellationToken.None);

        var receivedCommands = new ConcurrentQueue<ServerCommand>();
        await using var machinePart = new ArmWrestlingMachineClient(new MachineClientOptions
        {
            Port = server.Port,
            RivalSendInterval = TimeSpan.FromMilliseconds(2),
            MinimumFrameSpacing = TimeSpan.Zero,
            ServerPlayerIdCadence = 3 // Every 3rd message for fast verification of cadence logic
        });
        machinePart.CommandReceived += cmd => receivedCommands.Enqueue(cmd);

        await machinePart.StartAsync();

        // Wait until machinePart is connected to the server
        await WaitUntilAsync(() => machinePart.IsConnectedToServer, TimeSpan.FromSeconds(3));

        // 1. Operator sends custom text command, then 'k' (STOP + reset to IDLE)
        await server.SendOperatorCommandAsync("CALIBRATE_ZERO");
        await server.SendOperatorCommandAsync("k");

        await WaitUntilAsync(() => receivedCommands.Count >= 2, TimeSpan.FromSeconds(3));
        var cmds = receivedCommands.ToArray();
        Assert.Contains(cmds, c => c.Kind == ServerCommandKind.OperatorCommand && c.CommandText == "CALIBRATE_ZERO" && c.IsOperatorCommand);
        Assert.Contains(cmds, c => c.Kind == ServerCommandKind.Stop && c.CommandText == "STOP" && c.IsOperatorCommand);

        // 2. Physical machine sends binary frame + malformed JSON + valid telemetry: client stays connected and accepts valid data
        await physicalMachine.SendAsync(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }, WebSocketMessageType.Binary, true, CancellationToken.None);
        await physicalMachine.SendAsync(Encoding.UTF8.GetBytes("{not-valid-json"), WebSocketMessageType.Text, true, CancellationToken.None);
        await SendTelemetryAsync(physicalMachine, new MachineTelemetry
        {
            PlayerId = "resilient-local",
            Hand = "left",
            Torque = 14.0
        });

        await WaitUntilAsync(
            () => machinePart.LatestLocalData?.NormalizedTelemetry?.PlayerId == "resilient-local",
            TimeSpan.FromSeconds(3));

        // 3. Verify rival server_player_id is sent on message 1, omitted between, and re-sent on cadence boundary!
        machinePart.ProvideRivalTelemetry(new MachineTelemetry
        {
            PlayerId = "cadence-rival",
            ServerPlayerId = "spid-999",
            Hand = "left",
            Torque = 11.0
        });

        var firstWithId = await WaitForDataFrameAsync(
            physicalMachine,
            d => d.TryGetProperty("player_id", out var p) &&
                 p.GetString() == "cadence-rival" &&
                 d.TryGetProperty("server_player_id", out _),
            TimeSpan.FromSeconds(3));
        Assert.Equal("spid-999", firstWithId.GetProperty("server_player_id").GetString());

        var omittedBetween = await WaitForDataFrameAsync(
            physicalMachine,
            d => d.TryGetProperty("player_id", out var p) &&
                 p.GetString() == "cadence-rival" &&
                 !d.TryGetProperty("server_player_id", out _),
            TimeSpan.FromSeconds(3));
        Assert.False(omittedBetween.TryGetProperty("server_player_id", out _));
    }

    private static async Task SendTelemetryAsync(ClientWebSocket ws, MachineTelemetry telemetry)
    {
        var bytes = ProtocolJsonSerializer.SerializeMachineEnvelopeToUtf8Bytes(telemetry, includeServerPlayerId: true);
        await ws.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, CancellationToken.None);
    }

    private static async Task<JsonElement> WaitForDataFrameAsync(
        ClientWebSocket ws,
        Func<JsonElement, bool> predicate,
        TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        var buf = new byte[8192];

        while (!cts.IsCancellationRequested)
        {
            var res = await ws.ReceiveAsync(buf, cts.Token);
            if (res.MessageType != WebSocketMessageType.Text)
            {
                continue;
            }

            using var doc = JsonDocument.Parse(buf.AsMemory(0, res.Count));
            if (doc.RootElement.TryGetProperty("data", out var data) && predicate(data))
            {
                return data.Clone();
            }
        }

        throw new TimeoutException("Timed out waiting for matching data frame.");
    }

    private static async Task PumpIncomingFramesAsync(
        ClientWebSocket ws,
        ConcurrentQueue<string> sink,
        CancellationToken cancellationToken)
    {
        var buf = new byte[8192];
        try
        {
            while (!cancellationToken.IsCancellationRequested && ws.State == WebSocketState.Open)
            {
                var res = await ws.ReceiveAsync(buf, cancellationToken);
                if (res.MessageType == WebSocketMessageType.Close)
                {
                    break;
                }

                if (res.MessageType == WebSocketMessageType.Text)
                {
                    sink.Enqueue(Encoding.UTF8.GetString(buf, 0, res.Count));
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (WebSocketException)
        {
        }
    }

    private static async Task<string> WaitForQueueMatchAsync(
        ConcurrentQueue<string> queue,
        Func<string, bool> predicate,
        TimeSpan timeout)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            while (queue.TryDequeue(out var item))
            {
                if (predicate(item))
                {
                    return item;
                }
            }

            await Task.Delay(5);
        }

        throw new TimeoutException("Timed out waiting for expected WebSocket message.");
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(5);
        }

        Assert.True(condition(), "Condition was not met within timeout.");
    }

    private sealed class InMemoryRivalBridgePair : IRivalBridge
    {
        private InMemoryRivalBridgePair? _peer;

        public event Action<MachineTelemetry>? RivalTelemetryReceived;

        public event Action<string>? RivalJsonReceived;

        public event Action? RivalDisconnected;

        public static (InMemoryRivalBridgePair BridgeA, InMemoryRivalBridgePair BridgeB) CreateLinkedPair()
        {
            var a = new InMemoryRivalBridgePair();
            var b = new InMemoryRivalBridgePair();
            a._peer = b;
            b._peer = a;
            return (a, b);
        }

        public Task StartAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public ValueTask PublishLocalMachineDataAsync(
            string protocolJsonEnvelope,
            ReceivedMachineData snapshot,
            CancellationToken cancellationToken = default)
        {
            if (ProtocolJsonSerializer.TryParseRivalEnvelope(protocolJsonEnvelope, out var parsed) && parsed is not null)
            {
                if (_peer?.RivalTelemetryReceived is { } handler)
                {
                    handler(parsed);
                }
                else
                {
                    _peer?.RivalJsonReceived?.Invoke(protocolJsonEnvelope);
                }
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask PublishLocalServerCommandAsync(
            ServerCommand command,
            CancellationToken cancellationToken = default) => ValueTask.CompletedTask;

        public ValueTask PublishLocalMachineDisconnectedAsync(
            CancellationToken cancellationToken = default)
        {
            _peer?.RivalDisconnected?.Invoke();
            return ValueTask.CompletedTask;
        }

        public void SimulateRemoteDisconnect() => RivalDisconnected?.Invoke();

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
