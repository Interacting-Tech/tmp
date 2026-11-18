using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text.Json;
using ArmWrestling.MachineClient.Configuration;
using ArmWrestling.MachineClient.Networking;
using ArmWrestling.MachineClient.Protocol;

namespace ArmWrestling.MachineClient.Tests;

public sealed class ProtocolAndIpUnitTests
{
    [Fact]
    public void LocalIpResolver_ReadsLocalMachineIp_AndBuildsProtocolWebSocketUri()
    {
        var resolvedIp = LocalIpResolver.ResolveMachineIp();
        Assert.NotNull(resolvedIp);
        Assert.Equal(AddressFamily.InterNetwork, resolvedIp.AddressFamily);

        var allLocalIpv4s = NetworkInterface.GetAllNetworkInterfaces()
            .SelectMany(n => n.GetIPProperties().UnicastAddresses)
            .Select(u => u.Address)
            .Where(a => a.AddressFamily == AddressFamily.InterNetwork)
            .ToHashSet();

        Assert.Contains(resolvedIp, allLocalIpv4s);

        var uri = LocalIpResolver.BuildMachineWebSocketUri(resolvedIp);
        Assert.Equal($"ws://{resolvedIp}:8080/ws?role=machine", uri.ToString());
    }

    [Fact]
    public void LocalIpResolver_RespectsOverrideAndCustomPort()
    {
        var options = new MachineClientOptions
        {
            MachineIpOverride = "192.168.10.42",
            Port = 9090
        };

        var ip = LocalIpResolver.ResolveMachineIp(options);
        var uri = LocalIpResolver.BuildMachineWebSocketUri(ip, options);

        Assert.Equal(IPAddress.Parse("192.168.10.42"), ip);
        Assert.Equal("ws://192.168.10.42:9090/ws?role=machine", uri.ToString());
    }

    [Fact]
    public void SerializeMachineEnvelope_MatchesExactProtocolJsonShapeAndPreservesExtraFields()
    {
        var telemetry = new MachineTelemetry
        {
            MacAddress = "AA:BB:CC:DD:EE:FF",
            PlayerId = "player-1",
            ServerPlayerId = "abc123",
            Hand = "RIGHT",
            MultiplayerState = ProtocolConstants.StateRun,
            SinglePlayerState = "",
            Torque = 12.5,
            OpponentTorque = 10.2,
            Position = 3.1,
            Velocity = -0.4,
            LeftLimit = -45.0,
            RightLimit = 45.0,
            PeakForce = 30.0,
            TimeStamp = 123456789UL,
            ExtraFields = new Dictionary<string, JsonElement>
            {
                ["firmware_version"] = JsonSerializer.SerializeToElement("2.4.1"),
                // Server-injected fields must never leak into outbound machine payloads.
                ["is_connected"] = JsonSerializer.SerializeToElement(true),
                ["client_id"] = JsonSerializer.SerializeToElement(99),
                ["last_message_timestamp"] = JsonSerializer.SerializeToElement(55555)
            }
        };

        var jsonWithId = ProtocolJsonSerializer.SerializeMachineEnvelope(telemetry, includeServerPlayerId: true);
        using var doc1 = JsonDocument.Parse(jsonWithId);
        var rootKeys = doc1.RootElement.EnumerateObject().Select(p => p.Name).ToArray();
        Assert.Single(rootKeys);
        Assert.Equal("data", rootKeys[0]);

        var data = doc1.RootElement.GetProperty("data");
        Assert.Equal("AA:BB:CC:DD:EE:FF", data.GetProperty("MAC_address").GetString());
        Assert.Equal("player-1", data.GetProperty("player_id").GetString());
        Assert.Equal("abc123", data.GetProperty("server_player_id").GetString());
        Assert.Equal("right", data.GetProperty("hand").GetString());
        Assert.Equal("MULTIPLAYER_SM_RUN", data.GetProperty("multiplayer_state").GetString());
        Assert.Equal("", data.GetProperty("single_player_state").GetString());
        Assert.Equal(12.5, data.GetProperty("torque").GetDouble());
        Assert.Equal(10.2, data.GetProperty("opponent_torque").GetDouble());
        Assert.Equal(3.1, data.GetProperty("position").GetDouble());
        Assert.Equal(-0.4, data.GetProperty("velocity").GetDouble());
        Assert.Equal(-45.0, data.GetProperty("left_limit").GetDouble());
        Assert.Equal(45.0, data.GetProperty("right_limit").GetDouble());
        Assert.Equal(30.0, data.GetProperty("peak_force").GetDouble());
        Assert.Equal(123456789UL, data.GetProperty("timeStamp").GetUInt64());
        Assert.Equal("2.4.1", data.GetProperty("firmware_version").GetString());

        Assert.False(data.TryGetProperty("is_connected", out _));
        Assert.False(data.TryGetProperty("client_id", out _));
        Assert.False(data.TryGetProperty("last_message_timestamp", out _));

        // When includeServerPlayerId is false (messages 2..50), server_player_id is omitted.
        var jsonWithoutId = ProtocolJsonSerializer.SerializeMachineEnvelope(telemetry, includeServerPlayerId: false);
        using var doc2 = JsonDocument.Parse(jsonWithoutId);
        Assert.False(doc2.RootElement.GetProperty("data").TryGetProperty("server_player_id", out _));
    }

    [Fact]
    public void TryParseRivalEnvelope_AppliesAlphabeticalFirstKeyAndSection32Defaults()
    {
        // "zeta" appears first in text, but "alpha" is first alphabetically (PROTOCOL.md section 3.1).
        const string json = """
        {
          "zeta": { "player_id": "wrong", "torque": 999 },
          "alpha": { "player_id": "correct-rival", "torque": 18.75, "custom_tag": 42 }
        }
        """;

        Assert.True(ProtocolJsonSerializer.TryParseRivalEnvelope(json, out var parsed));
        Assert.NotNull(parsed);
        Assert.Equal("correct-rival", parsed.PlayerId);
        Assert.Equal(18.75, parsed.Torque);
        Assert.Equal("", parsed.MacAddress);
        Assert.Equal("", parsed.Hand);
        Assert.Equal("", parsed.MultiplayerState);
        Assert.Equal("", parsed.SinglePlayerState);
        Assert.Equal(0.0, parsed.OpponentTorque);
        Assert.Equal(0.0, parsed.Position);
        Assert.Equal(0.0, parsed.Velocity);
        Assert.Equal(0.0, parsed.LeftLimit);
        Assert.Equal(0.0, parsed.RightLimit);
        Assert.Equal(0.0, parsed.PeakForce);
        Assert.Equal(0UL, parsed.TimeStamp);
        Assert.NotNull(parsed.ExtraFields);
        Assert.Equal(42, parsed.ExtraFields["custom_tag"].GetInt32());
    }

    [Fact]
    public void TryParseServerMessage_HandlesBothCommandForms_DisconnectFrames_AndServerPlayerIdCaching()
    {
        // 1. Match commands (object form, section 6.2)
        Assert.True(ProtocolJsonSerializer.TryParseServerMessage(
            "{\"cmd\":{\"START\":1}}"u8, null, null, "", true, out var startMsg));
        Assert.Equal(ServerCommandKind.Start, startMsg!.Command!.Kind);
        Assert.False(startMsg.Command.IsOperatorCommand);
        Assert.Null(startMsg.Command.InferredLocalMachineGameOverState);

        Assert.True(ProtocolJsonSerializer.TryParseServerMessage(
            "{\"cmd\":{\"YOU_LOSE\":1}}"u8, null, null, "", true, out var loseMsg));
        Assert.Equal(ServerCommandKind.YouLose, loseMsg!.Command!.Kind);
        Assert.Equal(ProtocolConstants.StateGameOverWin, loseMsg.Command.InferredLocalMachineGameOverState);

        Assert.True(ProtocolJsonSerializer.TryParseServerMessage(
            "{\"cmd\":{\"YOU_WIN\":0}}"u8, null, null, "", true, out var winMsg));
        Assert.Equal(ServerCommandKind.YouWin, winMsg!.Command!.Kind);
        Assert.Equal(ProtocolConstants.StateGameOverLose, winMsg.Command.InferredLocalMachineGameOverState);

        // 2. Operator commands (string form, section 6.3)
        Assert.True(ProtocolJsonSerializer.TryParseServerMessage(
            "{\"cmd\":\"STOP\"}"u8, null, null, "", true, out var stopMsg));
        Assert.Equal(ServerCommandKind.Stop, stopMsg!.Command!.Kind);
        Assert.True(stopMsg.Command.IsOperatorCommand);

        Assert.True(ProtocolJsonSerializer.TryParseServerMessage(
            "{\"cmd\":\"RESET_CALIB\"}"u8, null, null, "", true, out var opMsg));
        Assert.Equal(ServerCommandKind.OperatorCommand, opMsg!.Command!.Kind);
        Assert.Equal("RESET_CALIB", opMsg.Command.CommandText);

        // 3. Empty data and section 4.4 left-hand disconnect {"data":{"opponent_torque":-0.0}}
        Assert.True(ProtocolJsonSerializer.TryParseServerMessage(
            "{\"data\":{}}"u8, "cached-id", 100L, "right", true, out var emptyData));
        Assert.False(emptyData!.TelemetryData!.HasMachineTelemetry);
        Assert.False(emptyData.TelemetryData.IsConnected);

        Assert.True(ProtocolJsonSerializer.TryParseServerMessage(
            "{\"data\":{\"opponent_torque\":-0.0}}"u8, "cached-id", 100L, "left", true, out var leftDiscData));
        Assert.False(leftDiscData!.TelemetryData!.HasMachineTelemetry);
        Assert.False(leftDiscData.TelemetryData.IsConnected);

        // 4. Caching server_player_id across messages where it is omitted + IsNewSample detection
        const string frame50 = """
        {
          "data": {
            "player_id": "p1",
            "server_player_id": "srv-777",
            "hand": "left",
            "opponent_torque": -14.5,
            "is_connected": true,
            "client_id": 1,
            "last_message_timestamp": 5000
          }
        }
        """;

        Assert.True(ProtocolJsonSerializer.TryParseServerMessage(
            System.Text.Encoding.UTF8.GetBytes(frame50),
            previousServerPlayerId: null,
            previousMessageTimestamp: null,
            lastSentRivalHand: "right",
            normalizeOpponentTorque: true,
            out var parsed50));

        Assert.True(parsed50!.TelemetryData!.IsNewSample);
        Assert.Equal("srv-777", parsed50.TelemetryData.EffectiveServerPlayerId);
        // Server flipped +14.5 to -14.5 because local="left" and rival="right"; UndoLocalServerFlip recovers +14.5.
        Assert.Equal(14.5, parsed50.TelemetryData.NormalizedTelemetry!.OpponentTorque);

        const string frame51 = """
        {
          "data": {
            "player_id": "p1",
            "hand": "left",
            "opponent_torque": -14.5,
            "is_connected": true,
            "client_id": 1,
            "last_message_timestamp": 5000
          }
        }
        """;

        Assert.True(ProtocolJsonSerializer.TryParseServerMessage(
            System.Text.Encoding.UTF8.GetBytes(frame51),
            previousServerPlayerId: parsed50.TelemetryData.EffectiveServerPlayerId,
            previousMessageTimestamp: parsed50.TelemetryData.LastMessageTimestamp,
            lastSentRivalHand: "right",
            normalizeOpponentTorque: true,
            out var parsed51));

        Assert.False(parsed51!.TelemetryData!.IsNewSample);
        Assert.False(parsed51.TelemetryData.FrameContainedServerPlayerId);
        Assert.Equal("srv-777", parsed51.TelemetryData.EffectiveServerPlayerId);
        Assert.Equal("srv-777", parsed51.TelemetryData.NormalizedTelemetry!.ServerPlayerId);
    }
}
