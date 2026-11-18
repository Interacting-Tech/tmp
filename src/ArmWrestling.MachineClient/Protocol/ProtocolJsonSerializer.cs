using System.Text;
using System.Text.Json;

namespace ArmWrestling.MachineClient.Protocol;

/// <summary>
/// Serializes and parses JSON messages strictly according to PROTOCOL.md rules.
/// </summary>
public static class ProtocolJsonSerializer
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = null,
        WriteIndented = false
    };

    /// <summary>
    /// Serializes a <see cref="MachineTelemetry"/> object into the single-key JSON envelope
    /// {"data": { ... }} defined in PROTOCOL.md section 3.1.
    /// </summary>
    public static string SerializeMachineEnvelope(
        MachineTelemetry telemetry,
        bool includeServerPlayerId = true,
        string? fallbackServerPlayerId = null)
    {
        ArgumentNullException.ThrowIfNull(telemetry);
        var wireTelemetry = telemetry.PrepareForWire(includeServerPlayerId, fallbackServerPlayerId);
        var envelope = new MachineTelemetryEnvelope { Data = wireTelemetry };
        return JsonSerializer.Serialize(envelope, SerializerOptions);
    }

    /// <summary>
    /// Serializes a <see cref="MachineTelemetry"/> object directly to UTF-8 bytes for WebSocket transmission.
    /// </summary>
    public static byte[] SerializeMachineEnvelopeToUtf8Bytes(
        MachineTelemetry telemetry,
        bool includeServerPlayerId = true,
        string? fallbackServerPlayerId = null)
    {
        ArgumentNullException.ThrowIfNull(telemetry);
        var wireTelemetry = telemetry.PrepareForWire(includeServerPlayerId, fallbackServerPlayerId);
        var envelope = new MachineTelemetryEnvelope { Data = wireTelemetry };
        return JsonSerializer.SerializeToUtf8Bytes(envelope, SerializerOptions);
    }

    /// <summary>
    /// Parses a machine/rival telemetry JSON message following PROTOCOL.md section 3.1 &amp; 3.2 rules:
    /// - Expects a JSON object with one top-level key (if multiple keys exist, selects the first
    ///   key alphabetically using ordinal comparison, matching section 3.1).
    /// - Applies section 3.2 defaults for any missing fields.
    /// - Strips server-added fields (is_connected, client_id, last_message_timestamp) if present,
    ///   while keeping all other extra fields unchanged.
    /// </summary>
    public static bool TryParseRivalEnvelope(
        string json,
        out MachineTelemetry? telemetry)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            telemetry = null;
            return false;
        }

        return TryParseRivalEnvelope(Encoding.UTF8.GetBytes(json), out telemetry);
    }

    public static bool TryParseRivalEnvelope(
        ReadOnlySpan<byte> utf8Json,
        out MachineTelemetry? telemetry)
    {
        telemetry = null;
        if (utf8Json.IsEmpty)
        {
            return false;
        }

        try
        {
            using var doc = JsonDocument.Parse(utf8Json.ToArray());
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            if (!TryGetAlphabeticallyFirstProperty(doc.RootElement, out var topProp))
            {
                return false;
            }

            if (string.Equals(topProp.Name, ProtocolConstants.CommandEnvelopeKey, StringComparison.Ordinal) ||
                topProp.Value.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            if (IsEmptyOrDisconnectOnlyObject(topProp.Value))
            {
                return false;
            }

            telemetry = ReadTelemetryElement(topProp.Value, out _);
            return telemetry is not null;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>
    /// Parses any inbound JSON text frame sent by the PROTOCOL.md server (sections 6.1, 6.2, 6.3).
    /// </summary>
    public static bool TryParseServerMessage(
        ReadOnlySpan<byte> utf8Json,
        string? previousServerPlayerId,
        long? previousMessageTimestamp,
        string? lastSentRivalHand,
        bool normalizeOpponentTorque,
        out InboundServerMessage? message)
    {
        message = null;
        if (utf8Json.IsEmpty)
        {
            return false;
        }

        try
        {
            using var doc = JsonDocument.Parse(utf8Json.ToArray());
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            // 1. Check for {"cmd": ...} (sections 6.2 & 6.3)
            if (root.TryGetProperty(ProtocolConstants.CommandEnvelopeKey, out var cmdElement))
            {
                if (!TryParseCommandElement(cmdElement, out var command) || command is null)
                {
                    return false;
                }

                message = new InboundServerMessage(InboundMessageKind.Command, Command: command);
                return true;
            }

            // 2. Otherwise parse telemetry envelope (section 6.1 / 3.1: "data" or alphabetically first key)
            if (!TryGetAlphabeticallyFirstProperty(root, out var envelopeProp) ||
                envelopeProp.Value.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            var dataElement = envelopeProp.Value;

            bool hasIsConnected = dataElement.TryGetProperty(ProtocolConstants.FieldIsConnected, out var isConnectedProp) &&
                                  isConnectedProp.ValueKind is JsonValueKind.True or JsonValueKind.False;
            bool isConnected = hasIsConnected && isConnectedProp.GetBoolean();

            int? clientId = null;
            if (dataElement.TryGetProperty(ProtocolConstants.FieldClientId, out var clientIdProp) &&
                clientIdProp.ValueKind == JsonValueKind.Number &&
                clientIdProp.TryGetInt32(out var parsedClientId))
            {
                clientId = parsedClientId;
            }

            long? lastTimestamp = null;
            if (dataElement.TryGetProperty(ProtocolConstants.FieldLastMessageTimestamp, out var tsProp) &&
                tsProp.ValueKind == JsonValueKind.Number &&
                tsProp.TryGetInt64(out var parsedTs))
            {
                lastTimestamp = parsedTs;
            }

            // Empty {"data":{}} or section 4.4 disconnect frame {"data":{"opponent_torque":-0.0}}
            if ((!hasIsConnected && IsEmptyOrDisconnectOnlyObject(dataElement)) ||
                (hasIsConnected && !isConnected))
            {
                var disconnectedSnapshot = new ReceivedMachineData
                {
                    HasMachineTelemetry = false,
                    IsConnected = false,
                    ClientId = clientId,
                    LastMessageTimestamp = lastTimestamp,
                    IsNewSample = false,
                    EffectiveServerPlayerId = null,
                    FrameContainedServerPlayerId = false,
                    RawTelemetry = null,
                    NormalizedTelemetry = null
                };

                message = new InboundServerMessage(InboundMessageKind.Telemetry, TelemetryData: disconnectedSnapshot);
                return true;
            }

            var rawTelemetry = ReadTelemetryElement(dataElement, out var frameContainedServerPlayerId);
            if (rawTelemetry is null)
            {
                return false;
            }

            var effectiveServerPlayerId = frameContainedServerPlayerId
                ? rawTelemetry.ServerPlayerId
                : previousServerPlayerId;

            var normalizedTelemetry = rawTelemetry.Clone();
            if (!string.IsNullOrEmpty(effectiveServerPlayerId) && string.IsNullOrEmpty(normalizedTelemetry.ServerPlayerId))
            {
                normalizedTelemetry.ServerPlayerId = effectiveServerPlayerId;
            }

            if (normalizeOpponentTorque)
            {
                normalizedTelemetry.OpponentTorque = TorqueSignRule.UndoLocalServerFlip(
                    rawTelemetry.OpponentTorque,
                    rawTelemetry.Hand,
                    lastSentRivalHand);
            }

            var isNewSample = !lastTimestamp.HasValue ||
                              !previousMessageTimestamp.HasValue ||
                              lastTimestamp.Value != previousMessageTimestamp.Value;

            var received = new ReceivedMachineData
            {
                HasMachineTelemetry = true,
                IsConnected = hasIsConnected ? isConnected : true,
                ClientId = clientId,
                LastMessageTimestamp = lastTimestamp,
                IsNewSample = isNewSample,
                EffectiveServerPlayerId = effectiveServerPlayerId,
                FrameContainedServerPlayerId = frameContainedServerPlayerId,
                RawTelemetry = rawTelemetry,
                NormalizedTelemetry = normalizedTelemetry
            };

            message = new InboundServerMessage(InboundMessageKind.Telemetry, TelemetryData: received);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static bool TryParseCommandElement(JsonElement cmdElement, out ServerCommand? command)
    {
        command = null;

        switch (cmdElement.ValueKind)
        {
            // Section 6.3: Operator commands: {"cmd":"STOP"} or {"cmd":"<text>"}
            case JsonValueKind.String:
            {
                var text = cmdElement.GetString() ?? "";
                var kind = string.Equals(text, ProtocolConstants.CommandStop, StringComparison.Ordinal)
                    ? ServerCommandKind.Stop
                    : ServerCommandKind.OperatorCommand;

                command = new ServerCommand(kind, text, IsOperatorCommand: true, RawCode: null);
                return true;
            }

            // Section 6.2: Match commands: {"cmd":{"START":1}}, {"cmd":{"YOU_WIN":0}}, {"cmd":{"YOU_LOSE":1}}
            case JsonValueKind.Object:
            {
                if (!TryGetAlphabeticallyFirstProperty(cmdElement, out var matchCmdProp))
                {
                    return false;
                }

                int? rawCode = null;
                if (matchCmdProp.Value.ValueKind == JsonValueKind.Number &&
                    matchCmdProp.Value.TryGetInt32(out var code))
                {
                    rawCode = code;
                }

                var kind = matchCmdProp.Name switch
                {
                    ProtocolConstants.CommandStart => ServerCommandKind.Start,
                    ProtocolConstants.CommandYouWin => ServerCommandKind.YouWin,
                    ProtocolConstants.CommandYouLose => ServerCommandKind.YouLose,
                    _ => ServerCommandKind.UnknownMatchCommand
                };

                command = new ServerCommand(kind, matchCmdProp.Name, IsOperatorCommand: false, RawCode: rawCode);
                return true;
            }

            default:
                return false;
        }
    }

    private static MachineTelemetry? ReadTelemetryElement(JsonElement dataElement, out bool hadServerPlayerId)
    {
        hadServerPlayerId = false;

        var telemetry = new MachineTelemetry();
        Dictionary<string, JsonElement>? extraFields = null;

        foreach (var prop in dataElement.EnumerateObject())
        {
            switch (prop.Name)
            {
                case "MAC_address":
                    telemetry.MacAddress = ReadStringOrDefault(prop.Value, "");
                    break;
                case "player_id":
                    telemetry.PlayerId = ReadStringOrDefault(prop.Value, "");
                    break;
                case "server_player_id":
                    if (prop.Value.ValueKind == JsonValueKind.String)
                    {
                        telemetry.ServerPlayerId = prop.Value.GetString();
                        hadServerPlayerId = true;
                    }
                    break;
                case "hand":
                    telemetry.Hand = MachineTelemetry.NormalizeHand(ReadStringOrDefault(prop.Value, ""));
                    break;
                case "multiplayer_state":
                    telemetry.MultiplayerState = ReadStringOrDefault(prop.Value, "");
                    break;
                case "single_player_state":
                    telemetry.SinglePlayerState = ReadStringOrDefault(prop.Value, "");
                    break;
                case "torque":
                    telemetry.Torque = ReadDoubleOrDefault(prop.Value, 0.0);
                    break;
                case "opponent_torque":
                    telemetry.OpponentTorque = ReadDoubleOrDefault(prop.Value, 0.0);
                    break;
                case "position":
                    telemetry.Position = ReadDoubleOrDefault(prop.Value, 0.0);
                    break;
                case "velocity":
                    telemetry.Velocity = ReadDoubleOrDefault(prop.Value, 0.0);
                    break;
                case "left_limit":
                    telemetry.LeftLimit = ReadDoubleOrDefault(prop.Value, 0.0);
                    break;
                case "right_limit":
                    telemetry.RightLimit = ReadDoubleOrDefault(prop.Value, 0.0);
                    break;
                case "peak_force":
                    telemetry.PeakForce = ReadDoubleOrDefault(prop.Value, 0.0);
                    break;
                case "timeStamp":
                    telemetry.TimeStamp = ReadUInt64OrDefault(prop.Value, 0UL);
                    break;
                case ProtocolConstants.FieldIsConnected:
                case ProtocolConstants.FieldClientId:
                case ProtocolConstants.FieldLastMessageTimestamp:
                    // Handled at the server metadata layer; never copy into clean machine telemetry.
                    break;
                default:
                    extraFields ??= new Dictionary<string, JsonElement>(StringComparer.Ordinal);
                    extraFields[prop.Name] = prop.Value.Clone();
                    break;
            }
        }

        telemetry.ExtraFields = extraFields;
        return telemetry;
    }

    private static bool TryGetAlphabeticallyFirstProperty(JsonElement obj, out JsonProperty selected)
    {
        selected = default;
        var found = false;

        foreach (var prop in obj.EnumerateObject())
        {
            if (!found || StringComparer.Ordinal.Compare(prop.Name, selected.Name) < 0)
            {
                selected = prop;
                found = true;
            }
        }

        return found;
    }

    private static bool IsEmptyOrDisconnectOnlyObject(JsonElement obj)
    {
        var count = 0;
        var onlyZeroOpponentTorque = false;

        foreach (var prop in obj.EnumerateObject())
        {
            count++;
            if (count == 1 &&
                string.Equals(prop.Name, "opponent_torque", StringComparison.Ordinal) &&
                prop.Value.ValueKind == JsonValueKind.Number &&
                prop.Value.TryGetDouble(out var torqueVal) &&
                torqueVal == 0.0)
            {
                onlyZeroOpponentTorque = true;
            }
            else
            {
                onlyZeroOpponentTorque = false;
            }

            if (count > 1)
            {
                break;
            }
        }

        return count == 0 || (count == 1 && onlyZeroOpponentTorque);
    }

    private static string ReadStringOrDefault(JsonElement element, string fallback) =>
        element.ValueKind == JsonValueKind.String ? (element.GetString() ?? fallback) : fallback;

    private static double ReadDoubleOrDefault(JsonElement element, double fallback) =>
        element.ValueKind == JsonValueKind.Number && element.TryGetDouble(out var val) && double.IsFinite(val)
            ? val
            : fallback;

    private static ulong ReadUInt64OrDefault(JsonElement element, ulong fallback)
    {
        if (element.ValueKind != JsonValueKind.Number)
        {
            return fallback;
        }

        if (element.TryGetUInt64(out var uval))
        {
            return uval;
        }

        if (element.TryGetDouble(out var dval) && dval >= 0 && dval <= ulong.MaxValue)
        {
            return (ulong)dval;
        }

        return fallback;
    }
}
