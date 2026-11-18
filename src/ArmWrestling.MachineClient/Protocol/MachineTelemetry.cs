using System.Text.Json;
using System.Text.Json.Serialization;

namespace ArmWrestling.MachineClient.Protocol;

/// <summary>
/// Telemetry payload exchanged inside the single top-level envelope key (e.g. {"data": { ... }})
/// as specified in PROTOCOL.md sections 3.1 and 3.2.
/// </summary>
public sealed class MachineTelemetry
{
    [JsonPropertyName("MAC_address")]
    public string MacAddress { get; set; } = "";

    [JsonPropertyName("player_id")]
    public string PlayerId { get; set; } = "";

    /// <summary>
    /// Sent by a machine in every 50th message; omitted when null (PROTOCOL.md sections 3.2 &amp; 6.1).
    /// </summary>
    [JsonPropertyName("server_player_id")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? ServerPlayerId { get; set; }

    /// <summary>
    /// "left" or "right", lowercase (PROTOCOL.md section 3.2).
    /// </summary>
    [JsonPropertyName("hand")]
    public string Hand { get; set; } = "";

    [JsonPropertyName("multiplayer_state")]
    public string MultiplayerState { get; set; } = "";

    [JsonPropertyName("single_player_state")]
    public string SinglePlayerState { get; set; } = "";

    [JsonPropertyName("torque")]
    public double Torque { get; set; } = 0.0;

    [JsonPropertyName("opponent_torque")]
    public double OpponentTorque { get; set; } = 0.0;

    [JsonPropertyName("position")]
    public double Position { get; set; } = 0.0;

    [JsonPropertyName("velocity")]
    public double Velocity { get; set; } = 0.0;

    [JsonPropertyName("left_limit")]
    public double LeftLimit { get; set; } = 0.0;

    [JsonPropertyName("right_limit")]
    public double RightLimit { get; set; } = 0.0;

    [JsonPropertyName("peak_force")]
    public double PeakForce { get; set; } = 0.0;

    [JsonPropertyName("timeStamp")]
    public ulong TimeStamp { get; set; } = 0;

    /// <summary>
    /// Preserves any extra telemetry fields unchanged per PROTOCOL.md section 3.2.
    /// </summary>
    [JsonExtensionData]
    public Dictionary<string, JsonElement>? ExtraFields { get; set; }

    public MachineTelemetry Clone()
    {
        Dictionary<string, JsonElement>? extraCopy = null;
        if (ExtraFields is { Count: > 0 })
        {
            extraCopy = new Dictionary<string, JsonElement>(ExtraFields.Count, StringComparer.Ordinal);
            foreach (var kvp in ExtraFields)
            {
                if (IsServerInjectedField(kvp.Key))
                {
                    continue;
                }

                extraCopy[kvp.Key] = kvp.Value.Clone();
            }

            if (extraCopy.Count == 0)
            {
                extraCopy = null;
            }
        }

        return new MachineTelemetry
        {
            MacAddress = MacAddress ?? "",
            PlayerId = PlayerId ?? "",
            ServerPlayerId = ServerPlayerId,
            Hand = NormalizeHand(Hand),
            MultiplayerState = MultiplayerState ?? "",
            SinglePlayerState = SinglePlayerState ?? "",
            Torque = TorqueSignRule.NormalizeNegativeZero(Torque),
            OpponentTorque = TorqueSignRule.NormalizeNegativeZero(OpponentTorque),
            Position = TorqueSignRule.NormalizeNegativeZero(Position),
            Velocity = TorqueSignRule.NormalizeNegativeZero(Velocity),
            LeftLimit = TorqueSignRule.NormalizeNegativeZero(LeftLimit),
            RightLimit = TorqueSignRule.NormalizeNegativeZero(RightLimit),
            PeakForce = TorqueSignRule.NormalizeNegativeZero(PeakForce),
            TimeStamp = TimeStamp,
            ExtraFields = extraCopy
        };
    }

    public MachineTelemetry PrepareForWire(bool includeServerPlayerId, string? fallbackServerPlayerId = null)
    {
        var copy = Clone();
        if (includeServerPlayerId)
        {
            var effectiveId = !string.IsNullOrEmpty(copy.ServerPlayerId)
                ? copy.ServerPlayerId
                : fallbackServerPlayerId;
            copy.ServerPlayerId = string.IsNullOrEmpty(effectiveId) ? null : effectiveId;
        }
        else
        {
            copy.ServerPlayerId = null;
        }

        return copy;
    }

    public static string NormalizeHand(string? hand)
    {
        if (string.IsNullOrWhiteSpace(hand))
        {
            return "";
        }

        return hand.Trim().ToLowerInvariant();
    }

    internal static bool IsServerInjectedField(string propertyName) =>
        string.Equals(propertyName, ProtocolConstants.FieldIsConnected, StringComparison.Ordinal) ||
        string.Equals(propertyName, ProtocolConstants.FieldClientId, StringComparison.Ordinal) ||
        string.Equals(propertyName, ProtocolConstants.FieldLastMessageTimestamp, StringComparison.Ordinal);
}

/// <summary>
/// Single-key JSON envelope sent by a machine: {"data": { ... }} (PROTOCOL.md section 3.1).
/// </summary>
public sealed class MachineTelemetryEnvelope
{
    [JsonPropertyName(ProtocolConstants.DefaultEnvelopeKey)]
    public required MachineTelemetry Data { get; init; }
}
