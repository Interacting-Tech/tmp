namespace ArmWrestling.MachineClient.Protocol;

/// <summary>
/// Implements the opponent_torque sign flip rules from PROTOCOL.md sections 4.3 and 4.4.
/// </summary>
public static class TorqueSignRule
{
    /// <summary>
    /// Returns true when one player uses "left" and the other uses "right" (PROTOCOL.md section 4.3).
    /// </summary>
    public static bool AreOppositeHands(string? handA, string? handB)
    {
        var a = MachineTelemetry.NormalizeHand(handA);
        var b = MachineTelemetry.NormalizeHand(handB);

        return (a == ProtocolConstants.LeftHand && b == ProtocolConstants.RightHand) ||
               (a == ProtocolConstants.RightHand && b == ProtocolConstants.LeftHand);
    }

    /// <summary>
    /// Matches the exact server flip behaviour documented across PROTOCOL.md sections 4.3 and 4.4:
    /// flips when exactly one slot reports "left" (which both flips "left" vs "right" AND produces
    /// the documented {"opponent_torque": -0.0} when a machine disconnects leaving a "left" player).
    /// </summary>
    public static bool DoesServerFlipOpponentTorque(string? handA, string? handB)
    {
        var aIsLeft = string.Equals(MachineTelemetry.NormalizeHand(handA), ProtocolConstants.LeftHand, StringComparison.Ordinal);
        var bIsLeft = string.Equals(MachineTelemetry.NormalizeHand(handB), ProtocolConstants.LeftHand, StringComparison.Ordinal);
        return aIsLeft != bIsLeft;
    }

    /// <summary>
    /// Applies the section 4.3 sign rule between two hands.
    /// </summary>
    public static double ApplyOpponentTorqueSign(double opponentTorque, string? handA, string? handB)
    {
        return AreOppositeHands(handA, handB) ? -opponentTorque : opponentTorque;
    }

    /// <summary>
    /// Undoes the local server's flip on received opponent_torque so the local machine's original
    /// telemetry value is recovered without double-flipping on a remote server.
    /// </summary>
    public static double UndoLocalServerFlip(double relayedOpponentTorque, string? localHand, string? lastSentRivalHand)
    {
        var unflipped = DoesServerFlipOpponentTorque(localHand, lastSentRivalHand)
            ? -relayedOpponentTorque
            : relayedOpponentTorque;

        return NormalizeNegativeZero(unflipped);
    }

    public static double NormalizeNegativeZero(double value) => value == 0.0 ? 0.0 : value;
}
