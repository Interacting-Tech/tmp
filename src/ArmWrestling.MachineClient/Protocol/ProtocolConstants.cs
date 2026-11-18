namespace ArmWrestling.MachineClient.Protocol;

/// <summary>
/// Protocol string constants defined in PROTOCOL.md sections 3.2, 5, 6.2, and 6.3.
/// </summary>
public static class ProtocolConstants
{
    public const string DefaultEnvelopeKey = "data";
    public const string CommandEnvelopeKey = "cmd";

    // Hand values (PROTOCOL.md section 3.2: lowercase "left" or "right")
    public const string LeftHand = "left";
    public const string RightHand = "right";

    // Machine multiplayer_state values recognised by the server (PROTOCOL.md section 3.2 & 5)
    public const string StateReady = "MULTIPLAYER_SM_READY";
    public const string StateGameOverWin = "MULTIPLAYER_SM_GAMEOVER_WIN";
    public const string StateGameOverLose = "MULTIPLAYER_SM_GAMEOVER_LOSE";

    // Additional relayed machine multiplayer_state values mentioned in PROTOCOL.md section 3.2
    public const string StateCountDown = "MULTIPLAYER_SM_COUNT_DOWN";
    public const string StateRun = "MULTIPLAYER_SM_RUN";

    // Server match state machine states (PROTOCOL.md section 5)
    public const string ServerIdle = "MULTIPLAYER_IDLE";
    public const string ServerWaiting = "MULTIPLAYER_WAITING";
    public const string ServerMidMatch = "MULTIPLAYER_MID_MATCH";
    public const string ServerPostMatch = "MULTIPLAYER_POST_MATCH";

    // Match & operator command names (PROTOCOL.md sections 6.2 & 6.3)
    public const string CommandStart = "START";
    public const string CommandYouWin = "YOU_WIN";
    public const string CommandYouLose = "YOU_LOSE";
    public const string CommandStop = "STOP";

    // Server-injected telemetry field names (PROTOCOL.md section 4.2)
    public const string FieldIsConnected = "is_connected";
    public const string FieldClientId = "client_id";
    public const string FieldLastMessageTimestamp = "last_message_timestamp";

    public static bool IsTerminalMatchState(string? multiplayerState) =>
        string.Equals(multiplayerState, StateGameOverWin, StringComparison.Ordinal) ||
        string.Equals(multiplayerState, StateGameOverLose, StringComparison.Ordinal);
}
