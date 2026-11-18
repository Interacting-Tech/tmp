namespace ArmWrestling.MachineClient.Protocol;

/// <summary>
/// Semantic command types sent by the server in {"cmd": ...} frames (PROTOCOL.md sections 6.2 &amp; 6.3).
/// </summary>
public enum ServerCommandKind
{
    Start,
    YouWin,
    YouLose,
    Stop,
    OperatorCommand,
    UnknownMatchCommand
}

/// <summary>
/// Represents a server command parsed from either the match-command object form
/// (e.g. {"cmd":{"START":1}}, section 6.2) or the operator string form
/// (e.g. {"cmd":"STOP"} / {"cmd":"..."}, section 6.3).
/// </summary>
public sealed record ServerCommand(
    ServerCommandKind Kind,
    string CommandText,
    bool IsOperatorCommand,
    int? RawCode = null)
{
    /// <summary>
    /// Per PROTOCOL.md section 6.2, YOU_LOSE / YOU_WIN is sent only to the opponent of the
    /// machine that reported GAMEOVER_WIN / GAMEOVER_LOSE. Receiving YOU_LOSE from the local
    /// server therefore means the local physical machine reported MULTIPLAYER_SM_GAMEOVER_WIN
    /// (and receiving YOU_WIN means it reported MULTIPLAYER_SM_GAMEOVER_LOSE).
    /// </summary>
    public string? InferredLocalMachineGameOverState => Kind switch
    {
        ServerCommandKind.YouLose => ProtocolConstants.StateGameOverWin,
        ServerCommandKind.YouWin => ProtocolConstants.StateGameOverLose,
        _ => null
    };
}
