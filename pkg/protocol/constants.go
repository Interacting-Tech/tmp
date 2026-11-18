package protocol

const (
	DefaultEnvelopeKey = "data"
	CommandEnvelopeKey = "cmd"

	// Hand values (PROTOCOL.md section 3.2: lowercase "left" or "right")
	HandLeft  = "left"
	HandRight = "right"

	// Machine multiplayer_state values recognised by the server (PROTOCOL.md sections 3.2 & 5)
	StateReady        = "MULTIPLAYER_SM_READY"
	StateGameOverWin  = "MULTIPLAYER_SM_GAMEOVER_WIN"
	StateGameOverLose = "MULTIPLAYER_SM_GAMEOVER_LOSE"

	// Other relayed machine multiplayer_state values (PROTOCOL.md section 3.2)
	StateCountDown = "MULTIPLAYER_SM_COUNT_DOWN"
	StateRun       = "MULTIPLAYER_SM_RUN"

	// Server match state machine states (PROTOCOL.md section 5)
	ServerIdle      = "MULTIPLAYER_IDLE"
	ServerWaiting   = "MULTIPLAYER_WAITING"
	ServerMidMatch  = "MULTIPLAYER_MID_MATCH"
	ServerPostMatch = "MULTIPLAYER_POST_MATCH"

	// Match and operator command names (PROTOCOL.md sections 6.2 & 6.3)
	CmdStart   = "START"
	CmdYouWin  = "YOU_WIN"
	CmdYouLose = "YOU_LOSE"
	CmdStop    = "STOP"

	// Server-added telemetry fields (PROTOCOL.md section 4.2)
	FieldIsConnected          = "is_connected"
	FieldClientID             = "client_id"
	FieldLastMessageTimestamp = "last_message_timestamp"
)

// IsTerminalMatchState reports whether state is a game-over transition that drives
// the server match state machine out of MULTIPLAYER_MID_MATCH.
func IsTerminalMatchState(state string) bool {
	return state == StateGameOverWin || state == StateGameOverLose
}

// IsServerInjectedField reports whether fieldName is one of the three metadata fields
// added by the server in PROTOCOL.md section 4.2.
func IsServerInjectedField(fieldName string) bool {
	return fieldName == FieldIsConnected ||
		fieldName == FieldClientID ||
		fieldName == FieldLastMessageTimestamp
}
