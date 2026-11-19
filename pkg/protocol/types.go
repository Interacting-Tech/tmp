package protocol

import "encoding/json"

// MachineTelemetry represents the telemetry object sent by a machine inside the single
// top-level JSON key (e.g. {"data": { ... }}) as defined in PROTOCOL.md sections 3.1 and 3.2.
type MachineTelemetry struct {
	MACAddress        string
	PlayerID          string
	ServerPlayerID    string
	HasServerPlayerID bool
	Hand              string
	MultiplayerState  string
	SinglePlayerState string
	// MachineTorque is the machine's handle torque, the only torque value in the protocol.
	MachineTorque float64
	Position      float64
	Velocity      float64
	LeftLimit     float64
	RightLimit    float64
	PeakForce     float64
	TimeStamp     uint64
	ExtraFields   map[string]json.RawMessage
}

// Clone returns a deep copy of t with normalized hand/float values and server-injected
// fields excluded from ExtraFields.
func (t *MachineTelemetry) Clone() *MachineTelemetry {
	if t == nil {
		return nil
	}

	cp := *t
	cp.Hand = NormalizeHand(t.Hand)
	cp.MachineTorque = SanitizeFloat(t.MachineTorque)
	cp.Position = SanitizeFloat(t.Position)
	cp.Velocity = SanitizeFloat(t.Velocity)
	cp.LeftLimit = SanitizeFloat(t.LeftLimit)
	cp.RightLimit = SanitizeFloat(t.RightLimit)
	cp.PeakForce = SanitizeFloat(t.PeakForce)

	if len(t.ExtraFields) > 0 {
		extra := make(map[string]json.RawMessage, len(t.ExtraFields))
		for k, v := range t.ExtraFields {
			if IsServerInjectedField(k) {
				continue
			}
			buf := make(json.RawMessage, len(v))
			copy(buf, v)
			extra[k] = buf
		}
		if len(extra) > 0 {
			cp.ExtraFields = extra
		} else {
			cp.ExtraFields = nil
		}
	} else {
		cp.ExtraFields = nil
	}

	return &cp
}

// CommandKind classifies inbound server {"cmd": ...} frames (PROTOCOL.md sections 6.2 & 6.3).
type CommandKind uint8

const (
	CommandStart CommandKind = iota + 1
	CommandYouWin
	CommandYouLose
	CommandStop
	CommandOperatorText
	CommandUnknownMatch
)

// ServerCommand represents either a match-state command object (section 6.2)
// or an operator command string (section 6.3).
type ServerCommand struct {
	Kind              CommandKind
	CommandText       string
	IsOperatorCommand bool
	RawCode           int
	HasRawCode        bool
}

// InferredLocalGameOverState derives the local physical machine's terminal state from a
// section 6.2 match result command: because the server sends YOU_LOSE / YOU_WIN only to the
// opponent of the reporting machine, receiving YOU_LOSE from the local server means the local
// machine reported MULTIPLAYER_SM_GAMEOVER_WIN (and YOU_WIN means MULTIPLAYER_SM_GAMEOVER_LOSE).
func (c ServerCommand) InferredLocalGameOverState() string {
	switch c.Kind {
	case CommandYouLose:
		return StateGameOverWin
	case CommandYouWin:
		return StateGameOverLose
	default:
		return ""
	}
}

// InferredLocalMatchState derives the multiplayer_state the local physical machine must have
// reported for the local server to send this command. START is sent only while both machines
// report MULTIPLAYER_SM_READY (section 5), so receiving it means the local machine was ready;
// results are inferred as InferredLocalGameOverState describes.
func (c ServerCommand) InferredLocalMatchState() string {
	if c.Kind == CommandStart {
		return StateReady
	}
	return c.InferredLocalGameOverState()
}

// ReceivedMachineData captures a 10 ms telemetry frame received from the local server (section 6.1).
type ReceivedMachineData struct {
	// HasMachineTelemetry is false when the server sent {"data":{}} or the section 4.4
	// {"data":{"machine_torque":-0.0}} disconnect frame.
	HasMachineTelemetry          bool
	IsConnected                  bool
	ClientID                     int
	HasClientID                  bool
	LastMessageTimestamp         int64
	HasLastMessageTimestamp      bool
	IsNewSample                  bool
	EffectiveServerPlayerID      string
	FrameContainedServerPlayerID bool
	RawTelemetry                 *MachineTelemetry
	NormalizedTelemetry          *MachineTelemetry
}

// InboundKind discriminates parsed server messages.
type InboundKind uint8

const (
	InboundTelemetry InboundKind = iota + 1
	InboundCommand
)

// InboundMessage is the parsed representation of a single WebSocket text frame from the server.
type InboundMessage struct {
	Kind      InboundKind
	Telemetry *ReceivedMachineData
	Command   *ServerCommand
}
