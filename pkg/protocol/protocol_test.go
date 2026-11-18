package protocol_test

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/Interacting-Tech/tmp/pkg/protocol"
)

// machineTorqueFrame is a frame from current machine firmware as relayed by a server
// (the machine's local server, or our server relaying the rival).
const machineTorqueFrame = `{
  "data": {
    "MAC_address": "11:22:33:44:55:66",
    "player_id": "player-2",
    "hand": "left",
    "multiplayer_state": "MULTIPLAYER_SM_RUN",
    "single_player_state": "",
    "machine_torque": -12.5,
    "position": -3.1,
    "velocity": 0.4,
    "left_limit": -45.0,
    "right_limit": 45.0,
    "peak_force": 28.0,
    "timeStamp": 123456790,
    "is_connected": true,
    "client_id": 2,
    "last_message_timestamp": 86400123
  }
}`

// machineTorqueForwarded is machineTorqueFrame as the bridge forwards it in either direction:
// same fields and values, minus the three fields the receiving server adds itself.
const machineTorqueForwarded = `{"data":{"MAC_address":"11:22:33:44:55:66","player_id":"player-2","hand":"left","multiplayer_state":"MULTIPLAYER_SM_RUN","single_player_state":"","machine_torque":-12.5,"position":-3.1,"velocity":0.4,"left_limit":-45,"right_limit":45,"peak_force":28,"timeStamp":123456790}}`

func forwardLocalFrame(t *testing.T, raw string) string {
	t.Helper()
	msg, ok := protocol.ParseServerMessage([]byte(raw), "", 0, false, "right", true)
	if !ok || msg.Kind != protocol.InboundTelemetry || !msg.Telemetry.HasMachineTelemetry {
		t.Fatalf("expected machine telemetry from local server frame %s, got %+v", raw, msg)
	}
	return string(protocol.MarshalMachineEnvelope(
		msg.Telemetry.NormalizedTelemetry, true, msg.Telemetry.EffectiveServerPlayerID))
}

func forwardRivalFrame(t *testing.T, raw string) string {
	t.Helper()
	rival, ok := protocol.ParseRivalEnvelope([]byte(raw))
	if !ok || rival == nil {
		t.Fatalf("expected rival telemetry from our server frame %s", raw)
	}
	return string(protocol.MarshalMachineEnvelope(rival, true, ""))
}

func TestMachineTorqueFrame_ForwardedBothWaysWithSameFieldsAndValues(t *testing.T) {
	if got := forwardLocalFrame(t, machineTorqueFrame); got != machineTorqueForwarded {
		t.Fatalf("machine -> our server\n got: %s\nwant: %s", got, machineTorqueForwarded)
	}
	if got := forwardRivalFrame(t, machineTorqueFrame); got != machineTorqueForwarded {
		t.Fatalf("our server -> machine\n got: %s\nwant: %s", got, machineTorqueForwarded)
	}

	zero := strings.Replace(machineTorqueFrame, `"machine_torque": -12.5`, `"machine_torque": 0`, 1)
	wantZero := strings.Replace(machineTorqueForwarded, `"machine_torque":-12.5`, `"machine_torque":0`, 1)
	if got := forwardRivalFrame(t, zero); got != wantZero {
		t.Fatalf("explicit zero machine_torque\n got: %s\nwant: %s", got, wantZero)
	}

	// A field with an unexpected type sends decoding through the per-field fallback path.
	oddType := strings.Replace(machineTorqueFrame, `"timeStamp": 123456790`, `"timeStamp": "late"`, 1)
	wantOdd := strings.Replace(machineTorqueForwarded, `"timeStamp":123456790`, `"timeStamp":0`, 1)
	if got := forwardLocalFrame(t, oddType); got != wantOdd {
		t.Fatalf("fallback decode\n got: %s\nwant: %s", got, wantOdd)
	}
}

func TestMachineTorqueFrame_DropsLegacySignRuleArtifactKeepsRealTorqueFields(t *testing.T) {
	withArtifact := strings.Replace(machineTorqueFrame,
		`"machine_torque": -12.5,`, `"machine_torque": -12.5, "opponent_torque": -0.0,`, 1)
	if got := forwardLocalFrame(t, withArtifact); got != machineTorqueForwarded {
		t.Fatalf("local frame with sign-rule artifact\n got: %s\nwant: %s", got, machineTorqueForwarded)
	}
	if got := forwardRivalFrame(t, withArtifact); got != machineTorqueForwarded {
		t.Fatalf("rival frame with sign-rule artifact\n got: %s\nwant: %s", got, machineTorqueForwarded)
	}

	withLegacyFields := strings.Replace(machineTorqueFrame,
		`"machine_torque": -12.5,`, `"machine_torque": -12.5, "torque": 7.5, "opponent_torque": 3.25,`, 1)
	want := strings.Replace(machineTorqueForwarded,
		`"machine_torque":-12.5,`, `"machine_torque":-12.5,"torque":7.5,"opponent_torque":3.25,`, 1)
	if got := forwardRivalFrame(t, withLegacyFields); got != want {
		t.Fatalf("rival frame with real legacy torque fields\n got: %s\nwant: %s", got, want)
	}
}

func TestMarshalMachineEnvelope_ExactProtocolFormatAndSanitization(t *testing.T) {
	telemetry := &protocol.MachineTelemetry{
		MACAddress:        "AA:BB:CC:DD:EE:FF",
		PlayerID:          "player-1",
		ServerPlayerID:    "abc123",
		HasServerPlayerID: true,
		Hand:              "RIGHT",
		MultiplayerState:  protocol.StateRun,
		SinglePlayerState: "",
		Torque:            12.5,
		OpponentTorque:    10.2,
		Position:          3.1,
		Velocity:          -0.4,
		LeftLimit:         -45.0,
		RightLimit:        45.0,
		PeakForce:         math.NaN(), // Sensor glitch NaN must sanitize to 0 instead of failing
		TimeStamp:         123456789,
		ExtraFields: map[string]json.RawMessage{
			"firmware":               json.RawMessage(`"3.1.0"`),
			"is_connected":           json.RawMessage(`true`),
			"client_id":              json.RawMessage(`9`),
			"last_message_timestamp": json.RawMessage(`8888`),
		},
	}

	withID := protocol.MarshalMachineEnvelope(telemetry, true, "")
	var top map[string]map[string]any
	if err := json.Unmarshal(withID, &top); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if len(top) != 1 {
		t.Fatalf("expected exactly 1 top-level key, got %d", len(top))
	}

	data, ok := top["data"]
	if !ok {
		t.Fatalf("missing top-level 'data' key")
	}

	if got := data["MAC_address"]; got != "AA:BB:CC:DD:EE:FF" {
		t.Errorf("MAC_address = %v", got)
	}
	if got := data["server_player_id"]; got != "abc123" {
		t.Errorf("server_player_id = %v", got)
	}
	if got := data["hand"]; got != "right" {
		t.Errorf("hand = %v", got)
	}
	if got := data["peak_force"]; got != float64(0) {
		t.Errorf("sanitized peak_force = %v, want 0", got)
	}
	if got := data["firmware"]; got != "3.1.0" {
		t.Errorf("extra field firmware = %v", got)
	}
	for _, forbidden := range []string{"is_connected", "client_id", "last_message_timestamp"} {
		if _, exists := data[forbidden]; exists {
			t.Errorf("server-injected field %q leaked into outbound machine envelope", forbidden)
		}
	}

	withoutID := protocol.MarshalMachineEnvelope(telemetry, false, "")
	var top2 map[string]map[string]any
	if err := json.Unmarshal(withoutID, &top2); err != nil {
		t.Fatalf("unmarshal withoutID: %v", err)
	}
	if _, exists := top2["data"]["server_player_id"]; exists {
		t.Errorf("expected server_player_id to be omitted when includeServerPlayerID=false")
	}
}

func TestParseRivalEnvelope_AlphabeticalFirstKeyAndDefaults(t *testing.T) {
	raw := []byte(`{
		"zeta": {"player_id": "wrong", "torque": 999},
		"alpha": {"player_id": "rival-ok", "torque": 19.25, "extra_num": 7}
	}`)

	parsed, ok := protocol.ParseRivalEnvelope(raw)
	if !ok || parsed == nil {
		t.Fatalf("expected ParseRivalEnvelope to succeed")
	}
	if parsed.PlayerID != "rival-ok" {
		t.Fatalf("PlayerID = %q, want rival-ok", parsed.PlayerID)
	}
	if parsed.Torque != 19.25 {
		t.Fatalf("Torque = %v, want 19.25", parsed.Torque)
	}
	if parsed.MACAddress != "" || parsed.Hand != "" || parsed.MultiplayerState != "" || parsed.TimeStamp != 0 {
		t.Fatalf("unexpected defaults: %+v", parsed)
	}
	if string(parsed.ExtraFields["extra_num"]) != "7" {
		t.Fatalf("ExtraFields[extra_num] = %s, want 7", parsed.ExtraFields["extra_num"])
	}

	// Reject empty/disconnect/malformed frames
	for _, invalid := range [][]byte{
		[]byte(`{"data":{}}`),
		[]byte(`{"data":{"opponent_torque":-0.0}}`),
		[]byte(`{"cmd":"STOP"}`),
		[]byte(`{bad json`),
		[]byte(`{"data":{"player_id":"x"}} trailing`),
	} {
		if _, ok := protocol.ParseRivalEnvelope(invalid); ok {
			t.Errorf("expected ParseRivalEnvelope(%s) to return false", invalid)
		}
	}
}

func TestParseServerMessage_CommandsDisconnectsTorqueAndCaching(t *testing.T) {
	// Match commands (section 6.2)
	msg, ok := protocol.ParseServerMessage([]byte(`{"cmd":{"START":1}}`), "", 0, false, "", true)
	if !ok || msg.Kind != protocol.InboundCommand || msg.Command.Kind != protocol.CommandStart || msg.Command.IsOperatorCommand {
		t.Fatalf("unexpected START parse: %+v", msg)
	}

	loseMsg, ok := protocol.ParseServerMessage([]byte(`{"cmd":{"YOU_LOSE":1}}`), "", 0, false, "", true)
	if !ok || loseMsg.Command.Kind != protocol.CommandYouLose || loseMsg.Command.InferredLocalGameOverState() != protocol.StateGameOverWin {
		t.Fatalf("unexpected YOU_LOSE parse: %+v", loseMsg)
	}

	winMsg, ok := protocol.ParseServerMessage([]byte(`{"cmd":{"YOU_WIN":0}}`), "", 0, false, "", true)
	if !ok || winMsg.Command.Kind != protocol.CommandYouWin || winMsg.Command.InferredLocalGameOverState() != protocol.StateGameOverLose {
		t.Fatalf("unexpected YOU_WIN parse: %+v", winMsg)
	}

	// Operator commands (section 6.3)
	stopMsg, ok := protocol.ParseServerMessage([]byte(`{"cmd":"STOP"}`), "", 0, false, "", true)
	if !ok || stopMsg.Command.Kind != protocol.CommandStop || !stopMsg.Command.IsOperatorCommand {
		t.Fatalf("unexpected STOP parse: %+v", stopMsg)
	}

	opMsg, ok := protocol.ParseServerMessage([]byte(`{"cmd":"CALIB"}`), "", 0, false, "", true)
	if !ok || opMsg.Command.Kind != protocol.CommandOperatorText || opMsg.Command.CommandText != "CALIB" {
		t.Fatalf("unexpected operator cmd parse: %+v", opMsg)
	}

	// Empty and section 4.4 left-hand disconnect {"data":{"opponent_torque":-0.0}}
	for _, disc := range [][]byte{
		[]byte(`{"data":{}}`),
		[]byte(`{"data":{"opponent_torque":-0.0}}`),
	} {
		m, ok := protocol.ParseServerMessage(disc, "cached", 500, true, "left", true)
		if !ok || m.Telemetry.HasMachineTelemetry || m.Telemetry.IsConnected {
			t.Fatalf("expected disconnect snapshot for %s, got %+v", disc, m)
		}
	}

	// Frame 50 (contains server_player_id) + opposite-hand torque unflip
	f50 := []byte(`{"data":{"player_id":"p1","server_player_id":"sp-50","hand":"left","opponent_torque":-16.5,"is_connected":true,"client_id":1,"last_message_timestamp":4200}}`)
	m50, ok := protocol.ParseServerMessage(f50, "", 0, false, "right", true)
	if !ok || !m50.Telemetry.IsNewSample || m50.Telemetry.EffectiveServerPlayerID != "sp-50" {
		t.Fatalf("unexpected f50 parse: %+v", m50)
	}
	if m50.Telemetry.NormalizedTelemetry.OpponentTorque != 16.5 {
		t.Fatalf("Normalized OpponentTorque = %v, want +16.5", m50.Telemetry.NormalizedTelemetry.OpponentTorque)
	}

	// Frame 51 (omits server_player_id, same timestamp -> IsNewSample=false, keeps sp-50)
	f51 := []byte(`{"data":{"player_id":"p1","hand":"left","opponent_torque":-16.5,"is_connected":true,"client_id":1,"last_message_timestamp":4200}}`)
	m51, ok := protocol.ParseServerMessage(f51, m50.Telemetry.EffectiveServerPlayerID, m50.Telemetry.LastMessageTimestamp, true, "right", true)
	if !ok || m51.Telemetry.IsNewSample || m51.Telemetry.FrameContainedServerPlayerID {
		t.Fatalf("unexpected f51 sample flags: %+v", m51.Telemetry)
	}
	if m51.Telemetry.EffectiveServerPlayerID != "sp-50" || m51.Telemetry.NormalizedTelemetry.ServerPlayerID != "sp-50" {
		t.Fatalf("expected cached server_player_id sp-50, got %q", m51.Telemetry.EffectiveServerPlayerID)
	}
}

func BenchmarkAppendMachineEnvelope(b *testing.B) {
	telemetry := &protocol.MachineTelemetry{
		MACAddress:        "AA:BB:CC:DD:EE:FF",
		PlayerID:          "player-1",
		ServerPlayerID:    "abc123",
		HasServerPlayerID: true,
		Hand:              "right",
		MultiplayerState:  protocol.StateRun,
		Torque:            24.5,
		OpponentTorque:    19.25,
		Position:          4.2,
		Velocity:          -0.8,
		LeftLimit:         -45.0,
		RightLimit:        45.0,
		PeakForce:         32.0,
		TimeStamp:         987654321,
	}
	buf := make([]byte, 0, 512)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = protocol.AppendMachineEnvelope(buf[:0], telemetry, true, "")
	}
}

func BenchmarkParseServerMessage(b *testing.B) {
	frame := []byte(`{"data":{"MAC_address":"11:22:33:44:55:66","player_id":"player-2","hand":"left","multiplayer_state":"MULTIPLAYER_SM_RUN","single_player_state":"","torque":10.2,"opponent_torque":-12.5,"position":-3.1,"velocity":0.4,"left_limit":-45.0,"right_limit":45.0,"peak_force":28.0,"timeStamp":123456790,"is_connected":true,"client_id":2,"last_message_timestamp":86400123}}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = protocol.ParseServerMessage(frame, "abc123", 86400120, true, "right", true)
	}
}
