package protocol

import (
	"bytes"
	"encoding/json"
	"math"
	"sort"
	"strconv"
)

type wireTelemetryDTO struct {
	MACAddress           *string  `json:"MAC_address"`
	PlayerID             *string  `json:"player_id"`
	ServerPlayerID       *string  `json:"server_player_id"`
	Hand                 *string  `json:"hand"`
	MultiplayerState     *string  `json:"multiplayer_state"`
	SinglePlayerState    *string  `json:"single_player_state"`
	MachineTorque        *float64 `json:"machine_torque"`
	Position             *float64 `json:"position"`
	Velocity             *float64 `json:"velocity"`
	LeftLimit            *float64 `json:"left_limit"`
	RightLimit           *float64 `json:"right_limit"`
	PeakForce            *float64 `json:"peak_force"`
	TimeStamp            *uint64  `json:"timeStamp"`
	IsConnected          *bool    `json:"is_connected"`
	ClientID             *int     `json:"client_id"`
	LastMessageTimestamp *int64   `json:"last_message_timestamp"`
}

type fastDataEnvelope struct {
	Data json.RawMessage `json:"data"`
	Cmd  json.RawMessage `json:"cmd"`
}

// AppendMachineEnvelope appends the exact PROTOCOL.md section 3.1 single-key JSON envelope
// {"data":{...}} to dst without reflection overhead (0 heap allocations).
func AppendMachineEnvelope(
	dst []byte,
	t *MachineTelemetry,
	includeServerPlayerID bool,
	fallbackServerPlayerID string,
) []byte {
	if t == nil {
		return append(dst, `{"data":{}}`...)
	}

	dst = append(dst, `{"data":{"MAC_address":`...)
	dst = strconv.AppendQuote(dst, t.MACAddress)

	dst = append(dst, `,"player_id":`...)
	dst = strconv.AppendQuote(dst, t.PlayerID)

	if includeServerPlayerID {
		spid := t.ServerPlayerID
		if spid == "" && !t.HasServerPlayerID {
			spid = fallbackServerPlayerID
		}
		if spid != "" || t.HasServerPlayerID {
			dst = append(dst, `,"server_player_id":`...)
			dst = strconv.AppendQuote(dst, spid)
		}
	}

	dst = append(dst, `,"hand":`...)
	dst = strconv.AppendQuote(dst, NormalizeHand(t.Hand))

	dst = append(dst, `,"multiplayer_state":`...)
	dst = strconv.AppendQuote(dst, t.MultiplayerState)

	dst = append(dst, `,"single_player_state":`...)
	dst = strconv.AppendQuote(dst, t.SinglePlayerState)

	dst = append(dst, `,"machine_torque":`...)
	dst = appendJSONFloat(dst, t.MachineTorque)

	dst = append(dst, `,"position":`...)
	dst = appendJSONFloat(dst, t.Position)

	dst = append(dst, `,"velocity":`...)
	dst = appendJSONFloat(dst, t.Velocity)

	dst = append(dst, `,"left_limit":`...)
	dst = appendJSONFloat(dst, t.LeftLimit)

	dst = append(dst, `,"right_limit":`...)
	dst = appendJSONFloat(dst, t.RightLimit)

	dst = append(dst, `,"peak_force":`...)
	dst = appendJSONFloat(dst, t.PeakForce)

	dst = append(dst, `,"timeStamp":`...)
	dst = strconv.AppendUint(dst, t.TimeStamp, 10)

	if len(t.ExtraFields) > 0 {
		keys := make([]string, 0, len(t.ExtraFields))
		for k, raw := range t.ExtraFields {
			if IsServerInjectedField(k) || len(raw) == 0 || !json.Valid(raw) {
				continue
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			dst = append(dst, ',')
			dst = strconv.AppendQuote(dst, k)
			dst = append(dst, ':')
			dst = append(dst, bytes.TrimSpace(t.ExtraFields[k])...)
		}
	}

	dst = append(dst, "}}"...)
	return dst
}

// AppendOpponentEnvelope appends t as a server sends it to the opponent (PROTOCOL.md section
// 6.1): the section 3.1 envelope followed by the three fields a server adds (section 4.2).
// lastMessageTimestamp is the server's monotonic time in milliseconds when t was received.
func AppendOpponentEnvelope(dst []byte, t *MachineTelemetry, clientID int, lastMessageTimestamp int64) []byte {
	if t == nil {
		return append(dst, `{"data":{}}`...)
	}
	dst = AppendMachineEnvelope(dst, t, true, "")
	dst = dst[:len(dst)-len("}}")]
	dst = append(dst, `,"is_connected":true,"client_id":`...)
	dst = strconv.AppendInt(dst, int64(clientID), 10)
	dst = append(dst, `,"last_message_timestamp":`...)
	dst = strconv.AppendInt(dst, lastMessageTimestamp, 10)
	return append(dst, "}}"...)
}

// MarshalMachineEnvelope serializes t into a new byte slice containing {"data":{...}}.
func MarshalMachineEnvelope(
	t *MachineTelemetry,
	includeServerPlayerID bool,
	fallbackServerPlayerID string,
) []byte {
	buf := make([]byte, 0, 384)
	return AppendMachineEnvelope(buf, t, includeServerPlayerID, fallbackServerPlayerID)
}

// ParseRivalEnvelope parses a machine/rival JSON message according to PROTOCOL.md sections 3.1 & 3.2:
//   - Requires a valid JSON object with at least one key; if multiple top-level keys exist, uses
//     the alphabetically first key (ordinal byte order, section 3.1).
//   - Applies section 3.2 defaults for missing fields.
//   - Preserves any extra fields while stripping server-injected fields (section 4.2).
//   - Rejects frames without rival machine data: empty or section 4.4 disconnect frames,
//     "is_connected": false, and frames holding nothing but server-injected fields.
func ParseRivalEnvelope(raw []byte) (*MachineTelemetry, bool) {
	innerRaw, isCmd, ok := extractEnvelopeInner(raw, false)
	if !ok || isCmd {
		return nil, false
	}

	dto, topKeys, ok := decodeDTO(innerRaw)
	if !ok {
		// Fallback to permissive per-field map decode if a field had a non-standard type.
		innerMap, mapOK := decodeStrictJSONObject(innerRaw)
		if !mapOK || rivalAbsentMap(innerMap) {
			return nil, false
		}
		return decodeTelemetryMap(innerMap), true
	}

	if rivalAbsentDTO(&dto, topKeys) {
		return nil, false
	}

	telemetry := dtoToTelemetry(&dto, innerRaw, topKeys)
	return telemetry, true
}

// ParseServerMessage parses any JSON text frame sent by the PROTOCOL.md server (sections 6.1, 6.2, 6.3).
func ParseServerMessage(
	raw []byte,
	prevServerPlayerID string,
	prevTimestamp int64,
	hasPrevTimestamp bool,
) (*InboundMessage, bool) {
	innerRaw, isCmd, ok := extractEnvelopeInner(raw, true)
	if !ok {
		return nil, false
	}

	// 1. {"cmd": ...} (sections 6.2 & 6.3)
	if isCmd {
		cmd, ok := parseCommandRaw(innerRaw)
		if !ok {
			return nil, false
		}
		return &InboundMessage{
			Kind:    InboundCommand,
			Command: cmd,
		}, true
	}

	// 2. Telemetry envelope (e.g. {"data": ...})
	dto, topKeys, dtoOK := decodeDTO(innerRaw)
	if !dtoOK {
		return parseServerMessageFallback(
			innerRaw,
			prevServerPlayerID,
			prevTimestamp,
			hasPrevTimestamp,
		)
	}

	hasIsConnected := dto.IsConnected != nil
	isConnected := hasIsConnected && *dto.IsConnected

	var clientID int
	hasClientID := dto.ClientID != nil
	if hasClientID {
		clientID = *dto.ClientID
	}

	var lastMsgTS int64
	hasLastMsgTS := dto.LastMessageTimestamp != nil
	if hasLastMsgTS {
		lastMsgTS = *dto.LastMessageTimestamp
	}

	// Empty {"data":{}} or section 4.4 left-hand disconnect {"data":{"machine_torque":-0.0}}
	if (!hasIsConnected && isEmptyOrDisconnectDTO(&dto, topKeys)) || (hasIsConnected && !isConnected) {
		return &InboundMessage{
			Kind: InboundTelemetry,
			Telemetry: &ReceivedMachineData{
				HasMachineTelemetry:     false,
				IsConnected:             false,
				ClientID:                clientID,
				HasClientID:             hasClientID,
				LastMessageTimestamp:    lastMsgTS,
				HasLastMessageTimestamp: hasLastMsgTS,
				IsNewSample:             false,
			},
		}, true
	}

	rawTelemetry := dtoToTelemetry(&dto, innerRaw, topKeys)
	return finalizeInboundTelemetry(
		rawTelemetry,
		hasIsConnected,
		isConnected,
		clientID,
		hasClientID,
		lastMsgTS,
		hasLastMsgTS,
		prevServerPlayerID,
		prevTimestamp,
		hasPrevTimestamp,
	), true
}

// extractEnvelopeInner uses a single-struct fast path when the root JSON object has 1 top-level key
// named "data" or "cmd", and falls back to alphabetical key selection when multiple or custom keys exist.
func extractEnvelopeInner(raw []byte, allowCmd bool) (inner json.RawMessage, isCmd bool, ok bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '{' || !json.Valid(trimmed) {
		return nil, false, false
	}

	topKeys := countObjectTopKeys(trimmed)
	if topKeys == 0 {
		return nil, false, false
	}

	if topKeys == 1 {
		var fast fastDataEnvelope
		if err := json.Unmarshal(trimmed, &fast); err == nil {
			if len(fast.Cmd) > 0 {
				if !allowCmd {
					return nil, true, false
				}
				return fast.Cmd, true, true
			}
			if len(fast.Data) > 0 {
				sub := bytes.TrimSpace(fast.Data)
				if len(sub) < 2 || sub[0] != '{' {
					return nil, false, false
				}
				return sub, false, true
			}
		}
	}

	topMap, mapOK := decodeStrictJSONObject(trimmed)
	if !mapOK || len(topMap) == 0 {
		return nil, false, false
	}

	if allowCmd {
		if cmdRaw, hasCmd := topMap[CommandEnvelopeKey]; hasCmd {
			return cmdRaw, true, true
		}
	}

	firstKey := alphabeticallyFirstKey(topMap)
	if firstKey == CommandEnvelopeKey {
		return nil, true, false
	}

	sub := bytes.TrimSpace(topMap[firstKey])
	if len(sub) < 2 || sub[0] != '{' {
		return nil, false, false
	}
	return sub, false, true
}

func decodeDTO(innerRaw []byte) (wireTelemetryDTO, int, bool) {
	trimmed := bytes.TrimSpace(innerRaw)
	if len(trimmed) < 2 || trimmed[0] != '{' {
		return wireTelemetryDTO{}, 0, false
	}
	topKeys := countObjectTopKeys(trimmed)
	if topKeys == 0 {
		return wireTelemetryDTO{}, 0, true
	}

	var dto wireTelemetryDTO
	if err := json.Unmarshal(trimmed, &dto); err != nil {
		return wireTelemetryDTO{}, 0, false
	}
	return dto, topKeys, true
}

func isEmptyOrDisconnectDTO(dto *wireTelemetryDTO, topKeys int) bool {
	if topKeys == 0 {
		return true
	}
	// Section 4.4: the sign rule leaves a lone zero torque in the otherwise empty object.
	return topKeys == 1 && isZero(dto.MachineTorque)
}

func isZero(v *float64) bool {
	return v != nil && *v == 0
}

// rivalAbsentDTO extends isEmptyOrDisconnectDTO to relayed rival frames, which may also carry the
// fields a server adds (section 4.2) around an empty or section 4.4 disconnect object.
func rivalAbsentDTO(dto *wireTelemetryDTO, topKeys int) bool {
	if dto.IsConnected != nil && !*dto.IsConnected {
		return true
	}
	nonData := 0
	for _, present := range [...]bool{
		dto.IsConnected != nil,
		dto.ClientID != nil,
		dto.LastMessageTimestamp != nil,
		isZero(dto.MachineTorque),
	} {
		if present {
			nonData++
		}
	}
	return topKeys <= nonData
}

func dtoToTelemetry(dto *wireTelemetryDTO, innerRaw []byte, topKeys int) *MachineTelemetry {
	t := &MachineTelemetry{}
	matched := 0

	if dto.MACAddress != nil {
		t.MACAddress = *dto.MACAddress
		matched++
	}
	if dto.PlayerID != nil {
		t.PlayerID = *dto.PlayerID
		matched++
	}
	if dto.ServerPlayerID != nil {
		t.ServerPlayerID = *dto.ServerPlayerID
		t.HasServerPlayerID = true
		matched++
	}
	if dto.Hand != nil {
		t.Hand = NormalizeHand(*dto.Hand)
		matched++
	}
	if dto.MultiplayerState != nil {
		t.MultiplayerState = *dto.MultiplayerState
		matched++
	}
	if dto.SinglePlayerState != nil {
		t.SinglePlayerState = *dto.SinglePlayerState
		matched++
	}
	if dto.MachineTorque != nil {
		t.MachineTorque = SanitizeFloat(*dto.MachineTorque)
		matched++
	}
	if dto.Position != nil {
		t.Position = SanitizeFloat(*dto.Position)
		matched++
	}
	if dto.Velocity != nil {
		t.Velocity = SanitizeFloat(*dto.Velocity)
		matched++
	}
	if dto.LeftLimit != nil {
		t.LeftLimit = SanitizeFloat(*dto.LeftLimit)
		matched++
	}
	if dto.RightLimit != nil {
		t.RightLimit = SanitizeFloat(*dto.RightLimit)
		matched++
	}
	if dto.PeakForce != nil {
		t.PeakForce = SanitizeFloat(*dto.PeakForce)
		matched++
	}
	if dto.TimeStamp != nil {
		t.TimeStamp = *dto.TimeStamp
		matched++
	}
	if dto.IsConnected != nil {
		matched++
	}
	if dto.ClientID != nil {
		matched++
	}
	if dto.LastMessageTimestamp != nil {
		matched++
	}

	// Only allocate a map when custom extra fields exist (PROTOCOL.md section 3.2).
	if topKeys > matched {
		var rawMap map[string]json.RawMessage
		if err := json.Unmarshal(innerRaw, &rawMap); err == nil {
			var extras map[string]json.RawMessage
			for k, v := range rawMap {
				if isKnownTelemetryOrServerKey(k) {
					continue
				}
				if extras == nil {
					extras = make(map[string]json.RawMessage)
				}
				cp := make(json.RawMessage, len(v))
				copy(cp, v)
				extras[k] = cp
			}
			t.ExtraFields = extras
		}
	}

	return t
}

func finalizeInboundTelemetry(
	rawTelemetry *MachineTelemetry,
	hasIsConnected bool,
	isConnected bool,
	clientID int,
	hasClientID bool,
	lastMsgTS int64,
	hasLastMsgTS bool,
	prevServerPlayerID string,
	prevTimestamp int64,
	hasPrevTimestamp bool,
) *InboundMessage {
	effectiveSPID := prevServerPlayerID
	if rawTelemetry.HasServerPlayerID {
		effectiveSPID = rawTelemetry.ServerPlayerID
	}

	normalized := rawTelemetry.Clone()
	if !normalized.HasServerPlayerID && effectiveSPID != "" {
		normalized.ServerPlayerID = effectiveSPID
		normalized.HasServerPlayerID = true
	}

	isNewSample := !hasLastMsgTS || !hasPrevTimestamp || lastMsgTS != prevTimestamp
	connectedVal := true
	if hasIsConnected {
		connectedVal = isConnected
	}

	return &InboundMessage{
		Kind: InboundTelemetry,
		Telemetry: &ReceivedMachineData{
			HasMachineTelemetry:          true,
			IsConnected:                  connectedVal,
			ClientID:                     clientID,
			HasClientID:                  hasClientID,
			LastMessageTimestamp:         lastMsgTS,
			HasLastMessageTimestamp:      hasLastMsgTS,
			IsNewSample:                  isNewSample,
			EffectiveServerPlayerID:      effectiveSPID,
			FrameContainedServerPlayerID: rawTelemetry.HasServerPlayerID,
			RawTelemetry:                 rawTelemetry,
			NormalizedTelemetry:          normalized,
		},
	}
}

func parseServerMessageFallback(
	innerRaw []byte,
	prevServerPlayerID string,
	prevTimestamp int64,
	hasPrevTimestamp bool,
) (*InboundMessage, bool) {
	dataMap, ok := decodeStrictJSONObject(innerRaw)
	if !ok {
		return nil, false
	}

	hasIsConnected := false
	isConnected := false
	if rawConn, exists := dataMap[FieldIsConnected]; exists {
		var b bool
		if err := json.Unmarshal(rawConn, &b); err == nil {
			hasIsConnected = true
			isConnected = b
		}
	}

	var clientID int
	hasClientID := false
	if rawCID, exists := dataMap[FieldClientID]; exists {
		var cid int
		if err := json.Unmarshal(rawCID, &cid); err == nil {
			clientID = cid
			hasClientID = true
		}
	}

	var lastMsgTS int64
	hasLastMsgTS := false
	if rawTS, exists := dataMap[FieldLastMessageTimestamp]; exists {
		var ts int64
		if err := json.Unmarshal(rawTS, &ts); err == nil {
			lastMsgTS = ts
			hasLastMsgTS = true
		}
	}

	if (!hasIsConnected && isEmptyOrDisconnectMap(dataMap)) || (hasIsConnected && !isConnected) {
		return &InboundMessage{
			Kind: InboundTelemetry,
			Telemetry: &ReceivedMachineData{
				HasMachineTelemetry:     false,
				IsConnected:             false,
				ClientID:                clientID,
				HasClientID:             hasClientID,
				LastMessageTimestamp:    lastMsgTS,
				HasLastMessageTimestamp: hasLastMsgTS,
				IsNewSample:             false,
			},
		}, true
	}

	rawTelemetry := decodeTelemetryMap(dataMap)
	return finalizeInboundTelemetry(
		rawTelemetry,
		hasIsConnected,
		isConnected,
		clientID,
		hasClientID,
		lastMsgTS,
		hasLastMsgTS,
		prevServerPlayerID,
		prevTimestamp,
		hasPrevTimestamp,
	), true
}

func parseCommandRaw(cmdRaw json.RawMessage) (*ServerCommand, bool) {
	trimmed := bytes.TrimSpace(cmdRaw)
	if len(trimmed) == 0 {
		return nil, false
	}

	switch trimmed[0] {
	case '"':
		var text string
		if err := json.Unmarshal(trimmed, &text); err != nil {
			return nil, false
		}
		kind := CommandOperatorText
		if text == CmdStop {
			kind = CommandStop
		}
		return &ServerCommand{
			Kind:              kind,
			CommandText:       text,
			IsOperatorCommand: true,
		}, true

	case '{':
		objMap, ok := decodeStrictJSONObject(trimmed)
		if !ok || len(objMap) == 0 {
			return nil, false
		}
		name := alphabeticallyFirstKey(objMap)
		var code int
		hasCode := false
		if err := json.Unmarshal(objMap[name], &code); err == nil {
			hasCode = true
		}

		kind := CommandUnknownMatch
		switch name {
		case CmdStart:
			kind = CommandStart
		case CmdYouWin:
			kind = CommandYouWin
		case CmdYouLose:
			kind = CommandYouLose
		}

		return &ServerCommand{
			Kind:              kind,
			CommandText:       name,
			IsOperatorCommand: false,
			RawCode:           code,
			HasRawCode:        hasCode,
		}, true

	default:
		return nil, false
	}
}

func decodeTelemetryMap(dataMap map[string]json.RawMessage) *MachineTelemetry {
	t := &MachineTelemetry{}
	var extras map[string]json.RawMessage

	for k, raw := range dataMap {
		switch k {
		case "MAC_address":
			t.MACAddress = unmarshalStringOrDefault(raw, "")
		case "player_id":
			t.PlayerID = unmarshalStringOrDefault(raw, "")
		case "server_player_id":
			if s, ok := unmarshalStrictString(raw); ok {
				t.ServerPlayerID = s
				t.HasServerPlayerID = true
			}
		case "hand":
			t.Hand = NormalizeHand(unmarshalStringOrDefault(raw, ""))
		case "multiplayer_state":
			t.MultiplayerState = unmarshalStringOrDefault(raw, "")
		case "single_player_state":
			t.SinglePlayerState = unmarshalStringOrDefault(raw, "")
		case "machine_torque":
			t.MachineTorque = unmarshalFloatOrDefault(raw, 0)
		case "position":
			t.Position = unmarshalFloatOrDefault(raw, 0)
		case "velocity":
			t.Velocity = unmarshalFloatOrDefault(raw, 0)
		case "left_limit":
			t.LeftLimit = unmarshalFloatOrDefault(raw, 0)
		case "right_limit":
			t.RightLimit = unmarshalFloatOrDefault(raw, 0)
		case "peak_force":
			t.PeakForce = unmarshalFloatOrDefault(raw, 0)
		case "timeStamp":
			t.TimeStamp = unmarshalUint64OrDefault(raw, 0)
		case FieldIsConnected, FieldClientID, FieldLastMessageTimestamp:
		default:
			if extras == nil {
				extras = make(map[string]json.RawMessage)
			}
			cp := make(json.RawMessage, len(raw))
			copy(cp, raw)
			extras[k] = cp
		}
	}

	t.ExtraFields = extras
	return t
}

func countObjectTopKeys(validObj []byte) int {
	inString := false
	escaped := false
	depth := 0
	commas := 0
	hasAnyTokenInside := false

	for i := 0; i < len(validObj); i++ {
		c := validObj[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			if c == '"' {
				inString = false
			}
			continue
		}

		switch c {
		case '"':
			if depth == 1 {
				hasAnyTokenInside = true
			}
			inString = true
		case '{', '[':
			if depth == 1 {
				hasAnyTokenInside = true
			}
			depth++
		case '}', ']':
			depth--
		case ',':
			if depth == 1 {
				commas++
			}
		case ' ', '\t', '\r', '\n':
		default:
			if depth == 1 {
				hasAnyTokenInside = true
			}
		}
	}

	if !hasAnyTokenInside {
		return 0
	}
	return commas + 1
}

func isKnownTelemetryOrServerKey(k string) bool {
	switch k {
	case "MAC_address", "player_id", "server_player_id", "hand",
		"multiplayer_state", "single_player_state", "machine_torque",
		"position", "velocity", "left_limit", "right_limit", "peak_force",
		"timeStamp", FieldIsConnected, FieldClientID, FieldLastMessageTimestamp:
		return true
	default:
		return false
	}
}

func decodeStrictJSONObject(raw []byte) (map[string]json.RawMessage, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '{' || !json.Valid(trimmed) {
		return nil, false
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &m); err != nil || m == nil {
		return nil, false
	}
	return m, true
}

func alphabeticallyFirstKey(m map[string]json.RawMessage) string {
	first := ""
	started := false
	for k := range m {
		if !started || k < first {
			first = k
			started = true
		}
	}
	return first
}

func isEmptyOrDisconnectMap(m map[string]json.RawMessage) bool {
	if len(m) == 0 {
		return true
	}
	return len(m) == 1 && isZeroRaw(m, "machine_torque")
}

func isZeroRaw(m map[string]json.RawMessage, key string) bool {
	raw, ok := m[key]
	if !ok {
		return false
	}
	var v float64
	return json.Unmarshal(raw, &v) == nil && v == 0
}

// rivalAbsentMap is rivalAbsentDTO for the per-field fallback decode.
func rivalAbsentMap(m map[string]json.RawMessage) bool {
	if raw, ok := m[FieldIsConnected]; ok {
		var connected bool
		if err := json.Unmarshal(raw, &connected); err == nil && !connected {
			return true
		}
	}
	nonData := 0
	for _, k := range [...]string{FieldIsConnected, FieldClientID, FieldLastMessageTimestamp} {
		if _, ok := m[k]; ok {
			nonData++
		}
	}
	if isZeroRaw(m, "machine_torque") {
		nonData++
	}
	return len(m) <= nonData
}

func unmarshalStrictString(raw json.RawMessage) (string, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '"' {
		return "", false
	}
	var s string
	if err := json.Unmarshal(trimmed, &s); err != nil {
		return "", false
	}
	return s, true
}

func unmarshalStringOrDefault(raw json.RawMessage, fallback string) string {
	if s, ok := unmarshalStrictString(raw); ok {
		return s
	}
	return fallback
}

func unmarshalFloatOrDefault(raw json.RawMessage, fallback float64) float64 {
	var v float64
	if err := json.Unmarshal(raw, &v); err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return fallback
	}
	return v
}

func unmarshalUint64OrDefault(raw json.RawMessage, fallback uint64) uint64 {
	var u uint64
	if err := json.Unmarshal(raw, &u); err == nil {
		return u
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil && f >= 0 && f <= float64(math.MaxUint64) {
		return uint64(f)
	}
	return fallback
}

func appendJSONFloat(dst []byte, v float64) []byte {
	v = SanitizeFloat(v)
	return strconv.AppendFloat(dst, v, 'g', -1, 64)
}
