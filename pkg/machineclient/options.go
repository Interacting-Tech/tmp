package machineclient

import (
	"time"

	"github.com/Interacting-Tech/tmp/pkg/protocol"
)

// Options configures a Client connecting to the local arm-wrestling server per PROTOCOL.md.
type Options struct {
	// Port of the local arm-wrestling server (default: 8080, PROTOCOL.md section 2).
	Port int

	// PathAndQuery of the WebSocket endpoint (default: "/ws?role=machine", PROTOCOL.md section 2).
	PathAndQuery string

	// MachineIPOverride optionally overrides automatic local machine IP discovery.
	MachineIPOverride string

	// PreferredInterface optionally selects a specific NIC (e.g. "eth0", "wlan0") when resolving IP.
	PreferredInterface string

	// AllowLoopbackFallback falls back to 127.0.0.1 if no active non-loopback IPv4 interface exists.
	AllowLoopbackFallback bool

	// RivalSendInterval is the periodic interval at which active rival telemetry is transmitted
	// to the local machine (default: 10ms, matching PROTOCOL.md section 7).
	RivalSendInterval time.Duration

	// MinFrameSpacing enforces minimum spacing when coalescing rapid rival updates
	// (default: 1ms, matching the server's 1ms main loop in PROTOCOL.md section 4.1).
	// Set negative to disable spacing in tests.
	MinFrameSpacing time.Duration

	// KeepSendingLatestRival controls whether the client keeps re-sending the latest rival state
	// every RivalSendInterval while a rival is active (default: true).
	KeepSendingLatestRival *bool

	// ServerPlayerIDCadence emits "server_player_id" once every N outbound rival messages
	// (default: 50, matching PROTOCOL.md section 3.2; set to 1 to emit on every message).
	ServerPlayerIDCadence int

	// NormalizeLocalOpponentTorque undoes the local server's section 4.3 opponent_torque sign flip
	// on NormalizedTelemetry so bridged servers never double-negate torque (default: true).
	NormalizeLocalOpponentTorque *bool

	// DialTimeout bounds WebSocket TCP handshake duration (default: 3s).
	DialTimeout time.Duration

	// WriteTimeout bounds each individual WebSocket frame write so a stalled socket never blocks (default: 1s).
	WriteTimeout time.Duration

	// ReconnectInitialDelay is the initial backoff when WebSocket connection fails or drops (default: 100ms).
	ReconnectInitialDelay time.Duration

	// ReconnectMaxDelay caps exponential reconnect backoff (default: 3s).
	ReconnectMaxDelay time.Duration

	// MaxInboundMessageBytes caps inbound WebSocket frame size (default: 64 KiB).
	MaxInboundMessageBytes int64

	// Callbacks (invoked synchronously on receive/supervisor goroutine; panic-safe).
	OnLocalData                    func(*protocol.ReceivedMachineData)
	OnCommand                      func(protocol.ServerCommand)
	OnServerConnectionChange       func(connected bool)
	OnLocalMachineConnectionChange func(connected bool)
	OnDiagnostic                   func(msg string, err error)
}

func (o Options) withDefaults() Options {
	out := o
	if out.Port <= 0 {
		out.Port = 8080
	}
	if out.PathAndQuery == "" {
		out.PathAndQuery = "/ws?role=machine"
	}
	if out.RivalSendInterval <= 0 {
		out.RivalSendInterval = 10 * time.Millisecond
	}
	if out.MinFrameSpacing == 0 {
		out.MinFrameSpacing = time.Millisecond
	} else if out.MinFrameSpacing < 0 {
		out.MinFrameSpacing = 0
	}
	if out.KeepSendingLatestRival == nil {
		b := true
		out.KeepSendingLatestRival = &b
	}
	if out.ServerPlayerIDCadence <= 0 {
		out.ServerPlayerIDCadence = 50
	}
	if out.NormalizeLocalOpponentTorque == nil {
		b := true
		out.NormalizeLocalOpponentTorque = &b
	}
	if out.DialTimeout <= 0 {
		out.DialTimeout = 3 * time.Second
	}
	if out.WriteTimeout <= 0 {
		out.WriteTimeout = time.Second
	}
	if out.ReconnectInitialDelay <= 0 {
		out.ReconnectInitialDelay = 100 * time.Millisecond
	}
	if out.ReconnectMaxDelay < out.ReconnectInitialDelay {
		out.ReconnectMaxDelay = 3 * time.Second
	}
	if out.MaxInboundMessageBytes <= 0 {
		out.MaxInboundMessageBytes = 64 * 1024
	}
	return out
}

// BoolPtr returns a pointer to b for configuring optional boolean settings.
func BoolPtr(b bool) *bool {
	return &b
}
