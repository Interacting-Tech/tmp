package bridge

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Interacting-Tech/tmp/pkg/machineclient"
	"github.com/Interacting-Tech/tmp/pkg/protocol"
)

var bridgeMarshalPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

// Config configures the bidirectional MachineBridge running on an arm-wrestling machine.
type Config struct {
	// MachineOptions configures the local machine connection (ws://<machine-ip>:8080/ws?role=machine).
	MachineOptions machineclient.Options

	// OutboundServerPlayerIDCadence controls how often "server_player_id" is included in JSON sent
	// to our server (default: 1 = include on every message when known so our server identifies the
	// player immediately; set to 50 to mirror raw machine cadence).
	OutboundServerPlayerIDCadence int

	// OnlyForwardNewSamples, when true, only sends telemetry to our server when last_message_timestamp
	// advances (skipping duplicate 10ms server repeats unless MultiplayerState changed). Default: false.
	OnlyForwardNewSamples bool
}

// MachineBridge runs on the physical arm-wrestling machine:
//  1. Resolves the local machine's IP and connects to ws://<machine-ip>:8080/ws?role=machine.
//  2. Reads the local machine's data according to PROTOCOL.md and streams it to our server.
//  3. Accepts the rival's data from our server and transfers it to the local machine according to PROTOCOL.md.
type MachineBridge struct {
	machineClient *machineclient.Client
	upstream      ServerTransport
	cadence       uint64
	onlyNew       bool

	mu                  sync.Mutex
	publishedFrames     uint64
	lastPublishedState  string
	clearingRival       atomic.Bool
}

// New creates a MachineBridge pairing a local machine client with an upstream ServerTransport.
func New(cfg Config, upstream ServerTransport) (*MachineBridge, error) {
	cadence := uint64(1)
	if cfg.OutboundServerPlayerIDCadence > 1 {
		cadence = uint64(cfg.OutboundServerPlayerIDCadence)
	}

	b := &MachineBridge{
		upstream: upstream,
		cadence:  cadence,
		onlyNew:  cfg.OnlyForwardNewSamples,
	}

	userOnLocalData := cfg.MachineOptions.OnLocalData
	userOnCommand := cfg.MachineOptions.OnCommand
	userOnLocalConn := cfg.MachineOptions.OnLocalMachineConnectionChange

	mcOpts := cfg.MachineOptions
	mcOpts.OnLocalData = func(snap *protocol.ReceivedMachineData) {
		b.handleLocalMachineData(snap)
		if userOnLocalData != nil {
			userOnLocalData(snap)
		}
	}
	mcOpts.OnCommand = func(cmd protocol.ServerCommand) {
		b.handleLocalServerCommand(cmd)
		if userOnCommand != nil {
			userOnCommand(cmd)
		}
	}
	mcOpts.OnLocalMachineConnectionChange = func(connected bool) {
		if !connected {
			b.handleLocalMachineDisconnected()
		}
		if userOnLocalConn != nil {
			userOnLocalConn(connected)
		}
	}

	mc, err := machineclient.New(mcOpts)
	if err != nil {
		return nil, err
	}
	b.machineClient = mc
	return b, nil
}

// LocalClient returns the underlying local machine WebSocket client.
func (b *MachineBridge) LocalClient() *machineclient.Client {
	return b.machineClient
}

// Start begins both the upstream connection to our server and the local connection to the machine.
func (b *MachineBridge) Start(ctx context.Context) error {
	if b.upstream != nil {
		err := b.upstream.Start(ctx, UpstreamHandlers{
			OnRivalTelemetry: func(rival *protocol.MachineTelemetry) {
				b.machineClient.ProvideRivalTelemetry(rival)
			},
			OnRivalDisconnected: func() {
				b.triggerClearRivalAsync()
			},
			OnServerStopCommand: func() {
				b.triggerClearRivalAsync()
			},
		})
		if err != nil {
			return err
		}
	}

	return b.machineClient.Start(ctx)
}

// Stop gracefully shuts down both the local machine connection and the upstream server connection.
func (b *MachineBridge) Stop(ctx context.Context) error {
	errLocal := b.machineClient.Stop(ctx)
	var errUp error
	if b.upstream != nil {
		errUp = b.upstream.Stop(ctx)
	}
	if errLocal != nil {
		return errLocal
	}
	return errUp
}

func (b *MachineBridge) handleLocalMachineData(snap *protocol.ReceivedMachineData) {
	if b.upstream == nil || snap == nil || !snap.HasMachineTelemetry || snap.NormalizedTelemetry == nil {
		return
	}

	state := snap.NormalizedTelemetry.MultiplayerState
	isTerminal := protocol.IsTerminalMatchState(state)

	b.mu.Lock()
	stateChanged := state != b.lastPublishedState
	if b.onlyNew && !snap.IsNewSample && !stateChanged {
		b.mu.Unlock()
		return
	}
	b.publishedFrames++
	frameIdx := b.publishedFrames
	alreadySentSameTerminal := isTerminal && !stateChanged
	b.lastPublishedState = state
	b.mu.Unlock()

	includeSPID := ((frameIdx - 1) % b.cadence) == 0

	bufPtr := bridgeMarshalPool.Get().(*[]byte)
	payload := protocol.AppendMachineEnvelope(
		(*bufPtr)[:0],
		snap.NormalizedTelemetry,
		includeSPID,
		snap.EffectiveServerPlayerID,
	)

	b.upstream.SendMachineData(payload, isTerminal && !alreadySentSameTerminal)

	*bufPtr = payload[:0]
	bridgeMarshalPool.Put(bufPtr)
}

func (b *MachineBridge) handleLocalServerCommand(cmd protocol.ServerCommand) {
	if b.upstream == nil {
		return
	}

	// Per PROTOCOL.md section 6.2, YOU_LOSE / YOU_WIN from the local server's 1ms loop proves the
	// physical machine reported GAMEOVER_WIN / GAMEOVER_LOSE even if it cleared its state before
	// the next 10ms telemetry tick. Synthesize and forward that terminal state if not yet sent.
	inferred := cmd.InferredLocalGameOverState()
	if inferred == "" {
		return
	}

	latest := b.machineClient.LatestLocalData()
	if latest == nil || !latest.HasMachineTelemetry || latest.NormalizedTelemetry == nil {
		return
	}

	b.mu.Lock()
	if b.lastPublishedState == inferred {
		b.mu.Unlock()
		return
	}
	b.publishedFrames++
	frameIdx := b.publishedFrames
	b.lastPublishedState = inferred
	b.mu.Unlock()

	synthesized := latest.NormalizedTelemetry.Clone()
	synthesized.MultiplayerState = inferred
	includeSPID := ((frameIdx - 1) % b.cadence) == 0

	bufPtr := bridgeMarshalPool.Get().(*[]byte)
	payload := protocol.AppendMachineEnvelope(
		(*bufPtr)[:0],
		synthesized,
		includeSPID,
		latest.EffectiveServerPlayerID,
	)

	b.upstream.SendMachineData(payload, true)

	*bufPtr = payload[:0]
	bridgeMarshalPool.Put(bufPtr)
}

func (b *MachineBridge) handleLocalMachineDisconnected() {
	b.mu.Lock()
	b.publishedFrames = 0
	b.lastPublishedState = ""
	b.mu.Unlock()

	if b.upstream != nil {
		b.upstream.SendMachineDisconnected()
	}
}

func (b *MachineBridge) triggerClearRivalAsync() {
	if !b.clearingRival.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer b.clearingRival.Store(false)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.machineClient.ClearRival(ctx)
	}()
}
