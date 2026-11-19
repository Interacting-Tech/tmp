package machinesim

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"dev.azure.com/InteractingTechnologies/Glenfiddich/_git/Arm-Wrestling/pkg/protocol"
	"github.com/gorilla/websocket"
)

// StateIdle is what a simulated machine reports when it is neither ready nor in a battle.
// PROTOCOL.md does not name that state; its server takes no action on values it does not know.
const StateIdle = "MULTIPLAYER_SM_IDLE"

const sentHistory = 256

// MachineConfig describes a simulated physical machine.
type MachineConfig struct {
	MACAddress string
	PlayerID   string
	// Hand is "left" or "right".
	Hand       string
	LeftLimit  float64
	RightLimit float64
	// SendInterval is the time between telemetry frames (default: 10ms). PROTOCOL.md does not say
	// how often a machine sends.
	SendInterval time.Duration
}

// Machine plays a physical machine connected to its local PROTOCOL.md server. It sends telemetry
// frames in the PROTOCOL.md section 3 format and obeys the server's commands: START begins the
// battle if the machine is ready, and YOU_WIN, YOU_LOSE and STOP end it.
type Machine struct {
	cfg      MachineConfig
	ws       *websocket.Conn
	started  time.Time
	commands chan string
	done     chan struct{}
	stop     sync.Once

	mu          sync.Mutex
	state       string
	position    float64
	speed       float64
	torque      float64
	peakForce   float64
	lastMove    time.Time
	sent        int
	history     [sentHistory]sentFrame
	rivalFrames int
	lastRival   string
}

type sentFrame struct {
	timeStamp uint64
	frame     string
}

// ConnectMachine connects a simulated machine to a local server and starts sending telemetry.
func ConnectMachine(endpointURL string, cfg MachineConfig) (*Machine, error) {
	if cfg.SendInterval <= 0 {
		cfg.SendInterval = 10 * time.Millisecond
	}
	ws, _, err := websocket.DefaultDialer.Dial(endpointURL, nil)
	if err != nil {
		return nil, fmt.Errorf("connect machine to %s: %w", endpointURL, err)
	}
	m := &Machine{
		cfg:      cfg,
		ws:       ws,
		started:  time.Now(),
		commands: make(chan string, 64),
		done:     make(chan struct{}),
		state:    StateIdle,
	}
	go m.sendLoop()
	go m.receiveLoop()
	return m, nil
}

// Close disconnects the machine.
func (m *Machine) Close() {
	m.stop.Do(func() {
		close(m.done)
		_ = m.ws.Close()
	})
}

// GetReady makes the machine report MULTIPLAYER_SM_READY until its local server starts the battle.
func (m *Machine) GetReady() {
	m.mu.Lock()
	m.state = protocol.StateReady
	m.mu.Unlock()
}

// Pull sets how the handle behaves during a battle: its speed in position units per second
// (negative moves towards left_limit) and the torque it reports. The handle stops at its limits.
func (m *Machine) Pull(speed, torque float64) {
	m.mu.Lock()
	m.speed, m.torque = speed, torque
	m.mu.Unlock()
}

// Commands delivers the name of each command the machine receives from its local server:
// "START", "YOU_WIN", "YOU_LOSE", or an operator command such as "STOP".
func (m *Machine) Commands() <-chan string {
	return m.commands
}

// Position is the handle's current position.
func (m *Machine) Position() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return round2(m.position)
}

// Counts reports how many frames the machine has sent and how many rival frames it has received.
func (m *Machine) Counts() (sent, rivalFrames int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sent, m.rivalFrames
}

// LastRivalFrame is the newest rival telemetry frame the machine received, or "" when its local
// server last reported no rival.
func (m *Machine) LastRivalFrame() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastRival
}

// SentFrame returns the frame the machine sent with the given timeStamp, if it was recent.
func (m *Machine) SentFrame(timeStamp uint64) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, f := range m.history {
		if f.frame != "" && f.timeStamp == timeStamp {
			return f.frame, true
		}
	}
	return "", false
}

func (m *Machine) sendLoop() {
	ticker := time.NewTicker(m.cfg.SendInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case now := <-ticker.C:
			if err := m.ws.WriteMessage(websocket.TextMessage, []byte(m.nextFrame(now))); err != nil {
				return
			}
		}
	}
}

func (m *Machine) nextFrame(now time.Time) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	torque, velocity := 0.0, 0.0
	if m.state == protocol.StateRun {
		m.position += m.speed * now.Sub(m.lastMove).Seconds()
		m.position = math.Max(m.cfg.LeftLimit, math.Min(m.cfg.RightLimit, m.position))
		velocity = m.speed
		// A steady pull with a small ripple, so consecutive frames differ like real sensor data.
		torque = round2(m.torque * (1 + 0.1*math.Sin(now.Sub(m.started).Seconds()*9)))
		m.peakForce = math.Max(m.peakForce, math.Abs(torque))
	}
	m.lastMove = now

	timeStamp := uint64(now.Sub(m.started).Microseconds())
	frame := fmt.Sprintf(`{"data":{"MAC_address":%q,"player_id":%q,"hand":%q,"multiplayer_state":%q,"single_player_state":"","machine_torque":%v,"position":%v,"velocity":%v,"left_limit":%v,"right_limit":%v,"peak_force":%v,"timeStamp":%d}}`,
		m.cfg.MACAddress, m.cfg.PlayerID, m.cfg.Hand, m.state, torque, round2(m.position), velocity,
		m.cfg.LeftLimit, m.cfg.RightLimit, m.peakForce, timeStamp)
	m.history[m.sent%sentHistory] = sentFrame{timeStamp: timeStamp, frame: frame}
	m.sent++
	return frame
}

func (m *Machine) receiveLoop() {
	defer m.Close()
	for {
		_, raw, err := m.ws.ReadMessage()
		if err != nil {
			return
		}
		var msg struct {
			Data map[string]json.RawMessage `json:"data"`
			Cmd  json.RawMessage            `json:"cmd"`
		}
		if json.Unmarshal(raw, &msg) != nil {
			continue
		}
		if len(msg.Cmd) > 0 {
			if name := commandName(msg.Cmd); name != "" {
				m.obey(name)
			}
			continue
		}

		m.mu.Lock()
		if _, hasRival := msg.Data["player_id"]; hasRival {
			m.rivalFrames++
			m.lastRival = string(raw)
		} else {
			m.lastRival = ""
		}
		m.mu.Unlock()
	}
}

// commandName returns the command of a section 6.2 match command object or a section 6.3
// operator command string.
func commandName(cmd json.RawMessage) string {
	var text string
	if json.Unmarshal(cmd, &text) == nil {
		return text
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(cmd, &obj) != nil || len(obj) == 0 {
		return ""
	}
	names := make([]string, 0, len(obj))
	for name := range obj {
		names = append(names, name)
	}
	sort.Strings(names)
	return names[0]
}

func (m *Machine) obey(command string) {
	m.mu.Lock()
	switch command {
	case protocol.CmdStart:
		if m.state == protocol.StateReady {
			m.state = protocol.StateRun
			m.position, m.peakForce = 0, 0
			m.lastMove = time.Now()
		}
	case protocol.CmdYouWin, protocol.CmdYouLose, protocol.CmdStop:
		m.state = StateIdle
	}
	m.mu.Unlock()

	select {
	case m.commands <- command:
	default:
	}
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}
