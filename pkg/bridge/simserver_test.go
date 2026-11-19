package bridge_test

import (
	"net"
	"time"

	"github.com/Interacting-Tech/tmp/pkg/machinesim"
)

type simulatedMachineServer = machinesim.LocalServer

func startSimulatedMachineServer(bindIP net.IP, postMatchDuration time.Duration) (*simulatedMachineServer, error) {
	return machinesim.StartLocalServer(bindIP, postMatchDuration)
}
