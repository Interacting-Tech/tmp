package bridge_test

import (
	"net"
	"time"

	"dev.azure.com/InteractingTechnologies/Glenfiddich/_git/Arm-Wrestling/pkg/machinesim"
)

type simulatedMachineServer = machinesim.LocalServer

func startSimulatedMachineServer(bindIP net.IP, postMatchDuration time.Duration) (*simulatedMachineServer, error) {
	return machinesim.StartLocalServer(bindIP, postMatchDuration)
}
