package netutil

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
)

var virtualInterfacePrefixes = []string{
	"docker",
	"veth",
	"br-",
	"virbr",
	"vmnet",
	"vboxnet",
	"wsl",
	"tun",
	"tap",
	"tailscale",
	"wg",
	"zt",
}

// IPResolveOptions controls how the local machine's IP address is discovered.
type IPResolveOptions struct {
	MachineIPOverride  string
	PreferredInterface string
	AllowLoopback      bool
}

// ResolveMachineIP reads the active IPv4 address of the host machine this process runs on.
func ResolveMachineIP(opts IPResolveOptions) (net.IP, error) {
	if override := strings.TrimSpace(opts.MachineIPOverride); override != "" {
		if ip := net.ParseIP(override); ip != nil {
			if v4 := ip.To4(); v4 != nil {
				return v4, nil
			}
			return ip, nil
		}

		addrs, err := net.LookupIP(override)
		if err != nil || len(addrs) == 0 {
			return nil, fmt.Errorf("resolve MachineIPOverride %q: %w", override, err)
		}
		for _, a := range addrs {
			if v4 := a.To4(); v4 != nil {
				return v4, nil
			}
		}
		return addrs[0], nil
	}

	if pref := strings.TrimSpace(opts.PreferredInterface); pref != "" {
		if ip := ipFromNamedInterface(pref); ip != nil {
			return ip, nil
		}
	}

	// 1. Query the OS routing table using a connectionless UDP socket (sends zero packets on wire).
	if routed := tryOutboundRoutedIPv4(); routed != nil {
		return routed, nil
	}

	// 2. Scan active network interfaces (handles isolated cabinet LANs without a default gateway).
	if nicIP := tryActiveInterfaceIPv4(); nicIP != nil {
		return nicIP, nil
	}

	if opts.AllowLoopback {
		return net.IPv4(127, 0, 0, 1), nil
	}

	return nil, errors.New("unable to determine a non-loopback IPv4 address for the local machine")
}

// BuildMachineWSURL builds the PROTOCOL.md section 2 WebSocket URL:
// ws://<machine-ip>:8080/ws?role=machine
func BuildMachineWSURL(machineIP net.IP, port int, pathAndQuery string) (string, error) {
	if len(machineIP) == 0 {
		return "", errors.New("machineIP is empty")
	}
	if port <= 0 || port > 65535 {
		return "", fmt.Errorf("invalid port %d", port)
	}

	pq := strings.TrimSpace(pathAndQuery)
	if pq == "" {
		pq = "/ws?role=machine"
	} else if !strings.HasPrefix(pq, "/") {
		pq = "/" + pq
	}

	host := machineIP.String()
	if v4 := machineIP.To4(); v4 != nil {
		host = v4.String()
	}

	return "ws://" + net.JoinHostPort(host, fmt.Sprintf("%d", port)) + pq, nil
}

func tryOutboundRoutedIPv4() net.IP {
	conn, err := net.Dial("udp4", "8.8.8.8:80")
	if err != nil {
		return nil
	}
	defer conn.Close()

	udpAddr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || udpAddr == nil {
		return nil
	}

	v4 := udpAddr.IP.To4()
	if !isUsableLANIPv4(v4) {
		return nil
	}
	return v4
}

func ipFromNamedInterface(name string) net.IP {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	for _, iface := range ifaces {
		if !strings.EqualFold(iface.Name, name) {
			continue
		}
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		if ip := firstUsableIPv4(iface); ip != nil {
			return ip
		}
	}
	return nil
}

func tryActiveInterfaceIPv4() net.IP {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}

	type candidate struct {
		priority int
		ip       net.IP
	}

	var candidates []candidate
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		if iface.Flags&(net.FlagLoopback|net.FlagPointToPoint) != 0 {
			continue
		}

		ip := firstUsableIPv4(iface)
		if ip == nil {
			continue
		}

		prio := 0
		if isLikelyVirtualInterface(iface.Name) {
			prio += 100
		}
		if iface.Flags&net.FlagBroadcast == 0 {
			prio += 10
		}

		candidates = append(candidates, candidate{priority: prio, ip: ip})
	}

	if len(candidates) == 0 {
		return nil
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].priority < candidates[j].priority
	})

	return candidates[0].ip
}

func firstUsableIPv4(iface net.Interface) net.IP {
	addrs, err := iface.Addrs()
	if err != nil {
		return nil
	}
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if v4 := ip.To4(); isUsableLANIPv4(v4) {
			return v4
		}
	}
	return nil
}

func isUsableLANIPv4(v4 net.IP) bool {
	if len(v4) != net.IPv4len {
		return false
	}
	if v4.IsLoopback() || v4.IsUnspecified() || v4.IsMulticast() {
		return false
	}
	// Exclude 169.254.0.0/16 IPv4 link-local (APIPA)
	if v4[0] == 169 && v4[1] == 254 {
		return false
	}
	return true
}

func isLikelyVirtualInterface(name string) bool {
	lower := strings.ToLower(name)
	for _, prefix := range virtualInterfacePrefixes {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}
