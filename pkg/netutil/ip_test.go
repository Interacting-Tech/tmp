package netutil_test

import (
	"net"
	"testing"

	"dev.azure.com/InteractingTechnologies/Glenfiddich/_git/Arm-Wrestling/pkg/netutil"
)

func TestResolveMachineIP_FindsLocalHostAddressAndBuildsProtocolURL(t *testing.T) {
	ip, err := netutil.ResolveMachineIP(netutil.IPResolveOptions{AllowLoopback: true})
	if err != nil {
		t.Fatalf("ResolveMachineIP failed: %v", err)
	}
	if ip.To4() == nil {
		t.Fatalf("expected IPv4 address, got %s", ip)
	}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatalf("InterfaceAddrs: %v", err)
	}
	found := false
	for _, a := range addrs {
		if ipNet, ok := a.(*net.IPNet); ok && ipNet.IP.Equal(ip) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("resolved IP %s not found among host InterfaceAddrs", ip)
	}

	wsURL, err := netutil.BuildMachineWSURL(ip, 8080, "/ws?role=machine")
	if err != nil {
		t.Fatalf("BuildMachineWSURL: %v", err)
	}
	want := "ws://" + ip.String() + ":8080/ws?role=machine"
	if wsURL != want {
		t.Fatalf("wsURL = %q, want %q", wsURL, want)
	}
}

func TestResolveMachineIP_OverrideAndIPv6URL(t *testing.T) {
	ip, err := netutil.ResolveMachineIP(netutil.IPResolveOptions{
		MachineIPOverride: "192.168.50.12",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ip.String() != "192.168.50.12" {
		t.Fatalf("got %s, want 192.168.50.12", ip)
	}

	v6URL, err := netutil.BuildMachineWSURL(net.ParseIP("::1"), 9090, "ws?role=machine")
	if err != nil {
		t.Fatalf("BuildMachineWSURL v6: %v", err)
	}
	if v6URL != "ws://[::1]:9090/ws?role=machine" {
		t.Fatalf("got %q", v6URL)
	}
}
