using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using ArmWrestling.MachineClient.Configuration;

namespace ArmWrestling.MachineClient.Networking;

/// <summary>
/// Resolves the IPv4 address of the host machine this process runs on and constructs
/// the PROTOCOL.md WebSocket endpoint URI (ws://&lt;machine-ip&gt;:8080/ws?role=machine).
/// </summary>
public static class LocalIpResolver
{
    private static readonly string[] VirtualInterfacePrefixes =
    [
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
        "zt"
    ];

    /// <summary>
    /// Reads the IPv4 address of the machine this process is running on.
    /// </summary>
    public static IPAddress ResolveMachineIp(MachineClientOptions? options = null)
    {
        options ??= new MachineClientOptions();

        if (!string.IsNullOrWhiteSpace(options.MachineIpOverride))
        {
            var trimmed = options.MachineIpOverride.Trim();
            if (IPAddress.TryParse(trimmed, out var parsedOverride))
            {
                return parsedOverride;
            }

            var hostAddresses = Dns.GetHostAddresses(trimmed);
            var ipv4FromHost = hostAddresses.FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork)
                ?? hostAddresses.FirstOrDefault();
            if (ipv4FromHost is not null)
            {
                return ipv4FromHost;
            }

            throw new InvalidOperationException(
                $"Unable to resolve configured MachineIpOverride '{options.MachineIpOverride}'.");
        }

        if (!string.IsNullOrWhiteSpace(options.PreferredNetworkInterfaceName))
        {
            var preferredIp = TryGetAddressFromNamedInterface(options.PreferredNetworkInterfaceName.Trim());
            if (preferredIp is not null)
            {
                return preferredIp;
            }
        }

        // 1. Query the OS routing table via a connectionless UDP socket (sends zero network packets).
        var routedIp = TryResolveOutboundRoutedIpv4();
        if (routedIp is not null && !IPAddress.IsLoopback(routedIp) && !IsLinkLocalIpv4(routedIp))
        {
            return routedIp;
        }

        // 2. Enumerate active network interfaces (handles isolated LANs with no default internet gateway).
        var interfaceIp = TryResolveFromNetworkInterfaces();
        if (interfaceIp is not null)
        {
            return interfaceIp;
        }

        // 3. Fallback to DNS host table non-loopback IPv4 if available.
        var dnsIp = TryResolveFromDnsHostEntry();
        if (dnsIp is not null)
        {
            return dnsIp;
        }

        if (options.AllowLoopbackFallback)
        {
            return IPAddress.Loopback;
        }

        throw new InvalidOperationException(
            "Could not determine a non-loopback IPv4 address for the local machine.");
    }

    /// <summary>
    /// Builds the machine WebSocket URI per PROTOCOL.md section 2:
    /// ws://&lt;server-ip&gt;:8080/ws?role=machine
    /// </summary>
    public static Uri BuildMachineWebSocketUri(IPAddress machineIp, MachineClientOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(machineIp);
        options ??= new MachineClientOptions();

        if (options.Port is <= 0 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(options), $"Invalid port: {options.Port}.");
        }

        var pathAndQuery = string.IsNullOrWhiteSpace(options.PathAndQuery)
            ? "/ws?role=machine"
            : options.PathAndQuery.Trim();

        if (!pathAndQuery.StartsWith('/'))
        {
            pathAndQuery = "/" + pathAndQuery;
        }

        var host = machineIp.AddressFamily == AddressFamily.InterNetworkV6
            ? $"[{machineIp}]"
            : machineIp.ToString();

        return new Uri($"ws://{host}:{options.Port}{pathAndQuery}", UriKind.Absolute);
    }

    private static IPAddress? TryResolveOutboundRoutedIpv4()
    {
        try
        {
            using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            socket.Connect("8.8.8.8", 80);
            if (socket.LocalEndPoint is IPEndPoint localEndPoint &&
                !localEndPoint.Address.Equals(IPAddress.Any))
            {
                return localEndPoint.Address;
            }
        }
        catch (SocketException)
        {
            // No default route available (e.g., isolated local LAN); fall back to interface inspection.
        }
        catch (ObjectDisposedException)
        {
        }

        return null;
    }

    private static IPAddress? TryGetAddressFromNamedInterface(string interfaceName)
    {
        foreach (var nic in NetworkInterface.GetAllNetworkInterfaces())
        {
            if (!string.Equals(nic.Name, interfaceName, StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(nic.Description, interfaceName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (nic.OperationalStatus != OperationalStatus.Up)
            {
                continue;
            }

            var ip = GetFirstUsableIpv4(nic);
            if (ip is not null)
            {
                return ip;
            }
        }

        return null;
    }

    private static IPAddress? TryResolveFromNetworkInterfaces()
    {
        NetworkInterface[] interfaces;
        try
        {
            interfaces = NetworkInterface.GetAllNetworkInterfaces();
        }
        catch (NetworkInformationException)
        {
            return null;
        }

        var candidates = new List<(int Priority, IPAddress Address)>();

        foreach (var nic in interfaces)
        {
            if (nic.OperationalStatus != OperationalStatus.Up)
            {
                continue;
            }

            if (nic.NetworkInterfaceType is NetworkInterfaceType.Loopback or NetworkInterfaceType.Tunnel)
            {
                continue;
            }

            var ipv4 = GetFirstUsableIpv4(nic);
            if (ipv4 is null)
            {
                continue;
            }

            var isVirtual = IsLikelyVirtualInterface(nic.Name, nic.Description);
            var hasGateway = nic.GetIPProperties().GatewayAddresses
                .Any(g => g.Address.AddressFamily == AddressFamily.InterNetwork &&
                          !g.Address.Equals(IPAddress.Any));

            var isPhysicalType = nic.NetworkInterfaceType is
                NetworkInterfaceType.Ethernet or
                NetworkInterfaceType.GigabitEthernet or
                NetworkInterfaceType.FastEthernetT or
                NetworkInterfaceType.FastEthernetFx or
                NetworkInterfaceType.Wireless80211;

            var priority = (isVirtual ? 100 : 0)
                + (hasGateway ? 0 : 10)
                + (isPhysicalType ? 0 : 5);

            candidates.Add((priority, ipv4));
        }

        return candidates
            .OrderBy(c => c.Priority)
            .Select(c => c.Address)
            .FirstOrDefault();
    }

    private static IPAddress? GetFirstUsableIpv4(NetworkInterface nic)
    {
        IPInterfaceProperties props;
        try
        {
            props = nic.GetIPProperties();
        }
        catch (NetworkInformationException)
        {
            return null;
        }

        foreach (var unicast in props.UnicastAddresses)
        {
            var address = unicast.Address;
            if (address.AddressFamily != AddressFamily.InterNetwork)
            {
                continue;
            }

            if (IPAddress.IsLoopback(address) ||
                address.Equals(IPAddress.Any) ||
                IsLinkLocalIpv4(address))
            {
                continue;
            }

            return address;
        }

        return null;
    }

    private static IPAddress? TryResolveFromDnsHostEntry()
    {
        try
        {
            var hostName = Dns.GetHostName();
            foreach (var addr in Dns.GetHostAddresses(hostName))
            {
                if (addr.AddressFamily == AddressFamily.InterNetwork &&
                    !IPAddress.IsLoopback(addr) &&
                    !IsLinkLocalIpv4(addr))
                {
                    return addr;
                }
            }
        }
        catch (SocketException)
        {
        }

        return null;
    }

    private static bool IsLikelyVirtualInterface(string? name, string? description)
    {
        foreach (var prefix in VirtualInterfacePrefixes)
        {
            if (!string.IsNullOrEmpty(name) &&
                name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        if (!string.IsNullOrEmpty(description) &&
            (description.Contains("Hyper-V", StringComparison.OrdinalIgnoreCase) ||
             description.Contains("Virtual", StringComparison.OrdinalIgnoreCase) ||
             description.Contains("Docker", StringComparison.OrdinalIgnoreCase) ||
             description.Contains("WSL", StringComparison.OrdinalIgnoreCase)))
        {
            return true;
        }

        return false;
    }

    private static bool IsLinkLocalIpv4(IPAddress address)
    {
        if (address.AddressFamily != AddressFamily.InterNetwork)
        {
            return false;
        }

        Span<byte> bytes = stackalloc byte[4];
        return address.TryWriteBytes(bytes, out _) && bytes[0] == 169 && bytes[1] == 254;
    }
}
