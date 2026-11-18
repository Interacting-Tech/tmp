using ArmWrestling.MachineAgent;
using ArmWrestling.MachineClient;
using ArmWrestling.MachineClient.Configuration;
using ArmWrestling.MachineClient.Protocol;

var options = ParseOptions(args, out var rivalWsUrl, out var verboseFrames);

await using var machineClient = new ArmWrestlingMachineClient(options);

Console.WriteLine($"[MachineAgent] Resolved local machine IP : {machineClient.MachineIp}");
Console.WriteLine($"[MachineAgent] Local server endpoint     : {machineClient.ServerUri}");

machineClient.ServerConnectionChanged += connected =>
    Console.WriteLine($"[MachineAgent] Local server WS {(connected ? "CONNECTED" : "DISCONNECTED")} ({machineClient.ServerUri})");

machineClient.LocalMachineConnectionChanged += connected =>
    Console.WriteLine($"[MachineAgent] Physical machine slot {(connected ? "ACTIVE" : "INACTIVE")}");

machineClient.DiagnosticLogged += (msg, _) =>
    Console.Error.WriteLine($"[MachineAgent] Diagnostic: {msg}");

IRivalBridge bridge = !string.IsNullOrWhiteSpace(rivalWsUrl)
    ? new WebSocketRivalBridge(new Uri(rivalWsUrl, UriKind.Absolute))
    : new ConsoleRivalBridge(verboseFrames);

await using var coordinator = new MachineRelayCoordinator(
    machineClient,
    bridge,
    options.ServerPlayerIdCadence);

using var shutdownCts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    shutdownCts.Cancel();
};

await coordinator.StartAsync(shutdownCts.Token);

try
{
    await Task.Delay(Timeout.InfiniteTimeSpan, shutdownCts.Token);
}
catch (OperationCanceledException)
{
}

await coordinator.StopAsync();

static MachineClientOptions ParseOptions(
    string[] args,
    out string? rivalWsUrl,
    out bool verboseFrames)
{
    var options = new MachineClientOptions
    {
        MachineIpOverride = Environment.GetEnvironmentVariable("MACHINE_IP"),
        PreferredNetworkInterfaceName = Environment.GetEnvironmentVariable("MACHINE_NIC")
    };

    if (int.TryParse(Environment.GetEnvironmentVariable("MACHINE_PORT"), out var envPort) &&
        envPort is > 0 and <= 65535)
    {
        options.Port = envPort;
    }

    rivalWsUrl = Environment.GetEnvironmentVariable("RIVAL_WS_URL");
    verboseFrames = string.Equals(Environment.GetEnvironmentVariable("VERBOSE_FRAMES"), "1", StringComparison.Ordinal);

    for (var i = 0; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--ip" when i + 1 < args.Length:
                options.MachineIpOverride = args[++i];
                break;
            case "--port" when i + 1 < args.Length && int.TryParse(args[i + 1], out var port):
                options.Port = port;
                i++;
                break;
            case "--interface" when i + 1 < args.Length:
                options.PreferredNetworkInterfaceName = args[++i];
                break;
            case "--rival-ws" when i + 1 < args.Length:
                rivalWsUrl = args[++i];
                break;
            case "--verbose":
                verboseFrames = true;
                break;
        }
    }

    return options;
}

internal sealed class ConsoleRivalBridge(bool verboseFrames) : IRivalBridge
{
    private CancellationTokenSource? _stdinCts;
    private Task? _stdinTask;
    private string? _lastLoggedState;

    public event Action<MachineTelemetry>? RivalTelemetryReceived;

    public event Action<string>? RivalJsonReceived;

    public event Action? RivalDisconnected;

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (!Console.IsInputRedirected)
        {
            Console.WriteLine(
                "[MachineAgent] Interactive mode: paste a PROTOCOL.md JSON line to inject rival data, or 'clear' to disconnect rival.");
        }

        _stdinCts = new CancellationTokenSource();
        _stdinTask = Task.Run(() => ReadStdinLoopAsync(_stdinCts.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_stdinCts is not null)
        {
            await _stdinCts.CancelAsync().ConfigureAwait(false);
            _stdinCts.Dispose();
            _stdinCts = null;
        }
    }

    public ValueTask PublishLocalMachineDataAsync(
        string protocolJsonEnvelope,
        ReceivedMachineData snapshot,
        CancellationToken cancellationToken = default)
    {
        var currentState = snapshot.NormalizedTelemetry?.MultiplayerState ?? "";
        if (verboseFrames ||
            (snapshot.IsNewSample && !string.Equals(_lastLoggedState, currentState, StringComparison.Ordinal)))
        {
            _lastLoggedState = currentState;
            Console.WriteLine($"[LocalMachine -> Rival] {protocolJsonEnvelope}");
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask PublishLocalServerCommandAsync(
        ServerCommand command,
        CancellationToken cancellationToken = default)
    {
        Console.WriteLine(
            $"[LocalServer Command] Kind={command.Kind}, Text={command.CommandText}, Operator={command.IsOperatorCommand}");
        return ValueTask.CompletedTask;
    }

    public ValueTask PublishLocalMachineDisconnectedAsync(
        CancellationToken cancellationToken = default)
    {
        _lastLoggedState = null;
        Console.WriteLine("[LocalMachine -> Rival] {\"data\":{}}");
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        if (_stdinTask is { IsCompleted: true })
        {
            await _stdinTask.ConfigureAwait(false);
        }
    }

    private async Task ReadStdinLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            string? line;
            try
            {
                line = await Console.In.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            if (line is null)
            {
                break;
            }

            var trimmed = line.Trim();
            if (trimmed.Length == 0)
            {
                continue;
            }

            if (string.Equals(trimmed, "clear", StringComparison.OrdinalIgnoreCase))
            {
                RivalDisconnected?.Invoke();
                Console.WriteLine("[MachineAgent] Cleared rival slot.");
                continue;
            }

            if (ProtocolJsonSerializer.TryParseRivalEnvelope(trimmed, out var parsed) && parsed is not null)
            {
                if (RivalTelemetryReceived is not null)
                {
                    RivalTelemetryReceived.Invoke(parsed);
                }
                else
                {
                    RivalJsonReceived?.Invoke(trimmed);
                }
            }
            else
            {
                Console.Error.WriteLine("[MachineAgent] Ignored input: expected single-key PROTOCOL.md JSON object or 'clear'.");
            }
        }
    }
}
