using System.Security.Cryptography;
using System.Text;

namespace ClickHouse.Driver.TestRunner;

/// <summary>
/// Imitates <c>clickhouse-client</c> well enough for the ClickHouse core test runner
/// (<c>tests/clickhouse-test</c>) to drive it, and runs every query through a client of this repository. It
/// reads the values that the client decoded and writes them as <c>TabSeparated</c> text, so the upstream
/// <c>.reference</c> files check the decoding of the client. See ../README.md.
/// </summary>
internal static class Program
{
    public const string ProtocolEnv = "CLICKHOUSE_TEST_RUNNER_PROTOCOL";

    public static async Task<int> Main(string[] args)
    {
        using var stdout = Console.OpenStandardOutput();
        using var stderr = new StreamWriter(Console.OpenStandardError(), new UTF8Encoding(false)) { AutoFlush = true };
        return await RunAsync(args, Console.OpenStandardInput(), stdout, stderr).ConfigureAwait(false);
    }

    /// <param name="argv">The command line.</param>
    /// <param name="stdin">The input.</param>
    /// <param name="stdout">The output.</param>
    /// <param name="stderr">The error output.</param>
    /// <param name="protocol">The protocol, or null for <c>CLICKHOUSE_TEST_RUNNER_PROTOCOL</c>.</param>
    public static async Task<int> RunAsync(string[] argv, Stream stdin, Stream stdout, TextWriter stderr, Protocol? protocol = null)
    {
        if (argv.Length > 0 && argv[0] == "extract-from-config")
        {
            return ExtractFromConfig(argv[1..], stdout);
        }

        var log = new InvocationLog(Environment.GetEnvironmentVariable(InvocationLog.PathEnv));
        try
        {
            log.Add("argv", string.Join(' ', InvocationLog.RedactPassword(argv)));
            return await RunCoreAsync(argv, stdin, stdout, stderr, log, protocol).ConfigureAwait(false);
        }
        finally
        {
            log.Flush();
        }
    }

    public static Protocol ProtocolFromEnvironment() => Environment.GetEnvironmentVariable(ProtocolEnv) switch
    {
        null or "" or "http" => Protocol.Http,
        "tcp" => Protocol.Tcp,
        var v => throw new ArgumentException($"unknown {ProtocolEnv} \"{v}\": use http or tcp"),
    };

    private static async Task<int> RunCoreAsync(string[] argv, Stream stdin, Stream stdout, TextWriter stderr, InvocationLog log, Protocol? protocolOverride)
    {
        Options opts;
        Protocol protocol;
        try
        {
            opts = Options.Parse(argv);
            protocol = protocolOverride ?? ProtocolFromEnvironment();
        }
        catch (ArgumentException e)
        {
            await stderr.WriteAsync($"Error: {e.Message}\n\n{Options.UsageText}").ConfigureAwait(false);
            return 1;
        }
        if (opts.Help)
        {
            await stdout.WriteAsync(Encoding.UTF8.GetBytes(Options.UsageText)).ConfigureAwait(false);
            return 0;
        }
        log.Add("protocol", protocol.ToString().ToLowerInvariant());

        string text;
        if (opts.Query is not null)
        {
            text = opts.Query;
        }
        else
        {
            using var reader = new StreamReader(stdin, new UTF8Encoding(false));
            text = await reader.ReadToEndAsync().ConfigureAwait(false);
        }
        if (string.IsNullOrWhiteSpace(text))
        {
            await stderr.WriteLineAsync("Error: no query. Use --query or write the queries to stdin.").ConfigureAwait(false);
            return 1;
        }

        var statements = Statements.Split(text);
        log.Add("statements", statements.Count.ToString(System.Globalization.CultureInfo.InvariantCulture));
        if (statements.Count == 0)
        {
            return 0;
        }

        TcpBackend? tcp = null;
        IBackend backend;
        if (protocol == Protocol.Tcp)
        {
            backend = tcp = new TcpBackend(opts, log);
        }
        else
        {
            backend = new HttpBackend(opts, "clickhouse-cs-test-runner-" + Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(16)), log);
        }
        var renderer = new ValueRenderer(tcp is null ? null : () => tcp.ServerTimeZone);
        await using (backend.ConfigureAwait(false))
        {
            try
            {
                var runner = new Runner(opts, backend, protocol, stdout, log, renderer);
                await runner.RunAsync(statements).ConfigureAwait(false);
                await stdout.FlushAsync().ConfigureAwait(false);
                return 0;
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                log.Add("error", e.ToString());
                return await ReportErrorAsync(stderr, e).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Writes the error to stderr and returns the exit code. As in <c>clickhouse-client</c>, the exit code is
    /// the error code modulo 256 when it is a server error.
    /// </summary>
    private static async Task<int> ReportErrorAsync(TextWriter stderr, Exception e)
    {
        int code;
        if (e is HintException hint)
        {
            await stderr.WriteLineAsync("Error: " + hint.Message).ConfigureAwait(false);
            code = hint.Code;
        }
        else if (ServerError.From(e) is { } server)
        {
            await stderr.WriteLineAsync("Received exception from server:\n" + server.Format()).ConfigureAwait(false);
            code = server.Code;
        }
        else
        {
            await stderr.WriteLineAsync($"Error: {e.GetType().Name}: {e.Message}").ConfigureAwait(false);
            code = 0;
        }
        return code % 256 != 0 ? code % 256 : 1;
    }

    /// <summary>
    /// Answers <c>clickhouse extract-from-config --key &lt;key&gt;</c>, which tests/clickhouse-test runs to read
    /// the server config. Only <c>listen_host</c> has an answer; for other keys the output is empty, as for a
    /// key that is not in the config.
    /// </summary>
    private static int ExtractFromConfig(string[] argv, Stream stdout)
    {
        string? key = null;
        for (var i = 0; i < argv.Length; i++)
        {
            if (argv[i] == "--key" && i + 1 < argv.Length)
            {
                key = argv[++i];
            }
            else if (argv[i].StartsWith("--key=", StringComparison.Ordinal))
            {
                key = argv[i]["--key=".Length..];
            }
        }
        if (key == "listen_host")
        {
            stdout.Write("127.0.0.1\n"u8);
        }
        return 0;
    }
}
