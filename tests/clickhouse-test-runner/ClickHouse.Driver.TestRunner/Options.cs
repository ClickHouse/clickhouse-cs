using System.Globalization;
using System.Text.RegularExpressions;

namespace ClickHouse.Driver.TestRunner;

/// <summary>
/// The <c>clickhouse-client</c> command line, as far as <c>tests/clickhouse-test</c> and the tests use it.
/// </summary>
internal sealed partial class Options
{
    public const string UsageText = """
        usage: clickhouse-client [options]

        A clickhouse-client imitation that runs queries through ClickHouse.Driver, so that the ClickHouse
        tests/clickhouse-test runner can drive it. If --query is not given, the queries are read from stdin.
        Every ";"-separated statement runs.

        Options:
          -h, --host HOST          Server host (default: localhost)
              --port PORT          Server port. The native TCP port of the test runner
                                   (CLICKHOUSE_PORT_TCP) maps to CLICKHOUSE_PORT_HTTP when the protocol
                                   is http.
          -u, --user USER          User name (default: default)
              --password PASSWORD  Password (default: empty)
          -d, --database DB        Database (default: default)
          -q, --query SQL          Queries to run
          -s, --secure             Use TLS
              --param_NAME=VALUE   Query parameter
              --SETTING=VALUE      log_comment, max_insert_threads, max_threads and send_logs_level go
                                   to the server, and async_insert and enable_parallel_replicas with the
                                   value 0; other settings are ignored
          -n, --multiquery         Ignored
          -m, --multiline          Ignored
              --help               Show this help

        Environment variables:
          CLICKHOUSE_TEST_RUNNER_PROTOCOL  http (default) or tcp
          CLICKHOUSE_CLIENT_CLI_LOG        Log file for troubleshooting

        """;

    // The command-line settings that the shim sends to the server. tests/clickhouse-test also adds many
    // randomized settings to the command line. The shim drops them: they only change performance-related
    // paths in the server, and some of them are client-only.
    private static readonly HashSet<string> ServerSettings = new(StringComparer.Ordinal)
    {
        "log_comment", "max_insert_threads", "max_threads", "send_logs_level",
    };

    // The feature-tag overrides of tests/clickhouse-test: a test with the tag `no-async-insert` or
    // `no-parallel-replicas` gets `--async_insert 0` or `--enable_parallel_replicas 0`, because it needs the
    // feature off (`async_insert` is on by default in current servers). The shim sends only the value 0: with
    // randomized settings, tests/clickhouse-test can also give `--enable_parallel_replicas 1` together with
    // other parallel-replica settings, which the shim drops.
    private static readonly HashSet<string> FeatureOffSettings = new(StringComparer.Ordinal)
    {
        "async_insert", "enable_parallel_replicas",
    };

    private static readonly OptionSpec[] KnownOptions =
    [
        new("host", "h", true),
        new("port", null, true),
        new("user", "u", true),
        new("password", null, true),
        new("database", "d", true),
        new("query", "q", true),
        new("secure", "s", false),
        new("no-secure", null, false),
        // As in clickhouse-client: `-n` is `--multiquery`, `-m` is `--multiline`. The shim always runs every
        // statement of the input.
        new("multiquery", "n", false),
        new("multi-query", null, false),
        new("multiline", "m", false),
        new("help", null, false),
        // Accepted and ignored.
        new("config-file", "C", true),
        new("client_logs_file", null, true),
        new("allow_repeated_settings", null, false),
        new("fake-drop", null, false),
    ];

    public string Host { get; private set; } = "localhost";

    /// <summary>Gets the port, or 0 when it is not given.</summary>
    public int Port { get; private set; }

    public string User { get; private set; } = "default";

    public string Password { get; private set; } = string.Empty;

    public string Database { get; private set; } = "default";

    public bool Secure { get; private set; }

    public string? Query { get; private set; }

    public bool Help { get; private set; }

    /// <summary>Gets the server settings of the command line, in the order they were given.</summary>
    public List<KeyValuePair<string, string>> Settings { get; } = [];

    /// <summary>Gets the query parameters from <c>--param_&lt;name&gt;=&lt;value&gt;</c>.</summary>
    public Dictionary<string, string> Params { get; } = new(StringComparer.Ordinal);

    public static Options Parse(IReadOnlyList<string> argv)
    {
        var opts = new Options();
        for (var i = 0; i < argv.Count; i++)
        {
            var arg = argv[i];
            if (arg == "--")
            {
                return opts;
            }
            if (arg.StartsWith("--", StringComparison.Ordinal))
            {
                var body = arg[2..];
                var eq = body.IndexOf('=', StringComparison.Ordinal);
                var name = eq >= 0 ? body[..eq] : body;
                string? value = eq >= 0 ? body[(eq + 1)..] : null;
                if (name.Length == 0)
                {
                    continue;
                }
                if (Find(name, null) is { } o)
                {
                    if (!o.HasArg)
                    {
                        opts.Set(o.Long, string.Empty);
                        continue;
                    }
                    if (value is null)
                    {
                        if (i + 1 >= argv.Count)
                        {
                            throw new ArgumentException($"option --{name} needs a value");
                        }
                        value = argv[++i];
                    }
                    opts.Set(o.Long, value);
                    continue;
                }
                // Any other long option is a setting (`--max_threads=1`) or a query parameter
                // (`--param_x=1`). The value is optional.
                if (value is null && i + 1 < argv.Count)
                {
                    var next = argv[i + 1];
                    if (!next.StartsWith('-') || NegativeNumber().IsMatch(next))
                    {
                        value = next;
                        i++;
                    }
                }
                value ??= "1";
                if (name.StartsWith("param_", StringComparison.Ordinal))
                {
                    opts.Params[name["param_".Length..]] = value;
                    continue;
                }
                var setting = name.Replace('-', '_');
                if (ServerSettings.Contains(setting) || (FeatureOffSettings.Contains(setting) && value == "0"))
                {
                    var index = opts.Settings.FindIndex(kv => kv.Key == setting);
                    if (index >= 0)
                    {
                        opts.Settings[index] = new(setting, value);
                    }
                    else
                    {
                        opts.Settings.Add(new(setting, value));
                    }
                }
            }
            else if (arg.Length > 1 && arg[0] == '-')
            {
                if (Find(null, arg[1..]) is not { } o)
                {
                    // Unknown short options are ignored, as the other language ports do.
                    continue;
                }
                if (!o.HasArg)
                {
                    opts.Set(o.Long, string.Empty);
                    continue;
                }
                if (i + 1 >= argv.Count)
                {
                    throw new ArgumentException($"option {arg} needs a value");
                }
                opts.Set(o.Long, argv[++i]);
            }
            // Positional arguments are ignored.
        }
        return opts;
    }

    private static OptionSpec? Find(string? longName, string? shortName) =>
        KnownOptions.FirstOrDefault(o => (longName != null && o.Long == longName) || (shortName != null && o.Short == shortName));

    [GeneratedRegex(@"^-(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$")]
    private static partial Regex NegativeNumber();

    private void Set(string name, string value)
    {
        switch (name)
        {
            case "host":
                Host = value;
                break;
            case "port":
                if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var port))
                {
                    throw new ArgumentException($"bad --port value \"{value}\"");
                }
                Port = port;
                break;
            case "user":
                User = value;
                break;
            case "password":
                Password = value;
                break;
            case "database":
                Database = value;
                break;
            case "query":
                Query = value;
                break;
            case "secure":
                Secure = true;
                break;
            case "no-secure":
                Secure = false;
                break;
            case "help":
                Help = true;
                break;
        }
    }

    private sealed record OptionSpec(string Long, string? Short, bool HasArg);
}
