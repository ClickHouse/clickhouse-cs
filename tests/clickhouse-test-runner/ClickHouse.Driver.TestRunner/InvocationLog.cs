using System.Text;

namespace ClickHouse.Driver.TestRunner;

/// <summary>
/// Collects troubleshooting lines for one invocation and appends them to the file that
/// <c>CLICKHOUSE_CLIENT_CLI_LOG</c> names. Without that variable, it does nothing. The lines are written in
/// one write, so the lines of parallel invocations do not mix.
/// </summary>
internal sealed class InvocationLog
{
    public const string PathEnv = "CLICKHOUSE_CLIENT_CLI_LOG";

    private readonly string? path;
    private readonly StringBuilder lines = new();

    public InvocationLog(string? path)
    {
        this.path = string.IsNullOrEmpty(path) ? null : path;
        Add("=== clickhouse-client invocation", DateTime.UtcNow.ToString("O", System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// Returns the arguments with the value of <c>--password</c> replaced by <c>***</c>, because the log is a
    /// file that CI keeps as an artifact.
    /// </summary>
    public static List<string> RedactPassword(IReadOnlyList<string> argv)
    {
        var result = new List<string>(argv.Count);
        for (var i = 0; i < argv.Count; i++)
        {
            if (argv[i] == "--password" && i + 1 < argv.Count)
            {
                result.Add(argv[i]);
                result.Add("***");
                i++;
            }
            else if (argv[i].StartsWith("--password=", StringComparison.Ordinal))
            {
                result.Add("--password=***");
            }
            else
            {
                result.Add(argv[i]);
            }
        }
        return result;
    }

    public void Add(string key, string value)
    {
        if (path is null)
        {
            return;
        }
        lines.Append(key).Append('=').Append(value.Replace("\n", "\\n", StringComparison.Ordinal)).Append('\n');
    }

    /// <summary>
    /// Appends the lines to the log file. A log that cannot be written must not change the result of a test,
    /// so a write error is ignored; the missing lines in the log show it.
    /// </summary>
    public void Flush()
    {
        if (path is null)
        {
            return;
        }
        try
        {
            File.AppendAllText(path, lines.ToString());
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
