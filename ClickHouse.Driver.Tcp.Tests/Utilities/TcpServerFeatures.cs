using System;
using System.Linq;
using System.Reflection;
using ClickHouse.Driver.Tcp.Client;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// Resolves the <see cref="TcpFeature"/> flags supported by the server under test.
/// </summary>
/// <remarks>
/// Reads <c>CLICKHOUSE_VERSION</c> first, because case sources run before a container starts and nothing can be
/// asked at that point. When that names no version — unset, or a moving tag such as <c>latest</c> — and a server
/// is already configured to connect to, the server is asked instead. That is the Cloud case: the version is
/// whatever the service happens to run, so pinning it in the workflow would be a guess that goes stale, and
/// treating it as <see cref="TcpFeature.All"/> runs cases the service rejects. Failing that too, everything
/// resolves to <see cref="TcpFeature.All"/>, so an unsupported feature fails rather than being skipped quietly.
/// </remarks>
public static class TcpServerFeatures
{
    /// <summary>The server version the gating is based on, or null when it could not be determined.</summary>
    public static Version Version { get; } =
        Parse(Environment.GetEnvironmentVariable("CLICKHOUSE_VERSION")) ?? AskTheServer();

    /// <summary>The features the server under test supports.</summary>
    public static TcpFeature Supported { get; } = Resolve(Version);

    /// <summary>Reports whether the server under test has a feature.</summary>
    /// <param name="feature">The feature to test for.</param>
    /// <returns>True when the server supports it.</returns>
    public static bool Has(TcpFeature feature) => Supported.HasFlag(feature);

    /// <summary>
    /// Asks the configured server for its version, for when nothing pins one. Only possible where a server is
    /// named by the environment: with no connection string and no host, the server is a container that
    /// <c>TcpServerFixture</c> has not started yet, and there is nothing to ask.
    /// </summary>
    /// <returns>The server's version, or null when there is no server to ask or it could not be reached.</returns>
    private static Version AskTheServer()
    {
        string connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_TCP_CONNECTION");
        if (string.IsNullOrEmpty(connectionString))
        {
            string host = Environment.GetEnvironmentVariable("CLICKHOUSE_TCP_HOST");
            if (string.IsNullOrEmpty(host))
            {
                return null;
            }

            var builder = new ClickHouseTcpConnectionStringBuilder { Host = host };
            if (int.TryParse(Environment.GetEnvironmentVariable("CLICKHOUSE_TCP_PORT"), out int port))
            {
                builder.Port = port;
            }

            builder.Username = Environment.GetEnvironmentVariable("CLICKHOUSE_TCP_USER") ?? "default";
            builder.Password = Environment.GetEnvironmentVariable("CLICKHOUSE_TCP_PASSWORD") ?? "clickhouse";
            connectionString = builder.ConnectionString;
        }

        // Swallowing everything on purpose: this runs in a static initializer, where a throw would take down
        // every test with a TypeInitializationException instead of the one thing that could not be determined.
        // Returning null leaves the gating exactly where it was before the server was asked.
        try
        {
            using var client = new ClickHouseTcpClient(ClickHouseTcpClientOptions.FromConnectionString(connectionString));
            object version = client.ExecuteScalarAsync("SELECT version()").GetAwaiter().GetResult();
            return Parse(version as string);
        }
        catch (Exception)
        {
            return null;
        }
    }

    /// <summary>Reads a version out of the environment variable's value.</summary>
    /// <param name="versionString">The value of <c>CLICKHOUSE_VERSION</c>, which may be null or a moving tag.</param>
    /// <returns>The version, or null when the value does not name one.</returns>
    internal static Version Parse(string versionString)
        => Version.TryParse(versionString?.Split(':').Last().Trim(), out Version parsed) ? parsed : null;

    /// <summary>Maps a server version to the features a server of that version has.</summary>
    /// <param name="version">The server version, or null when it is not known.</param>
    /// <returns>The supported features.</returns>
    internal static TcpFeature Resolve(Version version)
    {
        // A moving tag names a recent server but not which one, so it cannot gate anything.
        if (version is null)
        {
            return TcpFeature.All;
        }

        TcpFeature supported = TcpFeature.None;
        foreach (FieldInfo field in typeof(TcpFeature).GetFields(BindingFlags.Public | BindingFlags.Static))
        {
            SinceVersionAttribute since = field.GetCustomAttribute<SinceVersionAttribute>();
            if (since is not null && version >= since.Version)
            {
                supported |= (TcpFeature)field.GetRawConstantValue();
            }
        }

        return supported;
    }
}
