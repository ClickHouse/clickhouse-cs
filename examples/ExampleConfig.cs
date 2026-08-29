using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tcp;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// The one place the examples get their server from, so that pointing the whole suite at a different
/// ClickHouse is a matter of environment variables rather than editing every file.
/// </summary>
/// <remarks>
/// <para>
/// Every value falls back to what a stock <c>clickhouse/clickhouse-server</c> container exposes on
/// localhost, so the examples run with nothing set. Override any of:
/// </para>
/// <list type="table">
/// <item><term><c>CLICKHOUSE_HOST</c></term><description>default <c>localhost</c></description></item>
/// <item><term><c>CLICKHOUSE_HTTP_PORT</c></term><description>default <c>8123</c></description></item>
/// <item><term><c>CLICKHOUSE_TCP_PORT</c></term><description>default <c>9000</c>, the native protocol port</description></item>
/// <item><term><c>CLICKHOUSE_USER</c></term><description>default <c>default</c></description></item>
/// <item><term><c>CLICKHOUSE_PASSWORD</c></term><description>default empty</description></item>
/// <item><term><c>CLICKHOUSE_DATABASE</c></term><description>default <c>default</c></description></item>
/// </list>
/// <para>
/// <c>CLICKHOUSE_HTTP_CONNECTION_STRING</c> and <c>CLICKHOUSE_TCP_CONNECTION_STRING</c> replace the
/// whole assembled string for their transport, for a server the pieces above cannot describe — TLS, a
/// cloud endpoint, an extra setting. Either one also becomes what <see cref="HttpBuilder"/> and
/// <see cref="TcpBuilder"/> return, so an example that changes one key still starts from the override.
/// </para>
/// <para>
/// Four examples deliberately do not use this: <c>Core_002_ConnectionStringConfiguration</c> and
/// <c>Core_003_DependencyInjection</c>, whose subject is configuration itself, and
/// <c>Testing_001_Testcontainers</c> and <c>Tcp_030_Testcontainers</c>, which start their own servers.
/// </para>
/// </remarks>
public static class ExampleConfig
{
    // Private, and read only by the two component builders below. A whole-string override does not
    // reach them, so an example that asked one of these for the endpoint would print, proxy or dial
    // somewhere other than where it connected. HttpEndpoint and TcpEndpoint answer that question.
    private static readonly string Host = Env("CLICKHOUSE_HOST") ?? "localhost";
    private static readonly ushort HttpPort = ushort.Parse(Env("CLICKHOUSE_HTTP_PORT") ?? "8123");
    private static readonly ushort TcpPort = ushort.Parse(Env("CLICKHOUSE_TCP_PORT") ?? "9000");
    private static readonly string Username = Env("CLICKHOUSE_USER") ?? "default";
    private static readonly string Password = Env("CLICKHOUSE_PASSWORD") ?? string.Empty;
    private static readonly string Database = Env("CLICKHOUSE_DATABASE") ?? "default";

    /// <summary>The connection string for the HTTP transport.</summary>
    public static string HttpConnectionString { get; } =
        Env("CLICKHOUSE_HTTP_CONNECTION_STRING") ?? FromComponents().ConnectionString;

    /// <summary>The connection string for the native protocol.</summary>
    public static string TcpConnectionString { get; } =
        Env("CLICKHOUSE_TCP_CONNECTION_STRING") ?? TcpFromComponents().ToString();

    /// <summary>
    /// A builder pre-filled with the configured endpoint and credentials, for an example that has to
    /// change one key. Each call returns a fresh builder.
    /// </summary>
    /// <returns>A builder describing the configured HTTP endpoint.</returns>
    public static ClickHouseConnectionStringBuilder HttpBuilder() => new(HttpConnectionString);

    /// <summary>
    /// A builder pre-filled with the configured endpoint and credentials for the native protocol, for
    /// an example that has to change one key. Each call returns a fresh builder.
    /// </summary>
    /// <returns>A builder describing the configured native endpoint.</returns>
    public static ClickHouseTcpConnectionStringBuilder TcpBuilder() => new(TcpConnectionString);

    /// <summary>
    /// The host and port the HTTP examples reach, for an example that has to name or dial the endpoint
    /// itself rather than hand a connection string to a client.
    /// </summary>
    public static (string Host, ushort Port) HttpEndpoint
    {
        get
        {
            var builder = HttpBuilder();
            return (builder.Host, builder.Port);
        }
    }

    /// <summary>The host and port the native examples dial. Not interchangeable with <see cref="HttpEndpoint"/>.</summary>
    public static (string Host, int Port) TcpEndpoint
    {
        get
        {
            var builder = TcpBuilder();

            // The builder reports no port when the connection string omits one, which is the native
            // default rather than an absence.
            return (builder.Host, builder.Port ?? 9000);
        }
    }

    /// <summary>Creates a client against the configured server. The caller disposes it.</summary>
    /// <returns>A client for the configured HTTP endpoint.</returns>
    public static ClickHouseClient CreateHttpClient() => new(HttpConnectionString);

    /// <summary>
    /// Creates a native-protocol client against the configured server. The caller disposes it,
    /// asynchronously where it can.
    /// </summary>
    /// <returns>A client for the configured native endpoint.</returns>
    public static ClickHouseTcpClient CreateTcpClient() => new(TcpConnectionString);

    /// <summary>Creates an ADO.NET connection against the configured server. The caller disposes it.</summary>
    /// <returns>A connection for the configured HTTP endpoint.</returns>
    public static ClickHouseConnection CreateHttpConnection() => new(HttpConnectionString);

    private static ClickHouseConnectionStringBuilder FromComponents() => new()
    {
        Host = Host,
        Port = HttpPort,
        Username = Username,
        Password = Password,
        Database = Database,
    };

    private static ClickHouseTcpConnectionStringBuilder TcpFromComponents() => new()
    {
        Host = Host,
        Port = TcpPort,
        Username = Username,
        Password = Password,
        Database = Database,
    };

    private static string? Env(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
