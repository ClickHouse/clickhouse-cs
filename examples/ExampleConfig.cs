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
/// <c>CLICKHOUSE_HTTP_CONNECTION_STRING</c> replaces the whole assembled string, for a server the
/// pieces above cannot describe — TLS, a cloud endpoint, an extra setting.
/// </para>
/// <para>
/// Three examples deliberately do not use this: <c>Core_002_ConnectionStringConfiguration</c> and
/// <c>Core_003_DependencyInjection</c>, whose subject is configuration itself, and
/// <c>Testing_001_Testcontainers</c>, which starts its own server.
/// </para>
/// </remarks>
public static class ExampleConfig
{
    /// <summary>The server host name or address.</summary>
    public static string Host { get; } = Env("CLICKHOUSE_HOST") ?? "localhost";

    /// <summary>The HTTP interface port.</summary>
    public static ushort HttpPort { get; } = ushort.Parse(Env("CLICKHOUSE_HTTP_PORT") ?? "8123");

    /// <summary>The native protocol port. Not interchangeable with <see cref="HttpPort"/>.</summary>
    public static ushort TcpPort { get; } = ushort.Parse(Env("CLICKHOUSE_TCP_PORT") ?? "9000");

    /// <summary>The user to authenticate as.</summary>
    public static string Username { get; } = Env("CLICKHOUSE_USER") ?? "default";

    /// <summary>The password, empty for a server with no password set.</summary>
    public static string Password { get; } = Env("CLICKHOUSE_PASSWORD") ?? string.Empty;

    /// <summary>The default database for queries.</summary>
    public static string Database { get; } = Env("CLICKHOUSE_DATABASE") ?? "default";

    /// <summary>The connection string for the HTTP transport.</summary>
    public static string HttpConnectionString { get; } =
        Env("CLICKHOUSE_HTTP_CONNECTION_STRING") ?? HttpBuilder().ConnectionString;

    /// <summary>The connection string for the native protocol.</summary>
    public static string TcpConnectionString { get; } =
        Env("CLICKHOUSE_TCP_CONNECTION_STRING") ?? TcpBuilder().ToString();

    /// <summary>
    /// A builder pre-filled with the configured endpoint and credentials, for an example that has to
    /// change one key. Each call returns a fresh builder.
    /// </summary>
    /// <returns>A builder describing the configured HTTP endpoint.</returns>
    public static ClickHouseConnectionStringBuilder HttpBuilder() => new()
    {
        Host = Host,
        Port = HttpPort,
        Username = Username,
        Password = Password,
        Database = Database,
    };

    /// <summary>
    /// A builder pre-filled with the configured endpoint and credentials for the native protocol, for
    /// an example that has to change one key. Each call returns a fresh builder.
    /// </summary>
    /// <returns>A builder describing the configured native endpoint.</returns>
    public static ClickHouseTcpConnectionStringBuilder TcpBuilder() => new()
    {
        Host = Host,
        Port = TcpPort,
        Username = Username,
        Password = Password,
        Database = Database,
    };

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

    private static string? Env(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
