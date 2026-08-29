using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tcp;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>Creates example clients from shared environment settings.</summary>
/// <remarks>
/// Component settings use local Docker defaults. A transport-specific connection string overrides
/// all components. See the examples README for the supported environment variables.
/// </remarks>
public static class ExampleConfig
{
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

    /// <summary>Creates an HTTP builder from the configured connection string.</summary>
    public static ClickHouseConnectionStringBuilder HttpBuilder() => new(HttpConnectionString);

    /// <summary>Creates a native builder from the configured connection string.</summary>
    public static ClickHouseTcpConnectionStringBuilder TcpBuilder() => new(TcpConnectionString);

    /// <summary>Gets the configured HTTP host and port.</summary>
    public static (string Host, ushort Port) HttpEndpoint
    {
        get
        {
            var builder = HttpBuilder();
            return (builder.Host, builder.Port);
        }
    }

    /// <summary>Gets the configured native host and port.</summary>
    public static (string Host, int Port) TcpEndpoint
    {
        get
        {
            var builder = TcpBuilder();

            return (builder.Host, builder.Port ?? 9000);
        }
    }

    /// <summary>Creates an HTTP client. The caller owns it.</summary>
    public static ClickHouseClient CreateHttpClient() => new(HttpConnectionString);

    /// <summary>Creates a native client. The caller owns it.</summary>
    public static ClickHouseTcpClient CreateTcpClient() => new(TcpConnectionString);

    /// <summary>Creates an ADO.NET connection. The caller owns it.</summary>
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
