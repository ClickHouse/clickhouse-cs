using System;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Benchmark;

/// <summary>
/// Resolves the server both transports dial, so a cross-transport benchmark cannot accidentally
/// measure two different endpoints.
/// </summary>
/// <remarks>
/// <para>
/// <c>CLICKHOUSE_CONNECTION</c> carries the HTTP connection string, as the existing benchmarks
/// already expect. <c>CLICKHOUSE_TCP_CONNECTION_STRING</c> carries its native counterpart, named as
/// in the examples project.
/// </para>
/// <para>
/// With only the HTTP variable set, the native string is derived from it: same host, user, password
/// and database, on the native port. That keeps a local run honest without asking for two variables.
/// </para>
/// </remarks>
public static class BenchmarkServer
{
    private const int NativePort = 9000;

    /// <summary>The HTTP connection string.</summary>
    public static string Http { get; } = Env("CLICKHOUSE_CONNECTION") ?? "Host=localhost";

    /// <summary>The native-protocol connection string.</summary>
    public static string Tcp { get; } = Env("CLICKHOUSE_TCP_CONNECTION_STRING") ?? DeriveNativeFromHttp();

    /// <summary>Creates a native client for the resolved endpoint. The caller owns it.</summary>
    public static ClickHouseTcpClient CreateTcpClient() => new(Tcp);

    /// <summary>Creates a native client with one option overridden. The caller owns it.</summary>
    public static ClickHouseTcpClient CreateTcpClient(Func<ClickHouseTcpConnectionStringBuilder, ClickHouseTcpConnectionStringBuilder> configure)
    {
        var builder = configure(new ClickHouseTcpConnectionStringBuilder(Tcp));
        return new ClickHouseTcpClient(builder.ToString());
    }

    private static string DeriveNativeFromHttp()
    {
        var http = new ClickHouseConnectionStringBuilder(Http);
        return new ClickHouseTcpConnectionStringBuilder
        {
            Host = http.Host,
            Port = NativePort,
            Username = http.Username,
            Password = http.Password,
            Database = http.Database,
        }.ToString();
    }

    private static string Env(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
