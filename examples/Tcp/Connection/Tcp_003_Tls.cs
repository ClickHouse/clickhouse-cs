using System.Security.Authentication;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Configures TLS for the native protocol and connects to an optional secure endpoint.</summary>
public static class TcpTls
{
    private const string TlsConnectionStringVariable = "CLICKHOUSE_TCP_TLS_CONNECTION_STRING";

    public static async Task Run()
    {
        ClickHouseTcpClientOptions plain = ExampleConfig.TcpBuilder().ToOptions() with { Port = null };
        ClickHouseTcpClientOptions secure = plain with
        {
            UseTls = true,
            TlsServerName = plain.Host,
            ConfigureTls = tls => tls.EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
        };

        // ResolvedPort is the port a connection dials: Port when set, otherwise derived from UseTls.
        Console.WriteLine($"Plain endpoint:  {plain.Host}:{plain.ResolvedPort}");
        Console.WriteLine($"Secure endpoint: {secure.Host}:{secure.ResolvedPort}");

        // Use TlsCaCertificatePath for a private CA. It replaces the host trust store.
        // TlsAllowInvalidCertificates is intended only for local development.

        string? connectionString = Environment.GetEnvironmentVariable(TlsConnectionStringVariable);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            Console.WriteLine($"Set {TlsConnectionStringVariable} to test a TLS endpoint.");
            return;
        }

        await using var client = new ClickHouseTcpClient(connectionString);
        ClickHouseTcpServerInfo server = await client.GetServerInfoAsync();
        Console.WriteLine($"Connected securely to {server}.");
    }
}
