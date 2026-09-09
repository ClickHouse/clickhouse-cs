using System.Diagnostics;
using System.Net.Sockets;
using ClickHouse.Driver.Tcp;
using DotNet.Testcontainers.Builders;
using Testcontainers.ClickHouse;

namespace ClickHouse.Driver.Examples;

/// <summary>Starts ClickHouse with Testcontainers and connects through its native port.</summary>
public static class TcpTestcontainers
{
    private const string Image = "clickhouse/clickhouse-server:25.12-alpine";
    private const ushort NativePort = 9000;
    private const ushort HttpPort = 8123;
    private const string User = "example";
    private const string Password = "example";

    public static async Task Run()
    {
        await using ClickHouseContainer container = new ClickHouseBuilder(Image)
            .WithUsername(User)
            .WithPassword(Password)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(request => request.ForPath("/ping").ForPort(HttpPort))
                .UntilInternalTcpPortIsAvailable(NativePort))
            .Build();

        await container.StartAsync();

        // GetConnectionString() targets HTTP. Build a native connection from mapped port 9000.
        var builder = new ClickHouseTcpConnectionStringBuilder
        {
            Host = container.Hostname,
            Port = container.GetMappedPublicPort(NativePort),
            Username = User,
            Password = Password,
            Database = "default",
        };

        Console.WriteLine($"HTTP connection: {container.GetConnectionString()}");
        Console.WriteLine($"Native endpoint: {builder.Host}:{builder.Port}");

        await using var client = new ClickHouseTcpClient(builder.ToOptions());
        ClickHouseTcpServerInfo server = await WaitForHandshake(client);
        Console.WriteLine($"Connected to {server}.");

        await client.ExecuteAsync("""
            CREATE TABLE values (id UInt64, note String)
            ENGINE = MergeTree
            ORDER BY id
            """);
        await client.InsertRowsAsync(
            "INSERT INTO values (id, note) VALUES",
            new[] { new object[] { 1UL, "from the native client" } });

        object value = await client.ExecuteScalarAsync("SELECT note FROM values WHERE id = 1");
        Console.WriteLine(value);
    }

    private static async Task<ClickHouseTcpServerInfo> WaitForHandshake(ClickHouseTcpClient client)
    {
        var timeout = Stopwatch.StartNew();

        while (true)
        {
            try
            {
                return await client.GetServerInfoAsync();
            }
            catch (ClickHouseTcpTransportException ex)
                when (ex.InnerException is SocketException && timeout.Elapsed < TimeSpan.FromSeconds(30))
            {
                // A bound port can briefly refuse native handshakes while ClickHouse starts.
                await Task.Delay(100);
            }
        }
    }
}
