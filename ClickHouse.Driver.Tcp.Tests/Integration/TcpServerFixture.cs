using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using Testcontainers.ClickHouse;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Provides a real ClickHouse server for the native-TCP integration tests. Starts a container once for the
/// whole <c>Integration</c> namespace (pinned by the <c>CLICKHOUSE_VERSION</c> environment variable, matching
/// the main test suite), unless <c>CLICKHOUSE_TCP_HOST</c> points at an existing server. Because it lives in
/// the <c>Integration</c> namespace, the unit tests never pay for a container.
/// </summary>
[SetUpFixture]
public sealed class TcpServerFixture
{
    private const int NativePort = 9000;

    private static ClickHouseContainer container;

    /// <summary>The server host the integration tests connect to.</summary>
    public static string Host { get; private set; }

    /// <summary>The server's native-protocol port.</summary>
    public static int Port { get; private set; }

    /// <summary>The username the integration tests authenticate with.</summary>
    public static string Username { get; private set; } = "default";

    /// <summary>The password the integration tests authenticate with.</summary>
    public static string Password { get; private set; } = "clickhouse";

    [OneTimeSetUp]
    public async Task StartAsync()
    {
        string hostOverride = Environment.GetEnvironmentVariable("CLICKHOUSE_TCP_HOST");
        if (!string.IsNullOrEmpty(hostOverride))
        {
            Host = hostOverride;
            Port = int.TryParse(Environment.GetEnvironmentVariable("CLICKHOUSE_TCP_PORT"), out int overridePort) ? overridePort : NativePort;
            Username = Environment.GetEnvironmentVariable("CLICKHOUSE_TCP_USER") ?? Username;
            Password = Environment.GetEnvironmentVariable("CLICKHOUSE_TCP_PASSWORD") ?? Password;
            return;
        }

        string version = Environment.GetEnvironmentVariable("CLICKHOUSE_VERSION");
        string tag = string.IsNullOrEmpty(version) ? "latest" : version;

        container = new ClickHouseBuilder($"clickhouse/clickhouse-server:{tag}")
            .WithUsername(Username)
            .WithPassword(Password)
            .Build();

        await container.StartAsync();
        Host = container.Hostname;
        Port = container.GetMappedPublicPort(NativePort);
    }

    [OneTimeTearDown]
    public async Task StopAsync()
    {
        if (container is not null)
        {
            await container.DisposeAsync();
        }
    }

    /// <summary>Opens a native-TCP connection to the test server, optionally overriding the credentials.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <param name="username">The username to authenticate with, or null for the fixture default.</param>
    /// <param name="password">The password to authenticate with, or null for the fixture default.</param>
    /// <returns>A connected, handshaken connection.</returns>
    internal static ValueTask<ClickHouseTcpConnection> ConnectAsync(
        CancellationToken cancellationToken = default,
        string username = null,
        string password = null)
        => ClickHouseTcpConnection.ConnectAsync(
            Host,
            Port,
            new ClientHandshakeParameters
            {
                ClientName = "ClickHouse.Driver.Tcp.Tests",
                Username = username ?? Username,
                Password = password ?? Password,
            },
            cancellationToken);
}
