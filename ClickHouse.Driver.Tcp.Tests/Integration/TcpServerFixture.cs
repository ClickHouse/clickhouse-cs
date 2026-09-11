using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Client;
using ClickHouse.Driver.Tcp.Protocol;
using Testcontainers.ClickHouse;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Provides the ClickHouse server the native-TCP integration tests run against. Three ways to point it, in
/// precedence order:
/// <list type="number">
/// <item><c>CLICKHOUSE_TCP_CONNECTION</c>, a whole native connection string. Every option reaches the tests
/// through it, TLS included, so a Cloud run needs no second code path.</item>
/// <item><c>CLICKHOUSE_TCP_HOST</c>, with <c>_PORT</c>, <c>_USER</c> and <c>_PASSWORD</c>, for a server already
/// running.</item>
/// <item>A Testcontainers container, pinned by <c>CLICKHOUSE_VERSION</c> to match the main test suite.</item>
/// </list>
/// <para>
/// Because it lives in the <c>Integration</c> namespace, the unit tests never pay for a container.
/// </para>
/// <para>
/// One thing to know when writing a test that will also run on Cloud: <c>ENGINE = Memory</c> data is
/// node-local, and a Cloud service has several replicas. Sequential operations on one client stay on one
/// connection, and so on one replica, because the pool hands back the connection it just took in. Two shapes
/// leave that guarantee and read an empty table on Cloud: a second connection opened to read what the first
/// wrote, and a connection that retires mid-test — a server error, an enumerator dropped mid-response — before
/// the table it wrote is read back. Use <c>MergeTree ORDER BY tuple()</c> for either.
/// </para>
/// </summary>
[SetUpFixture]
public sealed class TcpServerFixture
{
    /// <summary>The connection string that supersedes both the host variables and the container.</summary>
    internal const string ConnectionVariable = "CLICKHOUSE_TCP_CONNECTION";

    /// <summary>Names the kind of server under test, matching the main suite's variable and values.</summary>
    internal const string EnvironmentVariable = "CLICKHOUSE_TEST_ENVIRONMENT";

    private const int NativePort = 9000;
    private const string ContainerUsername = "default";
    private const string ContainerPassword = "clickhouse";

    private static ClickHouseContainer container;
    private static TcpConnectionFactory factory;
    private static ClickHouseTcpClientOptions serverOptions;

    /// <summary>The server host the integration tests connect to.</summary>
    public static string Host => serverOptions.Host;

    /// <summary>The server's native-protocol port.</summary>
    public static int Port => serverOptions.ResolvedPort;

    /// <summary>The username the integration tests authenticate with.</summary>
    public static string Username => serverOptions.Username;

    /// <summary>The password the integration tests authenticate with.</summary>
    public static string Password => serverOptions.Password;

    /// <summary>Whether the server under test is a ClickHouse Cloud service.</summary>
    internal static bool IsCloud { get; } =
        string.Equals(Environment.GetEnvironmentVariable(EnvironmentVariable), "cloud", StringComparison.Ordinal);

    /// <summary>
    /// Settings a Cloud service will not let a query change. It answers such a query with "Setting ... should
    /// not be changed" instead of applying its own value, so a test that needs one cannot run there at all.
    /// </summary>
    private static readonly string[] CloudLockedSettings =
    [
        "allow_suspicious_low_cardinality_types",

        // Both names for the one setting: a test may set either, and the server reports the second.
        "enable_nullable_tuple_type",
        "allow_experimental_nullable_tuple_type",
    ];

    /// <summary>
    /// Ignores the calling test when it needs a setting Cloud locks. Keyed on the settings a test asks for
    /// rather than on a list of test names, so a new case carrying a locked setting is covered by writing it.
    /// </summary>
    /// <param name="settings">The settings the test applies, or null when it applies none.</param>
    internal static void SkipIfCloudLocksASetting(IReadOnlyDictionary<string, string> settings)
    {
        if (!IsCloud || settings is null)
        {
            return;
        }

        foreach (string locked in CloudLockedSettings)
        {
            if (settings.ContainsKey(locked))
            {
                Assert.Ignore($"ClickHouse Cloud does not allow a query to change {locked}.");
            }
        }
    }

    [OneTimeSetUp]
    public async Task StartAsync()
    {
        serverOptions = await ResolveServerAsync();

        if (IsCloud)
        {
            // A Cloud run reaches the service over the public internet, where the handshake carries the password
            // in the clear. Fail rather than connect: a suite that quietly ran without TLS would still pass, and
            // would prove nothing about the transport it is meant to cover.
            Assert.Multiple(() =>
            {
                Assert.That(
                    serverOptions.UseTls,
                    Is.True,
                    $"{ConnectionVariable} must set UseTls=true when {EnvironmentVariable}=cloud.");
                Assert.That(
                    serverOptions.TlsAllowInvalidCertificates,
                    Is.False,
                    $"{ConnectionVariable} must not set TlsAllowInvalidCertificates=true when {EnvironmentVariable}=cloud; " +
                    "the service certificate has to be validated.");
                Assert.That(
                    serverOptions.TlsCaCertificatePath,
                    Is.Null,
                    $"{ConnectionVariable} must not set TlsCaCertificatePath when {EnvironmentVariable}=cloud; " +
                    "the service certificate has to validate through the host's public trust store.");
            });
        }

        // One factory for the whole run, disposed last. It owns the TLS configuration, so a per-connection
        // factory would release the certificate authorities while later connections still need them.
        factory = new TcpConnectionFactory(serverOptions);
    }

    [OneTimeTearDown]
    public async Task StopAsync()
    {
        factory?.Dispose();

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
    internal static async ValueTask<ClickHouseTcpConnection> ConnectAsync(
        CancellationToken cancellationToken = default,
        string username = null,
        string password = null)
    {
        if (username is null && password is null)
        {
            return await factory.CreateAsync(cancellationToken);
        }

        // Its own factory: a factory resolves the TLS configuration from one set of options. Disposing it here
        // releases that configuration, which CreateAsync has finished the handshake with by the time it returns.
        using var scoped = new TcpConnectionFactory(Options(username, password));
        return await scoped.CreateAsync(cancellationToken);
    }

    /// <summary>Builds client options pointed at the test server, optionally overriding the credentials.</summary>
    /// <param name="username">The username to authenticate with, or null for the fixture default.</param>
    /// <param name="password">The password to authenticate with, or null for the fixture default.</param>
    /// <returns>Options for a <see cref="ClickHouseTcpClient"/> against the test server.</returns>
    internal static ClickHouseTcpClientOptions Options(string username = null, string password = null)
        => username is null && password is null
            ? serverOptions
            : serverOptions with
            {
                Username = username ?? serverOptions.Username,
                Password = password ?? serverOptions.Password,
            };

    /// <summary>Creates a client connected to the test server, optionally overriding the credentials.</summary>
    /// <param name="username">The username to authenticate with, or null for the fixture default.</param>
    /// <param name="password">The password to authenticate with, or null for the fixture default.</param>
    /// <returns>A client against the test server.</returns>
    internal static ClickHouseTcpClient CreateClient(string username = null, string password = null)
        => new(Options(username, password));

    /// <summary>The connection string that addresses the test server with the fixture credentials.</summary>
    internal static string ConnectionString
    {
        get
        {
            var builder = new ClickHouseTcpConnectionStringBuilder
            {
                Host = serverOptions.Host,
                Port = serverOptions.ResolvedPort,
                Username = serverOptions.Username,
                Password = serverOptions.Password,
                UseTls = serverOptions.UseTls,
            };

            // Carried only under TLS: Validate() refuses a Tls* key alongside UseTls=false, which is how a
            // plaintext run reads. All of them have to come across, or a run against a server whose certificate
            // needs a pinned authority would fail here and nowhere else.
            if (serverOptions.UseTls)
            {
                builder.TlsAllowInvalidCertificates = serverOptions.TlsAllowInvalidCertificates;

                if (serverOptions.TlsServerName is not null)
                {
                    builder.TlsServerName = serverOptions.TlsServerName;
                }

                if (serverOptions.TlsCaCertificatePath is not null)
                {
                    builder.TlsCaCertificatePath = serverOptions.TlsCaCertificatePath;
                }
            }

            return builder.ConnectionString;
        }
    }

    /// <summary>Works out which server to run against, starting a container only when nothing else points one out.</summary>
    /// <returns>The options the whole suite connects with.</returns>
    private static async Task<ClickHouseTcpClientOptions> ResolveServerAsync()
    {
        string connectionString = Environment.GetEnvironmentVariable(ConnectionVariable);
        if (!string.IsNullOrEmpty(connectionString))
        {
            return ClickHouseTcpClientOptions.FromConnectionString(connectionString);
        }

        string hostOverride = Environment.GetEnvironmentVariable("CLICKHOUSE_TCP_HOST");
        if (!string.IsNullOrEmpty(hostOverride))
        {
            return new ClickHouseTcpClientOptions
            {
                Host = hostOverride,
                Port = int.TryParse(Environment.GetEnvironmentVariable("CLICKHOUSE_TCP_PORT"), out int overridePort) ? overridePort : NativePort,
                Username = Environment.GetEnvironmentVariable("CLICKHOUSE_TCP_USER") ?? ContainerUsername,
                Password = Environment.GetEnvironmentVariable("CLICKHOUSE_TCP_PASSWORD") ?? ContainerPassword,
            };
        }

        string version = Environment.GetEnvironmentVariable("CLICKHOUSE_VERSION");
        string tag = string.IsNullOrEmpty(version) ? "latest" : version;

        container = new ClickHouseBuilder($"clickhouse/clickhouse-server:{tag}")
            .WithUsername(ContainerUsername)
            .WithPassword(ContainerPassword)

            // The image gives the user it creates no access management, so CREATE USER and GRANT are refused.
            // A fixture that cannot make a second user can only ever test what a superuser sees, and
            // ReadonlyUserIntegrationTests needs a user that is not this one.
            .WithEnvironment("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
            .Build();

        await container.StartAsync();

        return new ClickHouseTcpClientOptions
        {
            Host = container.Hostname,
            Port = container.GetMappedPublicPort(NativePort),
            Username = ContainerUsername,
            Password = ContainerPassword,
        };
    }
}
