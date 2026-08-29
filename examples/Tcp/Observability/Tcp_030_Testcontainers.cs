using System.Diagnostics;
using ClickHouse.Driver.Tcp;
using DotNet.Testcontainers.Builders;
using Testcontainers.ClickHouse;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Running the native client against a throwaway ClickHouse from Testcontainers. The HTTP counterpart is
/// <c>Testing_001_Testcontainers</c>; everything here is about the one difference that matters, which is the port.
///
/// <para>
/// The container publishes 8123 and 9000 on two random host ports. <c>GetConnectionString()</c> describes the
/// HTTP one, so a native client cannot use it: take the native port from
/// <c>GetMappedPublicPort(9000)</c> and build the connection string yourself.
/// </para>
///
/// <para>
/// Readiness is the second difference. The ClickHouse module's own wait strategy probes the HTTP interface, and
/// <c>WithWaitStrategy</c> <b>replaces</b> it rather than adding to it — so the strategy below asks for the HTTP
/// probe back and then also waits on 9000. A port check alone is not enough: measured on this image, a wait on
/// 9000 by itself reported ready about three seconds before the server would complete a native handshake, because
/// the port is bound before it is served.
/// </para>
///
/// <para>
/// Both probes together are still not proof. On a busy machine the first native handshake can be refused after
/// each one has passed, because neither tests the native protocol — one tests the HTTP listener and the other
/// tests only that the port is bound. So the client waits on a handshake instead, which is the one check that
/// succeeds exactly when the client can work. Forced with a no-condition wait strategy, that took 43 attempts
/// over 4.4 seconds.
/// </para>
///
/// <para>
/// This example starts its own server, so it is one of the few that does not take its endpoint from
/// <c>ExampleConfig</c>. It needs a working Docker daemon and will not run on a macOS runner.
/// </para>
/// </summary>
public static class TcpTestcontainers
{
    /// <summary>Pinned, so the example tests one known server rather than whatever <c>latest</c> is today.</summary>
    private const string Image = "clickhouse/clickhouse-server:25.12-alpine";

    private const ushort NativePort = 9000;
    private const ushort HttpPort = 8123;

    // The module creates this user from environment variables at startup. There is no GetUsername()/GetPassword()
    // on the container, so setting them here is also how the test gets to know them.
    private const string User = "example";
    private const string Password = "example";

    public static async Task Run()
    {
        Console.WriteLine($"Starting {Image}. First run pulls the image, which takes a while.\n");

        await using ClickHouseContainer container = new ClickHouseBuilder(Image)
            .WithUsername(User)
            .WithPassword(Password)

            // Both probes: the HTTP one is what says the server is really up, and the native one is what says the
            // port this client dials is bound. Testcontainers polls both; nothing here sleeps.
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(request => request.ForPath("/ping").ForPort(HttpPort))
                .UntilInternalTcpPortIsAvailable(NativePort))
            .Build();

        await container.StartAsync();

        Console.WriteLine($"   Container started. GetConnectionString() describes the HTTP interface only:");
        Console.WriteLine($"     {container.GetConnectionString()}");

        // The native endpoint, assembled from the mapped port. ClickHouseTcpConnectionStringBuilder rather than a
        // literal, so the escaping of anything in the password is not this example's problem.
        var builder = new ClickHouseTcpConnectionStringBuilder
        {
            Host = container.Hostname,
            Port = container.GetMappedPublicPort(NativePort),
            Username = User,
            Password = Password,
            Database = "default",
        };

        Console.WriteLine($"\n   Native endpoint, from GetMappedPublicPort({NativePort}):");
        Console.WriteLine($"     Host={builder.Host};Port={builder.Port};Username={builder.Username};Database={builder.Database}");
        Console.WriteLine($"     HTTP is on the other mapped port, {container.GetMappedPublicPort(HttpPort)} — the two are separate listeners.");

        await using var client = new ClickHouseTcpClient(builder.ToOptions());

        // No wait strategy can prove the native protocol is accepting. Both probes above test something adjacent:
        // /ping answers on the HTTP listener, and UntilInternalTcpPortIsAvailable only says the port is bound.
        // A bound port is not an accepting server, so on a loaded machine the first handshake can still be
        // refused. Waiting on a handshake is the only check that tests the thing being waited for.
        ClickHouseTcpServerInfo info = await HandshakeWhenReady(client);
        Console.WriteLine($"\n   Handshaken: {info}, protocol revision {info.ProtocolRevision}, timezone {info.Timezone}");
        Console.WriteLine($"   currentUser(): {await client.ExecuteScalarAsync("SELECT currentUser()")}");

        await client.ExecuteAsync("CREATE TABLE probe (id UInt64, note String) ENGINE = MergeTree ORDER BY id");
        await client.InsertRowsAsync("INSERT INTO probe (id, note) VALUES", [[1UL, "from the native client"]]);
        Console.WriteLine($"   Inserted and read back: {await client.ExecuteScalarAsync("SELECT note FROM probe WHERE id = 1")}");

        // No DROP TABLE: the container goes with the example, and so does everything in it. That is the whole
        // point of a throwaway server, and it is why a test suite built on one needs no cleanup between tests
        // beyond what the container's lifetime gives it.
        Console.WriteLine("\n   Nothing is dropped: DisposeAsync removes the container and the table with it.");
        Console.WriteLine("   In a test project the container is started once for the run (an NUnit [OneTimeSetUp],");
        Console.WriteLine("   an xUnit fixture) and shared, because a handshake is cheap and a container start is not.");
    }

    /// <summary>
    /// Handshakes as soon as the server will, retrying while the native listener refuses the connection.
    /// This is the readiness check a test fixture wants: it succeeds exactly when the client can work.
    /// </summary>
    private static async Task<ClickHouseTcpServerInfo> HandshakeWhenReady(ClickHouseTcpClient client)
    {
        var deadline = TimeSpan.FromSeconds(30);
        var started = Stopwatch.StartNew();

        for (int attempt = 1; ; attempt++)
        {
            try
            {
                ClickHouseTcpServerInfo info = await client.GetServerInfoAsync();

                if (attempt > 1)
                {
                    Console.WriteLine($"\n   The first handshake was refused: it took {attempt} attempts over " +
                                      $"{started.ElapsedMilliseconds} ms before the native listener accepted one, " +
                                      "even though both wait strategies had already passed.");
                }

                return info;
            }
            catch (ClickHouseTcpTransportException) when (started.Elapsed < deadline)
            {
                // Only a transport failure is worth retrying: the listener is not accepting yet. A server
                // exception would mean it answered and rejected us, which no amount of waiting fixes.
                await Task.Delay(100);
            }
        }
    }
}
