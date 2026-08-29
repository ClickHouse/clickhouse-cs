using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// <see cref="IClickHouseTcpOperations.GetServerInfoAsync"/> and
/// <see cref="ClickHouseTcpServerInfo"/>: what the server said about itself during the handshake, and the two
/// different things to gate a feature on — the <b>protocol revision</b> for anything the wire has to carry, and
/// the <b>server version</b> for anything SQL has to name.
///
/// <para>
/// Code that runs against one server you control needs none of this. Code that ships — a library, a migration
/// tool, an agent deployed across a fleet — meets 25.8 and 26.7 on the same afternoon, and the choice is between
/// asking first and catching an error afterwards. Asking is cheaper and says why in the log.
/// </para>
/// </summary>
public static class TcpServerInfo
{
    /// <summary>The protocol revision that added the query-parameters list to the Query packet.</summary>
    private const int ParametersRevision = 54459;

    /// <summary>The revision that introduced per-packet chunk framing, used here as a gate that does not pass.</summary>
    private const int ChunkedFramingRevision = 54470;

    /// <summary>The oldest server this driver is tested against.</summary>
    private static readonly Version SupportedFloor = new(25, 8);

    /// <summary>QBit(Int8, N) needs a newer server; QBit itself does not. Tcp_015 is about the type.</summary>
    private static readonly Version QBitInt8From = new(26, 7);

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();

        // One call, and the answer came from the handshake rather than from a query — there is no round trip
        // beyond opening a connection, so this is cheap enough to do at startup.
        //
        // It describes the connection this call borrowed, not the client. Every query below borrows its own, so
        // behind a load balancer over mixed versions a gate decided here can be wrong for the query it gates.
        // Open a session when that matters: one session is one connection for its whole life.
        ClickHouseTcpServerInfo server = await client.GetServerInfoAsync();

        EveryField(server);
        await VersionAgainstSqlVersion(client, server);
        await GatingOnTheRevision(client, server);
        await GatingOnTheVersion(client, server);
        WhichGateToUse();
    }

    private static void EveryField(ClickHouseTcpServerInfo server)
    {
        Console.WriteLine("1. What the handshake carried\n");
        Console.WriteLine($"   ToString()         {server}");
        Console.WriteLine($"   Name               {server.Name}");
        Console.WriteLine($"   Version            {server.Version}");
        Console.WriteLine($"   VersionMajor       {server.VersionMajor}");
        Console.WriteLine($"   VersionMinor       {server.VersionMinor}");
        Console.WriteLine($"   VersionPatch       {server.VersionPatch}");
        Console.WriteLine($"   ProtocolRevision   {server.ProtocolRevision}");
        Console.WriteLine($"   Timezone           {Quote(server.Timezone)}");
        Console.WriteLine($"   DisplayName        {Quote(server.DisplayName)}");
        Console.WriteLine();
        Console.WriteLine("   Timezone is the server's own, and it is what a bare DateTime column means — Tcp_012 is");
        Console.WriteLine("   about that. DisplayName is whatever display_name the server was configured with, which");
        Console.WriteLine("   is often the container's host name and is empty when nothing set it.");
        Console.WriteLine();
        Console.WriteLine("   ClickHouseTcpServerInfo is a record, so two readings compare equal by value and it is");
        Console.WriteLine("   safe to cache. None of it changes for the life of a connection.");
    }

    private static async Task VersionAgainstSqlVersion(ClickHouseTcpClient client, ClickHouseTcpServerInfo server)
    {
        Console.WriteLine("\n2. Version, and the fourth number that is not in it\n");

        object sqlVersion = await client.ExecuteScalarAsync("SELECT version()");
        object sqlTimezone = await client.ExecuteScalarAsync("SELECT timezone()");

        Console.WriteLine($"   server.Version        {server.Version}");
        Console.WriteLine($"   SELECT version()      {sqlVersion}");
        Console.WriteLine($"   server.Timezone       {server.Timezone}");
        Console.WriteLine($"   SELECT timezone()     {sqlTimezone}");
        Console.WriteLine();
        Console.WriteLine("   The handshake carries three numbers, so Version is major.minor.patch and the build");
        Console.WriteLine("   number that version() shows has nowhere to go. Compare against Version for a feature");
        Console.WriteLine("   gate — the three numbers are what a release note names — and read version() only when");
        Console.WriteLine("   you want the exact build for a bug report.");
    }

    private static async Task GatingOnTheRevision(ClickHouseTcpClient client, ClickHouseTcpServerInfo server)
    {
        Console.WriteLine("\n3. Gating on ProtocolRevision, for what the wire has to carry\n");
        Console.WriteLine("   The revision is the lower of what the client and the server support, so it can be");
        Console.WriteLine("   below what either alone offers. Everything the protocol grew — a field, a packet, a");
        Console.WriteLine("   framing — is switched on by a number like these.\n");

        Console.WriteLine($"   negotiated                                     {server.ProtocolRevision}");
        Console.WriteLine($"   query parameters need                          {ParametersRevision}   -> {Available(server.ProtocolRevision >= ParametersRevision)}");
        Console.WriteLine($"   per-packet chunk framing needs                 {ChunkedFramingRevision}   -> {Available(server.ProtocolRevision >= ChunkedFramingRevision)}");
        Console.WriteLine();

        if (server.ProtocolRevision >= ParametersRevision)
        {
            var options = new ClickHouseTcpQueryOptions
            {
                Parameters = new ClickHouseTcpParameterCollection { { "floor", 90UL } },
            };
            object count = await client.ExecuteScalarAsync(
                "SELECT count() FROM numbers(100) WHERE number >= {floor:UInt64}",
                options);

            Console.WriteLine($"   So the parameterized query ran: count() = {count}.");
            Console.WriteLine($"   Below {ParametersRevision} the query packet has no field for the parameters list, so the client");
            Console.WriteLine("   throws NotSupportedException before it sends anything, rather than sending the query");
            Console.WriteLine("   unparameterized. The connection stays usable. Tcp_007 covers parameters themselves.");
        }
        else
        {
            Console.WriteLine($"   Skipped the parameterized query: this connection negotiated {server.ProtocolRevision}, and query");
            Console.WriteLine($"   parameters need {ParametersRevision}. Interpolate the value into the SQL text instead, and quote it.");
        }

        Console.WriteLine();
        Console.WriteLine("   The chunk-framing row is a real gate rather than a hypothetical one: it is above the");
        Console.WriteLine("   revision this connection negotiated, so nothing on this connection uses it. That is the");
        Console.WriteLine("   asymmetry to remember — a newer server alone does not raise the number, because the");
        Console.WriteLine("   client has to offer the revision too.");
    }

    private static async Task GatingOnTheVersion(ClickHouseTcpClient client, ClickHouseTcpServerInfo server)
    {
        Console.WriteLine("\n4. Gating on Version, for what SQL has to name\n");

        // The passing direction: the floor the driver is tested against.
        bool supported = server.Version >= SupportedFloor;
        Console.WriteLine($"   this driver is tested from {SupportedFloor} upwards, and the server is {server.Version} -> {Available(supported)}");

        // The failing direction, with the same shape Tcp_015 uses. A type the server refuses outright cannot be
        // caught cheaply: the CREATE TABLE fails, so ask first.
        if (server.Version >= QBitInt8From)
        {
            const string table = "example_tcp_serverinfo_qbit";
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
            try
            {
                await client.ExecuteAsync($"CREATE TABLE {table} (v QBit(Int8, 8)) ENGINE = MergeTree ORDER BY tuple()");
                // Qualified by database as well as by name: system.columns spans every database on the server.
                object declared = await client.ExecuteScalarAsync(
                    $"SELECT type FROM system.columns WHERE database = currentDatabase() AND table = '{table}' AND name = 'v'");
                Console.WriteLine($"   QBit(Int8, 8) declared as {declared}");
            }
            finally
            {
                await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
            }
        }
        else
        {
            Console.WriteLine($"   QBit(Int8, N) needs ClickHouse {QBitInt8From} or newer, and this server is {server.Version}:");
            Console.WriteLine("     skipped. On 26.6 the server refuses the type outright — 'QBit data type only");
            Console.WriteLine("     supports BFloat16, Float32, or Float64 as element type' — so a client that");
            Console.WriteLine("     offers Int8 vectors has to know before it writes the DDL. Tcp_015 is about QBit.");
        }

        Console.WriteLine();
        Console.WriteLine("   A skip that prints why is worth more than a caught exception: the reason survives into");
        Console.WriteLine("   the log, and nothing had to be attempted against a server that would refuse it.");
    }

    private static void WhichGateToUse()
    {
        Console.WriteLine("\n5. Which of the two to read\n");
        Console.WriteLine("   ProtocolRevision   anything the wire carries: query parameters, the fields of a");
        Console.WriteLine("                      progress packet, chunk framing, custom serialization. The client");
        Console.WriteLine("                      already gates its own reads and writes on it, so this matters when");
        Console.WriteLine("                      your own code depends on a protocol-level capability.");
        Console.WriteLine();
        Console.WriteLine("   Version            anything SQL names: a data type, a function, a table setting, a");
        Console.WriteLine("                      SETTINGS key. None of it is visible in the revision, because the");
        Console.WriteLine("                      protocol did not change to carry it.");
        Console.WriteLine();
        Console.WriteLine("   Two things this record does not tell you, and where to get them:");
        Console.WriteLine("     the cluster        SELECT * FROM system.clusters");
        Console.WriteLine("     the build          SELECT * FROM system.build_options");
        Console.WriteLine();
        Console.WriteLine("   For a health check, prefer PingAsync: it is a protocol ping rather than a SELECT, so it");
        Console.WriteLine("   proves the connection without asking the server to plan anything.");
    }

    private static string Available(bool yes) => yes ? "available" : "not on this connection";

    private static string Quote(string value) => value.Length == 0 ? "(empty)" : $"'{value}'";
}
