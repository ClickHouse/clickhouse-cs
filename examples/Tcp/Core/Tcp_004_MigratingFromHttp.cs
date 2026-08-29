using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Moving code from the HTTP client to the native-protocol one: the experimental opt-in a consumer has to make,
/// the same task written both ways against one server, the call-for-call mapping, and an honest list of what the
/// native client does not do.
///
/// <para>
/// The list matters more than the mapping. Most HTTP calls have a native counterpart, but a few capabilities have
/// none at all, and they are the ones that decide whether a migration is possible.
/// </para>
/// </summary>
public static class TcpMigratingFromHttp
{
    private const string TableName = "example_tcp_migrating_from_http";

    public static async Task Run()
    {
        ShowTheOptIn();

        // Two clients, one server: the HTTP interface on 8123 and the native protocol on 9000.
        using var http = ExampleConfig.CreateHttpClient();
        await using var tcp = ExampleConfig.CreateTcpClient();

        await SameTaskBothWays(http, tcp);

        ShowTheMapping();
        ShowWhatIsMissing();
    }

    private static void ShowTheOptIn()
    {
        Console.WriteLine("1. The experimental opt-in\n");
        Console.WriteLine("   ClickHouseTcpClient, ClickHouseTcpDataSource, the three IClickHouseTcp* interfaces and");
        Console.WriteLine("   AddClickHouseTcpDataSource carry [Experimental(\"CHTCP0001\")], so naming any of them is a");
        Console.WriteLine("   compile error until you acknowledge that the surface may still change.");
        Console.WriteLine();
        Console.WriteLine("   The types around them — ClickHouseTcpClientOptions, the connection-string builder, Block,");
        Console.WriteLine("   the columns, the exceptions — do not carry it, so holding one raises no diagnostic even");
        Console.WriteLine("   though it is just as experimental.");
        Console.WriteLine();
        Console.WriteLine("   Per file:");
        Console.WriteLine("     #pragma warning disable CHTCP0001 // The native protocol client's API is not yet stable.");
        Console.WriteLine();
        Console.WriteLine("   Or once for a project:");
        Console.WriteLine("     <NoWarn>$(NoWarn);CHTCP0001</NoWarn>");
        Console.WriteLine();
        Console.WriteLine("   This examples project takes the project-wide route, which is why no file under Tcp/");
        Console.WriteLine("   opens with the pragma.");
    }

    private static async Task SameTaskBothWays(ClickHouseClient http, ClickHouseTcpClient tcp)
    {
        Console.WriteLine("\n2. The same task, both ways\n");

        try
        {
            await CompareTransports(http, tcp);
        }
        finally
        {
            await tcp.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
            Console.WriteLine($"\n   Dropped '{TableName}'");
        }
    }

    private static async Task CompareTransports(ClickHouseClient http, ClickHouseTcpClient tcp)
    {
        // DDL. HTTP: ExecuteNonQueryAsync, which returns the affected-row count ADO.NET expects.
        await http.ExecuteNonQueryAsync($@"
            CREATE TABLE {TableName} (id UInt64, name String, source String)
            ENGINE = MergeTree() ORDER BY id");
        Step("http.ExecuteNonQueryAsync(\"CREATE TABLE ...\")", "table created");

        // The native equivalent returns nothing: there is no row count on this path, only acknowledgement.
        await tcp.ExecuteAsync($"ALTER TABLE {TableName} MODIFY COMMENT 'written over both transports'");
        Step("tcp.ExecuteAsync(\"ALTER TABLE ...\")", "comment set");

        // Insert. HTTP names the table and the columns as arguments.
        await http.InsertBinaryAsync(
            TableName,
            new[] { "id", "name", "source" },
            new List<object[]>
            {
                new object[] { 1UL, "Ada", "http" },
                new object[] { 2UL, "Grace", "http" },
            });
        Step("http.InsertBinaryAsync(table, columns, rows)", "2 rows");

        // The native client takes the statement instead, ending at VALUES, and the rows follow it as blocks.
        await tcp.InsertRowsAsync(
            $"INSERT INTO {TableName} (id, name, source) VALUES",
            new List<object[]>
            {
                new object[] { 3UL, "Alan", "tcp" },
                new object[] { 4UL, "Edsger", "tcp" },
            });
        Step("tcp.InsertRowsAsync(\"INSERT ... VALUES\", rows)", "2 rows");

        Console.WriteLine();
        Console.WriteLine("   Reading the same four rows through each client:\n");
        Console.WriteLine("     ID  Name    Source   read by");
        Console.WriteLine("     --  ------  ------   -------");

        // HTTP reads through a DbDataReader, pulled row by row.
        using (var reader = await http.ExecuteReaderAsync($"SELECT id, name, source FROM {TableName} ORDER BY id"))
        {
            while (reader.Read())
            {
                Console.WriteLine($"     {reader.GetFieldValue<ulong>(0),2}  {reader.GetString(1),-6}  {reader.GetString(2),-6}   ExecuteReaderAsync");
            }
        }

        // The native client streams object[] rows instead. There is no DbDataReader on this transport.
        await foreach (object[] row in tcp.QueryAsync($"SELECT id, name, source FROM {TableName} ORDER BY id"))
        {
            Console.WriteLine($"     {(ulong)row[0],2}  {(string)row[1],-6}  {(string)row[2],-6}   QueryAsync");
        }

        // One scalar call, spelled the same on both, and the same boxed CLR type comes back.
        object httpCount = await http.ExecuteScalarAsync($"SELECT count() FROM {TableName}");
        object tcpCount = await tcp.ExecuteScalarAsync($"SELECT count() FROM {TableName}");
        Console.WriteLine();
        Console.WriteLine($"   http.ExecuteScalarAsync(\"SELECT count() ...\") = {httpCount} ({httpCount.GetType().Name})");
        Console.WriteLine($"   tcp.ExecuteScalarAsync(\"SELECT count() ...\")  = {tcpCount} ({tcpCount.GetType().Name})");
        Console.WriteLine($"   The two transports agree: {httpCount.Equals(tcpCount)}");
    }

    private static void ShowTheMapping()
    {
        Console.WriteLine("\n3. Call for call\n");

        Map("ClickHouseClient", "ClickHouseTcpClient");
        Map("ClickHouseClientSettings", "ClickHouseTcpClientOptions (an init-only record)");
        Map("ClickHouseConnectionStringBuilder", "ClickHouseTcpConnectionStringBuilder");
        Map("ExecuteNonQueryAsync(sql) -> int", "ExecuteAsync(sql)");
        Map("ExecuteScalarAsync(sql)", "ExecuteScalarAsync(sql)");
        Map("ExecuteReaderAsync(sql) -> DbDataReader", "QueryAsync(sql) -> IAsyncEnumerable<object[]>");
        Map(string.Empty, "QueryAsync<T>(sql) -> IAsyncEnumerable<T>");
        Map(string.Empty, "StreamAsync(sql) -> IAsyncEnumerable<Block>");
        Map("InsertBinaryAsync(table, columns, rows)", "InsertRowsAsync(\"INSERT INTO t (cols) VALUES\", rows)");
        Map("InsertBinaryAsync<T>(table, rows)", "InsertRowsAsync<T>(\"INSERT INTO t (cols) VALUES\", rows)");
        Map(string.Empty, "InsertAsync(sql, IColumn[]) — columnar, no per-row boxing");
        Map("PingAsync()", "PingAsync() — a protocol ping, not a SELECT 1");
        Map("QueryOptions", "ClickHouseTcpQueryOptions / ClickHouseTcpInsertOptions");
        Map("QueryOptions.CustomSettings (object values)", "Settings (string values)");
        Map("ClickHouseParameterCollection", "ClickHouseTcpParameterCollection");
        Map("@name, rewritten client-side", "{name:Type} only — nothing is rewritten");
        Map("UseSession / SessionId", "OpenSessionAsync() -> IClickHouseTcpSession");
        Map("AddClickHouseDataSource(...)", "AddClickHouseTcpDataSource(...)");
        Map("using (IDisposable)", "await using (IAsyncDisposable, and IDisposable)");
        Map("Port=8123, Protocol=https", "Port=9000, UseTls=true (9440 when Port is unset)");
        Map("Compression=true", "Compression=lz4|zstd|none");
        Map("ClickHouseConnection / ClickHouseCommand", "(nothing — see below)");
    }

    private static void ShowWhatIsMissing()
    {
        Console.WriteLine("\n4. What the native client does not do\n");
        Console.WriteLine("   A format other than Native. The protocol carries columnar blocks, so there is no CSV,");
        Console.WriteLine("   JSONEachRow or Parquet ingestion or export, and no raw stream insert.");
        Console.WriteLine();
        Console.WriteLine("   ADO.NET, and so any ORM. There is no DbConnection over this transport, so Dapper, EF");
        Console.WriteLine("   Core and linq2db need the HTTP client.");
        Console.WriteLine();
        Console.WriteLine("   JWT or bearer authentication. Username and password only, plus QuotaKey.");
        Console.WriteLine();
        Console.WriteLine("   Custom HTTP headers, which have no equivalent on the wire.");
        Console.WriteLine();
        Console.WriteLine("   A parameter type resolver, a parameter formatter, or a read value converter. There is no");
        Console.WriteLine("   hook for any of the three: a parameter's type comes from its {name:Type} placeholder or");
        Console.WriteLine("   from ClickHouseTcpParameter.ClickHouseType.");
        Console.WriteLine();
        Console.WriteLine("   Per-query Roles or Database. Run SET ROLE inside a session for the first; qualify the");
        Console.WriteLine("   name, or use a client per database, for the second.");

        Console.WriteLine("\n5. What only the native client does\n");
        Console.WriteLine("   Blocks and typed columns, so a read can skip materializing rows at all, and a column");
        Console.WriteLine("   read out of one block re-inserts without being rebuilt.");
        Console.WriteLine("   Sessions that are one pinned connection, so a temporary table or a SET survives.");
        Console.WriteLine("   Progress, profile info and profile events while the query runs, through callbacks.");
        Console.WriteLine("   Block compression on the wire, LZ4 by default.");
        Console.WriteLine();
        Console.WriteLine("   Both lists in full: examples/Tcp/README.md");
    }

    private static void Step(string call, string result)
        => Console.WriteLine($"   {call,-46}  {result}");

    private static void Map(string http, string tcp)
        => Console.WriteLine($"   {http,-44}  {tcp}");
}
