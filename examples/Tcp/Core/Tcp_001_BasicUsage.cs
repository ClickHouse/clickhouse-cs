using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// The native-protocol client from end to end: construct a <see cref="ClickHouseTcpClient"/>, run DDL with
/// <c>ExecuteAsync</c>, insert rows with <c>InsertRowsAsync</c>, read them back with <c>QueryAsync</c>, read one
/// value with <c>ExecuteScalarAsync</c>, and dispose it.
///
/// <para>
/// This client speaks ClickHouse's own TCP protocol on port 9000, and it is not an ADO.NET provider. See
/// <c>Tcp_004_MigratingFromHttp</c> for how each HTTP-client call maps onto it, and for what it cannot do.
/// </para>
/// </summary>
public static class TcpBasicUsage
{
    // These examples are not the test suite, so a fixed name is fine. It is dropped even if a step throws.
    private const string TableName = "example_tcp_basic_usage";

    public static async Task Run()
    {
        // One client per endpoint, kept for the life of the application: it owns a connection pool, is safe to
        // share across threads, and runs as many operations at once as the pool is wide. Building one per
        // operation would pay for a connect and a handshake every time.
        //
        // 'await using', not 'using': the client is IAsyncDisposable, and disposal closes sockets.
        await using var client = ExampleConfig.CreateTcpClient();

        Console.WriteLine($"Native protocol endpoint: {ExampleConfig.Host}:{ExampleConfig.TcpPort}, user '{ExampleConfig.Username}'");

        // Read out of the handshake the connection already made, so this costs no query.
        var server = await client.GetServerInfoAsync();
        Console.WriteLine($"Server: {server} (protocol revision {server.ProtocolRevision}, timezone {server.Timezone})");

        try
        {
            await CreateTable(client);
            await InsertRows(client);
            await ReadRows(client);
            await ReadOneValue(client);
            ShowTheReadTiers();
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
            Console.WriteLine($"\nDropped '{TableName}'. Disposing the client closes its pooled connections.");
        }
    }

    private static async Task CreateTable(ClickHouseTcpClient client)
    {
        // ExecuteAsync is for anything that returns no rows: DDL, and DML other than INSERT ... VALUES.
        await client.ExecuteAsync($@"
            CREATE TABLE {TableName}
            (
                id UInt64,
                name String,
                score Float64
            )
            ENGINE = MergeTree()
            ORDER BY id");

        Console.WriteLine($"\nCreated '{TableName}' (id UInt64, name String, score Float64)");
    }

    private static async Task InsertRows(ClickHouseTcpClient client)
    {
        // The statement ends at VALUES: the rows travel after it as native blocks, never as SQL text. Each
        // object[] is matched to the column list by position.
        //
        // A column takes the CLR type of its first non-null value, so keep one type per column: ulong for
        // UInt64, string for String, double for Float64.
        var rows = new List<object[]>
        {
            new object[] { 1UL, "Ada", 99.5 },
            new object[] { 2UL, "Grace", 97.25 },
            new object[] { 3UL, "Alan", 91.0 },
        };

        await client.InsertRowsAsync($"INSERT INTO {TableName} (id, name, score) VALUES", rows);

        Console.WriteLine($"Inserted {rows.Count} rows with InsertRowsAsync");
    }

    private static async Task ReadRows(ClickHouseTcpClient client)
    {
        Console.WriteLine("\nQueryAsync yields one object[] per row, values in the order the SELECT names them:");
        Console.WriteLine("  ID  Name   Score");
        Console.WriteLine("  --  -----  -----");

        // Rows arrive as they are read off the connection rather than after the whole result is buffered. Each
        // object[] is yours to keep; the enumeration holds a connection until it ends, so read it to the end.
        await foreach (object[] row in client.QueryAsync($"SELECT id, name, score FROM {TableName} ORDER BY id"))
        {
            Console.WriteLine($"  {(ulong)row[0],2}  {(string)row[1],-5}  {(double)row[2],5}");
        }
    }

    private static async Task ReadOneValue(ClickHouseTcpClient client)
    {
        // ExecuteScalarAsync returns the first column of the first row, boxed. It reads the whole result before
        // returning, so write a query that produces one row.
        object count = await client.ExecuteScalarAsync($"SELECT count() FROM {TableName}");

        // count() is UInt64, so the box holds a ulong: a value's CLR type follows the column's ClickHouse type.
        Console.WriteLine($"\nExecuteScalarAsync(\"SELECT count() ...\") = {count} (boxed {count.GetType().Name})");
    }

    private static void ShowTheReadTiers()
    {
        Console.WriteLine("\nThree read tiers, all on this client:");
        Console.WriteLine("  QueryAsync      one object[] per row, every value boxed");
        Console.WriteLine("  QueryAsync<T>   one POCO per row, filled by column name");
        Console.WriteLine("  StreamAsync     whole Blocks, typed columns, no per-row boxing");
        Console.WriteLine();
        Console.WriteLine("The row tier boxes the value the wire carried, which is not always the CLR type the column");
        Console.WriteLine("name suggests: a DateTime column arrives as uint epoch seconds. For a calendar value, read");
        Console.WriteLine("it through QueryAsync<T> into a DateTime or DateTimeOffset property, or on the block tier");
        Console.WriteLine("match the column to IDateTimeColumn (ITimeColumn for Time) and call GetDateTimeOffset.");
    }
}
