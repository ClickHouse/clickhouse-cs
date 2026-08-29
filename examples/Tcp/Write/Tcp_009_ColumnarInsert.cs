using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// The columnar insert tier: one <see cref="ClickHouseTcpColumn"/><c>.Create</c> call per target column, then
/// <c>InsertAsync(sql, columns)</c>. Columns are matched to the target by name, so their order is free and a
/// named subset is allowed; the ClickHouse type is never stated, because the server sends the target's schema
/// before any row data.
///
/// <para>
/// <c>Tcp_006_BlocksAndColumns</c> is the read side of this tier. This is the write side, and the two meet:
/// a column read out of a <see cref="Block"/> is a valid insert column. <c>Tcp_010_CompositeWrites</c> covers
/// the composite types and that round trip.
/// </para>
/// </summary>
public static class TcpColumnarInsert
{
    // These examples are not the test suite, so fixed names are fine. All four are dropped even if a step throws.
    private const string TableName = "example_tcp_columnar_insert";
    private const string DefaultsTable = "example_tcp_columnar_insert_defaults";
    private const string InstantsTable = "example_tcp_columnar_insert_instants";
    private const string BulkTable = "example_tcp_columnar_insert_bulk";

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();

        try
        {
            await OneColumnPerTargetColumn(client);
            await MatchedByName(client);
            await ANamedSubset(client);
            await TheServerStatesTheType(client);
            await BlockGeometry(client);
            await TheRowTierForComparison(client);
            RulesWorthKnowing();
        }
        finally
        {
            foreach (string table in new[] { TableName, DefaultsTable, InstantsTable, BulkTable })
            {
                await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
            }

            Console.WriteLine("\nDropped every table this example created.");
        }
    }

    private static async Task OneColumnPerTargetColumn(ClickHouseTcpClient client)
    {
        await client.ExecuteAsync($@"
            CREATE TABLE {TableName}
            (
                id UInt64,
                name String,
                score Float64
            )
            ENGINE = MergeTree()
            ORDER BY id");

        Console.WriteLine($"1. One column per target column\n");
        Console.WriteLine($"   Created '{TableName}' (id UInt64, name String, score Float64)\n");

        // The data is already grouped by column, which is how the wire wants it. Nothing is transposed and no
        // value is boxed, so this is the shape to reach for when the data is columnar to begin with: a parsed
        // file, a computed series, an ETL stage.
        //
        // Create takes the array over rather than copying it, so treat it as handed away: do not write to ids,
        // names or scores until the insert has completed.
        var ids = new ulong[] { 1, 2, 3, 4 };
        var names = new[] { "Ada", "Grace", "Alan", "Edsger" };
        var scores = new[] { 99.5, 97.25, 91.0, 94.75 };

        await client.InsertAsync(
            $"INSERT INTO {TableName} (id, name, score) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("id", ids),
                ClickHouseTcpColumn.Create("name", names),
                ClickHouseTcpColumn.Create("score", scores),
            });

        Console.WriteLine("   InsertAsync(\"INSERT INTO ... (id, name, score) VALUES\", [three columns])");
        Console.WriteLine("   The statement ends at VALUES. The rows travel after it as native blocks, never as SQL text.\n");
        await Show(client, $"SELECT id, name, score FROM {TableName} ORDER BY id", "id", "name", "score");

        // The generic argument is the CLR type of one row's value, and the factory reports it back as ElementType.
        // TypeName is null: an inserted column has no header of its own, so there is no ClickHouse type to report.
        IColumn<ulong> column = ClickHouseTcpColumn.Create("id", ids);
        Console.WriteLine($"\n   A built column reports: RowCount {column.RowCount}, ElementType {column.ElementType.Name}, TypeName {column.TypeName ?? "null"}");
        Console.WriteLine("   TypeName is null because the ClickHouse type is the server's to state, which is section 4.");
    }

    private static async Task MatchedByName(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n2. Matched by name, not by position\n");

        // Both orders differ from the table's and from each other, and the insert still lands correctly: the
        // server's schema block names its columns, and each supplied column is looked up by its own name.
        await client.InsertAsync(
            $"INSERT INTO {TableName} (score, id, name) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("name", new[] { "Barbara", "Frances" }),
                ClickHouseTcpColumn.Create("score", new[] { 96.5, 98.0 }),
                ClickHouseTcpColumn.Create("id", new ulong[] { 5, 6 }),
            });

        Console.WriteLine("   The statement lists (score, id, name); the columns are supplied as name, score, id.");
        Console.WriteLine("   Neither order is the table's, and both rows are still correct:\n");
        await Show(client, $"SELECT id, name, score FROM {TableName} WHERE id > 4 ORDER BY id", "id", "name", "score");

        Console.WriteLine("\n   Every column the statement lists must be supplied, and nothing else. Both mistakes are");
        Console.WriteLine("   caught before a single row is written, and both messages name the columns involved:\n");

        await ShowRejection(
            client,
            "score not supplied",
            $"INSERT INTO {TableName} (id, name, score) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("id", new ulong[] { 7 }),
                ClickHouseTcpColumn.Create("name", new[] { "Katherine" }),
            });

        // The lookup is ordinal, so 'ID' is not 'id': the target reports id as missing and ID as unexpected.
        await ShowRejection(
            client,
            "'ID' for 'id'",
            $"INSERT INTO {TableName} (id) VALUES",
            new IColumn[] { ClickHouseTcpColumn.Create("ID", new ulong[] { 7 }) });

        Console.WriteLine("\n   The second is why names are worth getting exactly right: the comparison is ordinal, as");
        Console.WriteLine("   ClickHouse's own is, so a case difference is a different column and not a near miss.");
    }

    private static async Task ANamedSubset(ClickHouseTcpClient client)
    {
        await client.ExecuteAsync($@"
            CREATE TABLE {DefaultsTable}
            (
                id UInt64,
                name String,
                region String DEFAULT 'unknown',
                attempts UInt8 DEFAULT 1
            )
            ENGINE = MergeTree()
            ORDER BY id");

        Console.WriteLine("\n3. A named subset, and the server fills the rest\n");
        Console.WriteLine($"   '{DefaultsTable}' has four columns, two of them with a DEFAULT.");
        Console.WriteLine("   The statement lists two, so the schema block describes two, so two columns are enough:\n");

        await client.InsertAsync(
            $"INSERT INTO {DefaultsTable} (id, name) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("id", new ulong[] { 1, 2 }),
                ClickHouseTcpColumn.Create("name", new[] { "north", "south" }),
            });

        await Show(client, $"SELECT id, name, region, attempts FROM {DefaultsTable} ORDER BY id", "id", "name", "region", "attempts");

        Console.WriteLine("\n   region and attempts were never sent, and hold the DEFAULT the table declares.");
        Console.WriteLine("   It is the statement's column list that decides the subset, not the columns you pass:");
        Console.WriteLine("   omit the list and the server describes every column, so every column must be supplied.");
    }

    private static async Task TheServerStatesTheType(ClickHouseTcpClient client)
    {
        await client.ExecuteAsync($@"
            CREATE TABLE {InstantsTable}
            (
                seconds DateTime('UTC'),
                millis DateTime64(3, 'UTC'),
                micros DateTime64(6, 'UTC')
            )
            ENGINE = MergeTree()
            ORDER BY seconds");

        Console.WriteLine("\n4. You never state the ClickHouse type\n");
        Console.WriteLine("   An INSERT over this protocol has two phases. The client sends the statement, the server");
        Console.WriteLine("   answers with a schema block naming and typing the target columns, and only then does the");
        Console.WriteLine("   client serialize. So the target type is known before a byte of data is encoded, and the");
        Console.WriteLine("   caller supplies CLR values only.\n");
        Console.WriteLine("   One DateTime[] into three columns of different precision, with no type stated anywhere:\n");

        var instants = new[]
        {
            new DateTime(2026, 6, 1, 10, 0, 0, 125, DateTimeKind.Utc),
            new DateTime(2026, 6, 1, 10, 0, 0, 875, DateTimeKind.Utc),
        };

        await client.InsertAsync(
            $"INSERT INTO {InstantsTable} (seconds, millis, micros) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("seconds", instants),
                ClickHouseTcpColumn.Create("millis", instants),
                ClickHouseTcpColumn.Create("micros", instants),
            });

        await Show(
            client,
            $"SELECT toString(seconds), toString(millis), toString(micros) FROM {InstantsTable} ORDER BY millis",
            26,
            "seconds", "millis", "micros");

        Console.WriteLine("\n   Same values, three encodings: whole seconds, milliseconds, microseconds.");
        Console.WriteLine("   The 125 and 875 milliseconds are gone from the DateTime column because DateTime holds");
        Console.WriteLine("   seconds, which is the target's decision and not the client's.\n");

        Console.WriteLine("   What the CLR type must satisfy is the target codec, and a mismatch is rejected before");
        Console.WriteLine("   any row is written:");

        await ShowRejection(
            client,
            "long into a DateTime column",
            $"INSERT INTO {InstantsTable} (seconds) VALUES",
            new IColumn[] { ClickHouseTcpColumn.Create("seconds", new[] { 1780308000L }) });

        Console.WriteLine("\n   And note what is not here: no DESCRIBE, no probe query. The HTTP client's");
        Console.WriteLine("   InsertBinaryAsync has to learn the schema itself, with a SELECT ... WHERE 1=0 per call");
        Console.WriteLine("   unless you pass InsertOptions.ColumnTypes or turn on InsertOptions.UseSchemaCache.");
        Console.WriteLine("   Here the schema arrives inside the insert, so there is nothing to cache or skip.");
    }

    private static async Task BlockGeometry(ClickHouseTcpClient client)
    {
        await client.ExecuteAsync($@"
            CREATE TABLE {BulkTable}
            (
                id UInt64,
                name String,
                score Float64
            )
            ENGINE = MergeTree()
            ORDER BY id");

        Console.WriteLine("\n5. ClickHouseTcpInsertOptions.MaxRowsPerBlock\n");
        Console.WriteLine("   One InsertAsync call is one statement, but not necessarily one wire block. The cap");
        Console.WriteLine("   splits the rows into blocks of at most that many, which bounds what the client holds");
        Console.WriteLine("   encoded at once. It defaults to 1,000,000 rows; null writes one block of any height.\n");

        Console.WriteLine("   The same six rows, once split into three blocks and once written as one:\n");
        Console.WriteLine("   MaxRowsPerBlock  Rows stored  Active parts");
        Console.WriteLine("   ---------------  -----------  ------------");

        await SixRowsAndCountParts(client, maxRowsPerBlock: 2);
        await SixRowsAndCountParts(client, maxRowsPerBlock: null);

        Console.WriteLine();
        Console.WriteLine("   The cap is a client-side concern only. This server recombines the blocks of one insert");
        Console.WriteLine("   before it writes, so the six rows land as one part either way: lowering the cap does not");
        Console.WriteLine("   create parts and raising it does not remove them.");
        Console.WriteLine("   Lower it to bound client memory on a very tall insert, and leave it alone otherwise.");
    }

    private static async Task SixRowsAndCountParts(ClickHouseTcpClient client, int? maxRowsPerBlock)
    {
        await client.ExecuteAsync($"TRUNCATE TABLE {BulkTable}");

        await client.InsertAsync(
            $"INSERT INTO {BulkTable} (id, name, score) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("id", new ulong[] { 1, 2, 3, 4, 5, 6 }),
                ClickHouseTcpColumn.Create("name", new[] { "a", "b", "c", "d", "e", "f" }),
                ClickHouseTcpColumn.Create("score", new[] { 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 }),
            },
            new ClickHouseTcpInsertOptions { MaxRowsPerBlock = maxRowsPerBlock });

        object rows = await client.ExecuteScalarAsync($"SELECT count() FROM {BulkTable}");
        object parts = await client.ExecuteScalarAsync(
            $"SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '{BulkTable}' AND active");

        Console.WriteLine($"   {maxRowsPerBlock?.ToString() ?? "null",-15}  {rows,11}  {parts,12}");
    }

    private static async Task TheRowTierForComparison(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n6. The row tier, for comparison\n");
        Console.WriteLine("   InsertRowsAsync takes one object[] per row and the same statement. It is the right");
        Console.WriteLine("   call when the data really is row-shaped, and it differs in three ways:\n");
        Console.WriteLine("     values are matched to the target columns by POSITION, not by name;");
        Console.WriteLine("     every value is boxed, which the caller pays for when it builds the rows;");
        Console.WriteLine("     the client then transposes those rows into one typed column per target.\n");

        await client.ExecuteAsync($"TRUNCATE TABLE {BulkTable}");

        var rows = new List<object[]>
        {
            new object[] { 1UL, "Ada", 99.5 },
            new object[] { 2UL, "Grace", 97.25 },
        };

        await client.InsertRowsAsync($"INSERT INTO {BulkTable} (id, name, score) VALUES", rows);
        await Show(client, $"SELECT id, name, score FROM {BulkTable} ORDER BY id", "id", "name", "score");

        // Positional matching is the trap: the values are the right types for the row, just not for the columns
        // in the order the statement names them. The message names the position, the column and both CLR types.
        try
        {
            await client.InsertRowsAsync(
                $"INSERT INTO {BulkTable} (id, name, score) VALUES",
                new List<object[]> { new object[] { "Alan", 3UL, 91.0 } });
        }
        catch (InvalidOperationException ex)
        {
            Console.WriteLine($"\n   Values in the wrong order: {ex.Message}");
        }

        // The shaping cost is measurable and synchronous, so this number is exact rather than a benchmark: it is
        // what the two shapes of the same 50,000 rows allocate before either call is made.
        const int Rows = 50_000;

        long before = GC.GetAllocatedBytesForCurrentThread();
        var ids = new ulong[Rows];
        var names = new string[Rows];
        var scores = new double[Rows];
        long columnar = GC.GetAllocatedBytesForCurrentThread() - before;

        before = GC.GetAllocatedBytesForCurrentThread();
        var boxed = new object[Rows][];
        for (int i = 0; i < Rows; i++)
        {
            boxed[i] = new object[] { ids[i], names[i], scores[i] };
        }

        long rowwise = GC.GetAllocatedBytesForCurrentThread() - before;

        Console.WriteLine($"\n   Holding the same {Rows:N0} rows of (UInt64, String, Float64):");
        Console.WriteLine($"     three typed arrays        {columnar,10:N0} bytes");
        Console.WriteLine($"     one object[] per row      {rowwise,10:N0} bytes  ({boxed.Length:N0} arrays, plus a box per number)");
        Console.WriteLine("   The columnar tier's saving is mostly this: the arrays are usually the shape the data is");
        Console.WriteLine("   already in, so neither the boxes nor the per-row arrays are ever created.");
    }

    private static void RulesWorthKnowing()
    {
        Console.WriteLine("\n7. Rules worth knowing\n");
        Console.WriteLine("   Create takes your array over, it does not copy it. Do not write to an array after");
        Console.WriteLine("     handing it to Create, until the insert has completed. The IEnumerable overload");
        Console.WriteLine("     enumerates once into an array, and takes over a T[] passed to it as is.");
        Console.WriteLine("   Every column must hold the same number of rows, and each name must be unique.");
        Console.WriteLine("   Zero rows is a no-op that still validates: the statement is sent, the schema is");
        Console.WriteLine("     matched, and no data block follows. An empty column list is a no-op too.");
        Console.WriteLine("   The columns you build are yours. The insert does not dispose them, and disposing one");
        Console.WriteLine("     before the insert empties it, so keep them alive until the call returns.");
        Console.WriteLine("   InsertAsync is a ValueTask: await it once, and do not await it twice.");
    }

    // Prints a small result set with a header, so each section can show what the server actually stored.
    private static Task Show(ClickHouseTcpClient client, string sql, params string[] headers)
        => Show(client, sql, 12, headers);

    private static async Task Show(ClickHouseTcpClient client, string sql, int width, params string[] headers)
    {
        Console.WriteLine("   " + string.Join("  ", headers.Select(h => h.PadRight(width))));
        Console.WriteLine("   " + string.Join("  ", headers.Select(_ => new string('-', width))));
        await foreach (object[] row in client.QueryAsync(sql))
        {
            Console.WriteLine("   " + string.Join("  ", row.Select(v => (v?.ToString() ?? "NULL").PadRight(width))));
        }
    }

    // Runs an insert that is expected to be rejected client-side and prints the reason. The client closes the row
    // stream cleanly before throwing, so the connection goes back to the pool usable.
    private static async Task ShowRejection(ClickHouseTcpClient client, string what, string sql, IReadOnlyList<IColumn> columns)
    {
        try
        {
            await client.InsertAsync(sql, columns);
            Console.WriteLine($"     {what}: accepted, which this example did not expect");
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine($"     {what}: {ex.Message.Split(" (Parameter")[0]}");
        }
    }
}
