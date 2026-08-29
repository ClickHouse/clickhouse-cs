using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Writing composite columns on the columnar tier: the two array shapes, and then <c>Map</c>, <c>Tuple</c>,
/// <c>Nullable</c> and <c>LowCardinality</c>.
///
/// <para>
/// An <c>Array(T)</c> column is accepted in two shapes. <b>Jagged</b> is one <c>T[]</c> per row, which is what
/// <see cref="ClickHouseTcpColumn"/><c>.Create</c> builds. <b>Dense</b> is a flat inner column
/// plus per-row offsets, which is the wire's own layout and what a read produces, so a column read out of a
/// <see cref="Block"/> re-inserts with nothing rebuilt. Section 3 is that round trip, and it is the reason the
/// tier exists.
/// </para>
///
/// <para>
/// <c>Tcp_009_ColumnarInsert</c> covers the tier itself: matching by name, the subset rule, and why no ClickHouse
/// type is ever stated. <c>Tcp_006_BlocksAndColumns</c> covers reading the same shapes.
/// </para>
/// </summary>
public static class TcpCompositeWrites
{
    // These examples are not the test suite, so fixed names are fine. All three are dropped even if a step throws.
    private const string ArraysTable = "example_tcp_composite_writes_arrays";
    private const string DenseTable = "example_tcp_composite_writes_dense";
    private const string OthersTable = "example_tcp_composite_writes_others";
    private const string OthersDenseTable = "example_tcp_composite_writes_others_dense";

    private const string ArrayColumns = "id, readings, tags, maybe";
    private const string OtherColumns = "id, attrs, point, score, city, nick";

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();

        try
        {
            await JaggedArrays(client);
            await TheNonNullableRowRule(client);
            await DenseArraysAndTheRoundTrip(client);
            await TheOtherComposites(client);
            await WhatEachTargetAccepts(client);
            WhatToRemember();
        }
        finally
        {
            foreach (string table in new[] { ArraysTable, DenseTable, OthersTable, OthersDenseTable })
            {
                await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
            }

            Console.WriteLine("\nDropped every table this example created.");
        }
    }

    private static async Task JaggedArrays(ClickHouseTcpClient client)
    {
        await client.ExecuteAsync(ArrayDdl(ArraysTable));
        await client.ExecuteAsync(ArrayDdl(DenseTable));

        Console.WriteLine("1. The jagged shape: one array per row\n");
        Console.WriteLine($"   '{ArraysTable}' (id UInt64, readings Array(Float64), tags Array(String),");
        Console.WriteLine("    maybe Array(Nullable(Int32)))\n");
        Console.WriteLine("   Create<T[]> builds an IColumn<T[]>, so the CLR type of one row is the array type the");
        Console.WriteLine("   target's element type maps to: double[] for Array(Float64), string[] for Array(String),");
        Console.WriteLine("   int?[] for Array(Nullable(Int32)).\n");

        // Array.Empty is an empty row, which is a value. It is not a null row: see section 2.
        var readings = new[] { new[] { 0.5, 0.75, 1.0 }, new[] { 1.25, 1.5 }, Array.Empty<double>() };
        var tags = new[] { new[] { "north", "roof" }, Array.Empty<string>(), new[] { "south" } };
        var maybe = new[] { new int?[] { 7, null, 9 }, new int?[] { null }, Array.Empty<int?>() };

        await client.InsertAsync(
            $"INSERT INTO {ArraysTable} ({ArrayColumns}) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("id", new ulong[] { 1, 2, 3 }),
                ClickHouseTcpColumn.Create("readings", readings),
                ClickHouseTcpColumn.Create("tags", tags),
                ClickHouseTcpColumn.Create("maybe", maybe),
            });

        await ShowArrays(client, ArraysTable);

        Console.WriteLine("\n   Nothing was flattened up front. The codec walks the rows once to build the offsets");
        Console.WriteLine("   the wire needs, then writes each row's elements straight from its own array, so the");
        Console.WriteLine("   only extra buffer is the offsets. Where the element type is itself composite the");
        Console.WriteLine("   elements go through a lazy concatenated view rather than a copy.");
    }

    private static async Task TheNonNullableRowRule(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n2. A row of Array(T) may not be null\n");
        Console.WriteLine("   ClickHouse has no such value: an Array(T) row is a run of elements, possibly of length");
        Console.WriteLine("   zero, and there is no bit on the wire that could say 'absent' instead. So a null row is");
        Console.WriteLine("   refused rather than quietly turned into an empty one:\n");

        // The offsets pass reaches this row and refuses it. That happens while the block is being encoded, after
        // the statement has gone out, so this failure costs the connection: the client cannot leave a half-written
        // block on the wire, and drops it instead. Validate your rows before you hand them over.
        try
        {
            await client.InsertAsync(
                $"INSERT INTO {ArraysTable} (id, readings) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("id", new ulong[] { 4 }),
                    ClickHouseTcpColumn.Create("readings", new double[]?[] { null }),
                });
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine($"     {ex.Message.Split(" (Parameter")[0]}");
        }

        Console.WriteLine("\n   The two ways out are in the message, and they mean different things:");
        Console.WriteLine("     Array.Empty<T>() is a row that exists and holds nothing.");
        Console.WriteLine("     Array(Nullable(T)) is a row whose ELEMENTS may be null, which is the 'maybe' column");
        Console.WriteLine("       above: row 2 is [NULL], one element long, and row 3 is [], zero elements long.");
        Console.WriteLine();
        Console.WriteLine("   Unlike the name and type checks in Tcp_009, this one fires while the block is being");
        Console.WriteLine("   encoded, so it is worth checking your rows before the call rather than after it.");
    }

    private static async Task DenseArraysAndTheRoundTrip(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n3. The dense shape, and the round trip it makes free\n");
        Console.WriteLine("   The wire does not carry one array per row. It carries every row's elements end to end");
        Console.WriteLine("   plus one cumulative offset per row, and that is exactly what a read hands back: an");
        Console.WriteLine("   IArrayColumn<T> over the server's own layout. Handed back to an insert, it is written");
        Console.WriteLine("   from that layout with no arrays rebuilt.\n");

        int blocks = 0;
        await foreach (Block block in client.StreamAsync($"SELECT {ArrayColumns} FROM {ArraysTable} ORDER BY id"))
        {
            blocks++;

            if (block["readings"] is IArrayColumn<double> dense)
            {
                Console.WriteLine($"   readings, as read: {block["readings"].TypeName}, {dense.RowCount} rows");
                Console.WriteLine($"     InnerValues = [{string.Join(", ", dense.InnerValues.ToArray())}]   (every row's elements, flat)");
                Console.WriteLine($"     Offsets     = [{string.Join(", ", dense.Offsets.ToArray())}]   (cumulative ends, one more entry than rows)");
                Console.WriteLine("     Row i is InnerValues.Slice(Offsets[i], Offsets[i + 1] - Offsets[i]), so row 2's");
                Console.WriteLine($"     slice is [{string.Join(", ", dense.InnerValues[dense.Offsets[2]..dense.Offsets[3]].ToArray())}] and it is empty because both offsets are {dense.Offsets[2]}.");
            }

            // The block is borrowed, so the re-insert happens inside this iteration. It runs on a second pooled
            // connection, because the first is busy streaming this result.
            await client.InsertAsync($"INSERT INTO {DenseTable} ({ArrayColumns}) VALUES", block.Columns.ToArray());
        }

        Console.WriteLine($"\n   Re-inserted {blocks} block into '{DenseTable}' with no column rebuilt at all:\n");
        await ShowArrays(client, DenseTable);

        Console.WriteLine("\n   Every value survived, including the empty rows and the null elements. That is the");
        Console.WriteLine("   whole point of the tier: a copy, a filter, or a backfill can read a block and write it");
        Console.WriteLine("   again without ever materializing a row.\n");
        Console.WriteLine("   Two things to know about it:");
        Console.WriteLine("     A read column carries the name the SELECT gave it, and an insert matches by name, so");
        Console.WriteLine("       rename in the query when the target column is named differently: SELECT readings AS");
        Console.WriteLine("       other_name. There is no way to rename a column object.");
        Console.WriteLine("     The dense shape is what you receive, not something you can build. Create only makes");
        Console.WriteLine("       the jagged shape, so a caller that already holds flat values and offsets has to");
        Console.WriteLine("       slice them into per-row arrays first.");
    }

    private static async Task TheOtherComposites(ClickHouseTcpClient client)
    {
        await client.ExecuteAsync(OthersDdl(OthersTable));
        await client.ExecuteAsync(OthersDdl(OthersDenseTable));

        Console.WriteLine("\n4. Map, Tuple, Nullable and LowCardinality\n");

        // A Map row is a pair array rather than a dictionary: the wire carries keys and values in order, so a
        // pair array can express what a Dictionary cannot.
        var attrs = new[]
        {
            new[] { new KeyValuePair<string, long>("floor", 3), new KeyValuePair<string, long>("room", 12) },
            Array.Empty<KeyValuePair<string, long>>(),
        };

        await client.InsertAsync(
            $"INSERT INTO {OthersTable} ({OtherColumns}) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("id", new ulong[] { 1, 2 }),
                ClickHouseTcpColumn.Create("attrs", attrs),
                ClickHouseTcpColumn.Create("point", new[] { (1, "one"), (2, "two") }),
                ClickHouseTcpColumn.Create("score", new double?[] { 1.25, null }),
                ClickHouseTcpColumn.Create("city", new[] { "Amsterdam", "Amsterdam" }),
                ClickHouseTcpColumn.Create("nick", new string?[] { "ada", null }),
            });

        Console.WriteLine("   Map(String, Int64)                is KeyValuePair<string, long>[] per row, not a");
        Console.WriteLine("                                     Dictionary: the wire carries the keys and the values");
        Console.WriteLine("                                     as two columns in order, which a pair array matches.");
        Console.WriteLine("   Tuple(x Int32, y String)          is (int, string) per row. The element names live in");
        Console.WriteLine("                                     the type string only, so an unnamed ValueTuple is");
        Console.WriteLine("                                     what a named tuple takes.");
        Console.WriteLine("   Nullable(Float64)                 is double? per row. A reference type is already");
        Console.WriteLine("                                     nullable, so Nullable(String) takes string.");
        Console.WriteLine("   LowCardinality(String)            is plain string per row. The client works out the");
        Console.WriteLine("                                     block's dictionary and its key width; you never");
        Console.WriteLine("                                     build either.");
        Console.WriteLine("   LowCardinality(Nullable(String))  is string per row, null allowed.\n");

        await ShowOthers(client, OthersTable);

        Console.WriteLine("\n   All five take section 3's round trip too. A Map arrives as its key and value columns,");
        Console.WriteLine("   a Tuple as its element columns, a Nullable as a null map plus its inner column, a");
        Console.WriteLine("   LowCardinality as a dictionary plus its keys, and each of those is the layout its codec");
        Console.WriteLine($"   writes from. Re-inserted into '{OthersDenseTable}' straight from the read:\n");

        await foreach (Block block in client.StreamAsync($"SELECT {OtherColumns} FROM {OthersTable} ORDER BY id"))
        {
            await client.InsertAsync($"INSERT INTO {OthersDenseTable} ({OtherColumns}) VALUES", block.Columns.ToArray());
        }

        await ShowOthers(client, OthersDenseTable);
    }

    private static async Task WhatEachTargetAccepts(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n5. What a composite refuses, and what it says\n");

        // Both of these are type checks against the target's schema, so they are decided before any row is
        // written and the message is the only cost.
        await ShowRejection(
            client,
            "a Dictionary for a Map row",
            $"INSERT INTO {OthersTable} (id, attrs) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("id", new ulong[] { 3 }),
                ClickHouseTcpColumn.Create("attrs", new[] { new Dictionary<string, long> { ["floor"] = 3 } }),
            });

        await ShowRejection(
            client,
            "a double for a Nullable(Float64) row",
            $"INSERT INTO {OthersTable} (id, score) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("id", new ulong[] { 3 }),
                ClickHouseTcpColumn.Create("score", new[] { 1.0 }),
            });

        Console.WriteLine();
        Console.WriteLine("   Both messages name the target type, which is the useful half. The CLR half is spelled as");
        Console.WriteLine("   the internal column class, so read its type argument (System.Double here) and compare");
        Console.WriteLine("   that with the list in section 4.\n");

        // A Map row has the same non-nullable rule as an Array row, and is checked at the same point: while the
        // block is being encoded, not before it.
        await ShowRejection(
            client,
            "a null Map row",
            $"INSERT INTO {OthersTable} (id, attrs) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("id", new ulong[] { 3 }),
                ClickHouseTcpColumn.Create("attrs", new KeyValuePair<string, long>[]?[] { null }),
            });

        Console.WriteLine("\n   Same rule as Array(T), same two ways out: an empty pair array for an empty map, or");
        Console.WriteLine("   Map(K, Nullable(V)) to carry null values. And like section 2's, it is a check on the");
        Console.WriteLine("   values rather than on the types, so it fires later than the two above.");
    }

    private static void WhatToRemember()
    {
        Console.WriteLine("\n6. What to remember\n");
        Console.WriteLine("   Pick the CLR type from the target, not from what is convenient: one array per row for");
        Console.WriteLine("     Array(T), one pair array per row for Map(K, V), a ValueTuple for Tuple(...), T? for");
        Console.WriteLine("     Nullable(T), and the plain value for LowCardinality(T).");
        Console.WriteLine("   A row of Array(T) or Map(K, V) is never null. Use an empty array, or make the elements");
        Console.WriteLine("     nullable.");
        Console.WriteLine("   A column read out of a block is a valid insert column, and the fastest one: it is");
        Console.WriteLine("     already in the layout the codec writes from. Re-insert it inside the iteration that");
        Console.WriteLine("     yielded it, because the block is borrowed.");
        Console.WriteLine("   Match the column's name to the target, in the SELECT if need be.");
    }

    private static string OthersDdl(string table) => $@"
        CREATE TABLE {table}
        (
            id UInt64,
            attrs Map(String, Int64),
            point Tuple(x Int32, y String),
            score Nullable(Float64),
            city LowCardinality(String),
            nick LowCardinality(Nullable(String))
        )
        ENGINE = MergeTree()
        ORDER BY id";

    private static string ArrayDdl(string table) => $@"
        CREATE TABLE {table}
        (
            id UInt64,
            readings Array(Float64),
            tags Array(String),
            maybe Array(Nullable(Int32))
        )
        ENGINE = MergeTree()
        ORDER BY id";

    private static async Task ShowArrays(ClickHouseTcpClient client, string table)
    {
        Console.WriteLine("   id  readings          tags               maybe");
        Console.WriteLine("   --  ----------------  -----------------  ----------------");
        await foreach (object[] row in client.QueryAsync(
            $"SELECT id, toString(readings), toString(tags), toString(maybe) FROM {table} ORDER BY id"))
        {
            Console.WriteLine($"   {row[0],2}  {row[1],-16}  {row[2],-17}  {row[3],-16}");
        }
    }

    private static async Task ShowOthers(ClickHouseTcpClient client, string table)
    {
        Console.WriteLine("   id  attrs                     point       score  city       nick");
        Console.WriteLine("   --  ------------------------  ----------  -----  ---------  ----");
        await foreach (object[] row in client.QueryAsync(
            $@"SELECT id, toString(attrs), toString(point), toString(score), city, toString(nick)
               FROM {table} ORDER BY id"))
        {
            Console.WriteLine($"   {row[0],2}  {row[1],-24}  {row[2],-10}  {Text(row[3]),-5}  {row[4],-9}  {Text(row[5])}");
        }
    }

    // toString of a NULL is the empty string, which is indistinguishable from an empty string in a table.
    private static string Text(object value) => value is string { Length: 0 } ? "NULL" : value?.ToString() ?? "NULL";

    // Runs an insert that is expected to be rejected client-side and prints the reason.
    private static async Task ShowRejection(ClickHouseTcpClient client, string what, string sql, IReadOnlyList<IColumn> columns)
    {
        try
        {
            await client.InsertAsync(sql, columns);
            Console.WriteLine($"     {what}: accepted, which this example did not expect");
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine($"     {what}:");
            Console.WriteLine($"       {ex.Message.Split(" (Parameter")[0]}");
        }
    }
}
