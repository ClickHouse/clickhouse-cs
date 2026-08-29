using System.Globalization;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// The block tier in depth: what <c>StreamAsync</c> yields, how a <see cref="Block"/> is addressed
/// (<c>ColumnNames</c>, the two indexers, <c>TryGetColumn</c>, the typed <c>Column&lt;T&gt;</c>), what an
/// <see cref="IColumn"/> reports about itself, and how its values are read as a <see cref="ReadOnlySpan{T}"/>.
///
/// <para>
/// Section 7 is the part to read twice. A yielded block is <b>borrowed</b>: its storage is returned to a pool
/// when the iteration moves on, so a column, a span, or the block itself is invalid the moment the loop
/// advances. Everything else here is convenience; this one is correctness.
/// </para>
/// </summary>
public static class TcpBlocksAndColumns
{
    private const string TableName = "example_tcp_blocks_and_columns";

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();

        try
        {
            await Seed(client);
            await OneResultManyBlocks(client);
            await AddressingAColumn(client);
            await WhatAColumnReports(client);
            await ValuesAsSpans(client);
            await DateAndTimeColumns(client);
            await ArrayColumns(client);
            await TheBorrowingContract(client);
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
            Console.WriteLine($"\nDropped '{TableName}'");
        }
    }

    private static async Task Seed(ClickHouseTcpClient client)
    {
        // captured_at names a zone with an offset and a daylight-saving rule, so the block tier has something to
        // report; uptime is a Time, which is a count from midnight and has no zone at all.
        await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        await client.ExecuteAsync($@"
            CREATE TABLE {TableName}
            (
                id UInt64,
                sensor String,
                voltage Float64,
                readings Array(Float64),
                captured_at DateTime64(3, 'Europe/Amsterdam'),
                uptime Time
            )
            ENGINE = MergeTree()
            ORDER BY id");

        var baseline = new DateTime(2026, 6, 1, 10, 0, 0, DateTimeKind.Utc);

        await client.InsertRowsAsync(
            $"INSERT INTO {TableName} (id, sensor, voltage, readings, captured_at, uptime) VALUES",
            new List<object[]>
            {
                new object[] { 1UL, "north", 3.31, new[] { 0.5, 0.75, 1.0 }, baseline.AddMilliseconds(125), TimeSpan.FromMinutes(90) },
                new object[] { 2UL, "north", 3.28, new[] { 1.25, 1.5 }, baseline.AddMilliseconds(250), TimeSpan.FromMinutes(150) },
                new object[] { 3UL, "south", 3.35, Array.Empty<double>(), baseline.AddMilliseconds(375), TimeSpan.FromMinutes(210) },
                new object[] { 4UL, "south", 3.30, new[] { 2.0 }, baseline.AddMilliseconds(500), TimeSpan.FromMinutes(270) },
                new object[] { 5UL, "west", 3.22, new[] { 2.25, 2.5, 2.75, 3.0 }, baseline.AddMilliseconds(625), TimeSpan.FromMinutes(330) },
                new object[] { 6UL, "west", 3.40, new[] { 3.25 }, baseline.AddMilliseconds(750), TimeSpan.FromMinutes(390) },
            });

        Console.WriteLine($"Seeded '{TableName}' with 6 rows:");
        Console.WriteLine("  id UInt64, sensor String, voltage Float64, readings Array(Float64),");
        Console.WriteLine("  captured_at DateTime64(3, 'Europe/Amsterdam'), uptime Time");
    }

    private static async Task OneResultManyBlocks(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n1. One result is a sequence of blocks\n");
        Console.WriteLine("   How many, and how tall, is the server's decision. This table's six rows fit in one");
        Console.WriteLine("   granule, so they arrive together:\n");
        Console.WriteLine("   Block  Rows  Columns  Name");
        Console.WriteLine("   -----  ----  -------  ----");
        await ShowShapes(client, $"SELECT id, sensor FROM {TableName}", null);

        // A generator does honour max_block_size row for row, which a six-row MergeTree read does not: the part is
        // read whole and the setting only caps it.
        var capped = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["max_block_size"] = "3" },
        };

        Console.WriteLine("\n   The same shape of query over a generator, with max_block_size = 3, splits:\n");
        Console.WriteLine("   Block  Rows  Columns  Name");
        Console.WriteLine("   -----  ----  -------  ----");
        await ShowShapes(client, "SELECT number, toString(number) AS text FROM system.numbers LIMIT 8", capped);

        Console.WriteLine();
        Console.WriteLine("   A result block carries no name. A named block is how the server labels the extras a");
        Console.WriteLine("   query can produce — WITH TOTALS, extremes — which reach a caller through");
        Console.WriteLine("   ClickHouseTcpQueryOptions.Callbacks rather than through this stream.");
        Console.WriteLine();
        Console.WriteLine("   So write the loop for any number of blocks of any height, and never for one.");
    }

    private static async Task ShowShapes(ClickHouseTcpClient client, string sql, ClickHouseTcpQueryOptions? options)
    {
        int index = 0;
        await foreach (Block block in client.StreamAsync(sql, options))
        {
            Console.WriteLine($"   {++index,5}  {block.RowCount,4}  {block.ColumnCount,7}  {(block.Name.Length == 0 ? "(empty)" : block.Name)}");
        }
    }

    private static async Task AddressingAColumn(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n2. Addressing a column\n");

        await foreach (Block block in client.StreamAsync(Sql))
        {
            // Owned, unlike the columns: computed once, cached, and safe to keep after the block is released.
            Console.WriteLine($"   block.ColumnNames  = [{string.Join(", ", block.ColumnNames)}]");
            Console.WriteLine($"   block.ColumnCount  = {block.ColumnCount}, block.RowCount = {block.RowCount}");
            Console.WriteLine($"   block[0]           = '{block[0].Name}' by position");
            Console.WriteLine($"   block[\"sensor\"]    = '{block["sensor"].Name}' by name — ordinal and case-sensitive, like ClickHouse itself");

            // The name lookup is a scan of the block's columns, so bind a column once and then loop over rows,
            // never the other way round.
            Console.WriteLine($"   TryGetColumn(\"sensor\", out _) = {block.TryGetColumn("sensor", out _)}");
            Console.WriteLine($"   TryGetColumn(\"Sensor\", out _) = {block.TryGetColumn("Sensor", out _)}  (capital S is a different name)");

            try
            {
                _ = block["missing"];
            }
            catch (ArgumentException ex)
            {
                Console.WriteLine($"   block[\"missing\"] throws: {ex.Message.Split(" (Parameter")[0]}");
            }

            // The typed overload is the same lookup plus a cast to IColumn<T>, which is where the values live.
            IColumn<double> voltage = block.Column<double>("voltage");
            Console.WriteLine($"   block.Column<double>(\"voltage\") = IColumn<double> over '{voltage.TypeName}'");

            try
            {
                _ = block.Column<DateTime>("captured_at");
            }
            catch (InvalidCastException ex)
            {
                Console.WriteLine($"   block.Column<DateTime>(\"captured_at\") throws: {ex.Message}");
                Console.WriteLine("     T must be the type the column's values are stored as, not the type you want them in.");
            }

            // The first block is enough for the sections that follow. Breaking out is allowed: the client tells the
            // server the result is abandoned, and drops that connection instead of returning it to the pool.
        }
    }

    private static async Task WhatAColumnReports(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n3. What a column reports about itself\n");
        Console.WriteLine("   Name         TypeName                             ElementType  Rows  Extra interface");
        Console.WriteLine("   -----------  -----------------------------------  -----------  ----  ---------------");

        await foreach (Block block in client.StreamAsync(Sql))
        {
            foreach (IColumn column in block.Columns)
            {
                Console.WriteLine(
                    $"   {column.Name,-11}  {column.TypeName,-35}  {Describe(column.ElementType),-11}  {column.RowCount,4}  {ExtraInterface(column)}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   TypeName is the header text the server sent, so it is the type as ClickHouse spells it.");
        Console.WriteLine("   ElementType is the T of the column's IColumn<T> — what Values hands back. Where those");
        Console.WriteLine("   two disagree in kind, a second interface bridges them (sections 5 and 6).");
    }

    private static async Task ValuesAsSpans(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n4. Values, as a span over the server's own layout\n");

        await foreach (Block block in client.StreamAsync(Sql))
        {
            IColumn<ulong> ids = block.Column<ulong>("id");
            IColumn<double> voltage = block.Column<double>("voltage");
            IColumn<string> sensor = block.Column<string>("sensor");

            // Read the span into a local: the property recomputes it on every access, and being a ref struct it
            // cannot be stored in a field. Do not let it escape this iteration.
            ReadOnlySpan<double> volts = voltage.Values;

            double min = double.MaxValue;
            double max = double.MinValue;
            for (int i = 0; i < volts.Length; i++)
            {
                min = Math.Min(min, volts[i]);
                max = Math.Max(max, volts[i]);
            }

            Console.WriteLine($"   voltage.Values is ReadOnlySpan<double> of {volts.Length}: min {min}, max {max} — no allocation, no boxing");
            Console.WriteLine($"   id.Values      is ReadOnlySpan<ulong>  of {ids.Values.Length}: {string.Join(", ", ids.Values.ToArray())}");
            Console.WriteLine($"   voltage[2]     = {voltage[2]}   (the indexer, for one value)");
            Console.WriteLine($"   block[\"voltage\"].GetValue(2) = {block["voltage"].GetValue(2)} boxed — the untyped escape hatch");
            Console.WriteLine();
            Console.WriteLine($"   A String column is a span too, of references: sensor.Values = [{string.Join(", ", sensor.Values.ToArray())}]");
            Console.WriteLine("   Reading it decodes one string per value, so the block tier saves less on String than");
            Console.WriteLine("   on a fixed-width type. It still saves the object[] and the boxes.");
        }
    }

    private static async Task DateAndTimeColumns(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n5. Date and time columns: a count, plus an interface that reads it\n");
        Console.WriteLine("   These types are stored as a plain integer, so IColumn<T> hands back that integer — the");
        Console.WriteLine("   layout the wire carried, at no conversion cost. Turning it into a calendar value needs");
        Console.WriteLine("   the column's timezone and scale, which only these two interfaces report.\n");

        await foreach (Block block in client.StreamAsync(Sql))
        {
            IColumn capturedAt = block["captured_at"];
            Console.WriteLine($"   captured_at  {capturedAt.TypeName}");
            Console.WriteLine($"     as IColumn<long>: {string.Join(", ", block.Column<long>("captured_at").Values[..3].ToArray())}, ...  (milliseconds since the epoch)");

            if (capturedAt is IDateTimeColumn instants)
            {
                Console.WriteLine($"     as IDateTimeColumn: TimeZone {instants.TimeZone.Id}, Scale {instants.Scale}");
                Console.WriteLine($"       GetDateTimeOffset(0) = {Format(instants.GetDateTimeOffset(0))}");
                Console.WriteLine($"       GetDateTimeOffset(5) = {Format(instants.GetDateTimeOffset(5))}");

                // Allocates one array, and that array is the caller's: it stays valid after the block is gone.
                DateTimeOffset[] all = instants.ToDateTimeOffsets();
                Console.WriteLine($"       ToDateTimeOffsets() = {all.Length} instants, and the array outlives the block");
                Console.WriteLine("       The +02:00 offset is the zone's, in June. The same column read in January");
                Console.WriteLine("       would report +01:00, which is why the timezone and not a fixed offset is what");
                Console.WriteLine("       the interface exposes.");
            }

            IColumn uptime = block["uptime"];
            Console.WriteLine($"   uptime       {uptime.TypeName}");
            Console.WriteLine($"     as IColumn<int>: {string.Join(", ", block.Column<int>("uptime").Values[..3].ToArray())}, ...  (seconds from midnight)");

            if (uptime is ITimeColumn times)
            {
                Console.WriteLine($"     as ITimeColumn: Scale {times.Scale}, no timezone — a Time is a time of day, not an instant");
                Console.WriteLine($"       GetTimeSpan(0) = {times.GetTimeSpan(0)}");
                Console.WriteLine($"       ToTimeSpans()  = {string.Join(", ", times.ToTimeSpans().Take(3))}, ...  (also caller-owned)");
                Console.WriteLine("       The count is signed and is not clamped to one day, so a TimeSpan here can be");
                Console.WriteLine("       negative or longer than 24 hours.");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   Pattern-match rather than test the type name: DateTime and DateTime64 both give an");
        Console.WriteLine("   IDateTimeColumn, Time and Time64 both give an ITimeColumn, and neither interface is");
        Console.WriteLine("   generic, so one branch handles both widths.");
        Console.WriteLine();
        Console.WriteLine("   A Nullable(DateTime) does not match, though — the wrapper is a column in its own right");
        Console.WriteLine("   and it is the wrapped column that reads the calendar. Go through INullableColumn:\n");

        // The inner column holds one entry per row, with a placeholder where the row is null, so the null map and
        // the inner column are indexed by the same row number.
        await foreach (Block block in client.StreamAsync(
            "SELECT if(number = 1, NULL, toDateTime(1780308000 + number, 'UTC')) AS maybe_at FROM system.numbers LIMIT 3"))
        {
            IColumn maybeAt = block["maybe_at"];
            Console.WriteLine($"     {maybeAt.TypeName}, ElementType {Describe(maybeAt.ElementType)}");
            Console.WriteLine($"       is IDateTimeColumn: {maybeAt is IDateTimeColumn,-5}  (the Nullable wrapper itself)");

            // INullableColumn<uint> because a DateTime is stored as uint: the wrapper's T is the inner storage
            // type, so reaching Inner means knowing that type.
            if (maybeAt is INullableColumn<uint> nullable && nullable.Inner is IDateTimeColumn inner)
            {
                Console.WriteLine($"       is IDateTimeColumn: {true,-5}  (INullableColumn<uint>.Inner)");
                ReadOnlySpan<byte> nulls = nullable.NullMap;
                for (int row = 0; row < maybeAt.RowCount; row++)
                {
                    string reading = nulls[row] != 0 ? "NULL" : Format(inner.GetDateTimeOffset(row));
                    Console.WriteLine($"         row {row}: NullMap {nulls[row]} -> {reading}");
                }
            }
        }
    }

    private static async Task ArrayColumns(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n6. An Array(T) column has two views, and they cost different things\n");

        await foreach (Block block in client.StreamAsync(Sql))
        {
            IColumn readings = block["readings"];
            Console.WriteLine($"   readings is {readings.TypeName}, ElementType {Describe(readings.ElementType)}");

            if (readings is IArrayColumn<double> arrays)
            {
                // The wire layout: every row's elements end to end, plus one offset per row boundary. Both spans
                // are borrowed, and this is the view that costs nothing to produce.
                ReadOnlySpan<double> flat = arrays.InnerValues;
                ReadOnlySpan<int> offsets = arrays.Offsets;

                Console.WriteLine();
                Console.WriteLine($"   Borrowed view — InnerValues + Offsets, no allocation at all:");
                Console.WriteLine($"     InnerValues ({flat.Length} elements) = {string.Join(", ", flat.ToArray())}");
                Console.WriteLine($"     Offsets     ({offsets.Length} entries, one more than the rows) = {string.Join(", ", offsets.ToArray())}");
                Console.WriteLine("     Row i is InnerValues.Slice(Offsets[i], Offsets[i + 1] - Offsets[i]):");

                for (int row = 0; row < readings.RowCount; row++)
                {
                    ReadOnlySpan<double> slice = flat.Slice(offsets[row], offsets[row + 1] - offsets[row]);
                    double sum = 0;
                    foreach (double value in slice)
                    {
                        sum += value;
                    }

                    Console.WriteLine($"       row {row}: {slice.Length} element(s), sum {sum}");
                }

                Console.WriteLine();
                Console.WriteLine($"     Inner is that same flat run as a column rather than a span — IColumn<{Describe(arrays.Inner.ElementType)}> here.");
                Console.WriteLine("     Use it for an Array(Tuple(...)) or an Array(Array(T)), where the inner column");
                Console.WriteLine("     pattern-matches to ITupleColumn or IArrayColumn in turn, so a nested composite");
                Console.WriteLine("     can be walked all the way down without materializing a level.");
            }

            // The other view. Each row is copied into a fresh double[], so these arrays are the caller's and stay
            // valid after the block is released — at one allocation per row.
            Console.WriteLine();
            Console.WriteLine("   Allocating view — Values and the indexer materialize one double[] per row:");
            IColumn<double[]> rows = block.Column<double[]>("readings");
            Console.WriteLine($"     rows[4]   = [{string.Join(", ", rows[4])}]   (the indexer: one double[], allocated here)");
            Console.WriteLine($"     Values[0] = [{string.Join(", ", rows.Values[0])}]   (Values: every row's array, built at once)");
            Console.WriteLine("     Those arrays outlive the block. The span holding them does not, being a span.");
            Console.WriteLine("     So prefer the indexer when only a few rows out of a tall block are wanted.");
        }

        Console.WriteLine();
        Console.WriteLine("   The same split runs through the other composites: Map, Tuple, Nested, Nullable and");
        Console.WriteLine("   LowCardinality each expose a borrowed columnar view plus a materializing one.");
    }

    private static async Task TheBorrowingContract(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n7. The borrowing contract\n");
        Console.WriteLine("   Valid only for the current iteration — released when the loop advances, you stop");
        Console.WriteLine("   enumerating, or the enumerator is disposed:");
        Console.WriteLine("     the Block, every IColumn on it, IColumn<T>.Values,");
        Console.WriteLine("     IArrayColumn<T>.InnerValues / Offsets / Inner, INullableColumn<T>.NullMap / Inner,");
        Console.WriteLine("     and the other composites' views.");
        Console.WriteLine();
        Console.WriteLine("   Yours to keep:");
        Console.WriteLine("     Block.ColumnNames, a string or a struct value you read out,");
        Console.WriteLine("     the per-row arrays from an Array(T) column's Values or indexer,");
        Console.WriteLine("     IDateTimeColumn.ToDateTimeOffsets() and ITimeColumn.ToTimeSpans(),");
        Console.WriteLine("     and anything you copy: Values.ToArray(), a slice's ToArray().");
        Console.WriteLine();
        Console.WriteLine("   Do not dispose a yielded block. Block is IDisposable because the reader that produced");
        Console.WriteLine("   it disposes it; doing so yourself returns pooled storage the reader still manages.");
        Console.WriteLine();
        Console.WriteLine("   So the shape of a correct loop is: read, aggregate, or copy — inside the body.\n");

        // The aggregate is a value type, and the copies are arrays of our own, so both are safe to use after the
        // enumeration has finished and every block has been released.
        long rowsSeen = 0;
        double voltageTotal = 0;
        var strongestSensor = string.Empty;
        double strongest = double.MinValue;
        double[]? firstRowReadings = null;

        await foreach (Block block in client.StreamAsync(Sql))
        {
            IColumn<string> sensors = block.Column<string>("sensor");
            ReadOnlySpan<double> volts = block.Column<double>("voltage").Values;

            for (int row = 0; row < block.RowCount; row++)
            {
                rowsSeen++;
                voltageTotal += volts[row];
                if (volts[row] > strongest)
                {
                    strongest = volts[row];

                    // A string read out of the block is a reference to an object the block does not own, so
                    // holding it is fine. A span is not.
                    strongestSensor = sensors[row];
                }
            }

            // ToArray inside the loop is the copy. Taking the span out of the loop instead would be reading
            // storage the next iteration has already handed back to the pool.
            firstRowReadings ??= block.Column<double[]>("readings")[0].ToArray();
        }

        Console.WriteLine($"   After the loop, from copies only: {rowsSeen} rows, mean voltage {voltageTotal / rowsSeen:0.####},");
        Console.WriteLine($"   highest on sensor '{strongestSensor}' at {strongest}, first row's readings [{string.Join(", ", firstRowReadings!)}]");
    }

    private static string Sql
        => $"SELECT id, sensor, voltage, readings, captured_at, uptime FROM {TableName} ORDER BY id";

    private static string Format(DateTimeOffset value)
        => value.ToString("yyyy-MM-dd HH:mm:ss.fff zzz", CultureInfo.InvariantCulture);

    private static string Describe(Type type) => type switch
    {
        _ when type == typeof(double[]) => "double[]",
        _ when type == typeof(uint?) => "uint?",
        _ => type.Name,
    };

    // Which of the block tier's extra read surfaces a column offers, found by pattern-matching rather than by
    // reading TypeName.
    private static string ExtraInterface(IColumn column) => column switch
    {
        IDateTimeColumn => "IDateTimeColumn",
        ITimeColumn => "ITimeColumn",
        IArrayColumn<double> => "IArrayColumn<double>",
        _ => "-",
    };
}
