using System.Globalization;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// The three ways the native client reads a result, run one after another over the same query: <c>QueryAsync</c>
/// (one <c>object[]</c> per row, value-type columns boxed), <c>QueryAsync&lt;T&gt;</c> (one POCO per row, values
/// converted) and <c>StreamAsync</c> (whole <see cref="Block"/>s, typed columns, no per-row work at all).
///
/// <para>
/// The tiers are not three spellings of one thing. They differ in what they allocate, and they differ in what a
/// timestamp looks like when it arrives — the row tier hands back the integer the wire carried, while the other
/// two convert it. Section 4 measures the first difference and section 1 shows the second.
/// </para>
/// </summary>
public static class TcpReadTiers
{
    private const string TableName = "example_tcp_read_tiers";

    // Rows for the allocation measurement in section 4. Large enough that the tiers separate, small enough that
    // the whole example stays under a second.
    private const int MeasuredRows = 200_000;

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();

        try
        {
            await Seed(client);
            await RowTier(client);
            await PocoTier(client);
            await BlockTier(client);
            await WhatEachCosts(client);
            ShowTheChoice();
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
            Console.WriteLine($"\nDropped '{TableName}'");
        }
    }

    private static async Task Seed(ClickHouseTcpClient client)
    {
        // recorded_at declares its timezone. A bare DateTime would take the server's, which is what the block
        // tier reports as the column's TimeZone; naming UTC makes this example's output the same everywhere.
        await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        await client.ExecuteAsync($@"
            CREATE TABLE {TableName}
            (
                id UInt64,
                city String,
                temperature Float64,
                recorded_at DateTime('UTC')
            )
            ENGINE = MergeTree()
            ORDER BY id");

        var midnight = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc);

        await client.InsertRowsAsync(
            $"INSERT INTO {TableName} (id, city, temperature, recorded_at) VALUES",
            new List<object[]>
            {
                new object[] { 1UL, "Amsterdam", 17.5, midnight.AddHours(6) },
                new object[] { 2UL, "Amsterdam", 21.0, midnight.AddHours(14) },
                new object[] { 3UL, "Reykjavik", 9.5, midnight.AddHours(6) },
                new object[] { 4UL, "Reykjavik", 11.25, midnight.AddHours(14) },
                new object[] { 5UL, "Singapore", 28.0, midnight.AddHours(6) },
                new object[] { 6UL, "Singapore", 31.75, midnight.AddHours(14) },
            });

        Console.WriteLine($"Seeded '{TableName}' with 6 rows (id UInt64, city String, temperature Float64, recorded_at DateTime('UTC'))");
    }

    private static string Sql => $"SELECT id, city, temperature, recorded_at FROM {TableName} ORDER BY id";

    private static async Task RowTier(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n1. QueryAsync — one object[] per row, value-type columns boxed\n");
        Console.WriteLine("   ID  City       Temp   recorded_at   CLR types");
        Console.WriteLine("   --  ---------  -----  -----------   ---------");

        object[]? first = null;

        // Values arrive in the order the SELECT names them; there are no names on this tier. Each array is yours
        // to keep, so collecting rows into a list is safe.
        await foreach (object[] row in client.QueryAsync(Sql))
        {
            first ??= row;
            Console.WriteLine(
                $"   {(ulong)row[0],2}  {(string)row[1],-9}  {(double)row[2],5}  {row[3],11}   " +
                string.Join(", ", row.Select(v => v.GetType().Name)));
        }

        Console.WriteLine();
        Console.WriteLine("   The last column is the trap. recorded_at is a DateTime('UTC'), but a DateTime column");
        Console.WriteLine("   is stored as a count of epoch seconds and that count is what the box holds:");

        // Reading a calendar value off this tier means converting the count by hand, which needs the timezone the
        // column declared — and nothing on this tier reports it. The other two tiers do the conversion for you.
        uint seconds = (uint)first![3];
        Console.WriteLine($"     row[3] is {first[3].GetType().Name} = {seconds}");

        try
        {
            _ = (DateTime)first[3];
        }
        catch (InvalidCastException ex)
        {
            Console.WriteLine($"     (DateTime)row[3] throws: {ex.Message}");
        }

        Console.WriteLine($"     converted by hand: {DateTimeOffset.FromUnixTimeSeconds(seconds).UtcDateTime:yyyy-MM-dd HH:mm:ss} UTC");
        Console.WriteLine("   Date, DateTime64, Time and Time64 behave the same way. Read them through one of the");
        Console.WriteLine("   next two tiers.");
    }

    private static async Task PocoTier(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n2. QueryAsync<T> — one POCO per row, values converted\n");
        Console.WriteLine("   Each column fills the property of the same name (case- and underscore-insensitively,");
        Console.WriteLine("   so recorded_at reaches RecordedAt), converting to the property's type on the way:\n");
        Console.WriteLine("   ID  City       Temp   RecordedAt (DateTime)  Kind");
        Console.WriteLine("   --  ---------  -----  ---------------------  ----");

        await foreach (Reading reading in client.QueryAsync<Reading>(Sql))
        {
            Console.WriteLine(
                $"   {reading.Id,2}  {reading.City,-9}  {reading.Temperature,5}  " +
                $"{reading.RecordedAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),-21}  {reading.RecordedAt.Kind}");
        }

        Console.WriteLine();
        Console.WriteLine("   Kind is Utc because the column declares UTC. A column in a zone with an offset yields");
        Console.WriteLine("   Kind=Unspecified — the wall-clock reading in that zone — so declare the property as a");
        Console.WriteLine("   DateTimeOffset when the offset matters.");
        Console.WriteLine();
        Console.WriteLine("   A column no property maps to is skipped, and a property no column maps to keeps its");
        Console.WriteLine("   default. Tcp_008_Poco covers the mapping rules and the insert direction.");
    }

    private static async Task BlockTier(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n3. StreamAsync — whole blocks, typed columns, nothing boxed\n");

        await foreach (Block block in client.StreamAsync(Sql))
        {
            Console.WriteLine($"   Block of {block.RowCount} rows x {block.ColumnCount} columns: {string.Join(", ", block.ColumnNames)}");

            // Bound once, outside any row loop: a name lookup is a scan of the block's columns.
            IColumn<double> temperature = block.Column<double>("temperature");
            IColumn<ulong> ids = block.Column<ulong>("id");

            // A span over the block's own buffer. Read into a local and iterate that; the property recomputes the
            // span on every access, and it cannot be cached in a field because it is a ref struct.
            ReadOnlySpan<double> values = temperature.Values;
            double total = 0;
            foreach (double value in values)
            {
                total += value;
            }

            Console.WriteLine($"   temperature is {temperature.TypeName} -> ReadOnlySpan<{temperature.ElementType.Name}>, mean {total / values.Length:0.###}");
            Console.WriteLine($"   id          is {ids.TypeName} -> ReadOnlySpan<{ids.ElementType.Name}>, {ids.RowCount} values, first {ids[0]}");

            // The typed view of a DateTime column is IColumn<uint> — the same count the row tier boxed. The
            // calendar reading lives on IDateTimeColumn, which the column also implements.
            IColumn recordedAt = block["recorded_at"];
            Console.WriteLine($"   recorded_at is {recordedAt.TypeName} -> ReadOnlySpan<{recordedAt.ElementType.Name}>, the raw epoch seconds");

            if (recordedAt is IDateTimeColumn instants)
            {
                Console.WriteLine($"   ... and it pattern-matches to IDateTimeColumn: TimeZone {instants.TimeZone.Id}, Scale {instants.Scale}");
                Console.WriteLine($"       GetDateTimeOffset(0) = {instants.GetDateTimeOffset(0).ToString("yyyy-MM-dd HH:mm:ss zzz", CultureInfo.InvariantCulture)}");

                // ToDateTimeOffsets allocates, and the array it returns is the caller's: unlike Values it stays
                // valid after the block is released.
                DateTimeOffset[] all = instants.ToDateTimeOffsets();
                Console.WriteLine($"       ToDateTimeOffsets() = {all.Length} instants, last {all[^1].ToString("HH:mm:ss zzz", CultureInfo.InvariantCulture)}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   A yielded block is borrowed: it is released when the loop advances. Copy out what has");
        Console.WriteLine("   to outlive the iteration. Tcp_006_BlocksAndColumns is the whole contract.");
    }

    private static async Task WhatEachCosts(ClickHouseTcpClient client)
    {
        Console.WriteLine($"\n4. What each costs, summing one Float64 column over {MeasuredRows:N0} rows\n");

        // Two numeric columns and no strings, so the measurement is the tier's own overhead rather than the cost
        // of materializing values every tier has to materialize anyway.
        string sql = $"SELECT number AS id, number * 0.5 AS temperature FROM system.numbers LIMIT {MeasuredRows}";

        // Warm up: the first read of a result compiles the POCO plan and grows the pooled buffers, and charging
        // that to whichever tier ran first would be the whole difference at this size.
        await SumWithRows(client, sql);
        await SumWithPoco(client, sql);
        await SumWithBlocks(client, sql);

        await Measure("QueryAsync      object[] per row, 2 boxes per row", () => SumWithRows(client, sql));
        await Measure("QueryAsync<T>   one POCO per row, no boxing", () => SumWithPoco(client, sql));
        await Measure("StreamAsync     spans over the block's buffers", () => SumWithBlocks(client, sql));

        Console.WriteLine();
        Console.WriteLine("   Allocation is measured process-wide (GC.GetTotalAllocatedBytes), so it includes the");
        Console.WriteLine("   client's own read buffers — which is why the block tier is not zero rather than why it");
        Console.WriteLine("   is small. Absolute numbers move with the machine; the ratio is the point.");
        Console.WriteLine();
        Console.WriteLine("   A String column narrows the gap, because every tier materializes one string per value.");
    }

    private static async Task Measure(string label, Func<Task<double>> read)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();

        long before = GC.GetTotalAllocatedBytes(precise: true);
        var started = System.Diagnostics.Stopwatch.StartNew();
        double sum = await read();
        started.Stop();
        long allocated = GC.GetTotalAllocatedBytes(precise: true) - before;

        Console.WriteLine($"   {label,-52}  {allocated / 1024.0 / 1024.0,7:0.00} MB  {started.ElapsedMilliseconds,4} ms  (sum {sum:0})");
    }

    private static async Task<double> SumWithRows(ClickHouseTcpClient client, string sql)
    {
        double sum = 0;
        await foreach (object[] row in client.QueryAsync(sql))
        {
            sum += (double)row[1];
        }

        return sum;
    }

    private static async Task<double> SumWithPoco(ClickHouseTcpClient client, string sql)
    {
        double sum = 0;
        await foreach (Reading reading in client.QueryAsync<Reading>(sql))
        {
            sum += reading.Temperature;
        }

        return sum;
    }

    private static async Task<double> SumWithBlocks(ClickHouseTcpClient client, string sql)
    {
        double sum = 0;
        await foreach (Block block in client.StreamAsync(sql))
        {
            ReadOnlySpan<double> values = block.Column<double>("temperature").Values;
            for (int i = 0; i < values.Length; i++)
            {
                sum += values[i];
            }
        }

        return sum;
    }

    private static void ShowTheChoice()
    {
        Console.WriteLine("\n5. Which to pick\n");
        Console.WriteLine("   QueryAsync      A result whose shape you do not know at compile time, or a few rows");
        Console.WriteLine("                   where the boxing does not matter. No names — pair it with");
        Console.WriteLine("                   Block.ColumnNames if you need them. Date and time columns arrive raw.");
        Console.WriteLine();
        Console.WriteLine("   QueryAsync<T>   The default for application code. One object per row instead of an");
        Console.WriteLine("                   array plus a box per value-type column, values converted to the");
        Console.WriteLine("                   property's type, and each row owns its values, so a row can be kept.");
        Console.WriteLine();
        Console.WriteLine("   StreamAsync     Aggregating, scanning, or handing a column to something that wants a");
        Console.WriteLine("                   span. Nothing is materialized per row, and a column read out of one");
        Console.WriteLine("                   block re-inserts without being rebuilt. The cost is the borrowing");
        Console.WriteLine("                   contract: nothing may outlive the iteration unless you copy it.");
        Console.WriteLine();
        Console.WriteLine("   All three hold a connection until the enumeration ends, so read to the end (or stop");
        Console.WriteLine("   with a break, which tells the server the result is abandoned and drops the connection");
        Console.WriteLine("   rather than returning it to the pool).");
    }

    // One property per column. String is initialized because the project enables nullable reference types and
    // the materializer assigns every mapped property anyway.
    private sealed class Reading
    {
        public ulong Id { get; set; }

        public string City { get; set; } = string.Empty;

        public double Temperature { get; set; }

        // The DateTime('UTC') column's epoch-second count, converted with the column's timezone. Declare this as
        // a DateTimeOffset instead to keep the offset.
        public DateTime RecordedAt { get; set; }
    }
}
