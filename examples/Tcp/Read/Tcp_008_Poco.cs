using System.Globalization;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Reading a result into a class with <c>QueryAsync&lt;T&gt;</c> and writing one back with
/// <c>InsertRowsAsync&lt;T&gt;</c> — one type, both directions, and the two attributes that adjust the mapping:
/// <see cref="ClickHouseTcpColumnAttribute"/> to rename a property and
/// <see cref="ClickHouseTcpNotMappedAttribute"/> to take one out of the mapping entirely.
///
/// <para>
/// This is the tier most application code should use. It converts values to the property's type, so a
/// <c>DateTime</c> column reaches a <c>DateTime</c> property, and each row owns its values, so a row can be
/// returned from the method that read it.
/// </para>
/// </summary>
public static class TcpPoco
{
    private const string TableName = "example_tcp_poco";

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();

        try
        {
            await CreateTable(client);
            await WriteFromPocos(client);
            await ReadIntoPocos(client);
            await ShowTheMapping(client);
            await ShowNotMappedOnInsert(client);
            await ShowWhatDoesNotMap(client);
            ShowTheRules();
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
            Console.WriteLine($"\nDropped '{TableName}'");
        }
    }

    private static async Task CreateTable(ClickHouseTcpClient client)
    {
        await client.ExecuteAsync($@"
            CREATE TABLE {TableName}
            (
                id UInt64,
                full_name String,
                signal_count UInt32,
                recorded_at DateTime('UTC'),
                internal_notes String
            )
            ENGINE = MergeTree()
            ORDER BY id");

        Console.WriteLine($"Created '{TableName}':");
        Console.WriteLine("  id UInt64, full_name String, signal_count UInt32, recorded_at DateTime('UTC'), internal_notes String");
    }

    private static async Task WriteFromPocos(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n1. InsertRowsAsync<T> — the columns the INSERT names are read off each object\n");

        var midnight = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc);

        var rows = new List<Observation>
        {
            new() { Id = 1, DisplayName = "Ada Lovelace", SignalCount = 12, RecordedAt = midnight.AddHours(6), Notes = "not written" },
            new() { Id = 2, DisplayName = "Grace Hopper", SignalCount = 7, RecordedAt = midnight.AddHours(9), Notes = "not written" },
            new() { Id = 3, DisplayName = "Alan Turing", SignalCount = 21, RecordedAt = midnight.AddHours(14), Notes = "not written" },
        };

        // The statement ends at VALUES and names the columns to fill. Each is matched to a property; a property no
        // named column matches is simply not read, which is how Notes stays out of this insert.
        await client.InsertRowsAsync(
            $"INSERT INTO {TableName} (id, full_name, signal_count, recorded_at) VALUES",
            rows);

        Console.WriteLine($"   Inserted {rows.Count} Observation objects into (id, full_name, signal_count, recorded_at)");
        Console.WriteLine("   id           <- Id             matched on the name");
        Console.WriteLine("   full_name    <- DisplayName    matched by [ClickHouseTcpColumn(Name = \"full_name\")]");
        Console.WriteLine("   signal_count <- SignalCount    matched by ignoring case and underscores");
        Console.WriteLine("   recorded_at  <- RecordedAt     a DateTime property written as epoch seconds");
        Console.WriteLine("   internal_notes is not in the statement, so nothing was read for it and it took the");
        Console.WriteLine("   column's default. Notes carries [ClickHouseTcpNotMapped] and could not fill it anyway.");
    }

    private static async Task ReadIntoPocos(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n2. QueryAsync<T> — every result column fills the property it maps to\n");
        Console.WriteLine("   ID  DisplayName     Signals  RecordedAt (Kind)          Notes");
        Console.WriteLine("   --  --------------  -------  -------------------------  -----");

        // SELECT * brings internal_notes back too, and it maps to nothing: Notes is [ClickHouseTcpNotMapped], so the
        // column is skipped rather than assigned.
        await foreach (Observation row in client.QueryAsync<Observation>($"SELECT * FROM {TableName} ORDER BY id"))
        {
            Console.WriteLine(
                $"   {row.Id,2}  {row.DisplayName,-14}  {row.SignalCount,7}  " +
                $"{row.RecordedAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)} ({row.RecordedAt.Kind})  " +
                $"{(row.Notes is null ? "(null)" : row.Notes)}");
        }

        Console.WriteLine();
        Console.WriteLine("   RecordedAt is a real DateTime, converted with the timezone the column declares — the");
        Console.WriteLine("   conversion the object[] tier does not do. Kind is Utc here because the column says UTC.");
        Console.WriteLine("   Notes is null: internal_notes was in the result and was skipped.");
    }

    private static async Task ShowTheMapping(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n3. How a column finds its property\n");
        Console.WriteLine("   In order, first match wins:");
        Console.WriteLine("     the exact property name, then case-insensitively, then ignoring underscores.");
        Console.WriteLine("   So signal_count reaches SignalCount with no attribute at all. Reach for");
        Console.WriteLine("   [ClickHouseTcpColumn(Name = ...)] only when the names differ by more than that —");
        Console.WriteLine("   full_name and DisplayName here.");
        Console.WriteLine();
        Console.WriteLine("   The names matched are the result's, not the table's, so a SELECT alias lines a query up");
        Console.WriteLine("   with a type just as well as an attribute does — and it is the only way to name a");
        Console.WriteLine("   computed column:\n");

        // The names in the result are the aliases the query chose, not the table's, so an alias is the other way to
        // line a result up with a type.
        await foreach (Summary row in client.QueryAsync<Summary>(
            $"SELECT count() AS rows, sum(signal_count) AS total_signals, max(recorded_at) AS latest FROM {TableName}"))
        {
            Console.WriteLine($"   SELECT count() AS rows, sum(signal_count) AS total_signals, max(recorded_at) AS latest");
            Console.WriteLine($"     Rows {row.Rows}, TotalSignals {row.TotalSignals}, Latest {row.Latest.ToString("u", CultureInfo.InvariantCulture)}");
            Console.WriteLine("     Latest is a DateTimeOffset property, so the offset the column's timezone gives is kept.");
        }
    }

    private static async Task ShowNotMappedOnInsert(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n4. [ClickHouseTcpNotMapped] excludes a property in both directions\n");
        Console.WriteLine("   Section 2 showed the read half: internal_notes was skipped. On an insert the exclusion");
        Console.WriteLine("   means the property cannot fill a column, so naming that column is an error rather than");
        Console.WriteLine("   a silent default:\n");

        try
        {
            await client.InsertRowsAsync(
                $"INSERT INTO {TableName} (id, full_name, internal_notes) VALUES",
                new List<Observation> { new() { Id = 9, DisplayName = "nobody", Notes = "would have gone here" } });
        }
        catch (InvalidOperationException ex)
        {
            Console.WriteLine($"     INSERT INTO ... (id, full_name, internal_notes) throws:");
            Console.WriteLine(Wrap(ex.Message, "       "));
        }

        Console.WriteLine();
        Console.WriteLine("   Without the attribute, Notes would match internal_notes by ignoring the underscore, so");
        Console.WriteLine("   the attribute is what makes a property the driver never touches — a cache key, a");
        Console.WriteLine("   computed column, something loaded from elsewhere.");
    }

    private static async Task ShowWhatDoesNotMap(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n5. What a mismatch does\n");

        // Mapping is resolved against the first block of the result, so a type nothing maps to fails on the first
        // row rather than yielding wrong values.
        try
        {
            await foreach (Unrelated _ in client.QueryAsync<Unrelated>($"SELECT id, full_name FROM {TableName}"))
            {
                break;
            }
        }
        catch (InvalidOperationException ex)
        {
            Console.WriteLine("   A type no result column maps to:");
            Console.WriteLine(Wrap(ex.Message, "     "));
        }

        // A property that some column does map to, but whose type the column cannot be read as.
        try
        {
            await foreach (WrongType _ in client.QueryAsync<WrongType>($"SELECT id, full_name FROM {TableName}"))
            {
                break;
            }
        }
        catch (InvalidOperationException ex)
        {
            Console.WriteLine("\n   A property whose type the column cannot be read as:");
            Console.WriteLine(Wrap(ex.Message, "     "));
        }

        Console.WriteLine();
        Console.WriteLine("   Both are checked against the first block, so an empty result yields nothing and");
        Console.WriteLine("   validates nothing. A property that no column reaches is not an error: it keeps its");
        Console.WriteLine("   default, which is what lets one type serve several queries.");
    }

    private static void ShowTheRules()
    {
        Console.WriteLine("\n6. What T has to be\n");
        Console.WriteLine("   A concrete class with a public parameterless constructor.");
        Console.WriteLine("   Every property a result column reaches needs a public setter — an init-only or");
        Console.WriteLine("     get-only property cannot be filled, so a record with positional parameters does not");
        Console.WriteLine("     work for reading.");
        Console.WriteLine("   Every column an INSERT names needs a public getter of a type that column can be");
        Console.WriteLine("     written from.");
        Console.WriteLine("   Rows own their values and stay valid after the enumeration advances. LowCardinality");
        Console.WriteLine("     elements can be shared within a block, so do not mutate an array-valued property");
        Console.WriteLine("     in place.");
        Console.WriteLine("   The read and write plans are compiled once per type per client, so a client meant to");
        Console.WriteLine("     be a singleton pays the reflection once.");
    }

    // Reflows a long driver message so the console output stays readable.
    private static string Wrap(string message, string indent)
    {
        var lines = new List<string>();
        var line = new System.Text.StringBuilder();
        foreach (string word in message.Split(' '))
        {
            if (line.Length + word.Length + 1 > 95)
            {
                lines.Add(line.ToString());
                line.Clear();
            }

            line.Append(line.Length == 0 ? word : " " + word);
        }

        lines.Add(line.ToString());
        return indent + string.Join("\n" + indent, lines);
    }

    /// <summary>
    /// One row of the example's table, used for both the insert and the read. The attributes are the only two the
    /// native client has.
    /// </summary>
    private sealed class Observation
    {
        public ulong Id { get; set; }

        // The column is full_name, which no name-matching rule reaches from DisplayName.
        [ClickHouseTcpColumn(Name = "full_name")]
        public string DisplayName { get; set; } = string.Empty;

        // signal_count matches this by ignoring case and underscores, so no attribute is needed.
        public uint SignalCount { get; set; }

        // A DateTime('UTC') column's epoch-second count, converted on the way in and out.
        public DateTime RecordedAt { get; set; }

        // Excluded in both directions. Without this it would match internal_notes.
        [ClickHouseTcpNotMapped]
        public string? Notes { get; set; }
    }

    // A second shape over the same table: the mapping is per query, so one table can feed several types.
    private sealed class Summary
    {
        public ulong Rows { get; set; }

        public ulong TotalSignals { get; set; }

        // DateTimeOffset keeps the offset the column's timezone gives; DateTime would flatten it.
        public DateTimeOffset Latest { get; set; }
    }

    private sealed class Unrelated
    {
        public string Something { get; set; } = string.Empty;
    }

    private sealed class WrongType
    {
        public ulong Id { get; set; }

        // full_name is a String column, and a String cannot be read as a Guid.
        public Guid FullName { get; set; }
    }
}
