namespace ClickHouse.Driver.Examples;

/// <summary>
/// A full end-to-end round trip mixing both transports:
///   1. An HTTP client creates a table and bulk-inserts rows (bulk insert is HTTP-only for now).
///   2. A Native (TCP) client reads the same data back, including Nullable and Array columns.
///   3. The Native client runs DDL (DROP TABLE) to show ExecuteNonQueryAsync works over TCP too.
///
/// This illustrates that the two protocols are interchangeable for reading and DDL, and see exactly
/// the same data — you can adopt the Native protocol for queries without changing anything else.
/// </summary>
public static class NativeEndToEnd
{
    public static async Task Run()
    {
        const string table = "example_native_end_to_end";

        // HTTP client for setup + bulk insert (InsertBinaryAsync currently uses the HTTP transport).
        using var http = new ClickHouseClient("Host=localhost");

        // Native (TCP) client for reads and DDL.
        using var native = new ClickHouseClient("Host=localhost;Protocol=native");

        await http.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        await http.ExecuteNonQueryAsync($@"
            CREATE TABLE {table}
            (
                id    UInt64,
                name  String,
                score Nullable(Float64),
                tags  Array(String)
            )
            ENGINE = MergeTree()
            ORDER BY id");

        var columns = new[] { "id", "name", "score", "tags" };
        var rows = new List<object[]>
        {
            new object[] { 1UL, "Alice", 9.5d,  new[] { "admin", "ops" } },
            new object[] { 2UL, "Bob",   null!, new[] { "dev" } },
            new object[] { 3UL, "Carol", 7.25d, Array.Empty<string>() },
        };
        await http.InsertBinaryAsync(table, columns, rows);
        Console.WriteLine($"Inserted {rows.Count} rows via HTTP.\n");

        Console.WriteLine("Reading them back over the Native protocol:");
        Console.WriteLine("  id  name   score  tags");
        Console.WriteLine("  --  -----  -----  ----------------");
        using (var reader = await native.ExecuteReaderAsync(
            $"SELECT id, name, score, tags FROM {table} ORDER BY id"))
        {
            while (await reader.ReadAsync())
            {
                var id = reader.GetUInt64(0);
                var name = reader.GetString(1);
                var score = reader.IsDBNull(2) ? "NULL" : reader.GetDouble(2).ToString("0.00");
                var tags = (string[])reader.GetValue(3);
                Console.WriteLine($"  {id,-3} {name,-6} {score,-6} [{string.Join(", ", tags)}]");
            }
        }

        // DDL over the Native protocol.
        await native.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        Console.WriteLine($"\nDropped table '{table}' over the Native protocol. Done.");
    }
}
