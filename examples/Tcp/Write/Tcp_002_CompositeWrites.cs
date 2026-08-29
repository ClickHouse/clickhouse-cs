using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Writes arrays, maps, tuples, nullable values, and low-cardinality strings.</summary>
public static class TcpCompositeWrites
{
    private const string SourceTable = "example_tcp_composite_writes";
    private const string CopyTable = "example_tcp_composite_writes_copy";
    private const string Columns = "id, readings, attributes, point, score, city";

    public static async Task Run()
    {
        ClickHouseTcpClientOptions options = ExampleConfig.TcpBuilder().ToOptions();

        // Streaming holds one connection while InsertAsync uses another, so this needs two pool slots.
        options = options with { MaxPoolSize = Math.Max(2, options.MaxPoolSize) };

        await using var client = new ClickHouseTcpClient(options);

        foreach (string table in new[] { SourceTable, CopyTable })
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
        }

        try
        {
            await client.ExecuteAsync(CreateTable(SourceTable));
            await client.ExecuteAsync(CreateTable(CopyTable));

            var readings = new[]
            {
                new[] { 0.5, 0.75, 1.0 },
                Array.Empty<double>(),
            };
            var attributes = new[]
            {
                new[]
                {
                    new KeyValuePair<string, long>("floor", 3),
                    new KeyValuePair<string, long>("room", 12),
                },
                Array.Empty<KeyValuePair<string, long>>(),
            };

            // Use one array or map per row, ValueTuple for Tuple, and nullable CLR values for Nullable.
            await client.InsertAsync(
                $"INSERT INTO {SourceTable} ({Columns}) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("id", new ulong[] { 1, 2 }),
                    ClickHouseTcpColumn.Create("readings", readings),
                    ClickHouseTcpColumn.Create("attributes", attributes),
                    ClickHouseTcpColumn.Create("point", new[] { (1, "one"), (2, "two") }),
                    ClickHouseTcpColumn.Create("score", new double?[] { 1.25, null }),
                    ClickHouseTcpColumn.Create("city", new[] { "Amsterdam", "Amsterdam" }),
                });

            await PrintRows(client, SourceTable);

            // Read columns already use the native layout. Reinsert them before the borrowed block expires.
            await foreach (Block block in client.StreamAsync(
                $"SELECT {Columns} FROM {SourceTable} ORDER BY id"))
            {
                await client.InsertAsync(
                    $"INSERT INTO {CopyTable} ({Columns}) VALUES",
                    block.Columns.ToArray());
            }

            Console.WriteLine("Copied directly from result blocks:");
            await PrintRows(client, CopyTable);
        }
        finally
        {
            foreach (string table in new[] { SourceTable, CopyTable })
            {
                await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
            }
        }
    }

    private static string CreateTable(string table) => $"""
        CREATE TABLE {table}
        (
            id UInt64,
            readings Array(Float64),
            attributes Map(String, Int64),
            point Tuple(x Int32, y String),
            score Nullable(Float64),
            city LowCardinality(String)
        )
        ENGINE = MergeTree
        ORDER BY id
        """;

    private static async Task PrintRows(ClickHouseTcpClient client, string table)
    {
        await foreach (object[] row in client.QueryAsync($"""
            SELECT id, toString(readings), toString(attributes), toString(point),
                   toString(score), city
            FROM {table}
            ORDER BY id
            """))
        {
            Console.WriteLine(string.Join(" | ", row.Select(value => value?.ToString() ?? "NULL")));
        }
    }
}
