using System.Globalization;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Reads typed columns and composite values from borrowed result blocks.</summary>
public static class TcpBlocksAndColumns
{
    private const string TableName = "example_tcp_blocks_and_columns";

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();
        await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");

        try
        {
            await client.ExecuteAsync($"""
                CREATE TABLE {TableName}
                (
                    id UInt64,
                    sensor String,
                    readings Array(Float64),
                    captured_at DateTime64(3, 'UTC')
                )
                ENGINE = MergeTree
                ORDER BY id
                """);

            var capturedAt = new DateTime(2026, 6, 1, 10, 0, 0, DateTimeKind.Utc);
            await client.InsertRowsAsync(
                $"INSERT INTO {TableName} (id, sensor, readings, captured_at) VALUES",
                new[]
                {
                    new object[] { 1UL, "north", new[] { 0.5, 0.75 }, capturedAt },
                    new object[] { 2UL, "south", new[] { 1.0, 1.25, 1.5 }, capturedAt.AddMinutes(1) },
                });

            double[]? readingsToKeep = null;

            await foreach (Block block in client.StreamAsync(
                $"SELECT id, sensor, readings, captured_at FROM {TableName} ORDER BY id"))
            {
                Console.WriteLine($"Block: {block.RowCount} rows, columns [{string.Join(", ", block.ColumnNames)}]");

                IColumn<ulong> ids = block.Column<ulong>("id");
                IColumn<string> sensors = block.Column<string>("sensor");
                Console.WriteLine($"First row: {ids[0]}, {sensors[0]}");

                if (block["readings"] is IArrayColumn<double> arrays)
                {
                    ReadOnlySpan<double> values = arrays.InnerValues;
                    ReadOnlySpan<int> offsets = arrays.Offsets;
                    Console.WriteLine($"Array storage: {values.Length} values, offsets " +
                                      $"[{string.Join(", ", offsets.ToArray())}]");

                    // Row i occupies values[offsets[i]..offsets[i + 1]].
                    // Copy data that must remain valid after this block is released.
                    readingsToKeep ??= values[offsets[0]..offsets[1]].ToArray();
                }

                if (block["captured_at"] is IDateTimeColumn timestamps)
                {
                    DateTimeOffset value = timestamps.GetDateTimeOffset(0);
                    Console.WriteLine(value.ToString("O", CultureInfo.InvariantCulture));
                }
            }

            Console.WriteLine($"Copied readings: [{string.Join(", ", readingsToKeep!)}]");
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        }
    }
}
