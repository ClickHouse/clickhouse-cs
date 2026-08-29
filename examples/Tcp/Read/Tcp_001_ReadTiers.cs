using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Reads the same result as rows, objects, and columnar blocks.</summary>
public static class TcpReadTiers
{
    private const string TableName = "example_tcp_read_tiers";
    private static string Sql => $"SELECT id, city, temperature FROM {TableName} ORDER BY id";

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
                    city String,
                    temperature Float64
                )
                ENGINE = MergeTree
                ORDER BY id
                """);

            await client.InsertRowsAsync(
                $"INSERT INTO {TableName} (id, city, temperature) VALUES",
                new[]
                {
                    new object[] { 1UL, "Amsterdam", 17.5 },
                    new object[] { 2UL, "Reykjavik", 9.5 },
                    new object[] { 3UL, "Singapore", 28.0 },
                });

            // Row reads need no model and expose the values in their wire representation.
            Console.WriteLine("QueryAsync: flexible object[] rows");
            await foreach (object[] row in client.QueryAsync(Sql))
            {
                Console.WriteLine($"  {row[0]}: {row[1]}, {row[2]} °C");
            }

            // POCO reads map column names to properties and convert compatible values.
            Console.WriteLine("QueryAsync<T>: strongly typed objects");
            await foreach (Reading row in client.QueryAsync<Reading>(Sql))
            {
                Console.WriteLine($"  {row.Id}: {row.City}, {row.Temperature} °C");
            }

            // Block reads are best for column-oriented work and avoid materializing each row.
            Console.WriteLine("StreamAsync: columnar blocks");
            await foreach (Block block in client.StreamAsync(Sql))
            {
                ReadOnlySpan<double> temperatures = block.Column<double>("temperature").Values;
                double total = 0;
                foreach (double temperature in temperatures)
                {
                    total += temperature;
                }

                Console.WriteLine($"  {block.RowCount} rows; average {total / block.RowCount:0.0} °C");
            }
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        }
    }

    private sealed class Reading
    {
        public ulong Id { get; set; }

        public string City { get; set; } = string.Empty;

        public double Temperature { get; set; }
    }
}
