using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Binds typed values and identifiers to native-protocol queries.</summary>
public static class TcpParameters
{
    private const string TableName = "example_tcp_parameters";

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
                    temperature Float64,
                    recorded_at DateTime('UTC')
                )
                ENGINE = MergeTree
                ORDER BY id
                """);

            var recordedAt = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc);
            await client.InsertRowsAsync(
                $"INSERT INTO {TableName} (id, city, temperature, recorded_at) VALUES",
                new[]
                {
                    new object[] { 1UL, "Amsterdam", 21.0, recordedAt },
                    new object[] { 2UL, "Reykjavik", 11.25, recordedAt },
                    new object[] { 3UL, "Singapore", 31.75, recordedAt },
                });

            var parameters = new ClickHouseTcpParameterCollection
            {
                { "city", "Amsterdam" },
                { "minimum", 18.0 },
                { "ids", new[] { 1UL, 2UL } },
            };

            var options = new ClickHouseTcpQueryOptions { Parameters = parameters };

            // Native queries use {name:Type}; @name placeholders are not rewritten.
            string sql = $$"""
                SELECT id, city, temperature
                FROM {{TableName}}
                WHERE city = {city:String}
                   OR (temperature >= {minimum:Float64} AND id IN {ids:Array(UInt64)})
                ORDER BY id
                """;

            await foreach (object[] row in client.QueryAsync(sql, options))
            {
                Console.WriteLine($"{row[0]}: {row[1]}, {row[2]} °C");
            }

            var identifierOptions = new ClickHouseTcpQueryOptions
            {
                Parameters = new ClickHouseTcpParameterCollection
                {
                    { "table", TableName },
                    { "column", "temperature" },
                },
            };

            object maximum = await client.ExecuteScalarAsync(
                "SELECT max({column:Identifier}) FROM {table:Identifier}",
                identifierOptions);
            Console.WriteLine($"Maximum temperature: {maximum}");

            var timeOptions = new ClickHouseTcpQueryOptions
            {
                Parameters = new ClickHouseTcpParameterCollection { { "start", recordedAt } },
            };

            // Declare a timezone when a DateTime or DateTimeOffset represents an instant.
            object count = await client.ExecuteScalarAsync(
                $"SELECT count() FROM {TableName} WHERE recorded_at >= {{start:DateTime('UTC')}}",
                timeOptions);
            Console.WriteLine($"Rows at or after {recordedAt:O}: {count}");
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        }
    }
}
