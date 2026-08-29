using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Reads composite values through their typed columnar views.</summary>
public static class TcpCompositeRead
{
    private const string TableName = "example_tcp_composite_read";
    private const string Columns = "id, readings, attributes, point, score, city";

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
                    readings Array(Float64),
                    attributes Map(String, Int64),
                    point Tuple(x Int32, y String),
                    score Nullable(Float64),
                    city LowCardinality(String)
                )
                ENGINE = MergeTree
                ORDER BY id
                """);

            await client.InsertAsync(
                $"INSERT INTO {TableName} ({Columns}) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("id", new ulong[] { 1, 2 }),
                    ClickHouseTcpColumn.Create(
                        "readings",
                        new[] { new[] { 0.5, 0.75 }, Array.Empty<double>() }),
                    ClickHouseTcpColumn.Create(
                        "attributes",
                        new[]
                        {
                            new[] { new KeyValuePair<string, long>("floor", 3) },
                            Array.Empty<KeyValuePair<string, long>>(),
                        }),
                    ClickHouseTcpColumn.Create("point", new[] { (1, "one"), (2, "two") }),
                    ClickHouseTcpColumn.Create("score", new double?[] { 1.25, null }),
                    ClickHouseTcpColumn.Create("city", new[] { "Amsterdam", "Amsterdam" }),
                });

            // Composite interfaces expose flattened native storage without allocating one object per row.
            await foreach (Block block in client.StreamAsync(
                $"SELECT {Columns} FROM {TableName} ORDER BY id"))
            {
                if (block["readings"] is IArrayColumn<double> readings)
                {
                    Console.WriteLine(
                        $"Array values [{string.Join(", ", readings.InnerValues.ToArray())}], " +
                        $"offsets [{string.Join(", ", readings.Offsets.ToArray())}]");
                }

                if (block["attributes"] is IMapColumn<string, long> attributes)
                {
                    Console.WriteLine(
                        $"Map keys [{string.Join(", ", attributes.KeyColumn.Values.ToArray())}], " +
                        $"values [{string.Join(", ", attributes.ValueColumn.Values.ToArray())}]");
                }

                if (block["point"] is ITupleColumn point)
                {
                    Console.WriteLine(
                        $"Tuple fields [{string.Join(", ", point.FieldNames ?? Array.Empty<string?>())}]");
                }

                if (block["score"] is INullableColumn<double> score)
                {
                    Console.WriteLine(
                        $"Nullable null map [{string.Join(", ", score.NullMap.ToArray())}]");
                }

                if (block["city"] is ILowCardinalityColumn<string> city)
                {
                    // LowCardinality stores values once and addresses them with integer keys.
                    Console.WriteLine(
                        $"LowCardinality dictionary [{string.Join(", ", city.Dictionary.Values.ToArray())}], " +
                        $"keys [{string.Join(", ", city.Keys.ToArray())}]");
                }
            }

            // Geo aliases use the same column shape as their underlying tuple types.
            await foreach (Block block in client.StreamAsync(
                "SELECT CAST((1.0, 2.0), 'Point') AS point"))
            {
                Console.WriteLine($"Point uses {block["point"].GetType().Name} and materializes as " +
                                  $"{block["point"].GetValue(0)}");
            }
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        }
    }
}
