using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Reads and writes Variant, Dynamic, and JSON columns.</summary>
public static class TcpVariantDynamicJson
{
    private const string VariantTable = "example_tcp_variant";
    private const string DynamicTable = "example_tcp_dynamic";
    private const string JsonTable = "example_tcp_json";

    public static async Task Run()
    {
        var builder = ExampleConfig.TcpBuilder();

        // These types are setting-gated on older supported ClickHouse versions.
        builder["set_allow_experimental_variant_type"] = 1;
        builder["set_allow_experimental_dynamic_type"] = 1;
        builder["set_allow_experimental_json_type"] = 1;

        await using var client = new ClickHouseTcpClient(builder.ToOptions());
        string[] tables = { VariantTable, DynamicTable, JsonTable };

        foreach (string table in tables)
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
        }

        try
        {
            await client.ExecuteAsync($"""
                CREATE TABLE {VariantTable}
                (id UInt64, value Variant(String, UInt64, Array(Int32)))
                ENGINE = MergeTree
                ORDER BY id
                """);
            await client.ExecuteAsync($"""
                CREATE TABLE {DynamicTable}
                (id UInt64, value Dynamic)
                ENGINE = MergeTree
                ORDER BY id
                """);
            await client.ExecuteAsync($"""
                CREATE TABLE {JsonTable}
                (id UInt64, document JSON)
                ENGINE = MergeTree
                ORDER BY id
                """);

            // Variant alternatives are fixed by its declaration.
            await client.InsertAsync(
                $"INSERT INTO {VariantTable} (id, value) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("id", new ulong[] { 1, 2, 3, 4 }),
                    ClickHouseTcpColumn.Create(
                        "value",
                        new object?[] { 42UL, "hello", new[] { 1, 2 }, null }),
                });

            // Dynamic records the concrete types that occur in the inserted values.
            await client.InsertAsync(
                $"INSERT INTO {DynamicTable} (id, value) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("id", new ulong[] { 1, 2, 3, 4 }),
                    ClickHouseTcpColumn.Create(
                        "value",
                        new object?[] { 42UL, "hello", 1.5, null }),
                });

            const string json = "{ \"b\": 1, \"a\": 2 }";
            await client.InsertAsync(
                $"INSERT INTO {JsonTable} (id, document) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("id", new ulong[] { 1 }),
                    ClickHouseTcpColumn.Create("document", new[] { json }),
                });

            await PrintVariant(client);
            await PrintDynamic(client);

            // ClickHouse parses and normalizes JSON, so the returned text may differ from the input.
            object normalized = await client.ExecuteScalarAsync(
                $"SELECT document FROM {JsonTable} WHERE id = 1");
            Console.WriteLine($"JSON written: {json}");
            Console.WriteLine($"JSON read:    {normalized}");
        }
        finally
        {
            foreach (string table in tables)
            {
                await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
            }
        }
    }

    private static async Task PrintVariant(ClickHouseTcpClient client)
    {
        await foreach (Block block in client.StreamAsync(
            $"SELECT value FROM {VariantTable} ORDER BY id"))
        {
            var column = (IVariantColumn)block["value"];
            Console.WriteLine(
                $"Variant: {column.TypeCount} alternatives, discriminators " +
                $"[{string.Join(", ", column.Discriminators.ToArray())}]");

            for (int row = 0; row < column.RowCount; row++)
            {
                Console.WriteLine($"  {FormatValue(block["value"].GetValue(row))}");
            }
        }
    }

    private static async Task PrintDynamic(ClickHouseTcpClient client)
    {
        await foreach (Block block in client.StreamAsync(
            $"SELECT value FROM {DynamicTable} ORDER BY id"))
        {
            var column = (IDynamicColumn)block["value"];
            Console.WriteLine($"Dynamic types: [{string.Join(", ", column.TypeNames)}]");

            for (int row = 0; row < column.RowCount; row++)
            {
                Console.WriteLine($"  {FormatValue(block["value"].GetValue(row))}");
            }
        }
    }

    private static string FormatValue(object? value) => value switch
    {
        Array items => $"[{string.Join(", ", items.Cast<object>())}]",
        null => "NULL",
        _ => value.ToString() ?? "NULL",
    };
}
