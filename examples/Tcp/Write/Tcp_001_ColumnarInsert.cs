using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Inserts data that is already organized into typed columns.</summary>
public static class TcpColumnarInsert
{
    private const string TableName = "example_tcp_columnar_insert";

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
                    name String,
                    score Float64,
                    region String DEFAULT 'unknown'
                )
                ENGINE = MergeTree
                ORDER BY id
                """);

            var ids = new ulong[] { 1, 2, 3 };
            var names = new[] { "Ada", "Grace", "Alan" };
            var scores = new[] { 99.5, 97.25, 91.0 };

            // Typed columns avoid the row-to-column projection performed by InsertRowsAsync.
            // Column names, not argument order, determine the target column.
            var columns = new IColumn[]
            {
                ClickHouseTcpColumn.Create("score", scores),
                ClickHouseTcpColumn.Create("id", ids),
                ClickHouseTcpColumn.Create("name", names),
            };

            await client.InsertAsync(
                $"INSERT INTO {TableName} (id, name, score) VALUES",
                columns,
                new ClickHouseTcpInsertOptions { MaxRowsPerBlock = 2 });

            // region is absent from the statement, so ClickHouse applies its default.
            await client.InsertAsync(
                $"INSERT INTO {TableName} (id, name) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("name", new[] { "Edsger" }),
                    ClickHouseTcpColumn.Create("id", new ulong[] { 4 }),
                });

            await foreach (object[] row in client.QueryAsync(
                $"SELECT id, name, score, region FROM {TableName} ORDER BY id"))
            {
                Console.WriteLine($"{row[0]}: {row[1]}, score {row[2]}, region {row[3]}");
            }
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        }
    }
}
