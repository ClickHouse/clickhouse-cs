using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Receives server logs, totals, and extremes through block callbacks.</summary>
public static class TcpMetadataBlocks
{
    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();

        var serverLogs = new List<string>();
        var totals = new Dictionary<string, object?>();
        var extremes = new List<Dictionary<string, object?>>();

        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string>
            {
                ["send_logs_level"] = "debug",
                ["extremes"] = "1",
            },
            Callbacks = new ClickHouseTcpQueryCallbacks
            {
                OnLog = block =>
                {
                    IColumn<string> source = block.Column<string>("source");
                    IColumn<string> text = block.Column<string>("text");
                    for (int row = 0; row < block.RowCount; row++)
                    {
                        serverLogs.Add($"{source[row]}: {text[row]}");
                    }
                },
                OnTotals = block =>
                {
                    for (int column = 0; column < block.ColumnCount; column++)
                    {
                        totals[block.ColumnNames[column]] = block[column].GetValue(0);
                    }
                },
                OnExtremes = block =>
                {
                    for (int row = 0; row < block.RowCount; row++)
                    {
                        var values = new Dictionary<string, object?>();
                        for (int column = 0; column < block.ColumnCount; column++)
                        {
                            values[block.ColumnNames[column]] = block[column].GetValue(row);
                        }

                        extremes.Add(values);
                    }
                },
            },
        };

        await foreach (object[] row in client.QueryAsync(
            """
            SELECT number % 3 AS bucket, count() AS rows
            FROM numbers(30)
            GROUP BY bucket WITH TOTALS
            ORDER BY bucket
            """,
            options))
        {
            Console.WriteLine($"bucket={row[0]}, rows={row[1]}");
        }

        Console.WriteLine($"Totals: {Format(totals)}");
        Console.WriteLine($"Minimums: {Format(extremes[0])}");
        Console.WriteLine($"Maximums: {Format(extremes[1])}");
        Console.WriteLine($"Server log lines: {serverLogs.Count}");

        // Callback blocks are borrowed. Copy values inside the callback, as above.
    }

    private static string Format(IReadOnlyDictionary<string, object?> values)
        => string.Join(", ", values.Select(item => $"{item.Key}={item.Value ?? "NULL"}"));
}
