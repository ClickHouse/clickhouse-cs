using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Uses every callback: insert-block progress, query progress and profile info, and server log,
/// profile-event, totals, and extremes metadata blocks.
/// </summary>
public static class TcpCallbacksAndMetadata
{
    private const string TableName = "example_tcp_callbacks";

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();
        await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");

        try
        {
            await client.ExecuteAsync($"CREATE TABLE {TableName} (id UInt64) ENGINE = Memory");

            int writtenBlocks = 0;
            int writtenRows = 0;
            await client.InsertAsync(
                $"INSERT INTO {TableName} (id) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create(
                        "id",
                        Enumerable.Range(0, 20).Select(value => (ulong)value).ToArray()),
                },
                new ClickHouseTcpInsertOptions
                {
                    MaxRowsPerBlock = 8,
                    Callbacks = new ClickHouseTcpQueryCallbacks
                    {
                        OnBlockWritten = block =>
                        {
                            writtenBlocks++;
                            writtenRows += block.RowCount;
                            Console.WriteLine(
                                $"OnBlockWritten: block {block.BlockIndex}, {block.RowCount} rows, " +
                                $"{block.UncompressedBytes} bytes encoded as {block.CompressedBytes}");
                        },
                    },
                });

            ClickHouseTcpProgress progress = default;
            ClickHouseTcpProfileInfo profile = default;
            bool receivedProfileInfo = false;
            int progressUpdates = 0;
            int logLines = 0;
            string? firstLogLine = null;
            int profileEventRows = 0;
            string? firstProfileEvent = null;
            var profileEventNames = new HashSet<string>(StringComparer.Ordinal);
            IReadOnlyDictionary<string, object?> totals = new Dictionary<string, object?>();
            IReadOnlyList<IReadOnlyDictionary<string, object?>> extremes =
                Array.Empty<IReadOnlyDictionary<string, object?>>();

            var options = new ClickHouseTcpQueryOptions
            {
                Settings = new Dictionary<string, string>
                {
                    ["interactive_delay"] = "30000",
                    ["max_block_size"] = "1",
                    ["send_logs_level"] = "trace",
                    ["extremes"] = "1",
                },
                Callbacks = new ClickHouseTcpQueryCallbacks
                {
                    OnProgress = increment =>
                    {
                        progress += increment;
                        progressUpdates++;
                    },
                    OnProfileInfo = info =>
                    {
                        profile = info;
                        receivedProfileInfo = true;
                    },
                    OnLog = block =>
                    {
                        IColumn<string> source = block.Column<string>("source");
                        IColumn<string> text = block.Column<string>("text");
                        logLines += block.RowCount;
                        if (firstLogLine is null && block.RowCount > 0)
                        {
                            firstLogLine = $"{source[0]}: {text[0]}";
                        }
                    },
                    OnProfileEvents = block =>
                    {
                        ReadOnlySpan<ulong> threadId = block.Column<ulong>("thread_id").Values;
                        ReadOnlySpan<sbyte> type = block.Column<sbyte>("type").Values;
                        IColumn<string> name = block.Column<string>("name");
                        ReadOnlySpan<long> value = block.Column<long>("value").Values;

                        profileEventRows += block.RowCount;
                        for (int row = 0; row < block.RowCount; row++)
                        {
                            string eventName = name[row];
                            profileEventNames.Add(eventName);
                            firstProfileEvent ??= type[row] == 1
                                ? $"thread {threadId[row]}: {eventName} += {value[row]}"
                                : $"thread {threadId[row]}: {eventName} = {value[row]}";
                        }
                    },
                    OnTotals = block => totals = CopyRow(block, 0),
                    OnExtremes = block => extremes = CopyRows(block),
                },
            };

            int resultRows = 0;
            await foreach (object[] row in client.QueryAsync(
                $"""
                SELECT id % 3 AS bucket, count() AS rows
                FROM {TableName}
                WHERE sleepEachRow(0.02) = 0
                GROUP BY bucket WITH TOTALS
                ORDER BY bucket
                """,
                options))
            {
                resultRows++;
                Console.WriteLine($"bucket={row[0]}, rows={row[1]}");
            }

            Console.WriteLine($"OnBlockWritten: {writtenBlocks} blocks, {writtenRows} rows");
            Console.WriteLine($"OnProgress: {progressUpdates} updates, {progress.Rows} rows read");
            Console.WriteLine(receivedProfileInfo
                ? $"OnProfileInfo: {profile.Rows} rows in {profile.Blocks} blocks"
                : "OnProfileInfo: no summary received");
            Console.WriteLine($"OnLog: {logLines} lines; first: {firstLogLine ?? "none"}");
            Console.WriteLine(
                $"OnProfileEvents: {profileEventRows} rows for {profileEventNames.Count} counters; " +
                $"first: {firstProfileEvent ?? "none"}");
            Console.WriteLine($"OnTotals: {Format(totals)}");
            Console.WriteLine($"OnExtremes: {string.Join(" | ", extremes.Select(Format))}");
            Console.WriteLine($"Result rows: {resultRows}");

            // Callbacks run synchronously while the response is drained. Block arguments are borrowed, so copy
            // values inside the callback, as above; retain neither the block nor its columns or spans.
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        }
    }

    private static IReadOnlyDictionary<string, object?> CopyRow(Block block, int row)
    {
        var values = new Dictionary<string, object?>(block.ColumnCount, StringComparer.Ordinal);
        for (int column = 0; column < block.ColumnCount; column++)
        {
            values[block.ColumnNames[column]] = block[column].GetValue(row);
        }

        return values;
    }

    private static IReadOnlyList<IReadOnlyDictionary<string, object?>> CopyRows(Block block)
    {
        var rows = new List<IReadOnlyDictionary<string, object?>>(block.RowCount);
        for (int row = 0; row < block.RowCount; row++)
        {
            rows.Add(CopyRow(block, row));
        }

        return rows;
    }

    private static string Format(IReadOnlyDictionary<string, object?> values)
        => values.Count == 0
            ? "none"
            : string.Join(", ", values.Select(item => $"{item.Key}={item.Value ?? "NULL"}"));
}
