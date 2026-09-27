using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Reads query results into strongly typed objects with custom column mappings.</summary>
public static class TcpPocoRead
{
    private const string TableName = "example_tcp_poco_read";

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
                    full_name String,
                    signal_count UInt32,
                    recorded_at DateTime('UTC'),
                    internal_notes String
                )
                ENGINE = MergeTree
                ORDER BY id
                """);

            await client.InsertAsync(
                $"INSERT INTO {TableName} (id, full_name, signal_count, recorded_at) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("id", new ulong[] { 1, 2 }),
                    ClickHouseTcpColumn.Create("full_name", new[] { "Ada Lovelace", "Grace Hopper" }),
                    ClickHouseTcpColumn.Create("signal_count", new uint[] { 12, 7 }),
                    ClickHouseTcpColumn.Create(
                        "recorded_at",
                        new[]
                        {
                            new DateTime(2026, 6, 1, 6, 0, 0, DateTimeKind.Utc),
                            new DateTime(2026, 6, 1, 9, 0, 0, DateTimeKind.Utc),
                        }),
                });

            // POCO mapping is usually slower than block iteration: it allocates and fills one object per row.
            // StreamAsync exposes borrowed column buffers and avoids those per-row object allocations.
            await foreach (Observation row in client.QueryAsync<Observation>(
                $"SELECT id, full_name, signal_count, recorded_at, internal_notes " +
                $"FROM {TableName} ORDER BY id"))
            {
                Console.WriteLine(
                    $"{row.Id}: {row.DisplayName}, {row.SignalCount} signals at {row.RecordedAt:O}");
            }
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        }
    }

    private sealed class Observation
    {
        public ulong Id { get; set; }

        [ClickHouseTcpColumn(Name = "full_name")]
        public string DisplayName { get; set; } = string.Empty;

        // signal_count matches SignalCount by ignoring case and underscores.
        public uint SignalCount { get; set; }

        public DateTime RecordedAt { get; set; }

        [ClickHouseTcpNotMapped]
        public string? InternalNotes { get; set; }
    }
}
