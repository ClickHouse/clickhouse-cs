using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Inserts and reads strongly typed objects with custom column mappings.</summary>
public static class TcpPoco
{
    private const string TableName = "example_tcp_poco";

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

            var rows = new[]
            {
                new Observation
                {
                    Id = 1,
                    DisplayName = "Ada Lovelace",
                    SignalCount = 12,
                    RecordedAt = new DateTime(2026, 6, 1, 6, 0, 0, DateTimeKind.Utc),
                },
                new Observation
                {
                    Id = 2,
                    DisplayName = "Grace Hopper",
                    SignalCount = 7,
                    RecordedAt = new DateTime(2026, 6, 1, 9, 0, 0, DateTimeKind.Utc),
                },
            };

            await client.InsertRowsAsync(
                $"INSERT INTO {TableName} (id, full_name, signal_count, recorded_at) VALUES",
                rows);

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
