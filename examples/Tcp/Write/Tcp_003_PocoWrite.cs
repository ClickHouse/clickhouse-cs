using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Inserts strongly typed objects by mapping their properties to target columns.</summary>
public static class TcpPocoWrite
{
    private const string TableName = "example_tcp_poco_write";

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
                    internal_notes String DEFAULT ''
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
                    InternalNotes = "not sent",
                },
                new Observation
                {
                    Id = 2,
                    DisplayName = "Grace Hopper",
                    SignalCount = 7,
                    RecordedAt = new DateTime(2026, 6, 1, 9, 0, 0, DateTimeKind.Utc),
                    InternalNotes = "not sent",
                },
            };

            // Target columns match case- and underscore-insensitively. The attribute supplies the one
            // non-conventional name, and the ignored property is left out of the INSERT column list.
            await client.InsertRowsAsync(
                $"INSERT INTO {TableName} (id, full_name, signal_count, recorded_at) VALUES",
                rows);

            await foreach (object[] row in client.QueryAsync(
                $"SELECT id, full_name, signal_count, formatDateTime(recorded_at, '%FT%TZ'), internal_notes " +
                $"FROM {TableName} ORDER BY id"))
            {
                Console.WriteLine(
                    $"{row[0]}: {row[1]}, {row[2]} signals at {row[3]}; notes='{row[4]}'");
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
