using System.Globalization;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Reads and writes ClickHouse date, timestamp, and time values.</summary>
public static class TcpDateTimeAndTimezones
{
    private const string TableName = "example_tcp_datetime_timezones";

    public static async Task Run()
    {
        var builder = ExampleConfig.TcpBuilder();

        // ClickHouse 25.8 requires both settings for Time and Time64.
        builder["set_enable_time_time64_type"] = 1;
        builder["set_allow_experimental_time_time64_type"] = 1;

        await using var client = new ClickHouseTcpClient(builder.ToOptions());
        await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");

        try
        {
            await client.ExecuteAsync($"""
                CREATE TABLE {TableName}
                (
                    day Date,
                    old_day Date32,
                    captured_at DateTime('Europe/Amsterdam'),
                    precise_at DateTime64(3, 'UTC'),
                    elapsed Time,
                    precise_elapsed Time64(3)
                )
                ENGINE = MergeTree
                ORDER BY day
                """);

            // UTC and Local DateTime values name an instant. Unspecified values use the column timezone.
            var instant = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc);
            await client.InsertAsync(
                $"INSERT INTO {TableName} " +
                "(day, old_day, captured_at, precise_at, elapsed, precise_elapsed) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("day", new[] { new DateOnly(2026, 6, 1) }),
                    ClickHouseTcpColumn.Create("old_day", new[] { new DateOnly(1920, 3, 4) }),
                    ClickHouseTcpColumn.Create("captured_at", new[] { instant }),
                    ClickHouseTcpColumn.Create(
                        "precise_at",
                        new[] { instant.AddMilliseconds(123) }),

                    // Time and Time64 surface as TimeSpan, including values longer than one day.
                    ClickHouseTcpColumn.Create("elapsed", new[] { TimeSpan.FromHours(27) }),
                    ClickHouseTcpColumn.Create(
                        "precise_elapsed",
                        new[] { TimeSpan.FromMilliseconds(1234) }),
                });

            await foreach (Block block in client.StreamAsync($"SELECT * FROM {TableName}"))
            {
                Console.WriteLine($"day: {block.Column<DateOnly>("day")[0]:yyyy-MM-dd}");
                Console.WriteLine($"old_day: {block.Column<DateOnly>("old_day")[0]:yyyy-MM-dd}");

                // IDateTimeColumn converts raw epoch counts with the column's timezone and scale.
                PrintTimestamp((IDateTimeColumn)block["captured_at"]);
                PrintTimestamp((IDateTimeColumn)block["precise_at"]);
                PrintTime((ITimeColumn)block["elapsed"]);
                PrintTime((ITimeColumn)block["precise_elapsed"]);

                // ReadAs converts a whole column to a reading its type offers, applying the same zone and scale.
                IColumn<DateTimeOffset> captured = block.ReadAs<DateTimeOffset>("captured_at");
                Console.WriteLine($"captured_at as DateTimeOffset: {captured[0]:O}");
            }

            var utc = new ClickHouseTcpQueryOptions
            {
                Settings = new Dictionary<string, string> { ["session_timezone"] = "UTC" },
            };
            var tokyo = new ClickHouseTcpQueryOptions
            {
                Settings = new Dictionary<string, string> { ["session_timezone"] = "Asia/Tokyo" },
            };

            // A DateTime type with no zone uses session_timezone, then the server's default timezone.
            Console.WriteLine("A DateTime without a declared zone uses session_timezone:");
            await PrintBareTimestamp(client, utc);
            await PrintBareTimestamp(client, tokyo);
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        }
    }

    private static void PrintTimestamp(IDateTimeColumn column)
    {
        DateTimeOffset value = column.GetDateTimeOffset(0);
        Console.WriteLine(
            $"{column.Name}: {value.ToString("O", CultureInfo.InvariantCulture)} " +
            $"(zone {column.TimeZone.Id}, scale {column.Scale})");
    }

    private static void PrintTime(ITimeColumn column)
        => Console.WriteLine($"{column.Name}: {column.GetTimeSpan(0)} (scale {column.Scale})");

    private static async Task PrintBareTimestamp(
        ClickHouseTcpClient client,
        ClickHouseTcpQueryOptions options)
    {
        await foreach (Block block in client.StreamAsync("SELECT toDateTime(0) AS value", options))
        {
            var column = (IDateTimeColumn)block["value"];
            Console.WriteLine($"  {column.TimeZone.Id}: {column.GetDateTimeOffset(0):O}");
        }
    }
}
