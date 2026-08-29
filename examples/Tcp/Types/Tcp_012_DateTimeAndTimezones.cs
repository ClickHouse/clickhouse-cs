using System.Globalization;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// The six date and time types — <c>Date</c>, <c>Date32</c>, <c>DateTime</c>, <c>DateTime64(scale)</c>,
/// <c>Time</c>, <c>Time64(scale)</c> — and the timezone model that decides what a value means.
///
/// <para>
/// Three facts carry the whole subject:
/// </para>
/// <list type="number">
/// <item><description>
/// A <c>DateTime</c> or <c>DateTime64</c> stores an <b>instant</b>: a count of seconds (or of
/// <c>10^-scale</c> seconds) since the Unix epoch, in UTC. A timezone in the type string changes no stored
/// byte. It decides only how that count is <i>presented</i>, and how a wall-clock value is turned into it.
/// </description></item>
/// <item><description>
/// When the type string names no timezone, the presentation timezone comes from the <c>session_timezone</c>
/// query setting, falling back to the server's own timezone. Section 3 measures it.
/// </description></item>
/// <item><description>
/// A <c>DateTime</c> whose <c>Kind</c> is <c>Utc</c> or <c>Local</c>, and any <c>DateTimeOffset</c>, names an
/// instant. On an insert that is lossless, because the target column's timezone is known. As a query
/// <i>parameter</i> it is refused, because a parameter travels as text with no timezone attached. Section 6.
/// </description></item>
/// </list>
///
/// <para>
/// <c>Tcp_006</c> covers the block-tier mechanics of <c>IDateTimeColumn</c> and <c>ITimeColumn</c>;
/// <c>Tcp_007</c> demonstrates the parameter refusal. This example is about what the values mean.
/// </para>
/// </summary>
public static class TcpDateTimeAndTimezones
{
    private const string TableName = "example_tcp_datetime_timezones";
    private const string KindTable = "example_tcp_datetime_kinds";

    // 2026-06-01 12:00:00 UTC. Europe/Amsterdam is +02:00 that day, Asia/Tokyo +09:00.
    private const long NoonUtcSeconds = 1780315200;

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();

        try
        {
            await Seed(client);
            await SixTypes(client);
            await WhatTheWireCarries(client);
            await WhereThePresentationTimezoneComesFrom(client);
            await Scale(client);
            await KindOnTheWritePath(client);
            await KindOnTheParameterPath(client);
            await TimeIsNotATimeOfDay(client);
            WhatToRemember();
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {KindTable}");
            Console.WriteLine("\nDropped every table this example created.");
        }
    }

    private static async Task Seed(ClickHouseTcpClient client)
    {
        await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        await client.ExecuteAsync($@"
            CREATE TABLE {TableName}
            (
                d Date,
                d32 Date32,
                dt DateTime,
                dt_tz DateTime('Europe/Amsterdam'),
                dt64 DateTime64(3),
                dt64_tz DateTime64(9, 'Asia/Tokyo'),
                t Time,
                t64 Time64(3)
            )
            ENGINE = MergeTree()
            ORDER BY d");

        await client.InsertAsync(
            $"INSERT INTO {TableName} (d, d32, dt, dt_tz, dt64, dt64_tz, t, t64) VALUES",
            new IColumn[]
            {
                // A Date is a day number, so it takes a DateOnly and nothing else — not a DateTime.
                ClickHouseTcpColumn.Create("d", new[] { new DateOnly(2026, 6, 1) }),
                ClickHouseTcpColumn.Create("d32", new[] { new DateOnly(1920, 3, 4) }),

                // Kind=Utc names the instant, which every one of these four columns then stores exactly.
                ClickHouseTcpColumn.Create("dt", new[] { DateTime.UnixEpoch.AddSeconds(NoonUtcSeconds) }),
                ClickHouseTcpColumn.Create("dt_tz", new[] { DateTime.UnixEpoch.AddSeconds(NoonUtcSeconds) }),
                ClickHouseTcpColumn.Create("dt64", new[] { DateTime.UnixEpoch.AddSeconds(NoonUtcSeconds).AddMilliseconds(123) }),

                // A DateTime cannot hold nanoseconds, so the raw count goes in directly. Every one of these
                // columns also accepts the integer the wire carries.
                ClickHouseTcpColumn.Create("dt64_tz", new[] { (NoonUtcSeconds * 1_000_000_000L) + 123456789L }),

                // A Time is a count from midnight, so it takes a TimeSpan, which can also be negative or
                // longer than a day. A TimeOnly cannot express either and is not accepted.
                ClickHouseTcpColumn.Create("t", new[] { new TimeSpan(12, 34, 56) }),
                ClickHouseTcpColumn.Create("t64", new[] { new TimeSpan(0, 12, 34, 56, 789) }),
            });

        ClickHouseTcpServerInfo server = await client.GetServerInfoAsync();
        Console.WriteLine($"Server {server.Version}, handshake timezone '{server.Timezone}'.");
        Console.WriteLine($"Seeded '{TableName}' with one row of each of the six types.");
    }

    private static async Task SixTypes(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n1. The six types, what they store, and what reads them\n");
        Console.WriteLine("   ClickHouse type               IColumn<T>  The raw count        Extra interface");
        Console.WriteLine("   ----------------------------  ----------  -------------------  ---------------");

        await foreach (Block block in client.StreamAsync($"SELECT * FROM {TableName}"))
        {
            foreach (IColumn column in block.Columns)
            {
                Console.WriteLine(
                    $"   {column.TypeName,-28}  {Describe(column.ElementType),-10}  {Raw(column.GetValue(0)),-19}  {Extra(column)}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   Date and Date32 are the two that already read as a calendar type: a day number needs no");
        Console.WriteLine("   timezone and no scale, so DateOnly loses nothing and there is no interface to add. The");
        Console.WriteLine("   other four read as the integer the wire carried, and the interface converts it.");
    }

    private static async Task WhatTheWireCarries(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n2. What each count counts\n");

        await foreach (Block block in client.StreamAsync($"SELECT * FROM {TableName}"))
        {
            var dt = (IDateTimeColumn)block["dt"];
            var dt64 = (IDateTimeColumn)block["dt64_tz"];
            var t = (ITimeColumn)block["t"];

            Console.WriteLine($"   Date            {Raw(block.Column<DateOnly>("d")[0]),-19}  days since 1970-01-01, unsigned 16-bit");
            Console.WriteLine($"   Date32          {Raw(block.Column<DateOnly>("d32")[0]),-19}  days since 1970-01-01, signed 32-bit, so it reaches before the epoch");
            Console.WriteLine($"   DateTime        {block.Column<uint>("dt")[0],-19}  seconds since the epoch, UTC");
            Console.WriteLine($"   DateTime64(9)   {block.Column<long>("dt64_tz")[0],-19}  nanoseconds since the epoch, UTC");
            Console.WriteLine($"   Time            {block.Column<int>("t")[0],-19}  seconds from midnight, signed");
            Console.WriteLine($"   Time64(3)       {block.Column<long>("t64")[0],-19}  milliseconds from midnight, signed");

            Console.WriteLine();
            Console.WriteLine("   For the two DateTime families that count is a UTC instant. The timezone the column");
            Console.WriteLine("   declares changes no stored byte, only the reading:");
            Console.WriteLine($"     dt      {block["dt"].TypeName,-30} count {block.Column<uint>("dt")[0]}  -> {Format(dt.GetDateTimeOffset(0))}");
            Console.WriteLine($"     dt_tz   {block["dt_tz"].TypeName,-30} count {block.Column<uint>("dt_tz")[0]}  -> {Format(((IDateTimeColumn)block["dt_tz"]).GetDateTimeOffset(0))}");
            Console.WriteLine("     Same count, different offset. One instant, two presentations.");

            Console.WriteLine();
            Console.WriteLine("   A Time carries no timezone at all, because it is not an instant:");
            Console.WriteLine($"     t       {block["t"].TypeName,-30} Scale {t.Scale}, GetTimeSpan(0) {t.GetTimeSpan(0)}");
            Console.WriteLine($"     dt64_tz {block["dt64_tz"].TypeName,-30} Scale {dt64.Scale}, TimeZone {dt64.TimeZone.Id}");
        }
    }

    private static async Task WhereThePresentationTimezoneComesFrom(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n3. Where the presentation timezone comes from\n");
        Console.WriteLine("   Measured, not assumed. The same query runs once per session_timezone over one fixed");
        Console.WriteLine("   instant, with a bare DateTime and a DateTime('Europe/Amsterdam') side by side:\n");
        Console.WriteLine("   session_timezone      DateTime count  bare presented as               declared presented as");
        Console.WriteLine("   --------------------  --------------  ------------------------------  ------------------------------");

        string sql = $@"SELECT toDateTime({NoonUtcSeconds}) AS bare,
                               toDateTime({NoonUtcSeconds}, 'Europe/Amsterdam') AS declared";

        foreach (string zone in new[] { string.Empty, "UTC", "Europe/Amsterdam", "Asia/Tokyo", "America/Los_Angeles" })
        {
            ClickHouseTcpQueryOptions? options = zone.Length == 0
                ? null
                : new ClickHouseTcpQueryOptions { Settings = new Dictionary<string, string> { ["session_timezone"] = zone } };

            await foreach (Block block in client.StreamAsync(sql, options))
            {
                var bare = (IDateTimeColumn)block["bare"];
                var declared = (IDateTimeColumn)block["declared"];
                Console.WriteLine(
                    $"   {(zone.Length == 0 ? "(not set)" : zone),-20}  {block.Column<uint>("bare")[0],-14}  {Format(bare.GetDateTimeOffset(0)),-30}  {Format(declared.GetDateTimeOffset(0))}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   What that shows:");
        Console.WriteLine("     The stored count never moves. It is the same instant in every row.");
        Console.WriteLine("     A bare DateTime is presented in the session timezone, and IDateTimeColumn.TimeZone");
        Console.WriteLine("       reports that zone.");
        Console.WriteLine("     A DateTime('Europe/Amsterdam') ignores the setting entirely. The type string wins.");
        Console.WriteLine("     With the setting unset, the presentation zone is the server's own — the one the");
        Console.WriteLine("       handshake reported, printed at the top of this example.");
        Console.WriteLine();
        Console.WriteLine("   So the presentation timezone is: the type string's, or else session_timezone, or else the");
        Console.WriteLine("   server's. The server also sends a TimezoneUpdate packet on the wire; it is not what the");
        Console.WriteLine("   client resolves a bare column against.");
        Console.WriteLine();
        Console.WriteLine("   The practical consequence: declare the timezone on any column you care about. A bare");
        Console.WriteLine("   DateTime read by two callers with different session settings gives two different");
        Console.WriteLine("   DateTimeOffsets — correctly, since they are the same instant, but a DateTime with");
        Console.WriteLine("   Kind=Unspecified taken from one of them is not comparable with the other's.");
    }

    private static async Task Scale(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n4. Scale: DateTime64(0..9), and where .NET stops\n");
        Console.WriteLine("   The scale is how many decimal digits of a second the count carries. A .NET tick is");
        Console.WriteLine("   100 ns, which is scale 7, so scales 8 and 9 hold digits DateTimeOffset cannot:\n");
        Console.WriteLine("   Type                  Raw count              GetDateTimeOffset(0)");
        Console.WriteLine("   --------------------  ---------------------  ------------------------------");

        await foreach (Block block in client.StreamAsync(
            @"SELECT toDateTime64('2026-06-01 12:00:00.123456789', 0, 'UTC') AS s0,
                     toDateTime64('2026-06-01 12:00:00.123456789', 3, 'UTC') AS s3,
                     toDateTime64('2026-06-01 12:00:00.123456789', 6, 'UTC') AS s6,
                     toDateTime64('2026-06-01 12:00:00.123456789', 7, 'UTC') AS s7,
                     toDateTime64('2026-06-01 12:00:00.123456789', 9, 'UTC') AS s9"))
        {
            foreach (IColumn column in block.Columns)
            {
                var instants = (IDateTimeColumn)column;
                Console.WriteLine($"   {column.TypeName,-20}  {column.GetValue(0),-21}  {instants.GetDateTimeOffset(0):yyyy-MM-dd HH:mm:ss.fffffff}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   Scale 9's last two digits (89) are gone from the DateTimeOffset and still present in the");
        Console.WriteLine("   count. Read IColumn<long>.Values when you need them; the truncation is toward zero.");
        Console.WriteLine();
        Console.WriteLine("   Time64 has the same scale range and the same limit: ITimeColumn.GetTimeSpan truncates to");
        Console.WriteLine("   ticks, and IColumn<long> keeps the count.");
    }

    private static async Task KindOnTheWritePath(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n5. DateTime.Kind on the way in\n");
        Console.WriteLine("   A .NET DateTime is a number plus a Kind, and the Kind is what says whether the number is");
        Console.WriteLine("   an instant or a wall-clock reading. An insert honours it, because the target column's");
        Console.WriteLine("   timezone comes from the schema the server sent, so the conversion is never a guess.\n");
        Console.WriteLine($"   The host's local zone is {TimeZoneInfo.Local.Id}. Same 12:00 in each case:\n");

        var noon = new DateTime(2026, 6, 1, 12, 0, 0);
        var values = new (string What, object Value)[]
        {
            ("Kind=Utc", DateTime.SpecifyKind(noon, DateTimeKind.Utc)),
            ("Kind=Unspecified", DateTime.SpecifyKind(noon, DateTimeKind.Unspecified)),
            ("Kind=Local", DateTime.SpecifyKind(noon, DateTimeKind.Local)),
            ("DateTimeOffset +05:00", new DateTimeOffset(2026, 6, 1, 12, 0, 0, TimeSpan.FromHours(5))),
        };

        foreach (string columnType in new[] { "DateTime('UTC')", "DateTime('Europe/Amsterdam')" })
        {
            Console.WriteLine($"   Target column {columnType}:");
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {KindTable}");
            await client.ExecuteAsync($"CREATE TABLE {KindTable} (t {columnType}) ENGINE = MergeTree ORDER BY tuple()");

            foreach ((string what, object value) in values)
            {
                IColumn column = value is DateTimeOffset offset
                    ? ClickHouseTcpColumn.Create("t", new[] { offset })
                    : ClickHouseTcpColumn.Create("t", new[] { (DateTime)value });

                await client.InsertAsync($"INSERT INTO {KindTable} (t) VALUES", new[] { column });

                await foreach (Block block in client.StreamAsync($"SELECT t FROM {KindTable}"))
                {
                    var stored = (IDateTimeColumn)block["t"];
                    Console.WriteLine($"     {what,-22} -> count {block.Column<uint>("t")[0]}, presented {Format(stored.GetDateTimeOffset(0))}");
                }

                await client.ExecuteAsync($"TRUNCATE TABLE {KindTable}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   Reading those two blocks together:");
        Console.WriteLine("     Kind=Utc and DateTimeOffset name an instant, so the count is the same whichever column");
        Console.WriteLine("       they go into. Lossless.");
        Console.WriteLine("     Kind=Unspecified is a wall clock, read in the COLUMN's timezone — the count differs");
        Console.WriteLine("       between the two targets by the offset. Lossless, and it means what you want when the");
        Console.WriteLine("       value came from a source that had no timezone.");
        Console.WriteLine("     Kind=Local is a wall clock read in the HOST's timezone, not the column's. Correct, and");
        Console.WriteLine("       it makes the stored value depend on where your process runs. Prefer Utc or");
        Console.WriteLine("       Unspecified in anything that is deployed more than once.");
        Console.WriteLine();
        Console.WriteLine("   A read produces Kind=Utc for a zero-offset column and Kind=Unspecified otherwise, so");
        Console.WriteLine("   an insert of a read value is lossless only if the two columns share a timezone. Take");
        Console.WriteLine("   DateTimeOffset from IDateTimeColumn.GetDateTimeOffset instead, which is unambiguous.");
    }

    private static async Task KindOnTheParameterPath(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n6. The same value as a query parameter: an instant needs a declared timezone\n");
        Console.WriteLine("   A parameter does not travel as a count. It travels as text in the Query packet's");
        Console.WriteLine("   settings list, and the text carries no timezone, so the server reads it in whatever");
        Console.WriteLine("   session_timezone is in force — which section 3 just showed is not something the client");
        Console.WriteLine("   controls. An instant would therefore move silently, so it is refused:\n");

        var noonUtc = DateTime.SpecifyKind(new DateTime(2026, 6, 1, 12, 0, 0), DateTimeKind.Utc);

        try
        {
            await client.ExecuteScalarAsync(
                "SELECT {t:DateTime}",
                new ClickHouseTcpQueryOptions { Parameters = new ClickHouseTcpParameterCollection { { "t", noonUtc } } });
            Console.WriteLine("     accepted, which this example did not expect");
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine($"     {{t:DateTime}} with Kind=Utc:");
            Console.WriteLine($"       {Wrap(ex.Message.Split(" (Parameter")[0])}");
        }

        Console.WriteLine();
        Console.WriteLine("   Declaring the timezone in the placeholder makes it lossless, because the client can");
        Console.WriteLine("   then move the instant into that zone before writing the text:");

        foreach ((string placeholder, object value, string note) in new (string, object, string)[]
        {
            ("{t:DateTime('UTC')}", noonUtc, "Kind=Utc, declared UTC"),
            ("{t:DateTime('Asia/Tokyo')}", noonUtc, "Kind=Utc, declared Tokyo — same instant, +09:00 wall clock"),
            ("{t:DateTime('UTC')}", new DateTimeOffset(2026, 6, 1, 17, 0, 0, TimeSpan.FromHours(5)), "DateTimeOffset +05:00 — the same instant again"),
            ("{t:DateTime}", DateTime.SpecifyKind(new DateTime(2026, 6, 1, 12, 0, 0), DateTimeKind.Unspecified), "Kind=Unspecified — a wall clock, so no timezone is needed"),
        })
        {
            // session_timezone is pinned, because the last case below is read in it and a server left on its
            // own default would make this comparison say something different on every machine.
            object? epoch = await client.ExecuteScalarAsync(
                $"SELECT toUnixTimestamp(toDateTime({placeholder}, 'UTC'))",
                new ClickHouseTcpQueryOptions
                {
                    Parameters = new ClickHouseTcpParameterCollection { { "t", value } },
                    Settings = new Dictionary<string, string> { ["session_timezone"] = "UTC" },
                });
            Console.WriteLine($"     {placeholder,-27} -> {epoch}   {note}");
        }

        Console.WriteLine();
        Console.WriteLine("   The last row is the one to notice: with Kind=Unspecified the count is whatever the");
        Console.WriteLine("   session timezone makes of 12:00, so it agrees with the others only because these queries");
        Console.WriteLine("   set session_timezone=UTC. Without that it follows the server, and the same value means a");
        Console.WriteLine("   different instant on a differently configured one. That is the ambiguity the refusal");
        Console.WriteLine("   above protects an instant from.");
        Console.WriteLine();
        Console.WriteLine("   Same rule for DateTime64: {t:DateTime64(3, 'UTC')} declares one, {t:DateTime64(3)} does");
        Console.WriteLine("   not. Date, Date32, Time and Time64 have no timezone to declare, so none of this applies");
        Console.WriteLine("   to them.");
    }

    private static async Task TimeIsNotATimeOfDay(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n7. Time is a duration from midnight, not a time of day\n");
        Console.WriteLine("   The count is signed and is not reduced modulo a day, so a Time holds values no clock");
        Console.WriteLine("   face has. That is why the CLR type is TimeSpan and not TimeOnly:\n");
        Console.WriteLine("   Literal           Raw count   GetTimeSpan(0)");
        Console.WriteLine("   ----------------  ----------  --------------");

        await foreach (Block block in client.StreamAsync(
            @"SELECT CAST('12:34:56', 'Time') AS ordinary,
                     CAST('-01:30:00', 'Time') AS negative,
                     CAST('999:00:00', 'Time') AS past_a_day,
                     CAST('12:34:56.789', 'Time64(3)') AS with_millis"))
        {
            foreach (IColumn column in block.Columns)
            {
                var times = (ITimeColumn)column;
                Console.WriteLine($"   {column.Name,-16}  {column.GetValue(0),-10}  {times.GetTimeSpan(0)}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   A TimeOnly is refused on an insert for the same reason — it can express neither:");

        await client.ExecuteAsync($"DROP TABLE IF EXISTS {KindTable}");
        await client.ExecuteAsync($"CREATE TABLE {KindTable} (t Time) ENGINE = MergeTree ORDER BY tuple()");

        try
        {
            await client.InsertAsync(
                $"INSERT INTO {KindTable} (t) VALUES",
                new[] { ClickHouseTcpColumn.Create("t", new[] { new TimeOnly(12, 34, 56) }) });
            Console.WriteLine("     accepted, which this example did not expect");
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine($"     {Wrap(ex.Message.Split(" (Parameter")[0])}");
        }

        Console.WriteLine();
        Console.WriteLine("   Pass a TimeSpan, or the raw count as an int (Time) or a long (Time64).");
    }

    private static void WhatToRemember()
    {
        Console.WriteLine("\n8. What to remember\n");
        Console.WriteLine("   Declare the timezone on a DateTime or DateTime64 column you care about. Without one the");
        Console.WriteLine("     reading depends on session_timezone, which is set per query and not by you.");
        Console.WriteLine("   Read instants through IDateTimeColumn.GetDateTimeOffset, not through a DateTime. An");
        Console.WriteLine("     offset is never ambiguous; a DateTime's Kind is Unspecified for any non-UTC column.");
        Console.WriteLine("   Write Kind=Utc or a DateTimeOffset for an instant, Kind=Unspecified for a wall clock.");
        Console.WriteLine("     Avoid Kind=Local unless the host's zone really is part of the value's meaning.");
        Console.WriteLine("   A parameter that names an instant needs {t:DateTime('Zone')}. An insert does not, and");
        Console.WriteLine("     that difference is not a bug: only one of the two carries the column's timezone.");
        Console.WriteLine("   Date and Date32 are DateOnly, Time and Time64 are TimeSpan, and none of the four has a");
        Console.WriteLine("     timezone at all.");
        Console.WriteLine("   Keep the raw count when the scale is 8 or 9, or when the precision matters more than the");
        Console.WriteLine("     calendar type: it is what the wire carried and it truncates nothing.");
    }

    private static string Describe(Type type) => type switch
    {
        _ when type == typeof(uint) => "uint",
        _ when type == typeof(int) => "int",
        _ when type == typeof(long) => "long",
        _ => type.Name,
    };

    private static string Extra(IColumn column) => column switch
    {
        IDateTimeColumn instants => $"IDateTimeColumn (TimeZone {instants.TimeZone.Id}, Scale {instants.Scale})",
        ITimeColumn times => $"ITimeColumn (Scale {times.Scale}, no timezone)",
        _ => "- (already a calendar type)",
    };

    private static string Format(DateTimeOffset value)
        => value.ToString("yyyy-MM-dd HH:mm:ss.fff zzz", CultureInfo.InvariantCulture);

    // The boxed wire value, rendered culture-invariantly so the output does not depend on the host.
    private static string Raw(object? value) => value switch
    {
        DateOnly day => day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        null => "NULL",
        _ => value.ToString() ?? "NULL",
    };

    // Reflows a long driver message so the console output stays readable.
    private static string Wrap(string message)
    {
        var lines = new List<string>();
        var line = new System.Text.StringBuilder();
        foreach (string word in message.Split(' '))
        {
            if (line.Length + word.Length + 1 > 90)
            {
                lines.Add(line.ToString());
                line.Clear();
            }

            line.Append(line.Length == 0 ? word : " " + word);
        }

        lines.Add(line.ToString());
        return string.Join("\n       ", lines);
    }
}
