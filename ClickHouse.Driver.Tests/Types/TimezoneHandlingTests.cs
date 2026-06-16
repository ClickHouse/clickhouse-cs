using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;
using NodaTime;
using NUnit.Framework;
#pragma warning disable CS0618 // Type or member is obsolete

namespace ClickHouse.Driver.Tests.Types;

[TestFixture]
public class ServerTimezoneDateTimeTests : IDisposable
{
    protected readonly ClickHouseConnection connection;

    public ServerTimezoneDateTimeTests()
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        connection = new ClickHouseConnection(builder.ToString());
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "CREATE DATABASE IF NOT EXISTS test;";
        command.ExecuteScalar();
    }

    public void Dispose() => connection?.Dispose();

    [Test]
    public async Task ShouldRoundtripUnspecifiedDateTime()
    {
        var dt = new DateTime(2022, 06, 13, 02, 00, 00, DateTimeKind.Unspecified);
        var query = $"SELECT parseDateTimeBestEffort('{dt.ToString("s", CultureInfo.InvariantCulture)}')";
        Assert.That(await connection.ExecuteScalarAsync(query), Is.EqualTo(dt));
    }

    [Test]
    public async Task ShouldReturnUTCDateTime()
    {
        var query = $"select toDateTime('2020/11/10 00:00:00', 'Etc/UTC')";
        Assert.That(await connection.ExecuteScalarAsync(query), Is.EqualTo(new DateTime(2020, 11, 10, 00, 00, 00, DateTimeKind.Utc)));
    }
}

/// <summary>
/// Tests for reading DateTime values from ClickHouse.
/// Verifies that DateTime.Kind is correctly set based on column timezone.
/// </summary>
[TestFixture]
public class ReadDateTimeTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task ReadDateTime_FromUtcColumn_ReturnsDateTimeKindUtc()
    {
        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT toDateTime('2024-01-15 12:30:45', 'UTC')");

        Assert.That(result.Kind, Is.EqualTo(DateTimeKind.Utc), "DateTime from UTC column should have Kind=Utc");
        Assert.That(result, Is.EqualTo(new DateTime(2024, 1, 15, 12, 30, 45, DateTimeKind.Utc)));
    }

    [Test]
    public async Task ReadDateTime_FromEtcUtcColumn_ReturnsDateTimeKindUtc()
    {
        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT toDateTime('2024-01-15 12:30:45', 'Etc/UTC')");

        Assert.That(result.Kind, Is.EqualTo(DateTimeKind.Utc), "DateTime from Etc/UTC column should have Kind=Utc");
        Assert.That(result, Is.EqualTo(new DateTime(2024, 1, 15, 12, 30, 45, DateTimeKind.Utc)));
    }

    [Test]
    public async Task ReadDateTime_FromNonUtcColumn_ReturnsDateTimeKindUnspecified()
    {
        // Europe/Amsterdam is UTC+1 in January (CET)
        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT toDateTime('2024-01-15 12:30:45', 'Europe/Amsterdam')");

        Assert.That(result.Kind, Is.EqualTo(DateTimeKind.Unspecified), "DateTime from non-UTC column should have Kind=Unspecified");
        // The value should be the local time in that timezone (12:30:45 Amsterdam time)
        Assert.That(result, Is.EqualTo(new DateTime(2024, 1, 15, 12, 30, 45, DateTimeKind.Unspecified)));
    }
    
    [Test]
    public async Task ReadDateTimeOffset_FromNonUtcColumn_ReturnsDateTimeWithCorrectOffset()
    {
        // Europe/Amsterdam is UTC+1 in January (CET)
        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT toDateTime('2024-01-15 12:30:45', 'Europe/Amsterdam')");
        reader.AssertHasFieldCount(1);
        ClassicAssert.IsTrue(reader.Read());
        var dto = reader.GetDateTimeOffset(0);
        Assert.That(dto.Offset.TotalHours, Is.EqualTo(1)); // Expected GMT+1
    }

    [Test]
    public async Task ReadDateTime64_FromUtcColumn_ReturnsDateTimeKindUtc()
    {
        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT toDateTime64('2024-01-15 12:30:45.123456', 6, 'UTC')");

        Assert.That(result.Kind, Is.EqualTo(DateTimeKind.Utc), "DateTime64 from UTC column should have Kind=Utc");
        Assert.That(result.Year, Is.EqualTo(2024));
        Assert.That(result.Month, Is.EqualTo(1));
        Assert.That(result.Day, Is.EqualTo(15));
        Assert.That(result.Hour, Is.EqualTo(12));
        Assert.That(result.Minute, Is.EqualTo(30));
        Assert.That(result.Second, Is.EqualTo(45));
    }

    [Test]
    public async Task ReadDateTime64_FromNonUtcColumn_ReturnsDateTimeKindUnspecified()
    {
        // Use Europe/Amsterdam which has non-zero offset (UTC+1 in January)
        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT toDateTime64('2024-01-15 12:30:45.123456', 6, 'Europe/Amsterdam')");

        Assert.That(result.Kind, Is.EqualTo(DateTimeKind.Unspecified), "DateTime64 from non-UTC column should have Kind=Unspecified");
    }
    
    [Test]
    public async Task ReadDateTimeOffset_FromNonUtcDateTime64Column_ReturnsDateTimeWithCorrectOffset()
    {
        // Europe/Amsterdam is UTC+1 in January (CET)
        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT toDateTime64('2024-01-15 12:30:45', 3, 'Europe/Amsterdam')");
        reader.AssertHasFieldCount(1);
        ClassicAssert.IsTrue(reader.Read());
        var dto = reader.GetDateTimeOffset(0);
        Assert.That(dto.Offset.TotalHours, Is.EqualTo(1)); // Expected GMT+1
    }

    [Test]
    public async Task ReadDateTime_FromColumnWithoutTimezone_ReturnsUnspecified()
    {
        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT toDateTime('2024-01-15 12:30:45')");
        reader.AssertHasFieldCount(1);
        Assert.That(reader.Read(), Is.True);
        var dt = reader.GetDateTime(0);

        // When no timezone is specified in the column, the DateTime is returned as Unspecified
        // This preserves the wall-clock time without making assumptions about timezone
        Assert.That(dt.Kind, Is.EqualTo(DateTimeKind.Unspecified));
        Assert.That(dt.Year, Is.EqualTo(2024));
        Assert.That(dt.Month, Is.EqualTo(1));
        Assert.That(dt.Day, Is.EqualTo(15));
        Assert.That(dt.Hour, Is.EqualTo(12));
        Assert.That(dt.Minute, Is.EqualTo(30));
        Assert.That(dt.Second, Is.EqualTo(45));
    }

    [Test]
    public async Task ReadDateTimeOffset_FromColumnWithoutTimezone_HasZeroOffset()
    {
        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT toDateTime('2024-01-15 12:30:45')");
        reader.AssertHasFieldCount(1);
        Assert.That(reader.Read(), Is.True);
        var dto = reader.GetDateTimeOffset(0);
        
        Assert.That(dto.Offset, Is.EqualTo(TimeSpan.Zero));
        Assert.That(dto.Year, Is.EqualTo(2024));
        Assert.That(dto.Month, Is.EqualTo(1));
        Assert.That(dto.Day, Is.EqualTo(15));
        Assert.That(dto.Hour, Is.EqualTo(12));
        Assert.That(dto.Minute, Is.EqualTo(30));
        Assert.That(dto.Second, Is.EqualTo(45));
    }
}

/// <summary>
/// Tests for writing DateTime values via HTTP parameters (command.AddParameter).
/// This path uses HttpParameterFormatter to format DateTime as strings.
/// Parameterized by the session_timezone setting: running against both UTC and a
/// non-UTC server timezone surfaces the parameter-path behavior reported in #350,
/// where bare {name:DateTime} hints let ClickHouse interpret the wire string in
/// session_timezone rather than UTC.
/// </summary>
[TestFixture("UTC")]
[TestFixture("Europe/Amsterdam")]
public class WriteDateTimeHttpParamTests : IDisposable
{
    protected readonly ClickHouseConnection connection;
    protected readonly ClickHouseClient client;
    private readonly string tableName;
    private readonly string sessionTimezone;

    public WriteDateTimeHttpParamTests(string sessionTimezone)
    {
        this.sessionTimezone = sessionTimezone;
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings.CustomSettings["session_timezone"] = sessionTimezone;
        client = new ClickHouseClient(settings);
        connection = client.CreateConnection();
        client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test;").GetAwaiter().GetResult();
        tableName = $"test.datetime_http_test_{sessionTimezone.Replace('/', '_').Replace('-', '_')}";
    }

    [OneTimeTearDown]
    public void Dispose()
    {
        connection?.Dispose();
        client?.Dispose();
    }

    [SetUp]
    public async Task SetUp()
    {
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE {tableName} (
                dt_utc DateTime('UTC'),
                dt_amsterdam DateTime('Europe/Amsterdam'),
                dt_no_tz DateTime
            ) ENGINE = Memory");
    }

    [TearDown]
    public async Task TearDown()
    {
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    [Test]
    [TestCase("dt_utc", "UTC")]
    [TestCase("dt_amsterdam", "Europe/Amsterdam")]
    public async Task HttpParam_UnspecifiedKind_WithMatchingHint_RoundtripsWallClock(string col, string columnTz)
    {
        // With a hint matching the column tz, the wall-clock roundtrips. The Kind of the
        // returned DateTime depends on the column tz (covered in ReadDateTimeTests).
        var original = new DateTime(2024, 6, 15, 14, 30, 0, DateTimeKind.Unspecified);

        var command = connection.CreateCommand();
        command.AddParameter("dt", original);
        command.CommandText = $"INSERT INTO {tableName} ({col}) VALUES ({{dt:DateTime('{columnTz}')}})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync($"SELECT {col} FROM {tableName}");
        Assert.That(result.Ticks, Is.EqualTo(original.Ticks));
    }

    [Test]
    public async Task HttpParam_UnspecifiedKind_ToAmsterdamColumn_ReturnsCorrectOffset()
    {
        // June 15 - Amsterdam is UTC+2 (CEST - summer time)
        var original = new DateTime(2024, 6, 15, 14, 30, 0, DateTimeKind.Unspecified);

        var command = connection.CreateCommand();
        command.AddParameter("dt", original);
        // NB! The timezone must be specified in the parameter type, otherwise it's assumed to be UTC
        command.CommandText = $"INSERT INTO {tableName} (dt_amsterdam) VALUES ({{dt:DateTime('Europe/Amsterdam')}})";
        await command.ExecuteNonQueryAsync();

        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync($"SELECT dt_amsterdam FROM {tableName}");
        Assert.That(reader.Read(), Is.True);
        var dto = reader.GetDateTimeOffset(0);

        // Should have Amsterdam's summer offset (UTC+2)
        Assert.That(dto.Offset.TotalHours, Is.EqualTo(2), "Should have Amsterdam summer time offset (CEST)");
        Assert.That(dto.DateTime, Is.EqualTo(original));
    }

    [Test]
    public async Task HttpParam_UnspecifiedKind_WithBareHint_FootGun_InterpretsInSessionTimezone()
    {
        // Foot-gun documentation: when the explicit param hint omits a timezone (bare {dt:DateTime}),
        // ClickHouse parses the wall-clock string in session_timezone, not the column's timezone.
        // The result therefore depends on the session/server timezone the test is running under,
        // making bare-hint Unspecified inserts non-portable across deployments.
        var original = new DateTime(2024, 6, 15, 14, 30, 0, DateTimeKind.Unspecified);

        var command = connection.CreateCommand();
        command.AddParameter("dt", original);
        command.CommandText = $"INSERT INTO {tableName} (dt_amsterdam) VALUES ({{dt:DateTime}})";
        await command.ExecuteNonQueryAsync();

        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync($"SELECT dt_amsterdam FROM {tableName}");
        Assert.That(reader.Read(), Is.True);
        var dto = reader.GetDateTimeOffset(0);

        // Compute the expected instant by interpreting the wire wall-clock in the fixture's session_timezone.
        var sessionTz = DateTimeZoneProviders.Tzdb.GetZoneOrNull(sessionTimezone);
        var serverInstant = sessionTz.AtLeniently(LocalDateTime.FromDateTime(original)).ToInstant();
        var amsterdamTz = DateTimeZoneProviders.Tzdb.GetZoneOrNull("Europe/Amsterdam");
        var expectedInAmsterdam = serverInstant.InZone(amsterdamTz).ToDateTimeOffset();

        Assert.That(dto, Is.EqualTo(expectedInAmsterdam),
            "Bare {dt:DateTime} hint causes server to interpret the wall-clock in session_timezone");
    }

    [Test]
    [TestCase("dt_utc", "UTC")]
    [TestCase("dt_amsterdam", "Europe/Amsterdam")]
    public async Task HttpParam_UtcKind_WithMatchingHint_PreservesInstant(string col, string columnTz)
    {
        var utcDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);
        var expected = ((DateTimeOffset)utcDt).ToUnixTimeSeconds();

        var command = connection.CreateCommand();
        command.AddParameter("dt", utcDt);
        command.CommandText = $"INSERT INTO {tableName} ({col}) VALUES ({{dt:DateTime('{columnTz}')}})";
        await command.ExecuteNonQueryAsync();

        var unix = Convert.ToInt64(await connection.ExecuteScalarAsync($"SELECT toUnixTimestamp({col}) FROM {tableName}"));
        Assert.That(unix, Is.EqualTo(expected));
    }

    [Test]
    [TestCase("dt_utc", "UTC")]
    [TestCase("dt_amsterdam", "Europe/Amsterdam")]
    public async Task HttpParam_LocalKind_WithMatchingHint_PreservesInstant(string col, string columnTz)
    {
        var localDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Local);
        var expected = new DateTimeOffset(localDt).ToUnixTimeSeconds();

        var command = connection.CreateCommand();
        command.AddParameter("dt", localDt);
        command.CommandText = $"INSERT INTO {tableName} ({col}) VALUES ({{dt:DateTime('{columnTz}')}})";
        await command.ExecuteNonQueryAsync();

        var unix = Convert.ToInt64(await connection.ExecuteScalarAsync($"SELECT toUnixTimestamp({col}) FROM {tableName}"));
        Assert.That(unix, Is.EqualTo(expected));
    }

    [Test]
    [TestCase(DateTimeKind.Unspecified)]
    [TestCase(DateTimeKind.Utc)]
    [TestCase(DateTimeKind.Local)]
    public async Task HttpParam_AnyKind_ToNoTimezoneColumn_WithUtcHint_PreservesUtcWallClock(DateTimeKind kind)
    {
        // With a UTC hint, the wall-clock the server stores is the UTC projection of the input.
        // For Unspecified/Utc that's the input itself; for Local it's the Local→UTC instant.
        var input = new DateTime(2024, 1, 15, 12, 30, 45, kind);
        var expectedUtcWallClock = kind == DateTimeKind.Local
            ? new DateTimeOffset(input).UtcDateTime
            : input;

        var command = connection.CreateCommand();
        command.AddParameter("dt", input);
        command.CommandText = $"INSERT INTO {tableName} (dt_no_tz) VALUES ({{dt:DateTime('UTC')}})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync($"SELECT dt_no_tz FROM {tableName}");
        Assert.That(result.Ticks, Is.EqualTo(expectedUtcWallClock.Ticks));
        Assert.That(result.Kind, Is.EqualTo(DateTimeKind.Unspecified));
    }

    [Test]
    [TestCase("dt_utc", "UTC")]
    [TestCase("dt_amsterdam", "Europe/Amsterdam")]
    public async Task HttpParam_DateTimeOffset_WithMatchingHint_PreservesInstant(string col, string columnTz)
    {
        var dto = new DateTimeOffset(2024, 1, 15, 15, 0, 0, TimeSpan.FromHours(3)); // 12:00 UTC
        var expected = dto.ToUnixTimeSeconds();

        var command = connection.CreateCommand();
        command.AddParameter("dt", dto);
        command.CommandText = $"INSERT INTO {tableName} ({col}) VALUES ({{dt:DateTime('{columnTz}')}})";
        await command.ExecuteNonQueryAsync();

        var unix = Convert.ToInt64(await connection.ExecuteScalarAsync($"SELECT toUnixTimestamp({col}) FROM {tableName}"));
        Assert.That(unix, Is.EqualTo(expected));
    }

    /// <summary>
    /// Regression test: DateTimeConversions.DateTimeEpochStart (1970-01-01 00:00:00 UTC) was
    /// formatted as Unix timestamp "0" which ClickHouse rejected.
    /// </summary>
    [Test]
    public async Task HttpParam_DateTimeUnixEpoch_ShouldWork()
    {
        var dt = DateTimeConversions.DateTimeEpochStart; // 1970-01-01 00:00:00 UTC

        var command = connection.CreateCommand();
        command.AddParameter("dt", dt);
        command.CommandText = $"INSERT INTO {tableName} (dt_utc) VALUES ({{dt:DateTime('UTC')}})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync($"SELECT dt_utc FROM {tableName}");
        Assert.That(result, Is.EqualTo(DateTimeConversions.DateTimeEpochStart));
    }

    /// <summary>
    /// Test that DateTimeOffset at Unix epoch also works.
    /// </summary>
    [Test]
    public async Task HttpParam_DateTimeOffsetUnixEpoch_ShouldWork()
    {
        var dto = new DateTimeOffset(DateTimeConversions.DateTimeEpochStart, TimeSpan.Zero); // 1970-01-01 00:00:00 +00:00

        var command = connection.CreateCommand();
        command.AddParameter("dt", dto);
        command.CommandText = $"INSERT INTO {tableName} (dt_utc) VALUES ({{dt:DateTime('UTC')}})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync($"SELECT dt_utc FROM {tableName}");
        Assert.That(result, Is.EqualTo(DateTimeConversions.DateTimeEpochStart));
    }
    
    /// <summary>
    /// Test 1 sec after epoch.
    /// </summary>
    [Test]
    public async Task HttpParam_DateTimeMinRange_ShouldWork()
    {
        // DateTime minimum for ClickHouse DateTime type is 1970-01-01 00:00:00
        var dt = new DateTime(1970, 1, 1, 0, 0, 1, DateTimeKind.Utc); // 1 second after epoch

        var command = connection.CreateCommand();
        command.AddParameter("dt", dt);
        command.CommandText = $"INSERT INTO {tableName} (dt_utc) VALUES ({{dt:DateTime('UTC')}})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync($"SELECT dt_utc FROM {tableName}");
        Assert.That(result, Is.EqualTo(dt));
    }
}

/// <summary>
/// Tests for writing DateTime values via HTTP parameters using ADO @-style placeholders.
/// This is the path reported in issue #350: when the parameter type is inferred (not
/// explicitly written in SQL), the driver must anchor the inferred type to UTC for
/// instant-bearing values so the server cannot mis-parse the wall-clock string in
/// session_timezone. Parameterized over three session timezones — UTC plus two non-UTC
/// zones spanning positive and negative offsets — to guard the fix across server tz.
/// </summary>
[TestFixture("UTC")]
[TestFixture("Europe/Amsterdam")]
[TestFixture("America/New_York")]
public class InferredDateTimeHttpParamTests : IDisposable
{
    protected readonly ClickHouseConnection connection;
    protected readonly ClickHouseClient client;
    private readonly string tableName;
    private readonly string sessionTimezone;

    public InferredDateTimeHttpParamTests(string sessionTimezone)
    {
        this.sessionTimezone = sessionTimezone;
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings.CustomSettings["session_timezone"] = sessionTimezone;
        client = new ClickHouseClient(settings);
        connection = client.CreateConnection();
        client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test;").GetAwaiter().GetResult();
        tableName = $"test.datetime_inferred_test_{sessionTimezone.Replace('/', '_').Replace('-', '_')}";
    }

    [OneTimeTearDown]
    public void Dispose()
    {
        connection?.Dispose();
        client?.Dispose();
    }

    [SetUp]
    public async Task SetUp()
    {
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE {tableName} (
                dt_utc DateTime('UTC'),
                dt_amsterdam DateTime('Europe/Amsterdam'),
                dt_ny DateTime('America/New_York'),
                dt_no_tz DateTime,
                arr_utc Array(DateTime('UTC')),
                arr_amsterdam Array(DateTime('Europe/Amsterdam')),
                arr_ny Array(DateTime('America/New_York'))
            ) ENGINE = Memory");
    }

    [TearDown]
    public async Task TearDown()
    {
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    private static long ToUnixSeconds(DateTime dt) => new DateTimeOffset(dt.Kind == DateTimeKind.Unspecified
        ? DateTime.SpecifyKind(dt, DateTimeKind.Utc)
        : dt).ToUnixTimeSeconds();

    private static long ToUnixSeconds(DateTimeOffset dto) => dto.ToUnixTimeSeconds();

    [Test]
    [TestCase("dt_utc")]
    [TestCase("dt_amsterdam")]
    [TestCase("dt_ny")]
    public async Task InferredHttpParam_UtcKind_ToUtcColumn_PreservesInstant(string col)
    {
        var utcDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);
        var expected = ToUnixSeconds(utcDt);

        var command = connection.CreateCommand();
        command.AddParameter("dt", utcDt);
        command.CommandText = $"INSERT INTO {tableName} ({col}) VALUES (@dt)";
        await command.ExecuteNonQueryAsync();

        var unix = (long)Convert.ToInt64(await connection.ExecuteScalarAsync($"SELECT toUnixTimestamp({col}) FROM {tableName}"));
        Assert.That(unix, Is.EqualTo(expected));
    }

    /// <summary>
    /// Issue #350 reproduction: bare ADO @-style parameter with a UTC DateTime
    /// must store the same UTC instant regardless of the server's session timezone.
    /// </summary>
    [Test]
    public async Task InferredHttpParam_UtcNow_ToBareDateTimeColumn_PreservesInstant()
    {
        // Specific reference instant to make the assertion deterministic across reruns.
        var dt = new DateTime(2026, 5, 22, 11, 48, 50, DateTimeKind.Utc);
        var expected = ToUnixSeconds(dt);

        var command = connection.CreateCommand();
        command.AddParameter("created_at", dt);
        command.CommandText = $"INSERT INTO {tableName} (dt_no_tz, dt_utc) VALUES (@created_at, @created_at)";
        await command.ExecuteNonQueryAsync();

        var unixNoTz = Convert.ToInt64(await connection.ExecuteScalarAsync($"SELECT toUnixTimestamp(dt_no_tz) FROM {tableName}"));
        var unixUtc = Convert.ToInt64(await connection.ExecuteScalarAsync($"SELECT toUnixTimestamp(dt_utc) FROM {tableName}"));
        Assert.Multiple(() =>
        {
            Assert.That(unixNoTz, Is.EqualTo(expected), "no-tz column under inferred @dt should match UTC instant");
            Assert.That(unixUtc, Is.EqualTo(expected), "UTC column under inferred @dt should match UTC instant");
        });
    }

    [Test]
    [TestCase("dt_utc")]
    [TestCase("dt_amsterdam")]
    [TestCase("dt_ny")]
    public async Task InferredHttpParam_LocalKind_PreservesInstant(string col)
    {
        var localDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Local);
        var expected = ToUnixSeconds(localDt);

        var command = connection.CreateCommand();
        command.AddParameter("dt", localDt);
        command.CommandText = $"INSERT INTO {tableName} ({col}) VALUES (@dt)";
        await command.ExecuteNonQueryAsync();

        var unix = Convert.ToInt64(await connection.ExecuteScalarAsync($"SELECT toUnixTimestamp({col}) FROM {tableName}"));
        Assert.That(unix, Is.EqualTo(expected));
    }

    [Test]
    [TestCase("dt_utc")]
    [TestCase("dt_amsterdam")]
    [TestCase("dt_ny")]
    public async Task InferredHttpParam_DateTimeOffset_PreservesInstant(string col)
    {
        var dto = new DateTimeOffset(2024, 1, 15, 15, 0, 0, TimeSpan.FromHours(3)); // 12:00 UTC
        var expected = ToUnixSeconds(dto);

        var command = connection.CreateCommand();
        command.AddParameter("dt", dto);
        command.CommandText = $"INSERT INTO {tableName} ({col}) VALUES (@dt)";
        await command.ExecuteNonQueryAsync();

        var unix = Convert.ToInt64(await connection.ExecuteScalarAsync($"SELECT toUnixTimestamp({col}) FROM {tableName}"));
        Assert.That(unix, Is.EqualTo(expected));
    }

    [Test]
    public async Task InferredHttpParam_UtcKind_Epoch_RoundtripsCorrectly()
    {
        var dt = DateTimeConversions.DateTimeEpochStart;
        var command = connection.CreateCommand();
        command.AddParameter("dt", dt);
        command.CommandText = $"INSERT INTO {tableName} (dt_utc) VALUES (@dt)";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync($"SELECT dt_utc FROM {tableName}");
        Assert.That(result, Is.EqualTo(DateTimeConversions.DateTimeEpochStart));
    }

    [Test]
    public async Task InferredHttpParam_DateTimeOffset_Epoch_RoundtripsCorrectly()
    {
        var dto = new DateTimeOffset(DateTimeConversions.DateTimeEpochStart, TimeSpan.Zero);
        var command = connection.CreateCommand();
        command.AddParameter("dt", dto);
        command.CommandText = $"INSERT INTO {tableName} (dt_utc) VALUES (@dt)";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync($"SELECT dt_utc FROM {tableName}");
        Assert.That(result, Is.EqualTo(DateTimeConversions.DateTimeEpochStart));
    }

    /// <summary>
    /// Type inference must propagate into composite structures so an Array of UTC DateTime
    /// values is inferred as Array(DateTime('UTC')), not Array(DateTime). Without this, each
    /// element's UTC wall-clock would be parsed in session_timezone on non-UTC servers.
    /// </summary>
    [Test]
    public async Task InferredHttpParam_ArrayOfUtcDateTime_PreservesInstants()
    {
        var arr = new[]
        {
            new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc),
            new DateTime(2024, 6, 15, 14, 30, 0, DateTimeKind.Utc),
        };
        var expected = arr.Select(dt => new DateTimeOffset(dt).ToUnixTimeSeconds()).ToArray();

        var command = connection.CreateCommand();
        command.AddParameter("arr", arr);
        command.CommandText = "SELECT arrayMap(x -> toUnixTimestamp(x), @arr)";

        using var reader = (ClickHouseDataReader)await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);
        var result = ((System.Collections.IEnumerable)reader.GetValue(0)).Cast<object>().Select(Convert.ToInt64).ToArray();
        Assert.That(result, Is.EqualTo(expected));
    }

    [Test]
    public async Task InferredHttpParam_ArrayOfDateTimeOffset_PreservesInstants()
    {
        var arr = new[]
        {
            new DateTimeOffset(2024, 1, 15, 15, 0, 0, TimeSpan.FromHours(3)),
            new DateTimeOffset(2024, 6, 15, 10, 30, 0, TimeSpan.FromHours(-5)),
        };
        var expected = arr.Select(dto => dto.ToUnixTimeSeconds()).ToArray();

        var command = connection.CreateCommand();
        command.AddParameter("arr", arr);
        command.CommandText = "SELECT arrayMap(x -> toUnixTimestamp(x), @arr)";

        using var reader = (ClickHouseDataReader)await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);
        var result = ((System.Collections.IEnumerable)reader.GetValue(0)).Cast<object>().Select(Convert.ToInt64).ToArray();
        Assert.That(result, Is.EqualTo(expected));
    }

    /// <summary>
    /// Array of Local DateTime inserted into Array(DateTime('tz')) columns: the inferred
    /// Array(DateTime('UTC')) lets the server preserve the UTC instant for every element,
    /// then implicitly convert to the destination column's tz on store (tz only changes
    /// the display side; the underlying Unix instant is preserved).
    /// </summary>
    [Test]
    [TestCase("arr_utc")]
    [TestCase("arr_amsterdam")]
    [TestCase("arr_ny")]
    public async Task InferredHttpParam_ArrayOfLocalDateTime_PreservesInstants(string col)
    {
        var arr = new[]
        {
            new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Local),
            new DateTime(2024, 6, 15, 14, 30, 0, DateTimeKind.Local),
        };
        var expected = arr.Select(dt => new DateTimeOffset(dt).ToUnixTimeSeconds()).ToArray();

        var command = connection.CreateCommand();
        command.AddParameter("arr", arr);
        command.CommandText = $"INSERT INTO {tableName} ({col}) VALUES (@arr)";
        await command.ExecuteNonQueryAsync();

        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync($"SELECT arrayMap(x -> toUnixTimestamp(x), {col}) FROM {tableName}");
        Assert.That(reader.Read(), Is.True);
        var result = ((System.Collections.IEnumerable)reader.GetValue(0)).Cast<object>().Select(Convert.ToInt64).ToArray();
        Assert.That(result, Is.EqualTo(expected));
    }

    /// <summary>
    /// Same propagation through tuples — Tuple element inference also recurses through
    /// the value-based TypeConverter chain.
    /// </summary>
    [Test]
    public async Task InferredHttpParam_TupleContainingUtcDateTime_PreservesInstant()
    {
        var dt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);
        var tup = ("hello", dt);
        var expected = new DateTimeOffset(dt).ToUnixTimeSeconds();

        var command = connection.CreateCommand();
        command.AddParameter("tup", tup);
        command.CommandText = "SELECT toUnixTimestamp(tupleElement(@tup, 2))";

        var unix = Convert.ToInt64(await command.ExecuteScalarAsync());
        Assert.That(unix, Is.EqualTo(expected));
    }

    /// <summary>
    /// Unspecified DateTime has no associated timezone, so inference deliberately leaves
    /// the type as bare DateTime. Documents that the result is therefore session_timezone
    /// dependent — users with wall-clock data must either pass it as UTC-anchored or
    /// supply an explicit {x:DateTime('Zone')} hint.
    /// </summary>
    [Test]
    public async Task InferredHttpParam_UnspecifiedKind_FallsThroughToBareHint_FootGun()
    {
        var dt = new DateTime(2024, 6, 15, 14, 30, 0, DateTimeKind.Unspecified);

        var command = connection.CreateCommand();
        command.AddParameter("dt", dt);
        command.CommandText = $"INSERT INTO {tableName} (dt_no_tz) VALUES (@dt)";
        await command.ExecuteNonQueryAsync();

        var unix = Convert.ToInt64(await connection.ExecuteScalarAsync($"SELECT toUnixTimestamp(dt_no_tz) FROM {tableName}"));

        // Expectation must mirror the foot-gun: wire wall-clock parsed in session_timezone.
        var sessionTz = DateTimeZoneProviders.Tzdb.GetZoneOrNull(sessionTimezone);
        var expected = sessionTz.AtLeniently(LocalDateTime.FromDateTime(dt)).ToInstant().ToUnixTimeSeconds();
        Assert.That(unix, Is.EqualTo(expected));
    }
}

/// <summary>
/// Tests for writing DateTime values via binary bulk copy.
/// This path uses AbstractDateTimeType.CoerceToDateTimeOffset and writes Unix timestamps.
/// </summary>
[TestFixture]
public class WriteDateTimeBulkCopyTests : AbstractConnectionTestFixture
{
    [SetUp]
    public async Task SetUp()
    {
        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.datetime_bulk_test");
        await connection.ExecuteStatementAsync(@"
            CREATE TABLE test.datetime_bulk_test (
                dt_utc DateTime('UTC'),
                dt_amsterdam DateTime('Europe/Amsterdam'),
                dt_no_tz DateTime
            ) ENGINE = Memory");
    }

    [TearDown]
    public async Task TearDown()
    {
        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.datetime_bulk_test");
    }

    [Test]
    [TestCase("dt_utc")]
    [TestCase("dt_amsterdam")]
    [TestCase("dt_no_tz")]
    public async Task BulkCopy_UnspecifiedKind_RoundtripsWallClock(string col)
    {
        // Unspecified wall-clock is interpreted by the binary writer as already-in-target-tz,
        // so reading back via the same column yields the same wall-clock regardless of column tz.
        var dt = new DateTime(2024, 6, 15, 14, 30, 0, DateTimeKind.Unspecified);

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime_bulk_test",
            ColumnNames = [col],
        };
        await bulkCopy.WriteToServerAsync([[dt]]);

        var result = (DateTime)await connection.ExecuteScalarAsync($"SELECT {col} FROM test.datetime_bulk_test");
        Assert.That(result.Ticks, Is.EqualTo(dt.Ticks));
    }

    [Test]
    public async Task BulkCopy_UnspecifiedKind_ToAmsterdamColumn_ReturnsCorrectOffset()
    {
        // June 15 - Amsterdam is UTC+2 (CEST - summer time)
        var original = new DateTime(2024, 6, 15, 14, 30, 0, DateTimeKind.Unspecified);

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime_bulk_test",
            ColumnNames = ["dt_amsterdam"],
        };
        await bulkCopy.WriteToServerAsync([[original]]);

        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT dt_amsterdam FROM test.datetime_bulk_test");
        Assert.That(reader.Read(), Is.True);
        var dto = reader.GetDateTimeOffset(0);

        Assert.That(dto.Offset.TotalHours, Is.EqualTo(2), "Should have Amsterdam summer time offset (CEST)");
        Assert.That(dto.DateTime, Is.EqualTo(original));
    }

    [Test]
    [TestCase("dt_utc")]
    [TestCase("dt_amsterdam")]
    [TestCase("dt_no_tz")]
    public async Task BulkCopy_UtcKind_PreservesInstant(string col)
    {
        var utcDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);
        var expected = new DateTimeOffset(utcDt).ToUnixTimeSeconds();

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime_bulk_test",
            ColumnNames = [col],
        };
        await bulkCopy.WriteToServerAsync([[utcDt]]);

        var unix = Convert.ToInt64(await connection.ExecuteScalarAsync($"SELECT toUnixTimestamp({col}) FROM test.datetime_bulk_test"));
        Assert.That(unix, Is.EqualTo(expected));
    }

    [Test]
    [TestCase("dt_utc")]
    [TestCase("dt_amsterdam")]
    [TestCase("dt_no_tz")]
    public async Task BulkCopy_LocalKind_PreservesInstant(string col)
    {
        var localDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Local);
        var expected = new DateTimeOffset(localDt).ToUnixTimeSeconds();

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime_bulk_test",
            ColumnNames = [col],
        };
        await bulkCopy.WriteToServerAsync([[localDt]]);

        var unix = Convert.ToInt64(await connection.ExecuteScalarAsync($"SELECT toUnixTimestamp({col}) FROM test.datetime_bulk_test"));
        Assert.That(unix, Is.EqualTo(expected));
    }

    [Test]
    [TestCase("dt_utc")]
    [TestCase("dt_amsterdam")]
    [TestCase("dt_no_tz")]
    public async Task BulkCopy_DateTimeOffset_PreservesInstant(string col)
    {
        var dto = new DateTimeOffset(2024, 1, 15, 15, 0, 0, TimeSpan.FromHours(3)); // 12:00 UTC
        var expected = dto.ToUnixTimeSeconds();

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime_bulk_test",
            ColumnNames = [col],
        };
        await bulkCopy.WriteToServerAsync([[dto]]);

        var unix = Convert.ToInt64(await connection.ExecuteScalarAsync($"SELECT toUnixTimestamp({col}) FROM test.datetime_bulk_test"));
        Assert.That(unix, Is.EqualTo(expected));
    }

    /// <summary>
    /// Test that DateTimeConversions.DateTimeEpochStart works with bulk copy (binary serialization).
    /// </summary>
    [Test]
    public async Task BulkCopy_DateTimeUnixEpoch_ShouldWork()
    {
        var dt = DateTimeConversions.DateTimeEpochStart; // 1970-01-01 00:00:00 UTC

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime_bulk_test",
            ColumnNames = ["dt_utc"]
        };
        
        await bulkCopy.WriteToServerAsync([[dt]]);

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_utc FROM test.datetime_bulk_test");
        Assert.That(result, Is.EqualTo(DateTimeConversions.DateTimeEpochStart));
    }

    /// <summary>
    /// Test that new DateTimeOffset(DateTimeConversions.DateTimeEpochStart, TimeSpan.Zero) works with bulk copy (binary serialization).
    /// </summary>
    [Test]
    public async Task BulkCopy_DateTimeOffsetUnixEpoch_ShouldWork()
    {
        var dto = new DateTimeOffset(DateTimeConversions.DateTimeEpochStart, TimeSpan.Zero); // 1970-01-01 00:00:00 +00:00

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime_bulk_test",
            ColumnNames = ["dt_utc"]
        };

        await bulkCopy.WriteToServerAsync([[dto]]);

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_utc FROM test.datetime_bulk_test");
        Assert.That(result, Is.EqualTo(DateTimeConversions.DateTimeEpochStart));
    }
}

/// <summary>
/// Tests for writing DateTime64 values via HTTP parameters with explicit type hints.
/// DateTime64 has sub-second precision and may have different edge cases than DateTime.
/// Parameterized over session_timezone so explicit-hint behavior is verified across server zones.
/// </summary>
[TestFixture("UTC")]
[TestFixture("Europe/Amsterdam")]
public class WriteDateTime64HttpParamTests : IDisposable
{
    protected readonly ClickHouseConnection connection;
    protected readonly ClickHouseClient client;
    private readonly string tableName;

    public WriteDateTime64HttpParamTests(string sessionTimezone)
    {
        var settings = TestUtilities.GetTestClickHouseClientSettings();
        settings.CustomSettings["session_timezone"] = sessionTimezone;
        client = new ClickHouseClient(settings);
        connection = client.CreateConnection();
        client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test;").GetAwaiter().GetResult();
        tableName = $"test.datetime64_http_test_{sessionTimezone.Replace('/', '_').Replace('-', '_')}";
    }

    [OneTimeTearDown]
    public void Dispose()
    {
        connection?.Dispose();
        client?.Dispose();
    }

    [SetUp]
    public async Task SetUp()
    {
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE {tableName} (
                dt64_utc DateTime64(3, 'UTC'),
                dt64_amsterdam DateTime64(3, 'Europe/Amsterdam'),
                dt64_no_tz DateTime64(3)
            ) ENGINE = Memory");
    }

    [TearDown]
    public async Task TearDown()
    {
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    [Test]
    public async Task HttpParam_DateTime64_UnixEpoch_ShouldWork()
    {
        var dt = DateTimeConversions.DateTimeEpochStart;

        var command = connection.CreateCommand();
        command.AddParameter("dt", dt);
        command.CommandText = $"INSERT INTO {tableName} (dt64_utc) VALUES ({{dt:DateTime64(3,'UTC')}})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync($"SELECT dt64_utc FROM {tableName}");
        Assert.That(result, Is.EqualTo(DateTimeConversions.DateTimeEpochStart));
    }

    [Test]
    [TestCase("dt64_utc")]
    [TestCase("dt64_amsterdam")]
    public async Task HttpParam_DateTime64_UtcKind_PreservesInstant(string col)
    {
        var utcDt = new DateTime(2024, 1, 15, 12, 30, 45, 123, DateTimeKind.Utc);
        var expected = new DateTimeOffset(utcDt).ToUnixTimeMilliseconds();

        var command = connection.CreateCommand();
        command.AddParameter("dt", utcDt);
        command.CommandText = $"INSERT INTO {tableName} ({col}) VALUES ({{dt:DateTime64(3,'UTC')}})";
        await command.ExecuteNonQueryAsync();

        var unixMs = Convert.ToInt64(await connection.ExecuteScalarAsync($"SELECT toUnixTimestamp64Milli({col}) FROM {tableName}"));
        Assert.That(unixMs, Is.EqualTo(expected));
    }

    [Test]
    public async Task HttpParam_DateTime64_UnspecifiedKind_ToUtcColumn_PreservesWallClock()
    {
        var dt = new DateTime(2024, 1, 15, 12, 30, 45, 123, DateTimeKind.Unspecified);

        var command = connection.CreateCommand();
        command.AddParameter("dt", dt);
        command.CommandText = $"INSERT INTO {tableName} (dt64_utc) VALUES ({{dt:DateTime64(3,'UTC')}})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync($"SELECT dt64_utc FROM {tableName}");

        Assert.That(result, Is.EqualTo(DateTime.SpecifyKind(dt, DateTimeKind.Utc)));
    }

    [Test]
    public async Task HttpParam_DateTime64_LocalKind_ToUtcColumn_PreservesInstant()
    {
        var localDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Local);
        var expectedUtc = new DateTimeOffset(localDt).UtcDateTime;

        var command = connection.CreateCommand();
        command.AddParameter("dt", localDt);
        command.CommandText = $"INSERT INTO {tableName} (dt64_utc) VALUES ({{dt:DateTime64(3,'UTC')}})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync($"SELECT dt64_utc FROM {tableName}");

        Assert.That(result, Is.EqualTo(DateTime.SpecifyKind(expectedUtc, DateTimeKind.Utc)).Within(TimeSpan.FromMilliseconds(1)));
    }

    [Test]
    public async Task HttpParam_DateTime64_DateTimeOffset_PreservesInstant()
    {
        var dto = new DateTimeOffset(2024, 1, 15, 15, 0, 0, 123, TimeSpan.FromHours(3)); // 15:00 +03:00 = 12:00 UTC

        var command = connection.CreateCommand();
        command.AddParameter("dt", dto);
        command.CommandText = $"INSERT INTO {tableName} (dt64_utc) VALUES ({{dt:DateTime64(3,'UTC')}})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync($"SELECT dt64_utc FROM {tableName}");

        Assert.That(result.Hour, Is.EqualTo(12));
        Assert.That(result.Millisecond, Is.EqualTo(123));
    }
}

/// <summary>
/// Tests for writing DateTime64 values via binary bulk copy.
/// </summary>
[TestFixture]
public class WriteDateTime64BulkCopyTests : AbstractConnectionTestFixture
{
    [SetUp]
    public async Task SetUp()
    {
        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.datetime64_bulk_test");
        await connection.ExecuteStatementAsync(@"
            CREATE TABLE test.datetime64_bulk_test (
                dt64_utc DateTime64(3, 'UTC'),
                dt64_amsterdam DateTime64(3, 'Europe/Amsterdam'),
                dt64_no_tz DateTime64(3)
            ) ENGINE = Memory");
    }

    [TearDown]
    public async Task TearDown()
    {
        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.datetime64_bulk_test");
    }

    [Test]
    public async Task BulkCopy_DateTime64_UnixEpoch_ShouldWork()
    {
        var dt = DateTimeConversions.DateTimeEpochStart;

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime64_bulk_test",
            ColumnNames = ["dt64_utc"]
        };
        
        await bulkCopy.WriteToServerAsync([[dt]]);

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt64_utc FROM test.datetime64_bulk_test");
        Assert.That(result, Is.EqualTo(DateTimeConversions.DateTimeEpochStart));
    }

    [Test]
    [TestCase("dt64_utc")]
    [TestCase("dt64_amsterdam")]
    [TestCase("dt64_no_tz")]
    public async Task BulkCopy_DateTime64_UtcKind_PreservesInstant(string col)
    {
        var utcDt = new DateTime(2024, 1, 15, 12, 30, 45, 123, DateTimeKind.Utc);
        var expected = new DateTimeOffset(utcDt).ToUnixTimeMilliseconds();

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime64_bulk_test",
            ColumnNames = [col],
        };
        await bulkCopy.WriteToServerAsync([[utcDt]]);

        var unixMs = Convert.ToInt64(await connection.ExecuteScalarAsync($"SELECT toUnixTimestamp64Milli({col}) FROM test.datetime64_bulk_test"));
        Assert.That(unixMs, Is.EqualTo(expected));
    }

    [Test]
    [TestCase("dt64_utc")]
    [TestCase("dt64_amsterdam")]
    [TestCase("dt64_no_tz")]
    public async Task BulkCopy_DateTime64_UnspecifiedKind_RoundtripsWallClock(string col)
    {
        // Unspecified wall-clock is interpreted as already-in-target-tz by the binary writer,
        // so reading back via the same column yields the same wall-clock + precision.
        var dt = new DateTime(2024, 1, 15, 12, 30, 45, 123, DateTimeKind.Unspecified);

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime64_bulk_test",
            ColumnNames = [col],
        };
        await bulkCopy.WriteToServerAsync([[dt]]);

        var result = (DateTime)await connection.ExecuteScalarAsync($"SELECT {col} FROM test.datetime64_bulk_test");
        Assert.That(result.Ticks, Is.EqualTo(dt.Ticks));
    }

    [Test]
    [TestCase("dt64_utc")]
    [TestCase("dt64_amsterdam")]
    [TestCase("dt64_no_tz")]
    public async Task BulkCopy_DateTime64_LocalKind_PreservesInstant(string col)
    {
        var localDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Local);
        var expected = new DateTimeOffset(localDt).ToUnixTimeMilliseconds();

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime64_bulk_test",
            ColumnNames = [col],
        };
        await bulkCopy.WriteToServerAsync([[localDt]]);

        var unixMs = Convert.ToInt64(await connection.ExecuteScalarAsync($"SELECT toUnixTimestamp64Milli({col}) FROM test.datetime64_bulk_test"));
        Assert.That(unixMs, Is.EqualTo(expected));
    }
}

/// <summary>
/// Regression tests for GitHub issue #370: DateTime/DateTime64 columns declared with
/// ClickHouse's synthetic fixed-offset timezone names (e.g. <c>Fixed/UTC+05:30:00</c>) were
/// silently dropped because <c>DateTimeZoneProviders.Tzdb.GetZoneOrNull</c> returns null for
/// them, shifting the wall-clock value by the offset amount.
/// </summary>
[TestFixture]
public class ReadDateTimeFixedUtcOffsetTests : AbstractConnectionTestFixture
{
    // (sql, expected wall-clock, expected DateTime.Kind, expected GetDateTimeOffset offset)
    private static IEnumerable<TestCaseData> FixedUtcOffsetCases()
    {
        var wallClock = new DateTime(2024, 1, 15, 10, 30, 0);

        // A non-zero fixed offset preserves the displayed wall-clock and yields Kind=Unspecified.
        yield return new TestCaseData(
            "SELECT toDateTime('2024-01-15 10:30:00', 'Fixed/UTC+05:30:00')",
            wallClock, DateTimeKind.Unspecified, new TimeSpan(5, 30, 0))
            .SetName("ReadDateTime_PositiveFixedUtcOffset");
        yield return new TestCaseData(
            "SELECT toDateTime('2024-01-15 10:30:00', 'Fixed/UTC-07:00:00')",
            wallClock, DateTimeKind.Unspecified, new TimeSpan(-7, 0, 0))
            .SetName("ReadDateTime_NegativeFixedUtcOffset");

        // A zero fixed offset resolves to a UTC-equivalent zone, so Kind=Utc and the offset is zero.
        yield return new TestCaseData(
            "SELECT toDateTime('2024-01-15 10:30:00', 'Fixed/UTC+00:00:00')",
            wallClock, DateTimeKind.Utc, TimeSpan.Zero)
            .SetName("ReadDateTime_ZeroFixedUtcOffset_IsUtc");

        // DateTime64 uses the same offset handling and preserves sub-second precision.
        yield return new TestCaseData(
            "SELECT toDateTime64('2024-01-15 10:30:00.123', 3, 'Fixed/UTC+05:30:00')",
            new DateTime(2024, 1, 15, 10, 30, 0, 123), DateTimeKind.Unspecified, new TimeSpan(5, 30, 0))
            .SetName("ReadDateTime64_PositiveFixedUtcOffset");
        yield return new TestCaseData(
            "SELECT toDateTime64('2024-01-15 10:30:00.456', 3, 'Fixed/UTC-07:00:00')",
            new DateTime(2024, 1, 15, 10, 30, 0, 456), DateTimeKind.Unspecified, new TimeSpan(-7, 0, 0))
            .SetName("ReadDateTime64_NegativeFixedUtcOffset");
    }

    /// <summary>
    /// A resolvable Fixed/UTC column must preserve the wall-clock value the server displays,
    /// set DateTime.Kind from the offset (Utc when the offset is zero, otherwise Unspecified),
    /// and expose the fixed offset through GetDateTimeOffset(). Before the fix the synthetic
    /// name resolved to null and the value was silently shifted to the UTC projection.
    /// </summary>
    [TestCaseSource(nameof(FixedUtcOffsetCases))]
    public async Task ReadDateTime_WithFixedUtcOffset_PreservesWallClockKindAndOffset(
        string sql, DateTime expectedWallClock, DateTimeKind expectedKind, TimeSpan expectedOffset)
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(sql);
        Assert.That(reader.Read(), Is.True);

        var dateTime = reader.GetDateTime(0);
        var dateTimeOffset = reader.GetDateTimeOffset(0);

        Assert.Multiple(() =>
        {
            Assert.That(dateTime, Is.EqualTo(expectedWallClock), "wall-clock value");
            Assert.That(dateTime.Kind, Is.EqualTo(expectedKind), "DateTime.Kind");
            Assert.That(dateTimeOffset.Offset, Is.EqualTo(expectedOffset), "GetDateTimeOffset offset");
            Assert.That(dateTimeOffset.DateTime, Is.EqualTo(expectedWallClock), "GetDateTimeOffset wall-clock");
        });
    }

    /// <summary>
    /// Out-of-range Fixed/UTC offset (>18 h) cannot be represented by NodaTime's Offset, so
    /// ResolveTimezone returns null and the driver falls back to the UTC wall-clock with
    /// Kind=Unspecified and a zero offset. Pins that fallback and covers the return-null branch.
    /// </summary>
    [Test]
    public async Task ReadDateTime_WithOutOfRangeFixedUtcOffset_FallsBackToUtcWallClock()
    {
        // Fixed/UTC+19:00:00 exceeds NodaTime's ±18 h cap → ResolveTimezone returns null.
        // The stored instant for wall-clock 2024-01-15 10:30:00 in UTC+19 is
        // 2024-01-14 15:30:00 UTC; the null-timezone fallback returns that UTC time.
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            "SELECT toDateTime('2024-01-15 10:30:00', 'Fixed/UTC+19:00:00')");
        Assert.That(reader.Read(), Is.True);

        var dateTime = reader.GetDateTime(0);
        var dateTimeOffset = reader.GetDateTimeOffset(0);

        Assert.Multiple(() =>
        {
            Assert.That(dateTime, Is.EqualTo(new DateTime(2024, 1, 14, 15, 30, 0)), "UTC-projected wall-clock");
            Assert.That(dateTime.Kind, Is.EqualTo(DateTimeKind.Unspecified), "null-timezone fallback Kind");
            Assert.That(dateTimeOffset.Offset, Is.EqualTo(TimeSpan.Zero), "UTC fallback offset");
            Assert.That(dateTimeOffset.DateTime, Is.EqualTo(new DateTime(2024, 1, 14, 15, 30, 0)), "GetDateTimeOffset wall-clock");
        });
    }

    /// <summary>
    /// Contrast case: a same-offset IANA zone (Asia/Kolkata, UTC+05:30) keeps its existing
    /// behaviour and is unaffected by the Fixed/UTC parsing path, proving the fix is targeted.
    /// </summary>
    [Test]
    public async Task ReadDateTime_WithIanaTimezone_IsUnaffectedByFix()
    {
        // Asia/Kolkata is UTC+05:30 — the same offset as Fixed/UTC+05:30:00.
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            "SELECT toDateTime('2024-01-15 10:30:00', 'Asia/Kolkata')");
        Assert.That(reader.Read(), Is.True);

        var dateTime = reader.GetDateTime(0);
        var dateTimeOffset = reader.GetDateTimeOffset(0);

        Assert.Multiple(() =>
        {
            Assert.That(dateTime, Is.EqualTo(new DateTime(2024, 1, 15, 10, 30, 0)), "wall-clock value");
            Assert.That(dateTime.Kind, Is.EqualTo(DateTimeKind.Unspecified), "DateTime.Kind");
            Assert.That(dateTimeOffset.Offset, Is.EqualTo(new TimeSpan(5, 30, 0)), "Asia/Kolkata offset (UTC+05:30)");
        });
    }
}

/// <summary>
/// Unit tests for ResolveTimezone via type-string parsing. These exercise branches that cannot
/// be reached through integration tests (the server never emits unrecognised or malformed
/// timezone names) but are needed to cover the fallback paths in
/// AbstractDateTimeType.ResolveTimezone.
/// </summary>
[TestFixture]
public class ResolveTimezoneParseTests
{
    private static IEnumerable<TestCaseData> UnresolvableTimezoneCases()
    {
        // Not IANA and not a Fixed/UTC offset → regex no-match → null TimeZone.
        yield return new TestCaseData("DateTime('Unknown/TZ')").SetName("ParseDateTime_UnrecognizedName");
        yield return new TestCaseData("DateTime64(3, 'Unknown/TZ')").SetName("ParseDateTime64_UnrecognizedName");
        // Malformed Fixed/UTC: minutes/seconds outside 00-59 must be rejected by the tightened
        // regex rather than misread as a different valid offset (e.g. 60 minutes as an extra hour).
        yield return new TestCaseData("DateTime('Fixed/UTC+05:60:00')").SetName("ParseDateTime_FixedUtcMinutesOutOfRange");
        yield return new TestCaseData("DateTime('Fixed/UTC+05:00:60')").SetName("ParseDateTime_FixedUtcSecondsOutOfRange");
    }

    /// <summary>
    /// Names that cannot be resolved (non-IANA, non-matching, or malformed Fixed/UTC) leave
    /// TimeZone null so the driver uses its UTC-wall-clock fallback. Covers the regex-no-match
    /// branch of ResolveTimezone for both DateTime and DateTime64.
    /// </summary>
    [TestCaseSource(nameof(UnresolvableTimezoneCases))]
    public void Parse_WithUnresolvableTimezone_HasNullTimeZone(string typeString)
    {
        var type = (AbstractDateTimeType)TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default);

        Assert.That(type.TimeZone, Is.Null);
    }

    // (type string, expected fixed offset in seconds)
    private static IEnumerable<TestCaseData> ValidFixedUtcOffsetCases()
    {
        yield return new TestCaseData("DateTime('Fixed/UTC+05:59:59')", (5 * 3600) + (59 * 60) + 59)
            .SetName("ParseDateTime_FixedUtcMaxMinutesSeconds");
        yield return new TestCaseData("DateTime('Fixed/UTC+18:00:00')", 18 * 3600)
            .SetName("ParseDateTime_FixedUtcMaxInRangeHours");
        yield return new TestCaseData("DateTime('Fixed/UTC-18:00:00')", -18 * 3600)
            .SetName("ParseDateTime_FixedUtcMinInRangeHours");
    }

    /// <summary>
    /// A well-formed Fixed/UTC name within NodaTime's ±18 h range resolves to a fixed-offset
    /// zone with exactly that offset. The boundary cases (59:59 minutes/seconds and the ±18 h
    /// cap) prove the tightened MM/SS regex and the range guard accept all valid values.
    /// </summary>
    [TestCaseSource(nameof(ValidFixedUtcOffsetCases))]
    public void Parse_WithValidFixedUtcOffset_ResolvesToFixedOffsetZone(string typeString, int expectedOffsetSeconds)
    {
        var type = (AbstractDateTimeType)TypeConverter.ParseClickHouseType(typeString, TypeSettings.Default);

        Assert.That(type.TimeZone, Is.Not.Null);
        Assert.That(type.TimeZone.MaxOffset, Is.EqualTo(Offset.FromSeconds(expectedOffsetSeconds)));
    }
}
