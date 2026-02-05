using System;
using System.Globalization;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;
using NodaTime;
using NUnit.Framework;

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
/// </summary>
[TestFixture]
public class WriteDateTimeHttpParamTests : AbstractConnectionTestFixture
{
    [SetUp]
    public async Task SetUp()
    {
        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.datetime_http_test");
        await connection.ExecuteStatementAsync(@"
            CREATE TABLE test.datetime_http_test (
                dt_utc DateTime('UTC'),
                dt_amsterdam DateTime('Europe/Amsterdam'),
                dt_no_tz DateTime
            ) ENGINE = Memory");
    }

    [TearDown]
    public async Task TearDown()
    {
        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.datetime_http_test");
    }

    [Test]
    public async Task HttpParam_UnspecifiedKind_ToUtcColumn_TreatedAsTargetTimezone()
    {
        // Unspecified DateTime should be treated as if it's already in the target column's timezone
        var dt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Unspecified);

        var command = connection.CreateCommand();
        command.AddParameter("dt", dt);
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_utc) VALUES ({dt:DateTime})";
        await command.ExecuteNonQueryAsync();

        // When reading back from UTC column, should get the same wall-clock time
        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_utc FROM test.datetime_http_test");
        Assert.That(result, Is.EqualTo(new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc)));
    }

    [Test]
    public async Task HttpParam_UnspecifiedKind_ToAmsterdamColumn_RoundtripsCorrectly()
    {
        var original = new DateTime(2024, 6, 15, 14, 30, 0, DateTimeKind.Unspecified);

        var command = connection.CreateCommand();
        command.AddParameter("dt", original);
        // NB! The timezone must be specified in the parameter type, otherwise it's assumed to be UTC
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_amsterdam) VALUES ({dt:DateTime('Europe/Amsterdam')})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_amsterdam FROM test.datetime_http_test");

        // Unspecified should roundtrip perfectly when target timezone matches
        Assert.That(result, Is.EqualTo(original));
    }

    [Test]
    public async Task HttpParam_UnspecifiedKind_ToAmsterdamColumn_ReturnsCorrectOffset()
    {
        // June 15 - Amsterdam is UTC+2 (CEST - summer time)
        var original = new DateTime(2024, 6, 15, 14, 30, 0, DateTimeKind.Unspecified);

        var command = connection.CreateCommand();
        command.AddParameter("dt", original);
        // NB! The timezone must be specified in the parameter type, otherwise it's assumed to be UTC
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_amsterdam) VALUES ({dt:DateTime('Europe/Amsterdam')})";
        await command.ExecuteNonQueryAsync();

        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT dt_amsterdam FROM test.datetime_http_test");
        Assert.That(reader.Read(), Is.True);
        var dto = reader.GetDateTimeOffset(0);

        // Should have Amsterdam's summer offset (UTC+2)
        Assert.That(dto.Offset.TotalHours, Is.EqualTo(2), "Should have Amsterdam summer time offset (CEST)");
        Assert.That(dto.DateTime, Is.EqualTo(original));
    }

    [Test]
    public async Task HttpParam_UnspecifiedKind_WithoutTimezoneHint_InterpretsAsUtc()
    {
        // IMPORTANT: When the parameter type hint does NOT include a timezone (e.g., {dt:DateTime} instead of {dt:DateTime('Europe/Amsterdam')}),
        // ClickHouse interprets the string value in UTC, not the column's timezone.
        var original = new DateTime(2024, 6, 15, 14, 30, 0, DateTimeKind.Unspecified);

        var command = connection.CreateCommand();
        command.AddParameter("dt", original);
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_amsterdam) VALUES ({dt:DateTime})";
        await command.ExecuteNonQueryAsync();

        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT dt_amsterdam FROM test.datetime_http_test");
        Assert.That(reader.Read(), Is.True);
        var dto = reader.GetDateTimeOffset(0);

        // The value stored depends on UTC, NOT the column timezone.
        // UTC: 14:30 UTC → stored → read as 16:30 Amsterdam (UTC+2 in summer)
        // To get correct behavior, use {dt:DateTime('Europe/Amsterdam')} in the parameter type hint.
        var utcTz = DateTimeZoneProviders.Tzdb.GetZoneOrNull("UTC");
        var serverInstant = utcTz.AtLeniently(LocalDateTime.FromDateTime(original)).ToInstant();
        var amsterdamTz = DateTimeZoneProviders.Tzdb.GetZoneOrNull("Europe/Amsterdam");
        var expectedInAmsterdam = serverInstant.InZone(amsterdamTz).ToDateTimeOffset();

        Assert.That(dto, Is.EqualTo(expectedInAmsterdam),
            $"Without timezone hint, value is interpreted in UTC, then converted to column timezone");
    }

    [Test]
    public async Task HttpParam_UtcKind_ToUtcColumn_PreservesInstant()
    {
        // UTC DateTime represents a specific instant - should be stored as that instant
        var utcDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);

        var command = connection.CreateCommand();
        command.AddParameter("dt", utcDt);
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_utc) VALUES ({dt:DateTime})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_utc FROM test.datetime_http_test");

        // 12:00 UTC should be stored as 12:00 UTC
        Assert.That(result, Is.EqualTo(new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc)));
    }

    [Test]
    public async Task HttpParam_UtcKind_ToAmsterdamColumnWithoutTzHint_PreservesInstant()
    {
        // UTC DateTime(12:00 UTC) written to Amsterdam column should store as 13:00 Amsterdam
        // because 12:00 UTC = 13:00 Amsterdam (Amsterdam is UTC+1 in January)
        var utcDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);

        var command = connection.CreateCommand();
        command.AddParameter("dt", utcDt);
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_amsterdam) VALUES ({dt:DateTime})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_amsterdam FROM test.datetime_http_test");

        // 12:00 UTC = 13:00 Amsterdam
        Assert.That(result.Hour, Is.EqualTo(13), "UTC DateTime should be converted to target timezone");
    }
    
    [Test]
    public async Task HttpParam_UtcKind_ToAmsterdamColumnWithTzHint_PreservesInstant()
    {
        // UTC DateTime(12:00 UTC) written to Amsterdam column should store as 13:00 Amsterdam
        // because 12:00 UTC = 13:00 Amsterdam (Amsterdam is UTC+1 in January)
        var utcDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);

        var command = connection.CreateCommand();
        command.AddParameter("dt", utcDt);
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_amsterdam) VALUES ({dt:DateTime('Europe/Amsterdam')})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_amsterdam FROM test.datetime_http_test");

        // 12:00 UTC = 13:00 Amsterdam
        Assert.That(result.Hour, Is.EqualTo(13), "UTC DateTime should be converted to target timezone");
    }

    [Test]
    public async Task HttpParam_LocalKind_ToUtcColumn_PreservesInstant()
    {
        // Local DateTime is converted to UTC using system timezone (preserves the instant)
        var localDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Local);
        var expectedUtc = new DateTimeOffset(localDt).UtcDateTime;

        var command = connection.CreateCommand();
        command.AddParameter("dt", localDt);
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_utc) VALUES ({dt:DateTime})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_utc FROM test.datetime_http_test");

        // Local is converted to UTC preserving the instant
        Assert.That(result, Is.EqualTo(DateTime.SpecifyKind(expectedUtc, DateTimeKind.Utc)));
    }

    [Test]
    public async Task HttpParam_LocalKind_ToAmsterdamColumnWithNoTzHint_PreservesInstant()
    {
        // Local DateTime(12:00 Local) written to Amsterdam column should preserve the instant
        // For example: if local is UTC+0, 12:00 Local = 12:00 UTC = 13:00 Amsterdam (UTC+1 in January)
        var localDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Local);
        var expectedUtc = new DateTimeOffset(localDt).UtcDateTime;

        var command = connection.CreateCommand();
        command.AddParameter("dt", localDt);
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_amsterdam) VALUES ({dt:DateTime})";
        await command.ExecuteNonQueryAsync();

        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT dt_amsterdam FROM test.datetime_http_test");
        Assert.That(reader.Read(), Is.True);
        var resultDto = reader.GetDateTimeOffset(0);

        // The instant should be preserved - compare UTC times
        Assert.That(resultDto.UtcDateTime, Is.EqualTo(expectedUtc).Within(TimeSpan.FromSeconds(1)));
    }
    
    [Test]
    public async Task HttpParam_LocalKind_ToAmsterdamColumn_PreservesInstant()
    {
        // Local DateTime(12:00 Local) written to Amsterdam column should preserve the instant
        // For example: if local is UTC+0, 12:00 Local = 12:00 UTC = 13:00 Amsterdam (UTC+1 in January)
        var localDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Local);
        var expectedUtc = new DateTimeOffset(localDt).UtcDateTime;

        var command = connection.CreateCommand();
        command.AddParameter("dt", localDt);
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_amsterdam) VALUES ({dt:DateTime('Europe/Amsterdam')})";
        await command.ExecuteNonQueryAsync();

        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT dt_amsterdam FROM test.datetime_http_test");
        Assert.That(reader.Read(), Is.True);
        var resultDto = reader.GetDateTimeOffset(0);

        // The instant should be preserved - compare UTC times
        Assert.That(resultDto.UtcDateTime, Is.EqualTo(expectedUtc).Within(TimeSpan.FromSeconds(1)));
    }

    [Test]
    public async Task HttpParam_UnspecifiedKind_ToNoTimezoneColumn_PreservesWallClock()
    {
        // Unspecified DateTime to column without timezone - should preserve wall-clock time
        var dt = new DateTime(2024, 1, 15, 12, 30, 45, DateTimeKind.Unspecified);

        var command = connection.CreateCommand();
        command.AddParameter("dt", dt);
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_no_tz) VALUES ({dt:DateTime})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_no_tz FROM test.datetime_http_test");

        Assert.That(result, Is.EqualTo(dt));
    }

    [Test]
    public async Task HttpParam_UtcKind_ToNoTimezoneColumn_WritesUtcValue()
    {
        // UTC DateTime to column without timezone - the UTC value should be written
        var utcDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);

        var command = connection.CreateCommand();
        command.AddParameter("dt", utcDt);
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_no_tz) VALUES ({dt:DateTime})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_no_tz FROM test.datetime_http_test");

        // The UTC time (12:00) should be stored as-is
        Assert.That(result.Hour, Is.EqualTo(12));
        Assert.That(result.Kind, Is.EqualTo(DateTimeKind.Unspecified));
    }

    [Test]
    public async Task HttpParam_LocalKind_ToNoTimezoneColumn_WritesUtcValue()
    {
        // Local DateTime to column without timezone - should be converted to UTC first
        var localDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Local);
        var expectedUtc = new DateTimeOffset(localDt).UtcDateTime;

        var command = connection.CreateCommand();
        command.AddParameter("dt", localDt);
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_no_tz) VALUES ({dt:DateTime})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_no_tz FROM test.datetime_http_test");

        // The UTC-converted time should be stored
        Assert.That(result.Hour, Is.EqualTo(expectedUtc.Hour));
        Assert.That(result.Kind, Is.EqualTo(DateTimeKind.Unspecified));
    }

    [Test]
    public async Task HttpParam_DateTimeOffset_ToUtcColumn_PreservesInstant()
    {
        // DateTimeOffset should correctly preserve the instant
        var dto = new DateTimeOffset(2024, 1, 15, 15, 0, 0, TimeSpan.FromHours(3)); // 15:00 +03:00 = 12:00 UTC

        var command = connection.CreateCommand();
        command.AddParameter("dt", dto);
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_utc) VALUES ({dt:DateTime})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_utc FROM test.datetime_http_test");

        // DateTimeOffset correctly converts to UTC
        Assert.That(result, Is.EqualTo(new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc)));
    }

    [Test]
    public async Task HttpParam_DateTimeOffset_ToAmsterdamColumn_PreservesInstant()
    {
        // DateTimeOffset 15:00 +03:00 = 12:00 UTC = 13:00 Amsterdam (UTC+1 in January)
        // When ClickHouse type is specified on the parameter, the formatter converts to that timezone
        var dto = new DateTimeOffset(2024, 1, 15, 15, 0, 0, TimeSpan.FromHours(3));

        var command = connection.CreateCommand();
        command.AddParameter("dt", dto);
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_amsterdam) VALUES ({dt:DateTime('Europe/Amsterdam')})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_amsterdam FROM test.datetime_http_test");

        // 12:00 UTC = 13:00 Amsterdam
        Assert.That(result.Hour, Is.EqualTo(13), "DateTimeOffset instant should be preserved and converted to Amsterdam time");
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
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_utc) VALUES ({dt:DateTime})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_utc FROM test.datetime_http_test");
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
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_utc) VALUES ({dt:DateTime})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_utc FROM test.datetime_http_test");
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
        command.CommandText = "INSERT INTO test.datetime_http_test (dt_utc) VALUES ({dt:DateTime})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_utc FROM test.datetime_http_test");
        Assert.That(result, Is.EqualTo(dt));
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
    public async Task BulkCopy_UnspecifiedKind_ToUtcColumn_TreatedAsTargetTimezone()
    {
        var dt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Unspecified);

        using var bulkCopy = new ClickHouseBulkCopy(connection) 
        { 
            DestinationTableName = "test.datetime_bulk_test", 
            ColumnNames = ["dt_utc"],
        };
        await bulkCopy.WriteToServerAsync([[dt]]);

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_utc FROM test.datetime_bulk_test");
        Assert.That(result, Is.EqualTo(new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc)));
    }

    [Test]
    public async Task BulkCopy_UnspecifiedKind_ToAmsterdamColumn_RoundtripsCorrectly()
    {
        var original = new DateTime(2024, 6, 15, 14, 30, 0, DateTimeKind.Unspecified);

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime_bulk_test",
            ColumnNames = ["dt_amsterdam"]
        };
        await bulkCopy.WriteToServerAsync([[original]]);

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_amsterdam FROM test.datetime_bulk_test");

        Assert.That(result.Year, Is.EqualTo(original.Year));
        Assert.That(result.Month, Is.EqualTo(original.Month));
        Assert.That(result.Day, Is.EqualTo(original.Day));
        Assert.That(result.Hour, Is.EqualTo(original.Hour));
        Assert.That(result.Minute, Is.EqualTo(original.Minute));
        Assert.That(result.Second, Is.EqualTo(original.Second));
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

        // Should have Amsterdam's summer offset (UTC+2)
        Assert.That(dto.Offset.TotalHours, Is.EqualTo(2), "Should have Amsterdam summer time offset (CEST)");
        Assert.That(dto.DateTime, Is.EqualTo(original));
    }

    [Test]
    public async Task BulkCopy_UtcKind_ToUtcColumn_PreservesInstant()
    {
        // UTC DateTime represents a specific instant - should be stored as that instant
        var utcDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime_bulk_test",
            ColumnNames = ["dt_utc"]
        };
        await bulkCopy.WriteToServerAsync([[utcDt]]);

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_utc FROM test.datetime_bulk_test");

        // 12:00 UTC should be stored as 12:00 UTC
        Assert.That(result, Is.EqualTo(new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc)));
    }

    [Test]
    public async Task BulkCopy_UtcKind_ToAmsterdamColumn_PreservesInstant()
    {
        // UTC DateTime(12:00 UTC) written to Amsterdam column should store as 13:00 Amsterdam
        // because 12:00 UTC = 13:00 Amsterdam (Amsterdam is UTC+1 in January)
        var utcDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime_bulk_test",
            ColumnNames = ["dt_amsterdam"]
        };
        await bulkCopy.WriteToServerAsync([[utcDt]]);

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_amsterdam FROM test.datetime_bulk_test");

        // 12:00 UTC = 13:00 Amsterdam
        Assert.That(result.Hour, Is.EqualTo(13), "UTC DateTime should be converted to target timezone");
    }

    [Test]
    public async Task BulkCopy_LocalKind_ToUtcColumn_PreservesInstant()
    {
        // Local DateTime is converted to UTC using system timezone (preserves the instant)
        var localDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Local);
        var expectedUtc = new DateTimeOffset(localDt).UtcDateTime;

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime_bulk_test",
            ColumnNames = ["dt_utc"]
        };
        await bulkCopy.WriteToServerAsync([[localDt]]);

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_utc FROM test.datetime_bulk_test");

        // Local is converted to UTC preserving the instant
        Assert.That(result, Is.EqualTo(DateTime.SpecifyKind(expectedUtc, DateTimeKind.Utc)));
    }

    [Test]
    public async Task BulkCopy_LocalKind_ToAmsterdamColumn_PreservesInstant()
    {
        // Local DateTime written to Amsterdam column should preserve the instant
        var localDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Local);
        var expectedUtc = new DateTimeOffset(localDt).UtcDateTime;

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime_bulk_test",
            ColumnNames = ["dt_amsterdam"]
        };
        
        await bulkCopy.WriteToServerAsync([[localDt]]);

        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT dt_amsterdam FROM test.datetime_bulk_test");
        Assert.That(reader.Read(), Is.True);
        var resultDto = reader.GetDateTimeOffset(0);

        // The instant should be preserved - compare UTC times
        Assert.That(resultDto.UtcDateTime, Is.EqualTo(expectedUtc).Within(TimeSpan.FromSeconds(1)));
    }

    [Test]
    public async Task BulkCopy_UnspecifiedKind_ToNoTimezoneColumn_PreservesWallClock()
    {
        // Unspecified DateTime to column without timezone - should preserve wall-clock time
        var dt = new DateTime(2024, 1, 15, 12, 30, 45, DateTimeKind.Unspecified);

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime_bulk_test",
            ColumnNames = ["dt_no_tz"]
        };
        
        await bulkCopy.WriteToServerAsync([[dt]]);

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_no_tz FROM test.datetime_bulk_test");

        Assert.That(result, Is.EqualTo(dt));
    }

    [Test]
    public async Task BulkCopy_UtcKind_ToNoTimezoneColumn_PreservesInstant()
    {
        // UTC DateTime to column without timezone
        var utcDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime_bulk_test",
            ColumnNames = ["dt_no_tz"]
        };
        
        await bulkCopy.WriteToServerAsync([[utcDt]]);

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_no_tz FROM test.datetime_bulk_test");

        // The UTC time should be stored
        Assert.That(result, Is.EqualTo(utcDt));
    }

    [Test]
    public async Task BulkCopy_LocalKind_ToNoTimezoneColumn_PreservesInstant()
    {
        // Local DateTime to column without timezone
        var localDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Local);
        var expectedUtc = new DateTimeOffset(localDt).UtcDateTime;

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime_bulk_test",
            ColumnNames = ["dt_no_tz"]
        };
        
        await bulkCopy.WriteToServerAsync([[localDt]]);

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_no_tz FROM test.datetime_bulk_test");

        // The UTC-converted time should be stored, but returned as Unspecified kind
        Assert.That(result, Is.EqualTo(DateTime.SpecifyKind(expectedUtc, DateTimeKind.Unspecified)));
    }

    [Test]
    public async Task BulkCopy_DateTimeOffset_PreservesCorrectInstant()
    {
        // DateTimeOffset should correctly preserve the instant
        var dto = new DateTimeOffset(2024, 1, 15, 15, 0, 0, TimeSpan.FromHours(3)); // 15:00 +03:00 = 12:00 UTC

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime_bulk_test",
            ColumnNames = ["dt_utc"]
        };
        await bulkCopy.WriteToServerAsync([[dto]]);

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt_utc FROM test.datetime_bulk_test");

        // DateTimeOffset correctly converts to UTC
        Assert.That(result, Is.EqualTo(new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc)));
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
/// Tests for writing DateTime64 values via HTTP parameters.
/// DateTime64 has sub-second precision and may have different edge cases than DateTime.
/// </summary>
[TestFixture]
public class WriteDateTime64HttpParamTests : AbstractConnectionTestFixture
{
    [SetUp]
    public async Task SetUp()
    {
        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.datetime64_http_test");
        await connection.ExecuteStatementAsync(@"
            CREATE TABLE test.datetime64_http_test (
                dt64_utc DateTime64(3, 'UTC'),
                dt64_amsterdam DateTime64(3, 'Europe/Amsterdam'),
                dt64_no_tz DateTime64(3)
            ) ENGINE = Memory");
    }

    [TearDown]
    public async Task TearDown()
    {
        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.datetime64_http_test");
    }

    [Test]
    public async Task HttpParam_DateTime64_UnixEpoch_ShouldWork()
    {
        var dt = DateTimeConversions.DateTimeEpochStart;

        var command = connection.CreateCommand();
        command.AddParameter("dt", dt);
        command.CommandText = "INSERT INTO test.datetime64_http_test (dt64_utc) VALUES ({dt:DateTime64(3)})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt64_utc FROM test.datetime64_http_test");
        Assert.That(result, Is.EqualTo(DateTimeConversions.DateTimeEpochStart));
    }

    [Test]
    public async Task HttpParam_DateTime64_UtcKind_ToUtcColumn_PreservesInstant()
    {
        var utcDt = new DateTime(2024, 1, 15, 12, 30, 45, 123, DateTimeKind.Utc);

        var command = connection.CreateCommand();
        command.AddParameter("dt", utcDt);
        command.CommandText = "INSERT INTO test.datetime64_http_test (dt64_utc) VALUES ({dt:DateTime64(3)})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt64_utc FROM test.datetime64_http_test");

        Assert.That(result, Is.EqualTo(utcDt));
    }

    [Test]
    public async Task HttpParam_DateTime64_UnspecifiedKind_ToUtcColumn_PreservesWallClock()
    {
        var dt = new DateTime(2024, 1, 15, 12, 30, 45, 123, DateTimeKind.Unspecified);

        var command = connection.CreateCommand();
        command.AddParameter("dt", dt);
        command.CommandText = "INSERT INTO test.datetime64_http_test (dt64_utc) VALUES ({dt:DateTime64(3)})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt64_utc FROM test.datetime64_http_test");

        Assert.That(result, Is.EqualTo(DateTime.SpecifyKind(dt, DateTimeKind.Utc)));
    }

    [Test]
    public async Task HttpParam_DateTime64_UtcKind_ToAmsterdamColumn_PreservesInstant()
    {
        // 12:00 UTC = 13:00 Amsterdam (UTC+1 in January)
        var utcDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);

        var command = connection.CreateCommand();
        command.AddParameter("dt", utcDt);
        command.CommandText = "INSERT INTO test.datetime64_http_test (dt64_amsterdam) VALUES ({dt:DateTime64(3)})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt64_amsterdam FROM test.datetime64_http_test");

        Assert.That(result.Hour, Is.EqualTo(13), "UTC DateTime should be converted to Amsterdam timezone");
        Assert.That(result.Kind, Is.EqualTo(DateTimeKind.Unspecified));
    }

    [Test]
    public async Task HttpParam_DateTime64_LocalKind_ToUtcColumn_PreservesInstant()
    {
        var localDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Local);
        var expectedUtc = new DateTimeOffset(localDt).UtcDateTime;

        var command = connection.CreateCommand();
        command.AddParameter("dt", localDt);
        command.CommandText = "INSERT INTO test.datetime64_http_test (dt64_utc) VALUES ({dt:DateTime64(3)})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt64_utc FROM test.datetime64_http_test");

        Assert.That(result, Is.EqualTo(DateTime.SpecifyKind(expectedUtc, DateTimeKind.Utc)).Within(TimeSpan.FromMilliseconds(1)));
    }

    [Test]
    public async Task HttpParam_DateTime64_DateTimeOffset_PreservesInstant()
    {
        var dto = new DateTimeOffset(2024, 1, 15, 15, 0, 0, 123, TimeSpan.FromHours(3)); // 15:00 +03:00 = 12:00 UTC

        var command = connection.CreateCommand();
        command.AddParameter("dt", dto);
        command.CommandText = "INSERT INTO test.datetime64_http_test (dt64_utc) VALUES ({dt:DateTime64(3)})";
        await command.ExecuteNonQueryAsync();

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt64_utc FROM test.datetime64_http_test");

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
    public async Task BulkCopy_DateTime64_UtcKind_ToUtcColumn_PreservesInstant()
    {
        var utcDt = new DateTime(2024, 1, 15, 12, 30, 45, 123, DateTimeKind.Utc);

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime64_bulk_test",
            ColumnNames = ["dt64_utc"]
        };
        
        await bulkCopy.WriteToServerAsync([[utcDt]]);

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt64_utc FROM test.datetime64_bulk_test");

        Assert.That(result, Is.EqualTo(utcDt));
    }

    [Test]
    public async Task BulkCopy_DateTime64_UnspecifiedKind_ToUtcColumn_PreservesWallClock()
    {
        var dt = new DateTime(2024, 1, 15, 12, 30, 45, 123, DateTimeKind.Unspecified);

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime64_bulk_test",
            ColumnNames = ["dt64_utc"]
        };
        
        await bulkCopy.WriteToServerAsync([[dt]]);

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt64_utc FROM test.datetime64_bulk_test");

        Assert.That(result.Hour, Is.EqualTo(12));
        Assert.That(result.Minute, Is.EqualTo(30));
        Assert.That(result.Second, Is.EqualTo(45));
        Assert.That(result.Millisecond, Is.EqualTo(123));
    }

    [Test]
    public async Task BulkCopy_DateTime64_UtcKind_ToAmsterdamColumn_PreservesInstant()
    {
        // 12:00 UTC = 13:00 Amsterdam (UTC+1 in January)
        var utcDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime64_bulk_test",
            ColumnNames = ["dt64_amsterdam"]
        };
        
        await bulkCopy.WriteToServerAsync([[utcDt]]);

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt64_amsterdam FROM test.datetime64_bulk_test");

        Assert.That(result.Hour, Is.EqualTo(13), "UTC DateTime should be converted to Amsterdam timezone");
    }

    [Test]
    public async Task BulkCopy_DateTime64_LocalKind_ToUtcColumn_PreservesInstant()
    {
        var localDt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Local);
        var expectedUtc = new DateTimeOffset(localDt).UtcDateTime;

        using var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "test.datetime64_bulk_test",
            ColumnNames = ["dt64_utc"]
        };
        
        await bulkCopy.WriteToServerAsync([[localDt]]);

        var result = (DateTime)await connection.ExecuteScalarAsync("SELECT dt64_utc FROM test.datetime64_bulk_test");

        Assert.That(result, Is.EqualTo(DateTime.SpecifyKind(expectedUtc, DateTimeKind.Utc)).Within(TimeSpan.FromMilliseconds(1)));
    }
}
