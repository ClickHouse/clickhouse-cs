using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Tests.Attributes;
using NodaTime;
using NUnit.Framework.Interfaces;

namespace ClickHouse.Driver.Tests.Types;

/// <summary>
/// End-to-end coverage of binary-insert range validation for Date / Date32 / DateTime / DateTime64.
/// Inserts via <c>InsertBinaryAsync</c> against a real ClickHouse server and asserts both round-trip
/// at the canonical bounds and an <see cref="ArgumentOutOfRangeException"/> just outside them
/// (wrapped by the bulk-copy serializer as the inner exception).
/// </summary>
/// <remarks>
/// <c>DateTime32</c> is intentionally not covered as a distinct case: the server canonicalises a
/// <c>DateTime32</c> column to <c>DateTime</c> in its schema response, so the binary write path
/// resolves to <c>DateTimeType</c> in practice. The range-validation behaviour is inherited.
/// </remarks>
[TestFixture]
public class DateTimeBinaryRangeTests : AbstractConnectionTestFixture
{
    private string DateTimeTable { get; set; }
    private string DateTable { get; set; }
    private string Date32Table { get; set; }

    [SetUp]
    public async Task SetUp()
    {
        // Deliberately the static helper rather than the fixture's tracking overload: TearDown below
        // keeps the table when a test fails so it can be inspected, and a registered name would be
        // dropped by the fixture's own teardown regardless.
        // [CallerMemberName] would resolve to SetUp here, so the owning test's name is passed explicitly.
        var testName = TestContext.CurrentContext.Test.Name;
        DateTimeTable = TestUtilities.CreateTableName($"dt_range_{testName}");
        DateTable = TestUtilities.CreateTableName($"date_range_{testName}");
        Date32Table = TestUtilities.CreateTableName($"date32_range_{testName}");

        // Range validation happens in client-side RowBinary serialization; Memory is enough here.
        await client.ExecuteNonQueryAsync($"CREATE TABLE {DateTimeTable} (t DateTime('UTC')) ENGINE = Memory");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {DateTable} (d Date) ENGINE = Memory");
        if (TestUtilities.SupportedFeatures.HasFlag(Feature.Date32))
            await client.ExecuteNonQueryAsync($"CREATE TABLE {Date32Table} (d Date32) ENGINE = Memory");
    }

    [TearDown]
    public async Task TearDown()
    {
        if (TestContext.CurrentContext.Result.Outcome.Status == TestStatus.Failed)
            return;

        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {DateTimeTable}");
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {DateTable}");
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {Date32Table}");
    }

    private static void AssertRangeException(ClickHouseBulkCopySerializationException? outer, string expectedTypeName)
    {
        Assert.That(outer, Is.Not.Null);
        Assert.That(outer!.InnerException, Is.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(outer.InnerException!.Message, Does.Contain($"ClickHouse {expectedTypeName}"));
    }

    // DateTime: ClickHouse range is [1970-01-01 00:00:00 UTC, 2106-02-07 06:28:15 UTC].

    [Test]
    public async Task InsertBinaryAsync_DateTime_AtLowerBound_RoundTrips()
    {
        var value = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        await client.InsertBinaryAsync(DateTimeTable, new[] { "t" }, new[] { new object[] { value } });

        var actual = (string)await client.ExecuteScalarAsync($"SELECT toString(t) FROM {DateTimeTable}");
        Assert.That(actual, Is.EqualTo("1970-01-01 00:00:00"));
    }

    [Test]
    public async Task InsertBinaryAsync_DateTime_AtUpperBound_RoundTrips()
    {
        // 2106-02-07 06:28:15 UTC == uint.MaxValue seconds since epoch.
        var value = DateTimeOffset.FromUnixTimeSeconds(uint.MaxValue).UtcDateTime;

        await client.InsertBinaryAsync(DateTimeTable, new[] { "t" }, new[] { new object[] { value } });

        var actual = (string)await client.ExecuteScalarAsync($"SELECT toString(t) FROM {DateTimeTable}");
        Assert.That(actual, Is.EqualTo("2106-02-07 06:28:15"));
    }

    [Test]
    public void InsertBinaryAsync_DateTime_BelowRange_ThrowsArgumentOutOfRangeException()
    {
        // Issue #85 repro: previously silently wrapped to 2036-02-07 on the server.
        var value = new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
            await client.InsertBinaryAsync(DateTimeTable, new[] { "t" }, new[] { new object[] { value } }));

        AssertRangeException(ex, "DateTime");
    }

    [Test]
    public void InsertBinaryAsync_DateTime_AboveRange_ThrowsArgumentOutOfRangeException()
    {
        // One second past the inclusive upper bound (uint.MaxValue seconds).
        var value = new DateTime(2106, 2, 7, 6, 28, 16, DateTimeKind.Utc);

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
            await client.InsertBinaryAsync(DateTimeTable, new[] { "t" }, new[] { new object[] { value } }));

        AssertRangeException(ex, "DateTime");
    }

    [Test]
    public async Task InsertBinaryAsync_DateTime_BeyondInt32MaxSeconds_DoesNotSilentlyWrap()
    {
        // Y2038 regression: 2038-01-19 03:14:08 UTC is one second past int.MaxValue seconds.
        // The previous (int) cast wrapped these values; the fix writes them as UInt32.
        var value = new DateTime(2038, 1, 19, 3, 14, 8, DateTimeKind.Utc);

        await client.InsertBinaryAsync(DateTimeTable, new[] { "t" }, new[] { new object[] { value } });

        var actual = (string)await client.ExecuteScalarAsync($"SELECT toString(t) FROM {DateTimeTable}");
        Assert.That(actual, Is.EqualTo("2038-01-19 03:14:08"));
    }

    // Date: ClickHouse range is [1970-01-01, 2149-06-06].

    [Test]
    public async Task InsertBinaryAsync_Date_AtLowerBound_RoundTrips()
    {
        var value = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        await client.InsertBinaryAsync(DateTable, new[] { "d" }, new[] { new object[] { value } });

        var actual = (string)await client.ExecuteScalarAsync($"SELECT toString(d) FROM {DateTable}");
        Assert.That(actual, Is.EqualTo("1970-01-01"));
    }

    [Test]
    public async Task InsertBinaryAsync_Date_AtUpperBound_RoundTrips()
    {
        // 1970-01-01 + 65535 days == 2149-06-06 (the canonical upper bound).
        var value = new DateTime(2149, 6, 6, 0, 0, 0, DateTimeKind.Utc);

        await client.InsertBinaryAsync(DateTable, new[] { "d" }, new[] { new object[] { value } });

        var actual = (string)await client.ExecuteScalarAsync($"SELECT toString(d) FROM {DateTable}");
        Assert.That(actual, Is.EqualTo("2149-06-06"));
    }

    [Test]
    public void InsertBinaryAsync_Date_BelowRange_ThrowsArgumentOutOfRangeException()
    {
        var value = new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc);

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
            await client.InsertBinaryAsync(DateTable, new[] { "d" }, new[] { new object[] { value } }));

        AssertRangeException(ex, "Date");
    }

    [Test]
    public void InsertBinaryAsync_Date_AboveRange_ThrowsArgumentOutOfRangeException()
    {
        var value = new DateTime(2149, 6, 7, 0, 0, 0, DateTimeKind.Utc);

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
            await client.InsertBinaryAsync(DateTable, new[] { "d" }, new[] { new object[] { value } }));

        AssertRangeException(ex, "Date");
    }

    // Date32: ClickHouse range is [1900-01-01, 2299-12-31].

    [Test]
    [RequiredFeature(Feature.Date32)]
    public async Task InsertBinaryAsync_Date32_AtLowerBound_RoundTrips()
    {
        var value = new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        await client.InsertBinaryAsync(Date32Table, new[] { "d" }, new[] { new object[] { value } });

        var actual = (string)await client.ExecuteScalarAsync($"SELECT toString(d) FROM {Date32Table}");
        Assert.That(actual, Is.EqualTo("1900-01-01"));
    }

    [Test]
    [RequiredFeature(Feature.Date32)]
    public async Task InsertBinaryAsync_Date32_AtUpperBound_RoundTrips()
    {
        var value = new DateTime(2299, 12, 31, 0, 0, 0, DateTimeKind.Utc);

        await client.InsertBinaryAsync(Date32Table, new[] { "d" }, new[] { new object[] { value } });

        var actual = (string)await client.ExecuteScalarAsync($"SELECT toString(d) FROM {Date32Table}");
        Assert.That(actual, Is.EqualTo("2299-12-31"));
    }

    [Test]
    [RequiredFeature(Feature.Date32)]
    public void InsertBinaryAsync_Date32_BelowRange_ThrowsArgumentOutOfRangeException()
    {
        var value = new DateTime(1899, 12, 31, 0, 0, 0, DateTimeKind.Utc);

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
            await client.InsertBinaryAsync(Date32Table, new[] { "d" }, new[] { new object[] { value } }));

        AssertRangeException(ex, "Date32");
    }

    [Test]
    [RequiredFeature(Feature.Date32)]
    public void InsertBinaryAsync_Date32_AboveRange_ThrowsArgumentOutOfRangeException()
    {
        var value = new DateTime(2300, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
            await client.InsertBinaryAsync(Date32Table, new[] { "d" }, new[] { new object[] { value } }));

        AssertRangeException(ex, "Date32");
    }

    // DateTime64: the wire value is an Int64 count of 10^-scale seconds since the epoch. Scales 8 and 9 are finer
    // than a .NET tick, so they reach the Int64 limit inside the DateTime range.

    private static readonly DateTime DateTime64NanoMin = new DateTime(1677, 9, 21, 0, 12, 43, DateTimeKind.Utc).AddTicks(1_452_242);
    private static readonly DateTime DateTime64NanoMax = new DateTime(2262, 4, 11, 23, 47, 16, DateTimeKind.Utc).AddTicks(8_547_758);
    private static readonly DateTime DateTime64ScaleEightMax = new DateTime(4892, 10, 7, 21, 52, 48, DateTimeKind.Utc).AddTicks(5_477_580);

    // An Unspecified DateTime is read in the column timezone: Asia/Tokyo is UTC+9.
    private static DateTime InTokyo(DateTime utc) => DateTime.SpecifyKind(utc.AddHours(9), DateTimeKind.Unspecified);

    private const string DateTime64NanoRange = "(1677-09-21 00:12:43.1452242 UTC to 2262-04-11 23:47:16.8547758 UTC)";

    // DateTime64(8) has no lower limit within the DateTime range.
    private const string DateTime64ScaleEightRange = "(up to 4892-10-07 21:52:48.5477580 UTC)";

    private static IEnumerable<TestCaseData> DateTime64AtInt64LimitCases()
    {
        yield return new TestCaseData("DateTime64(9, 'UTC')", DateTime64NanoMin, -9_223_372_036_854_775_800L).SetArgDisplayNames("9", "min");
        yield return new TestCaseData("DateTime64(9, 'UTC')", DateTime64NanoMax, 9_223_372_036_854_775_800L).SetArgDisplayNames("9", "max");
        yield return new TestCaseData("DateTime64(8, 'UTC')", DateTime64ScaleEightMax, 9_223_372_036_854_775_800L).SetArgDisplayNames("8", "max");
        // The limit applies to the instant, not to the wall-clock time.
        yield return new TestCaseData("DateTime64(9, 'UTC')", new DateTimeOffset(DateTime64NanoMax).ToOffset(TimeSpan.FromHours(1)), 9_223_372_036_854_775_800L)
            .SetArgDisplayNames("9", "max at +01:00");
        yield return new TestCaseData("DateTime64(9, 'Asia/Tokyo')", InTokyo(DateTime64NanoMax), 9_223_372_036_854_775_800L)
            .SetArgDisplayNames("9 Asia/Tokyo", "max as Unspecified");
    }

    private static IEnumerable<TestCaseData> DateTime64PastInt64LimitCases()
    {
        yield return new TestCaseData("DateTime64(9, 'UTC')", DateTime64NanoMin.AddTicks(-1), DateTime64NanoRange).SetArgDisplayNames("9", "min - 1 tick");
        yield return new TestCaseData("DateTime64(9, 'UTC')", DateTime64NanoMax.AddTicks(1), DateTime64NanoRange).SetArgDisplayNames("9", "max + 1 tick");
        yield return new TestCaseData("DateTime64(8, 'UTC')", DateTime64ScaleEightMax.AddTicks(1), DateTime64ScaleEightRange).SetArgDisplayNames("8", "max + 1 tick");
        yield return new TestCaseData("DateTime64(9, 'UTC')", new DateTimeOffset(DateTime64NanoMax.AddTicks(1)).ToOffset(TimeSpan.FromHours(-1)), DateTime64NanoRange)
            .SetArgDisplayNames("9", "max + 1 tick at -01:00");
        yield return new TestCaseData("DateTime64(9, 'Asia/Tokyo')", InTokyo(DateTime64NanoMax.AddTicks(1)), DateTime64NanoRange)
            .SetArgDisplayNames("9 Asia/Tokyo", "max + 1 tick as Unspecified");
        yield return new TestCaseData("DateTime64(9, 'UTC')", DateTime64NanoMax.AddTicks(1).ToLocalTime(), DateTime64NanoRange)
            .SetArgDisplayNames("9", "max + 1 tick as Local");
        yield return new TestCaseData("DateTime64(9, 'UTC')", Instant.FromDateTimeUtc(DateTime64NanoMax.AddTicks(1)), DateTime64NanoRange)
            .SetArgDisplayNames("9", "max + 1 tick as Instant");
        yield return new TestCaseData("DateTime64(9, 'UTC')", new DateOnly(1500, 6, 1), DateTime64NanoRange).SetArgDisplayNames("9", "DateOnly 1500-06-01");
    }

    [TestCaseSource(nameof(DateTime64AtInt64LimitCases))]
    public async Task InsertBinaryAsync_DateTime64_AtInt64Limit_RoundTrips(string type, object value, long expectedCount)
    {
        var table = CreateTableName($"dt64_{type}");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (t {type}) ENGINE = Memory");

        await client.InsertBinaryAsync(table, new[] { "t" }, new[] { new object[] { value } });

        // The stored count, not toString(t): some supported servers (e.g. 26.3) render an instant before 1900 as 1900.
        var actual = (long)await client.ExecuteScalarAsync($"SELECT reinterpretAsInt64(t) FROM {table}");
        Assert.That(actual, Is.EqualTo(expectedCount));
    }

    [TestCaseSource(nameof(DateTime64PastInt64LimitCases))]
    public async Task InsertBinaryAsync_DateTime64_PastInt64Limit_ThrowsArgumentOutOfRangeException(string type, object value, string expectedRange)
    {
        var table = CreateTableName($"dt64_{type}");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (t {type}) ENGINE = Memory");

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
            await client.InsertBinaryAsync(table, new[] { "t" }, new[] { new object[] { value } }));

        AssertRangeException(ex, type);
        var inner = (ArgumentOutOfRangeException)ex!.InnerException!;
        Assert.That(inner.Message, Does.Contain(expectedRange));
        Assert.That(inner.ActualValue, Is.EqualTo(value));
    }
}
