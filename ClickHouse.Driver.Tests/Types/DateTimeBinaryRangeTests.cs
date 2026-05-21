using System;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Tests.Attributes;

namespace ClickHouse.Driver.Tests.Types;

/// <summary>
/// End-to-end coverage of binary-insert range validation for Date / Date32 / DateTime.
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
    private const string DateTimeTable = "test.dt_range";
    private const string DateTable = "test.date_range";
    private const string Date32Table = "test.date32_range";

    [SetUp]
    public async Task SetUp()
    {
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {DateTimeTable}");
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {DateTable}");
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {Date32Table}");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {DateTimeTable} (t DateTime('UTC')) ENGINE = Memory");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {DateTable} (d Date) ENGINE = Memory");
        if (TestUtilities.SupportedFeatures.HasFlag(Feature.Date32))
            await client.ExecuteNonQueryAsync($"CREATE TABLE {Date32Table} (d Date32) ENGINE = Memory");
    }

    [TearDown]
    public async Task TearDown()
    {
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
}
