using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Tests.Types;

/// <summary>
/// Coverage for string values written to Date / Date32 / DateTime / DateTime64, which every other
/// scalar type already accepts, on both write paths: binary insert and HTTP query parameters.
/// </summary>
[TestFixture]
public class DateTimeStringCoercionTests : AbstractConnectionTestFixture
{
    public static IEnumerable<TestCaseData> StringValues()
    {
        yield return new TestCaseData("Date", "2020-01-01", "2020-01-01");
        yield return new TestCaseData("Date32", "1950-03-04", "1950-03-04");
        yield return new TestCaseData("DateTime('UTC')", "2020-01-01 12:34:56", "2020-01-01 12:34:56");
        yield return new TestCaseData("DateTime64(3, 'UTC')", "2020-01-01 12:34:56.789", "2020-01-01 12:34:56.789");
        yield return new TestCaseData("DateTime('UTC')", "2020-01-01T12:34:56Z", "2020-01-01 12:34:56");
        yield return new TestCaseData("Nullable(DateTime('UTC'))", "2020-01-01 12:34:56", "2020-01-01 12:34:56");
        yield return new TestCaseData("Array(Date)", new[] { "2020-01-01", "2020-01-02" }, "['2020-01-01','2020-01-02']");
    }

    [Test]
    [TestCaseSource(nameof(StringValues))]
    public async Task InsertBinaryAsync_StringValue_RoundTripsAsColumnType(string clickHouseType, object value, string expected)
    {
        var table = CreateTableName($"str_{clickHouseType}");
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (t {clickHouseType}) ENGINE = Memory");

        await client.InsertBinaryAsync(table, new[] { "t" }, new[] { new object[] { value } });

        var actual = (string)await client.ExecuteScalarAsync($"SELECT toString(t) FROM {table}");
        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public async Task InsertBinaryAsync_StringWithoutOffset_IsWallClockTimeInColumnTimezone()
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (t DateTime('Europe/Berlin')) ENGINE = Memory");

        await client.InsertBinaryAsync(table, new[] { "t" }, new[] { new object[] { "2020-01-01 12:34:56" } });

        // Europe/Berlin is UTC+1 on that date, so the wall-clock time denotes 11:34:56 UTC.
        var actual = (string)await client.ExecuteScalarAsync($"SELECT toString(t, 'UTC') FROM {table}");
        Assert.That(actual, Is.EqualTo("2020-01-01 11:34:56"));
    }

    [Test]
    public async Task InsertBinaryAsync_StringWithOffset_PreservesInstant()
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (t DateTime('UTC')) ENGINE = Memory");

        await client.InsertBinaryAsync(table, new[] { "t" }, new[] { new object[] { "2020-01-01T12:34:56+03:00" } });

        var actual = (string)await client.ExecuteScalarAsync($"SELECT toString(t) FROM {table}");
        Assert.That(actual, Is.EqualTo("2020-01-01 09:34:56"));
    }

    [Test]
    public async Task InsertBinaryAsync_StringMapKey_RoundTripsAsDate()
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (m Map(Date, String)) ENGINE = Memory");

        var value = new Dictionary<string, string> { ["2020-01-01"] = "x" };
        await client.InsertBinaryAsync(table, new[] { "m" }, new[] { new object[] { value } });

        var actual = (string)await client.ExecuteScalarAsync($"SELECT toString(mapKeys(m)[1]) FROM {table}");
        Assert.That(actual, Is.EqualTo("2020-01-01"));
    }

    public static IEnumerable<TestCaseData> ParameterStringValues()
    {
        yield return new TestCaseData("Date", "2020-01-01", "2020-01-01");
        yield return new TestCaseData("Date32", "1950-03-04", "1950-03-04");
        yield return new TestCaseData("DateTime('UTC')", "2020-01-01 12:34:56", "2020-01-01 12:34:56");
        yield return new TestCaseData("DateTime('Europe/Berlin')", "2020-01-01 12:34:56", "2020-01-01 12:34:56");
        yield return new TestCaseData("DateTime64(3, 'UTC')", "2020-01-01 12:34:56.789", "2020-01-01 12:34:56.789");
    }

    [Test]
    [TestCaseSource(nameof(ParameterStringValues))]
    public async Task ExecuteScalarAsync_StringParameter_IsSentAsColumnType(string clickHouseType, string value, string expected)
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("p", value);

        var actual = (string)await client.ExecuteScalarAsync($"SELECT toString({{p:{clickHouseType}}})", parameters);
        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public async Task ExecuteScalarAsync_StringParameterWithOffset_PreservesInstant()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("p", "2020-01-01T12:34:56+03:00");

        var actual = (string)await client.ExecuteScalarAsync("SELECT toString({p:DateTime('UTC')})", parameters);
        Assert.That(actual, Is.EqualTo("2020-01-01 09:34:56"));
    }

    [Test]
    public async Task ExecuteScalarAsync_StringDateParameterWithOffset_KeepsTheDateOfThatOffset()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("p", "2020-01-01T01:30:00+03:00");

        // The date is the one the string's own offset denotes, not the one the machine timezone would give.
        var actual = (string)await client.ExecuteScalarAsync("SELECT toString({p:Date})", parameters);
        Assert.That(actual, Is.EqualTo("2020-01-01"));
    }

    [Test]
    public void InsertBinaryAsync_UnparseableString_ThrowsWithValueAndColumnType()
    {
        var table = CreateTableName();
        client.ExecuteNonQueryAsync($"CREATE TABLE {table} (t DateTime('UTC')) ENGINE = Memory").GetAwaiter().GetResult();

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
            await client.InsertBinaryAsync(table, new[] { "t" }, new[] { new object[] { "not a timestamp" } }));

        Assert.That(ex.InnerException, Is.TypeOf<System.FormatException>());
        Assert.That(ex.InnerException.Message, Does.Contain("not a timestamp").And.Contain("DateTime('UTC')"));
    }

    [Test]
    public async Task InsertBinaryAsync_UnsupportedValueType_ThrowsWithValueTypeAndColumnType()
    {
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (t DateTime('UTC')) ENGINE = Memory");

        var ex = Assert.ThrowsAsync<ClickHouseBulkCopySerializationException>(async () =>
            await client.InsertBinaryAsync(table, new[] { "t" }, new[] { new object[] { 42 } }));

        Assert.That(ex.InnerException, Is.TypeOf<System.NotSupportedException>());
        Assert.That(ex.InnerException.Message, Does.Contain("Int32").And.Contain("DateTime('UTC')"));
    }
}
