using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// Parameterized queries end to end. These are the cases that prove the wire contract: the server restores the
// parameter value as a Field before the {name:Type} substitution reads it, so the value crosses two unescape
// stages. Only a live server can show that the client's escaping survives both.
[TestFixture]
[Category("Integration")]
public class ClickHouseTcpParameterIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static IEnumerable<TestCaseData> RoundTripCases()
    {
        yield return new TestCaseData("Int32", 42).Returns("42").SetName("Int32");
        yield return new TestCaseData("UInt8", (byte)7).Returns("7").SetName("UInt8");
        yield return new TestCaseData("Int64", long.MinValue).Returns("-9223372036854775808").SetName("Int64 min");
        yield return new TestCaseData("UInt64", ulong.MaxValue).Returns("18446744073709551615").SetName("UInt64 max");
        yield return new TestCaseData("Int128", Int128.MinValue).Returns("-170141183460469231731687303715884105728").SetName("Int128 min");
        yield return new TestCaseData("Float64", 1.5d).Returns("1.5").SetName("Float64");
        yield return new TestCaseData("Bool", true).Returns("true").SetName("Bool");
        yield return new TestCaseData("Decimal64(4)", 1.2345m).Returns("1.2345").SetName("Decimal64");

        yield return new TestCaseData("String", "plain").Returns("plain").SetName("String");
        yield return new TestCaseData("String", "O'Brien").Returns("O'Brien").SetName("String with a quote");
        yield return new TestCaseData("String", @"a\b").Returns(@"a\b").SetName("String with a backslash");
        yield return new TestCaseData("String", "a\nb").Returns("a\nb").SetName("String with a newline");
        yield return new TestCaseData("String", "a\tb").Returns("a\tb").SetName("String with a tab");
        yield return new TestCaseData("String", string.Empty).Returns(string.Empty).SetName("Empty string");

        // A carriage return and a NUL need no escape, unlike a newline and a tab, which end the server's
        // reader. Pinned because the escape set is easy to widen or narrow by accident.
        yield return new TestCaseData("String", "a\rb").Returns("a\rb").SetName("String with a carriage return");
        yield return new TestCaseData("String", "a\0b").Returns("a\0b").SetName("String with a NUL");
        yield return new TestCaseData("String", @"ends\").Returns(@"ends\").SetName("String ending in a backslash");
        yield return new TestCaseData("String", "héllo").Returns("héllo").SetName("String with non-ASCII");
        yield return new TestCaseData("String", "' OR 1=1 --").Returns("' OR 1=1 --").SetName("Injection attempt stays data");
        yield return new TestCaseData("LowCardinality(String)", "x").Returns("x").SetName("LowCardinality");
        yield return new TestCaseData("Enum8('a' = 1, 'b' = 2)", "a").Returns("a").SetName("Enum label");

        yield return new TestCaseData("UUID", Guid.Parse("61f0c404-5cb3-11e7-907b-a6006ad3dba0"))
            .Returns("61f0c404-5cb3-11e7-907b-a6006ad3dba0").SetName("UUID");
        yield return new TestCaseData("IPv4", IPAddress.Parse("192.168.1.1")).Returns("192.168.1.1").SetName("IPv4");
        yield return new TestCaseData("IPv6", IPAddress.Parse("::1")).Returns("::1").SetName("IPv6");

        yield return new TestCaseData("Date", new DateOnly(2024, 1, 2)).Returns("2024-01-02").SetName("Date");
        yield return new TestCaseData("DateTime", new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified))
            .Returns("2024-01-02 03:04:05").SetName("DateTime");
        yield return new TestCaseData("DateTime64(3)", new DateTime(2024, 1, 2, 3, 4, 5, 123, DateTimeKind.Unspecified))
            .Returns("2024-01-02 03:04:05.123").SetName("DateTime64");

        yield return new TestCaseData("Array(Int32)", new[] { 1, 2, 3 }).Returns("[1,2,3]").SetName("Array of Int32");
        yield return new TestCaseData("Array(String)", new[] { "a", "b" }).Returns("['a','b']").SetName("Array of String");
        yield return new TestCaseData("Array(String)", new[] { "O'B" }).Returns(@"['O\'B']").SetName("Array element with a quote");
        yield return new TestCaseData("Array(Int32)", Array.Empty<int>()).Returns("[]").SetName("Empty array");
        yield return new TestCaseData("Array(Array(Int32))", new[] { new[] { 1, 2 }, new[] { 3 } }).Returns("[[1,2],[3]]").SetName("Jagged array");
        yield return new TestCaseData("Array(Nullable(Int32))", new int?[] { 1, null, 3 }).Returns("[1,NULL,3]").SetName("Array with a null element");
        yield return new TestCaseData("Tuple(String, Int32)", ("a", 1)).Returns("('a',1)").SetName("Tuple");
        yield return new TestCaseData("Tuple(String, Int32)", ("O'B", 1)).Returns(@"('O\'B',1)").SetName("Tuple element with a quote");

        // Types whose text form the unit tests pin but no round-trip reached, so nothing proved the server
        // reads back what the formatter writes.
        yield return new TestCaseData("Date32", new DateOnly(1950, 3, 4)).Returns("1950-03-04").SetName("Date32");
        yield return new TestCaseData("Time", new TimeSpan(1, 1, 1)).Returns("01:01:01").SetName("Time");
        yield return new TestCaseData("Time64(3)", new TimeSpan(0, 1, 1, 1, 500)).Returns("01:01:01.500").SetName("Time64");
        yield return new TestCaseData("FixedString(3)", Encoding.UTF8.GetBytes("abc")).Returns("abc").SetName("FixedString from bytes");
        yield return new TestCaseData("String", Encoding.UTF8.GetBytes("abc")).Returns("abc").SetName("String from bytes");
        yield return new TestCaseData("IntervalSecond", 5L).Returns("5").SetName("IntervalSecond");
        yield return new TestCaseData("IntervalDay", -3L).Returns("-3").SetName("IntervalDay negative");
        yield return new TestCaseData("Nested(a UInt8, b String)", new object[] { (1, "x"), (2, "y") })
            .Returns("[(1,'x'),(2,'y')]").SetName("Nested rows");
        yield return new TestCaseData("Variant(Int64, String)", 7L).Returns("7").SetName("Variant picks the integer");
        yield return new TestCaseData("Variant(Int64, String)", "x").Returns("x").SetName("Variant picks the string");
    }

    [Test]
    public async Task QueryAsync_MapWithSpecialCharacters_RoundTripsThroughTheServer()
    {
        await using var client = TcpServerFixture.CreateClient();
        var value = new Dictionary<string, string> { ["k'1"] = @"v\1" };
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "p", value } },
        };

        object read = await ScalarAsync(client, "SELECT toString({p:Map(String, String)})", options);

        Assert.That(read, Is.EqualTo(@"{'k\'1':'v\\1'}"));
    }

    [TestCase("limit")]
    [TestCase("offset")]
    public async Task QueryAsync_ParameterNamedAfterAServerSetting_NeverBindsToTheWrongValue(string name)
    {
        // The parameter list is the settings list, so a server that reads the name as that setting applies it
        // instead of binding it. Whether it does is version-dependent: 25.8 through 26.6 reject the query, and
        // newer servers bind it correctly. Both are acceptable; a wrong count is not, and that is the outcome
        // this pins. See the remark on ClickHouseTcpQueryOptions.Parameters.
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { name, 3 } },
        };
        string sql = $"SELECT count() FROM (SELECT number FROM numbers(10) LIMIT {{{name}:UInt64}})";

        try
        {
            Assert.That(await ScalarAsync(client, sql, options), Is.EqualTo(3UL));
        }
        catch (ClickHouseServerException)
        {
            Assert.Pass($"The server applied '{name}' as a setting and rejected the query, which is the older behaviour.");
        }
    }

    [Test]
    public async Task QueryAsync_SameParameterRenamedOffASettingName_Works()
    {
        // The other half of the case above: the query is fine, only the name was.
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "row_limit", 3 } },
        };

        object value = await ScalarAsync(
            client, "SELECT count() FROM (SELECT number FROM numbers(10) LIMIT {row_limit:UInt64})", options);

        Assert.That(value, Is.EqualTo(3UL));
    }

    [Test]
    public async Task QueryAsync_SequenceReadableOnlyOnce_IsNotConsumedByTypeInference()
    {
        // With no placeholder the client infers the type, which reads the sequence, and then formats it, which
        // reads it again. A one-shot sequence would arrive empty the second time.
        await using var client = TcpServerFixture.CreateClient();
        IEnumerable<int> once = OneShot();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "p", once } },
        };

        object value = await ScalarAsync(client, "SELECT toString({p:Array(Int32)})", options);

        Assert.That(value, Is.EqualTo("[1,2,3]"));

        static IEnumerable<int> OneShot()
        {
            yield return 1;
            yield return 2;
            yield return 3;
        }
    }

    [TestCaseSource(nameof(RoundTripCases))]
    public async Task<string> QueryAsync_ParameterOfDeclaredType_RoundTripsThroughTheServer(string clickHouseType, object value)
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "p", value } },
        };

        return (string)await ScalarAsync(client, "SELECT toString({p:" + clickHouseType + "})", options);
    }

    [Test]
    public async Task QueryAsync_MapParameter_RoundTripsThroughTheServer()
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "p", new Dictionary<string, int> { ["a"] = 1 } } },
        };

        object value = await ScalarAsync(client, "SELECT toString({p:Map(String, Int32)})", options);

        Assert.That(value, Is.EqualTo("{'a':1}"));
    }

    [Test]
    public async Task QueryAsync_NullParameter_ArrivesAsNull()
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "p", null } },
        };

        object value = await ScalarAsync(client, "SELECT isNull({p:Nullable(String)})", options);

        Assert.That(value, Is.EqualTo((byte)1));
    }

    [Test]
    public async Task QueryAsync_StringParameterHoldingTheNullMarker_StaysText()
    {
        // The marker only means null when the client wrote it for a null value; a caller's own backslash-N is data.
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "p", @"\N" } },
        };

        object value = await ScalarAsync(client, "SELECT toString({p:String})", options);

        Assert.That(value, Is.EqualTo(@"\N"));
    }

    [Test]
    public async Task QueryAsync_IdentifierParameter_NamesAColumn()
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "column", "value" } },
        };

        object value = await ScalarAsync(client, "SELECT {column:Identifier} FROM (SELECT 'found' AS value)", options);

        Assert.That(value, Is.EqualTo("found"));
    }

    [Test]
    public async Task QueryAsync_SeveralParameters_AllReachTheServer()
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "a", "x" }, { "b", 7 } },
        };

        object value = await ScalarAsync(client, "SELECT concat({a:String}, toString({b:Int32}))", options);

        Assert.That(value, Is.EqualTo("x7"));
    }

    [Test]
    public async Task QueryAsync_ParameterTheQueryDoesNotName_IsIgnoredByTheServer()
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "used", 1 }, { "unused", "spare" } },
        };

        object value = await ScalarAsync(client, "SELECT {used:Int32}", options);

        Assert.That(value, Is.EqualTo(1));
    }

    [Test]
    public async Task QueryAsync_ExplicitTypeOverride_FormatsAsTheOverride()
    {
        // The value is a DateTimeOffset naming an instant; the override moves it into the declared zone.
        await using var client = TcpServerFixture.CreateClient();
        var instant = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero);
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "p", instant, "DateTime('Europe/Amsterdam')" } },
        };

        object value = await ScalarAsync(client, "SELECT toString({p:DateTime('Europe/Amsterdam')})", options);

        Assert.That(value, Is.EqualTo("2024-01-02 04:04:05"));
    }

    [Test]
    public async Task InsertAsync_ParameterizedTarget_WritesToTheNamedTable()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = $"tcp_param_test_{Guid.NewGuid():N}";
        await client.ExecuteAsync($"CREATE TABLE {table} (id Int32) ENGINE = Memory", cancellationToken: None);
        try
        {
            var options = new ClickHouseTcpQueryOptions
            {
                Parameters = new ClickHouseTcpParameterCollection { { "threshold", 2 } },
            };

            await client.ExecuteAsync($"INSERT INTO {table} SELECT number FROM numbers(5) WHERE number > {{threshold:Int32}}", options, None);
            object count = await ScalarAsync(client, $"SELECT count() FROM {table}", null);

            Assert.That(count, Is.EqualTo(2UL));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task QueryAsync_ParameterValueTheDeclaredTypeRejects_ReportsTheServerError()
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "p", "not-a-number" } },
        };

        Assert.ThrowsAsync<ClickHouseServerException>(
            async () => await ScalarAsync(client, "SELECT {p:Int32}", options));
    }

    private static async Task<object> ScalarAsync(ClickHouseTcpClient client, string sql, ClickHouseTcpQueryOptions options)
    {
        await foreach (object[] row in client.QueryAsync(sql, options, None))
        {
            return row[0];
        }

        return null;
    }
}
