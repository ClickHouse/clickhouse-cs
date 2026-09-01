using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// Live-server coverage for the two parsing stages native parameter values cross.
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
        yield return new TestCaseData("Float32", 1.5f).Returns("1.5").SetName("Float32");
        yield return new TestCaseData("Bool", true).Returns("true").SetName("Bool");
        yield return new TestCaseData("Bool", false).Returns("false").SetName("Bool false");
        yield return new TestCaseData("Decimal64(4)", 1.2345m).Returns("1.2345").SetName("Decimal64");

        // The names a caller writes rather than the ones a header carries. The hint reaches the server verbatim,
        // so these prove the server takes the same spellings the client resolves — including a two-word alias and
        // one nested inside a composite.
        yield return new TestCaseData("VARCHAR", "abc").Returns("abc").SetName("VARCHAR, an alias of String");
        yield return new TestCaseData("BIGINT", -5L).Returns("-5").SetName("BIGINT, an alias of Int64");
        yield return new TestCaseData("DOUBLE PRECISION", 1.5d).Returns("1.5").SetName("DOUBLE PRECISION, a two-word alias");
        yield return new TestCaseData("Boolean", true).Returns("true").SetName("Boolean, an alias of Bool");
        yield return new TestCaseData("datetime64(3)", new DateTime(2024, 1, 2, 3, 4, 5, 123, DateTimeKind.Unspecified))
            .Returns("2024-01-02 03:04:05.123").SetName("A case variant of a case-insensitive family");
        yield return new TestCaseData("Array(TINYINT UNSIGNED)", new byte[] { 1, 2 }).Returns("[1,2]").SetName("An alias inside a composite");

        // The server accepts .NET's NaN and Infinity spellings.
        yield return new TestCaseData("Float64", double.NaN).Returns("nan").SetName("Float64 NaN");
        yield return new TestCaseData("Float64", double.PositiveInfinity).Returns("inf").SetName("Float64 +Infinity");
        yield return new TestCaseData("Float64", double.NegativeInfinity).Returns("-inf").SetName("Float64 -Infinity");

        // Composite Bool values require true/false; 1/0 works only for scalars.
        yield return new TestCaseData("Array(Bool)", new[] { true, false }).Returns("[true,false]").SetName("Array of Bool");

        yield return new TestCaseData("String", "plain").Returns("plain").SetName("String");
        yield return new TestCaseData("String", "O'Brien").Returns("O'Brien").SetName("String with a quote");
        yield return new TestCaseData("String", @"a\b").Returns(@"a\b").SetName("String with a backslash");
        yield return new TestCaseData("String", "a\nb").Returns("a\nb").SetName("String with a newline");
        yield return new TestCaseData("String", "a\tb").Returns("a\tb").SetName("String with a tab");
        yield return new TestCaseData("String", string.Empty).Returns(string.Empty).SetName("Empty string");

        // Carriage return and NUL are data; newline and tab terminate the reader unless escaped.
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

        // The range ends, where a formatter that overflows or clamps shows it.
        yield return new TestCaseData("Date", new DateOnly(1970, 1, 1)).Returns("1970-01-01").SetName("Date epoch");
        yield return new TestCaseData("Date", new DateOnly(2149, 6, 6)).Returns("2149-06-06").SetName("Date maximum");
        yield return new TestCaseData("Date32", new DateOnly(1900, 1, 1)).Returns("1900-01-01").SetName("Date32 minimum");
        yield return new TestCaseData("Date32", new DateOnly(2299, 12, 31)).Returns("2299-12-31").SetName("Date32 maximum");

        // Pre-epoch, where a naive seconds-and-fraction split puts the sign on the wrong part.
        yield return new TestCaseData("DateTime64(3, 'UTC')", new DateTime(1969, 12, 31, 23, 59, 59, 500, DateTimeKind.Utc))
            .Returns("1969-12-31 23:59:59.500").SetName("DateTime64 just before the epoch");
        yield return new TestCaseData("DateTime64(9)", new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified).AddTicks(1234567))
            .Returns("2024-01-02 03:04:05.123456700").SetName("DateTime64 at scale 9");

        yield return new TestCaseData("Array(Int32)", new[] { 1, 2, 3 }).Returns("[1,2,3]").SetName("Array of Int32");
        yield return new TestCaseData("Array(String)", new[] { "a", "b" }).Returns("['a','b']").SetName("Array of String");
        yield return new TestCaseData("Array(String)", new[] { "O'B" }).Returns(@"['O\'B']").SetName("Array element with a quote");
        yield return new TestCaseData("Array(Int32)", Array.Empty<int>()).Returns("[]").SetName("Empty array");
        yield return new TestCaseData("Array(Array(Int32))", new[] { new[] { 1, 2 }, new[] { 3 } }).Returns("[[1,2],[3]]").SetName("Jagged array");
        yield return new TestCaseData("Array(Nullable(Int32))", new int?[] { 1, null, 3 }).Returns("[1,NULL,3]").SetName("Array with a null element");
        yield return new TestCaseData("Tuple(String, Int32)", ("a", 1)).Returns("('a',1)").SetName("Tuple");
        yield return new TestCaseData("Tuple(String, Int32)", ("O'B", 1)).Returns(@"('O\'B',1)").SetName("Tuple element with a quote");

        // Covers empty, escaped-key, non-string-key, and nested map literals.
        yield return new TestCaseData("Map(String, String)", new Dictionary<string, string>())
            .Returns("{}").SetName("Empty map");
        yield return new TestCaseData("Map(String, UInt8)", new Dictionary<string, byte> { [@"a'b\c"] = 1 })
            .Returns(@"{'a\'b\\c':1}").SetName("Map key needing escapes");
        yield return new TestCaseData("Map(Bool, String)", new Dictionary<bool, string> { [true] = "x" })
            .Returns("{true:'x'}").SetName("Map with a Bool key");
        yield return new TestCaseData("Map(String, Array(Bool))", new Dictionary<string, bool[]> { ["a"] = [true, false] })
            .Returns("{'a':[true,false]}").SetName("Map holding an array");

        // Adds live-server coverage for formatter-only unit cases.
        yield return new TestCaseData("Date32", new DateOnly(1950, 3, 4)).Returns("1950-03-04").SetName("Date32");
        yield return new TestCaseData("Time", new TimeSpan(1, 1, 1)).Returns("01:01:01").SetName("Time");
        yield return new TestCaseData("Time64(3)", new TimeSpan(0, 1, 1, 1, 500)).Returns("01:01:01.500").SetName("Time64");

        // The time-of-day type, which the shipped HTTP driver takes for both of these.
        yield return new TestCaseData("Time", new TimeOnly(1, 1, 1)).Returns("01:01:01").SetName("Time from a TimeOnly");
        yield return new TestCaseData("Time64(3)", new TimeOnly(1, 1, 1, 500)).Returns("01:01:01.500").SetName("Time64 from a TimeOnly");
        yield return new TestCaseData("Time64(7)", TimeOnly.MaxValue).Returns("23:59:59.9999999").SetName("Time64 from the last tick of the day");
        yield return new TestCaseData("FixedString(3)", Encoding.UTF8.GetBytes("abc")).Returns("abc").SetName("FixedString from bytes");
        yield return new TestCaseData("String", Encoding.UTF8.GetBytes("abc")).Returns("abc").SetName("String from bytes");
        yield return new TestCaseData("IntervalSecond", 5L).Returns("5").SetName("IntervalSecond");
        yield return new TestCaseData("IntervalDay", -3L).Returns("-3").SetName("IntervalDay negative");
        yield return new TestCaseData("Nested(a UInt8, b String)", new object[] { (1, "x"), (2, "y") })
            .Returns("[(1,'x'),(2,'y')]").SetName("Nested rows");
        yield return new TestCaseData("Variant(Int64, String)", 7L).Returns("7").SetName("Variant picks the integer");
        yield return new TestCaseData("Variant(Int64, String)", "x").Returns("x").SetName("Variant picks the string");

        // Covers both JSON spellings and geo types backed by tuple/array shapes.
        yield return new TestCaseData("Json", "{\"a\":1}").Returns("{\"a\":1}").SetName("Json in the lowercase spelling");
        yield return new TestCaseData("JSON", "{\"a\":1}").Returns("{\"a\":1}").SetName("JSON in the uppercase spelling");
        yield return new TestCaseData("JSON", "{\n\t\"a\": 1,\n\t\"b\": \"x\"\n}")
            .Returns("{\"a\":1,\"b\":\"x\"}").SetName("JSON with formatting whitespace");
        yield return new TestCaseData("Variant(JSON, UInt64)", new Dictionary<string, int> { ["a"] = 1 })
            .Returns("{\"a\":1}").SetName("Variant picks the JSON alternative");
        yield return new TestCaseData("Point", (10.0, 20.0)).Returns("(10,20)").SetName("Point");
        yield return new TestCaseData("Ring", new[] { (0.0, 0.0), (1.0, 1.0) }).Returns("[(0,0),(1,1)]").SetName("Ring");
        yield return new TestCaseData("LineString", new[] { (0.0, 0.0), (1.0, 1.0) }).Returns("[(0,0),(1,1)]").SetName("LineString");
        yield return new TestCaseData("Polygon", new[] { new[] { (0.0, 0.0), (1.0, 0.0), (1.0, 1.0) } })
            .Returns("[[(0,0),(1,0),(1,1)]]").SetName("Polygon");
        yield return new TestCaseData("MultiPolygon", new[] { new[] { new[] { (0.0, 0.0), (1.0, 0.0), (1.0, 1.0) } } })
            .Returns("[[[(0,0),(1,0),(1,1)]]]").SetName("MultiPolygon");

        // A geo alternative is matched by the shape its name stands for; the name alone fits no CLR value.
        yield return new TestCaseData("Variant(Point, String)", (10.0, 20.0)).Returns("(10,20)")
            .SetName("Variant holding a Point");
        yield return new TestCaseData("Variant(Ring, String)", new[] { (0.0, 0.0), (1.0, 1.0) }).Returns("[(0,0),(1,1)]")
            .SetName("Variant holding a Ring");

        // Bytes reach the String arm from a ReadOnlyMemory as well, which used to print the CLR type name.
        yield return new TestCaseData("String", new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("héllo"))).Returns("héllo")
            .SetName("String from a ReadOnlyMemory of bytes");

        if (TcpServerFeatures.Has(TcpFeature.QBit))
        {
            yield return new TestCaseData("QBit(Float32, 4)", new[] { 1f, 2f, 3f, 4f }).Returns("[1,2,3,4]").SetName("QBit");
        }

        // An Enum bound by its numeric value rather than its label. Neither transport had a case for it.
        yield return new TestCaseData("Enum8('a' = 1, 'b' = 2)", 2).Returns("b").SetName("Enum by number");

        // A bare Enum, whose width the client has to pick before it can format the value. The hint reaches the
        // server verbatim, so this is what shows the server reads the same spelling the client resolved.
        yield return new TestCaseData("Enum('a' = 1, 'b' = 2)", "b").Returns("b").SetName("Enum with no width");
        yield return new TestCaseData("Enum('a' = 1, 'b' = 200)", "b").Returns("b").SetName("Enum with no width, past the Int8 range");

        // A wide decimal past what a CLR decimal can hold, so the BigInteger path is the one under test.
        yield return new TestCaseData("Decimal128(0)", new string('1', 30)).Returns(new string('1', 30)).SetName("Decimal128 of 30 digits");
        yield return new TestCaseData("Decimal256(0)", new string('1', 50)).Returns(new string('1', 50)).SetName("Decimal256 of 50 digits");

        // Negative and past-24h durations, where the sign belongs on the whole value and not on one part.
        yield return new TestCaseData("Time", new TimeSpan(-5, -25, -5)).Returns("-05:25:05").SetName("Time negative");
        yield return new TestCaseData("Time", new TimeSpan(55, 25, 5)).Returns("55:25:05").SetName("Time past 24 hours");
        yield return new TestCaseData("Time64(6)", new TimeSpan(-(new TimeSpan(5, 25, 5) + TimeSpan.FromTicks(1234560)).Ticks))
            .Returns("-05:25:05.123456").SetName("Time64 negative");

        // A surrogate pair, which a formatter that walks chars rather than runes can split.
        yield return new TestCaseData("String", "a\U0001F600b").Returns("a\U0001F600b").SetName("String with an emoji");

        // A ValueTuple past 7 elements nests its tail in TRest, which ITuple flattens back out.
        yield return new TestCaseData("Tuple(Int32, Int32, Int32, Int32, Int32, Int32, Int32, String, String)",
            (1, 2, 3, 4, 5, 6, 7, "eight", "nine")).Returns("(1,2,3,4,5,6,7,'eight','nine')").SetName("Tuple of nine");
    }

    // Compare hex to verify non-UTF-8 bytes survive without replacement-character conversion.
    [TestCase("String", "FFFE41", TestName = "String keeps bytes that are not UTF-8")]
    [TestCase("FixedString(3)", "FFFE41", TestName = "FixedString keeps bytes that are not UTF-8")]
    public async Task QueryAsync_ByteArrayThatIsNotUtf8_ArrivesByteForByte(string clickHouseType, string expectedHex)
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "p", new byte[] { 0xFF, 0xFE, 0x41 } } },
        };

        object read = await ScalarAsync(client, "SELECT hex({p:" + clickHouseType + "})", options);

        Assert.That(read, Is.EqualTo(expectedHex));
    }

    [Test]
    public async Task QueryAsync_ByteArrayHoldingANulAndHighBytes_ArrivesByteForByte()
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "p", new byte[] { 0x00, 0x01, 0xFF, 0x80 } } },
        };

        object read = await ScalarAsync(client, "SELECT hex({p:String})", options);

        Assert.That(read, Is.EqualTo("0001FF80"));
    }

    [Test]
    public async Task QueryAsync_ByteArrayThatIsValidUtf8_KeepsTheReadableForm()
    {
        // The fallback must not swallow the common case: valid UTF-8 still travels as text, not as \xHH.
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "p", Encoding.UTF8.GetBytes("héllo") } },
        };

        object read = await ScalarAsync(client, "SELECT {p:String}", options);

        Assert.That(read, Is.EqualTo("héllo"));
    }

    [Test]
    public async Task QueryAsync_MapReadBackAsPairs_CanBeBoundAgain()
    {
        // Map reads as KeyValuePair[] to preserve order and duplicate keys; that shape must be reusable.
        await using var client = TcpServerFixture.CreateClient();
        KeyValuePair<string, int>[] pairs = [new("b", 2), new("a", 1)];
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "p", pairs } },
        };

        object read = await ScalarAsync(client, "SELECT toString({p:Map(String, Int32)})", options);

        Assert.That(read, Is.EqualTo("{'b':2,'a':1}"));
    }

    [Test]
    public async Task QueryAsync_SqlStringHoldingABackslashEscapedQuote_RunsAsWritten()
    {
        // Scanner unit tests cover hint detection; this verifies the escaped SQL reaches the server unchanged.
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "p", 7 } },
        };

        object read = await ScalarAsync(client, @"SELECT concat('it\'s ', toString({p:Int32}))", options);

        Assert.That(read, Is.EqualTo("it's 7"));
    }

    [Test]
    public async Task QueryAsync_KindedDateTimeWithATimezoneInTheHint_KeepsTheInstant()
    {
        // A declared timezone keeps the instant independent of the session timezone.
        await using var client = TcpServerFixture.CreateClient();
        var instant = new DateTimeOffset(2020, 1, 2, 12, 0, 0, TimeSpan.FromHours(9));
        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["session_timezone"] = "Asia/Tokyo" },
            Parameters = new ClickHouseTcpParameterCollection { { "d", instant } },
        };

        object epoch = await ScalarAsync(client, "SELECT toUnixTimestamp({d:DateTime('UTC')})", options);

        Assert.That(Convert.ToInt64(epoch), Is.EqualTo(instant.ToUnixTimeSeconds()));
    }

    [Test]
    public async Task QueryAsync_KindedDateTimeWithABareHint_IsRefusedBeforeItReachesTheServer()
    {
        // Refuse before the server can reinterpret the instant in its session timezone.
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["session_timezone"] = "Asia/Tokyo" },
            Parameters = new ClickHouseTcpParameterCollection
            {
                { "d", new DateTimeOffset(2020, 1, 2, 12, 0, 0, TimeSpan.FromHours(9)) },
            },
        };

        var exception = Assert.ThrowsAsync<ArgumentException>(
            async () => await ScalarAsync(client, "SELECT toUnixTimestamp({d:DateTime})", options));

        Assert.That(exception.Message, Does.Contain("declares no timezone"));
    }

    [Test]
    public async Task QueryAsync_UnspecifiedDateTimeWithABareHint_IsReadInTheSessionTimezone()
    {
        // An unspecified DateTime is wall-clock time, so the session timezone may interpret it.
        await using var client = TcpServerFixture.CreateClient();
        var wallClock = new DateTime(2020, 1, 2, 12, 0, 0, DateTimeKind.Unspecified);
        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["session_timezone"] = "Asia/Tokyo" },
            Parameters = new ClickHouseTcpParameterCollection { { "d", wallClock } },
        };

        object read = await ScalarAsync(client, "SELECT toString({d:DateTime})", options);

        Assert.That(read, Is.EqualTo("2020-01-02 12:00:00"));
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
        // ClickHouse 25.8 through 26.6 may treat this as a setting and reject it; newer versions bind it.
        // Either result is safe, but binding the wrong value is not.
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
        catch (ClickHouseTcpServerException)
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
    public async Task QueryAsync_LazySequenceParameter_ReachesTheServerWhole()
    {
        // A typed placeholder must enumerate the lazy value only once.
        await using var client = TcpServerFixture.CreateClient();
        IEnumerable<int> lazy = Enumerable.Range(1, 3).Select(i => i);
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "p", lazy } },
        };

        object value = await ScalarAsync(client, "SELECT toString({p:Array(Int32)})", options);

        Assert.That(value, Is.EqualTo("[1,2,3]"));
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
            Parameters = new ClickHouseTcpParameterCollection { { "used", 1 }, { "unused", "spare", "String" } },
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
    public async Task ExecuteAsync_ParameterizedInsertSelect_WritesOnlyTheMatchingRows()
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

    // Exercise both insert paths by parameterizing the target; ExecuteAsync uses a different path.
    [Test]
    public async Task InsertRowsAsync_ParameterizedTarget_WritesToTheNamedTable()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = $"tcp_param_rows_{Guid.NewGuid():N}";
        await client.ExecuteAsync($"CREATE TABLE {table} (id Int32) ENGINE = Memory", cancellationToken: None);
        try
        {
            var options = new ClickHouseTcpInsertOptions
            {
                Parameters = new ClickHouseTcpParameterCollection { { "target", table, "Identifier" } },
            };
            object[][] rows = [[1], [2], [3]];

            await client.InsertRowsAsync("INSERT INTO {target:Identifier} (id) VALUES", rows, options, None);

            object sum = await ScalarAsync(client, $"SELECT sum(id) FROM {table}", null);
            Assert.That(Convert.ToInt64(sum), Is.EqualTo(6));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task InsertAsync_ColumnsWithAParameterizedTarget_WritesToTheNamedTable()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = $"tcp_param_cols_{Guid.NewGuid():N}";
        await client.ExecuteAsync($"CREATE TABLE {table} (id Int32) ENGINE = Memory", cancellationToken: None);
        try
        {
            var options = new ClickHouseTcpInsertOptions
            {
                Parameters = new ClickHouseTcpParameterCollection { { "target", table, "Identifier" } },
            };
            IReadOnlyList<IColumn> columns = [new ArrayColumn<int>("id", "Int32", new[] { 4, 5 })];

            await client.InsertAsync("INSERT INTO {target:Identifier} (id) VALUES", columns, options, None);

            object sum = await ScalarAsync(client, $"SELECT sum(id) FROM {table}", null);
            Assert.That(Convert.ToInt64(sum), Is.EqualTo(9));
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

        Assert.ThrowsAsync<ClickHouseTcpServerException>(
            async () => await ScalarAsync(client, "SELECT {p:Int32}", options));
    }

    // Each of these is a query the server runs, holding a brace the scanner must not read as a placeholder.
    // Reaching past the brace for a colon took the hint of the parameter after it, and binding then failed
    // on a query the server would have answered.
    [TestCase("SELECT 1 AS \"col{x}\", {p:Int32}", TestName = "brace inside a double-quoted identifier")]
    [TestCase("SELECT 1 AS `col{x}`, {p:Int32}", TestName = "brace inside a backtick-quoted identifier")]
    [TestCase("SELECT $$ {x} $$ != '', {p:Int32}", TestName = "brace inside a heredoc")]
    [TestCase("SELECT 1 // {x}\n, {p:Int32}", TestName = "brace inside a double-slash comment")]
    public async Task QueryAsync_BraceThatIsNotAPlaceholder_StillBindsTheRealParameter(string sql)
    {
        await using var client = TcpServerFixture.CreateClient();
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "p", 7 } },
        };

        object read = null;
        await foreach (object[] row in client.QueryAsync(sql, options, None))
        {
            read = row[1];
        }

        Assert.That(read, Is.EqualTo(7));
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
