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
        yield return new TestCaseData("Float32", 1.5f).Returns("1.5").SetName("Float32");
        yield return new TestCaseData("Bool", true).Returns("true").SetName("Bool");
        yield return new TestCaseData("Bool", false).Returns("false").SetName("Bool false");
        yield return new TestCaseData("Decimal64(4)", 1.2345m).Returns("1.2345").SetName("Decimal64");

        // .NET spells these NaN/Infinity and ClickHouse spells them nan/inf. The server's reader accepts both,
        // so the formatter needs no special case — but only a round-trip can show that.
        yield return new TestCaseData("Float64", double.NaN).Returns("nan").SetName("Float64 NaN");
        yield return new TestCaseData("Float64", double.PositiveInfinity).Returns("inf").SetName("Float64 +Infinity");
        yield return new TestCaseData("Float64", double.NegativeInfinity).Returns("-inf").SetName("Float64 -Infinity");

        // A Bool inside a composite must stay true/false. The server rejects 1/0 there, though it takes them
        // for a scalar Bool, so a formatter that emitted digits would pass the scalar case and fail this one.
        yield return new TestCaseData("Array(Bool)", new[] { true, false }).Returns("[true,false]").SetName("Array of Bool");

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

        // Map shapes past the one-pair case: the empty literal, a key needing escapes, a non-string key, and
        // two levels of nesting. Each has its own way of producing text the server will not read back.
        yield return new TestCaseData("Map(String, String)", new Dictionary<string, string>())
            .Returns("{}").SetName("Empty map");
        yield return new TestCaseData("Map(String, UInt8)", new Dictionary<string, byte> { [@"a'b\c"] = 1 })
            .Returns(@"{'a\'b\\c':1}").SetName("Map key needing escapes");
        yield return new TestCaseData("Map(Bool, String)", new Dictionary<bool, string> { [true] = "x" })
            .Returns("{true:'x'}").SetName("Map with a Bool key");
        yield return new TestCaseData("Map(String, Array(Bool))", new Dictionary<string, bool[]> { ["a"] = [true, false] })
            .Returns("{'a':[true,false]}").SetName("Map holding an array");

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

        // Types the HTTP formatter accepts and this one used to reject. The server takes both JSON spellings,
        // and the geo names stand for shapes over Point, so each must reach the arm it stands for.
        yield return new TestCaseData("Json", "{\"a\":1}").Returns("{\"a\":1}").SetName("Json in the lowercase spelling");
        yield return new TestCaseData("JSON", "{\"a\":1}").Returns("{\"a\":1}").SetName("JSON in the uppercase spelling");
        yield return new TestCaseData("Point", (10.0, 20.0)).Returns("(10,20)").SetName("Point");
        yield return new TestCaseData("Ring", new[] { (0.0, 0.0), (1.0, 1.0) }).Returns("[(0,0),(1,1)]").SetName("Ring");
        yield return new TestCaseData("LineString", new[] { (0.0, 0.0), (1.0, 1.0) }).Returns("[(0,0),(1,1)]").SetName("LineString");
        yield return new TestCaseData("Polygon", new[] { new[] { (0.0, 0.0), (1.0, 0.0), (1.0, 1.0) } })
            .Returns("[[(0,0),(1,0),(1,1)]]").SetName("Polygon");
        yield return new TestCaseData("MultiPolygon", new[] { new[] { new[] { (0.0, 0.0), (1.0, 0.0), (1.0, 1.0) } } })
            .Returns("[[[(0,0),(1,0),(1,1)]]]").SetName("MultiPolygon");

        if (TcpServerFeatures.Has(TcpFeature.QBit))
        {
            yield return new TestCaseData("QBit(Float32, 4)", new[] { 1f, 2f, 3f, 4f }).Returns("[1,2,3,4]").SetName("QBit");
        }

        // An Enum bound by its numeric value rather than its label. Neither transport had a case for it.
        yield return new TestCaseData("Enum8('a' = 1, 'b' = 2)", 2).Returns("b").SetName("Enum by number");

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

    // A ClickHouse String holds bytes, not characters, so a value can hold a sequence that is not UTF-8 —
    // which is exactly what the read path returns for a byte-array column. Decoding it turned every bad byte
    // into U+FFFD and sent EF BF BD, changing the value with no error. Compared as hex, because the whole
    // point is the bytes rather than the text.
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
        // A Map column reads back as KeyValuePair[] so duplicate keys and pair order survive. Binding that
        // value straight back used to hit the "cannot convert" arm, because only IDictionary was accepted.
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
        // Coverage of the scanner fix itself lives in SqlParameterTypeExtractorTests, where the mis-scan is
        // observable. It is not observable here: whichever type the client resolves, a plain value formats to
        // the same text, so this only pins that the server accepts the query and the escape survives it.
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
        // The safe form. The client moves the instant into the timezone the hint declares, and the server reads
        // the wall-clock text back in that same timezone, so the two agree whatever the session is set to.
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
        // A bare {d:DateTime} hint names no timezone, so the server would read the wall-clock text in
        // session_timezone and the instant would move with no error raised. The client refuses instead. This
        // runs against the server to prove the refusal happens first, so no wrong value is ever stored.
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
        // The other half, and the reason the refusal above is limited to a value that names an instant. An
        // unspecified DateTime is a wall-clock time with no instant attached, so reading it in the session
        // timezone is what the caller asked for. It stays legal.
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
        // A lazy sequence must reach the server whole. The placeholder here means inference does not run, so
        // this covers the formatter's own single pass; BuildParametersTests covers the inference pass, which
        // needs SQL that does not name the parameter and so cannot be observed through a query at all.
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

    // The insert methods take their own options type and their own call paths, so the test above — which goes
    // through ExecuteAsync — proves nothing about them. A dropped parameters argument here would have
    // passed the whole suite. The target table is the parameter, which is the only place one fits in an
    // INSERT whose values arrive as a data block.
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

    private static async Task<object> ScalarAsync(ClickHouseTcpClient client, string sql, ClickHouseTcpQueryOptions options)
    {
        await foreach (object[] row in client.QueryAsync(sql, options, None))
        {
            return row[0];
        }

        return null;
    }
}
