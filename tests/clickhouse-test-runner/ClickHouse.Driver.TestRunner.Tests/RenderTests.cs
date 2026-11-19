using System.Net;
using System.Numerics;
using NUnit.Framework;

namespace ClickHouse.Driver.TestRunner.Tests;

// Ported from render_test.go of the clickhouse-go harness, with the .NET values of both clients.
public class RenderTests
{
    // The expected text is what ClickHouse 26.10 writes for FORMAT TSV.
    private static readonly object[] DoubleCases =
    [
        new object[] { 1e15, "1000000000000000" },
        new object[] { 1e20, "100000000000000000000" },
        new object[] { 1e21, "1e21" },
        new object[] { 1.2345e21, "1.2345e21" },
        new object[] { 1.2345e20, "123450000000000000000" },
        new object[] { 0.0001, "0.0001" },
        new object[] { 1e-6, "0.000001" },
        new object[] { 1.5e-6, "0.0000015" },
        new object[] { 1e-7, "1e-7" },
        new object[] { 1.5e-7, "1.5e-7" },
        new object[] { 1.5e300, "1.5e300" },
        new object[] { 0.30000000000000004, "0.30000000000000004" },
        new object[] { 5e-324, "5e-324" },
        new object[] { -12345678.9, "-12345678.9" },
        new object[] { 100.0, "100" },
        new object[] { double.NaN, "nan" },
        new object[] { double.PositiveInfinity, "inf" },
        new object[] { double.NegativeInfinity, "-inf" },
        new object[] { -0.0, "-0" },
        new object[] { 0.0, "0" },
    ];

    [TestCaseSource(nameof(DoubleCases))]
    public void FormatFloat_Double_WritesTheServerText(double value, string expected) =>
        Assert.That(ValueRenderer.FormatFloat(value), Is.EqualTo(expected));

    [TestCase(0.1f, "0.1")]
    [TestCase(3.4028235e38f, "3.4028235e38")]
    [TestCase(16777216f, "16777216")]
    public void FormatFloat_Single_WritesTheShortestFloat32Text(float value, string expected) =>
        Assert.That(ValueRenderer.FormatFloat(value), Is.EqualTo(expected));

    [TestCase("::ffff:1.2.3.4", "::ffff:1.2.3.4")]
    [TestCase("::1.2.3.4", "::1.2.3.4")]
    [TestCase("::", "::")]
    [TestCase("::1", "::1")]
    [TestCase("2001:db8::1", "2001:db8::1")]
    [TestCase("1::", "1::")]
    [TestCase("2001:db8:0:1::1", "2001:db8:0:1::1")]
    [TestCase("1:0:0:2:0:0:0:3", "1:0:0:2::3")]
    [TestCase("1:2:3:4:5:6:7:8", "1:2:3:4:5:6:7:8")]
    public void FormatIPv6_Address_FollowsFormatIPv6OfTheServer(string input, string expected) =>
        Assert.That(ValueRenderer.FormatIPv6(IPAddress.Parse(input).GetAddressBytes()), Is.EqualTo(expected));

    [TestCase("UInt8")]
    [TestCase("Nullable(String)")]
    [TestCase("Array(Nullable(DateTime64(3, 'UTC')))")]
    [TestCase("Map(String, Array(Tuple(UInt8, String)))")]
    [TestCase("Tuple(a Int32, b String)")]
    [TestCase("Enum8('a' = 1, 'b,c' = 2)")]
    [TestCase("Decimal(9, 2)")]
    [TestCase("LowCardinality(Nullable(String))")]
    [TestCase("SimpleAggregateFunction(sum, UInt64)")]
    [TestCase("AggregateFunction(quantiles(0.5, 0.9), UInt64)")]
    public void ParseChType_TypeName_RoundTrips(string input) =>
        Assert.That(ChType.Parse(input).ToString(), Is.EqualTo(input));

    [Test]
    public void ParseChType_QuotedElementName_IsTheName()
    {
        var t = ChType.Parse("Tuple(`a b` Int32, c Array(String))");
        Assert.That(t.Args, Has.Count.EqualTo(2));
        Assert.That(t.Args[0].Name, Is.EqualTo("a b"));
        Assert.That(t.Args[1].Name, Is.EqualTo("c"));
        Assert.That(t.Args[1].Type!.Base, Is.EqualTo("Array"));
        Assert.Throws<FormatException>(() => ChType.Parse("Array(String"));
    }

    private static readonly object?[] ValueCases =
    [
        new object?[] { "String", "a\tb\\c\nd'e\0\b\f\x01", "a\\tb\\\\c\\nd\\'e\\0\\b\\f\x01" },
        new object?[] { "Nullable(String)", null, "\\N" },
        new object?[] { "Nullable(String)", DBNull.Value, "\\N" },
        new object?[] { "Nullable(String)", "x", "x" },
        new object?[] { "FixedString(3)", new byte[] { 0x61, 0, 0xff }, "a\\0\xff" },
        new object?[] { "Array(Nullable(String))", new[] { "a'b", null }, "['a\\'b',NULL]" },
        new object?[] { "Tuple(UInt8, String, Nullable(UInt8))", Tuple.Create<byte, string, byte?>(1, "x", null), "(1,'x',NULL)" },
        new object?[] { "Tuple(a UInt8, b String)", ((byte)1, "x"), "(1,'x')" },
        new object?[] { "Tuple()", ValueTuple.Create(), "()" },
        new object?[] { "Map(String, UInt8)", new List<KeyValuePair<string, byte>> { new("k", 1), new("a", 2) }, "{'k':1,'a':2}" },
        new object?[] { "Map(String, UInt8)", new[] { new KeyValuePair<string, byte>("k", 1) }, "{'k':1}" },
        new object?[] { "Int128", new BigInteger(-5), "-5" },
        new object?[] { "Int128", (Int128)(-5), "-5" },
        new object?[] { "Decimal(9, 2)", 1.50m, "1.5" },
        new object?[] { "Decimal(9, 2)", 0.00m, "0" },
        new object?[] { "Decimal(18, 5)", -0.00100m, "-0.001" },
        new object?[] { "Decimal(9, 0)", 100m, "100" },
        new object?[] { "DateTime64(3)", new DateTime(2020, 1, 1, 0, 0, 0, 120), "2020-01-01 00:00:00.120" },
        new object?[] { "DateTime64(3, 'UTC')", 1577836800120L, "2020-01-01 00:00:00.120" },
        new object?[] { "DateTime64(1, 'UTC')", -1L, "1969-12-31 23:59:59.9" },
        // TCP counts outside the .NET date range: the server's text (year 0000; a year outside [0000, 9999]
        // clamps to the end of the range, the fraction stays).
        new object?[] { "DateTime64(6, 'UTC')", -62161776000000000L, "0000-03-04 00:00:00.000000" },
        new object?[] { "DateTime64(3, 'UTC')", long.MinValue, "0000-01-01 00:00:00.192" },
        new object?[] { "DateTime64(3, 'UTC')", -62167219200001L, "0000-01-01 00:00:00.999" },
        new object?[] { "DateTime64(3, 'UTC')", 9223372036854775616L, "9999-12-31 23:59:59.616" },
        new object?[] { "DateTime64(7, 'Asia/Istanbul')", 9223372036854775806L, "9999-12-31 23:59:59.4775806" },
        new object?[] { "DateTime64(8, 'Asia/Istanbul')", 9223372036854775806L, "4892-10-08 00:52:48.54775806" },
        new object?[] { "DateTime('Asia/Tokyo')", 1577836800u, "2020-01-01 09:00:00" },
        new object?[] { "DateTime", new DateTime(2020, 1, 1, 9, 0, 0), "2020-01-01 09:00:00" },
        new object?[] { "Array(Date)", new[] { new DateOnly(2020, 1, 2) }, "['2020-01-02']" },
        new object?[] { "Date", new DateTime(2020, 1, 2, 0, 0, 0, DateTimeKind.Utc), "2020-01-02" },
        new object?[] { "Time", -(new TimeSpan(1, 2, 3)), "-01:02:03" },
        new object?[] { "Time", -3723, "-01:02:03" },
        new object?[] { "Time64(3)", new TimeSpan(100, 0, 0) + TimeSpan.FromMilliseconds(500), "100:00:00.500" },
        new object?[] { "Time64(7)", 11234567L, "00:00:01.1234567" },
        // The server writes a value above 999:59:59 as 999:59:59.
        new object?[] { "Time64(2)", TimeSpan.FromHours(2160) + TimeSpan.FromSeconds(12), "999:59:59.00" },
        new object?[] { "Time", -7200000, "-999:59:59" },
        new object?[] { "UUID", Guid.Parse("61f0c404-5cb3-11e7-907b-a6006ad3dba0"), "61f0c404-5cb3-11e7-907b-a6006ad3dba0" },
        new object?[] { "IPv4", IPAddress.Parse("1.2.3.4"), "1.2.3.4" },
        new object?[] { "Array(IPv6)", new[] { IPAddress.Parse("::ffff:1.2.3.4") }, "['::ffff:1.2.3.4']" },
        new object?[] { "Bool", true, "true" },
        new object?[] { "Array(Float64)", new[] { double.NaN, double.PositiveInfinity }, "[nan,inf]" },
        new object?[] { "IntervalDay", 3L, "3" },
        new object?[] { "LowCardinality(String)", "a", "a" },
        new object?[] { "SimpleAggregateFunction(sum, UInt64)", 7UL, "7" },
        new object?[] { "Enum8('a' = 1, 'b' = 2)", "b", "b" },
        new object?[] { "Enum8('a' = 1, 'b' = 2)", (sbyte)2, "b" },
        new object?[] { "Array(Enum16('it''s' = -1))", new short[] { -1 }, "['it\\'s']" },
        new object?[] { "Point", (1.5, 2.5), "(1.5,2.5)" },
        new object?[] { "Ring", new[] { Tuple.Create(1.0, 2.0) }, "[(1,2)]" },
        new object?[] { "SimpleAggregateFunction(anyLast, JSON)", "{\"a\":1}", "{\"a\":1}" },
        // HTTP Variant: the .NET value tells the alternative.
        new object?[] { "Variant(String, UInt64)", "a\tb", "a\\tb" },
        new object?[] { "Variant(String, UInt64)", 7UL, "7" },
        new object?[] { "Variant(String, UInt64)", DBNull.Value, "\\N" },
        new object?[] { "Variant(Array(UInt64), String)", new ulong[] { 1, 2 }, "[1,2]" },
        new object?[] { "Variant(Array(UInt8), String)", new byte[] { 1, 2 }, "[1,2]" },
        new object?[] { "Array(Variant(String, UInt8))", new object[] { "a", (byte)1, DBNull.Value }, "['a',1,NULL]" },
        new object?[] { "Variant(Enum8('a' = 1), String)", "a", "a" },
        new object?[] { "Variant(Map(String, UInt8), Array(UInt8))", new List<KeyValuePair<string, byte>>(), "{}" },
        new object?[] { "Variant(Map(String, UInt8), Array(UInt8))", Array.Empty<byte>(), "[]" },
        new object?[] { "Variant(Date, DateTime64(3))", DBNull.Value, "\\N" },
        new object?[] { "Variant(Tuple(UInt8, String), Tuple(String))", Tuple.Create((byte)1, "x"), "(1,'x')" },
        new object?[] { "Geometry", Tuple.Create(1.5, 2.0), "(1.5,2)" },
        new object?[] { "Geometry", new[] { Tuple.Create(1.0, 2.0), Tuple.Create(3.0, 4.0) }, "[(1,2),(3,4)]" },
        new object?[] { "Geometry", new[] { new[] { Tuple.Create(1.0, 2.0) } }, "[[(1,2)]]" },
        new object?[] { "Geometry", new[] { new[] { new[] { Tuple.Create(1.0, 2.0) } } }, "[[[(1,2)]]]" },
        new object?[] { "Geometry", Array.Empty<Tuple<double, double>>(), "[]" },
        // HTTP Dynamic: the .NET type gives the text.
        new object?[] { "Dynamic", 1L, "1" },
        new object?[] { "Dynamic", (byte)255, "255" },
        new object?[] { "Dynamic", "it's", "it\\'s" },
        new object?[] { "Dynamic", DBNull.Value, "\\N" },
        new object?[] { "Dynamic", 1.5, "1.5" },
        new object?[] { "Dynamic", 0.1f, "0.1" },
        new object?[] { "Dynamic", true, "true" },
        new object?[] { "Dynamic", 1.50m, "1.5" },
        new object?[] { "Dynamic", IPAddress.Parse("1.2.3.4"), "1.2.3.4" },
        new object?[] { "Dynamic", IPAddress.Parse("::ffff:1.2.3.4"), "::ffff:1.2.3.4" },
        new object?[] { "Dynamic", new long[] { 1, 2 }, "[1,2]" },
        new object?[] { "Dynamic", new string?[] { "a", null }, "['a',NULL]" },
        new object?[] { "Dynamic", Tuple.Create(1L, "x"), "(1,'x')" },
        new object?[] { "Dynamic", new List<KeyValuePair<string, long>> { new("b", 1), new("a", 2) }, "{'b':1,'a':2}" },
        new object?[] { "Array(Dynamic)", new object[] { 1L, "a", DBNull.Value }, "[1,'a',NULL]" },
    ];

    // Values that the shim cannot write: the text depends on a type that the client does not give.
    private static readonly object?[][] UnknownTextCases =
    [
        new object?[] { "Variant(Date, DateTime)", new DateTime(2020, 1, 1) },
        new object?[] { "Variant(JSON, String)", new System.Text.Json.Nodes.JsonObject() },
        new object?[] { "Variant(AggregateFunction(sum, UInt64), String)", "a" },
        new object?[] { "Dynamic", new DateTime(2020, 1, 1) },
        new object?[] { "Dynamic", TimeSpan.FromSeconds(1) },
        new object?[] { "Dynamic", new System.Text.Json.Nodes.JsonObject() },
        new object?[] { "Dynamic", new[] { new DateTime(2020, 1, 1) } },
        new object?[] { "Json", new System.Text.Json.Nodes.JsonObject() },
    ];

    [TestCaseSource(nameof(UnknownTextCases))]
    public void Render_ValueWithoutItsType_IsUnsupported(string type, object? value)
    {
        var b = new Utf8Buffer();
        Assert.Throws<NotSupportedException>(() => new ValueRenderer().Render(b, ChType.Parse(type), value, false));
    }

    [Test]
    public void Render_MapAndArrayForms_AreNotMixedUp()
    {
        // An empty array is no map, and empty key-value pairs are no array.
        var b = new Utf8Buffer();
        Assert.Throws<InvalidOperationException>(() => new ValueRenderer().Render(b, ChType.Parse("Map(String, UInt8)"), Array.Empty<byte>(), false));
        Assert.Throws<InvalidOperationException>(() => new ValueRenderer().Render(b, ChType.Parse("Array(UInt8)"), new List<KeyValuePair<string, byte>>(), false));
        Assert.Throws<InvalidOperationException>(() => new ValueRenderer().Render(b, ChType.Parse("Variant(String, UInt64)"), 1.5, false));
    }

    [TestCaseSource(nameof(ValueCases))]
    public void Render_Value_WritesTheServerText(string type, object? value, string expected)
    {
        var b = new Utf8Buffer();
        new ValueRenderer().Render(b, ChType.Parse(type), value, false);
        Assert.That(b.Span.ToArray(), Is.EqualTo(Bytes(expected)));
    }

    [Test]
    public void Render_DictionaryWithTwoEntries_IsUnsupported()
    {
        // A Dictionary has no defined order.
        var b = new Utf8Buffer();
        Assert.Throws<NotSupportedException>(() => new ValueRenderer().Render(b, ChType.Parse("Map(String, UInt8)"), new Dictionary<string, byte> { ["a"] = 1, ["b"] = 2 }, false));
        Assert.Throws<NotSupportedException>(() => new ValueRenderer().Render(b, ChType.Parse("AggregateFunction(sum, UInt64)"), 1, false));
    }

    [Test]
    public void Render_WrongDotNetType_FailsTheStatement()
    {
        var b = new Utf8Buffer();
        Assert.Throws<InvalidOperationException>(() => new ValueRenderer().Render(b, ChType.Parse("UInt8"), "x", false));
        // HTTP without ReadStringsAsByteArrays: a String is a string, and a byte[] is an Array(UInt8).
        Assert.Throws<InvalidOperationException>(() => new ValueRenderer().Render(b, ChType.Parse("String"), new byte[] { 0x61 }, false));
    }

    // The HTTP client of the shim gives a String as string; a byte[] is an Array(UInt8), also in a Variant or a
    // Dynamic value (a String that is not UTF-8 is not readable without ReadStringsAsByteArrays, and with it a
    // String and an Array(UInt8) in a Dynamic value are both byte[]).
    [TestCase("Dynamic", "[97]")]
    [TestCase("Array(Dynamic)", "[[97]]")]
    public void Render_HttpByteArray_IsAnArrayOfUInt8(string type, string expected)
    {
        var value = type.StartsWith("Array", StringComparison.Ordinal) ? new object[] { new byte[] { 0x61 } } : (object)new byte[] { 0x61 };
        var b = new Utf8Buffer();
        new ValueRenderer().Render(b, ChType.Parse(type), value, false);
        Assert.That(b.ToString(), Is.EqualTo(expected));
    }

    [Test]
    public void TryRenderRaw_StringValue_WritesTheBytesWithoutEscapes()
    {
        // A FixedString of the TCP client is byte[]; a String without its column is the string of the client.
        var renderer = new ValueRenderer(() => TimeZoneInfo.Utc);
        var b = new Utf8Buffer();
        Assert.That(renderer.TryRenderRaw(b, ChType.Parse("LowCardinality(Nullable(FixedString(3)))"), new byte[] { 0xff, 0x09, 0x5c }, null, -1), Is.True);
        Assert.That(renderer.TryRenderRaw(b, ChType.Parse("Nullable(String)"), "a\tb", null, -1), Is.True);
        Assert.That(b.Span.ToArray(), Is.EqualTo(new byte[] { 0xff, 0x09, 0x5c, 0x61, 0x09, 0x62 }));
        // The TCP client gives an enum as its ordinal; without its column, the label is from the type.
        b.Clear();
        Assert.That(renderer.TryRenderRaw(b, ChType.Parse("Enum8('a\tb' = 1)"), (sbyte)1, null, -1), Is.True);
        Assert.That(renderer.TryRenderRaw(b, ChType.Parse("SimpleAggregateFunction(any, String)"), "\\", null, -1), Is.True);
        Assert.That(b.ToString(), Is.EqualTo("a\tb\\"));
        Assert.That(renderer.TryRenderRaw(b, ChType.Parse("String"), 1, null, -1), Is.False);
    }

    [Test]
    public void Render_TcpStringColumn_WritesTheBytesOfTheRow()
    {
        // Rows: FF 41, 41.
        var column = new StubStringColumn([0xff, 0x41, 0x41], [0, 2, 3]);
        var renderer = new ValueRenderer(() => TimeZoneInfo.Utc);
        var b = new Utf8Buffer();
        renderer.Render(b, ChType.Parse("String"), "\uFFFDA", false, column, 0);
        b.Append((byte)'|');
        renderer.Render(b, ChType.Parse("Array(String)"), new[] { "\uFFFDA" }, false, null, -1);
        Assert.That(b.Span.ToArray(), Is.EqualTo(new byte[] { 0xff, 0x41, (byte)'|', (byte)'[', (byte)'\'', 0xef, 0xbf, 0xbd, 0x41, (byte)'\'', (byte)']' }));
        // The value must be the UTF-8 decoding of the bytes of its row: else the row is wrong.
        Assert.Throws<InvalidOperationException>(() => renderer.Render(b, ChType.Parse("String"), "\uFFFDA", false, column, 1));
        Assert.Throws<IndexOutOfRangeException>(() => renderer.Render(b, ChType.Parse("String"), "\uFFFDA", false, column, 2));
    }

    // A String column of the TCP client: the rows are slices of Bytes.
    private sealed class StubStringColumn(byte[] bytes, int[] offsets) : ClickHouse.Driver.Tcp.IStringColumn
    {
        public string Name => "s";

        public string TypeName => "String";

        public int RowCount => offsets.Length - 1;

        public ReadOnlySpan<byte> Bytes => bytes;

        public ReadOnlySpan<int> Offsets => offsets;

        public ReadOnlySpan<string> Values => Enumerable.Range(0, RowCount).Select(i => this[i]).ToArray();

        public string this[int row] => GetString(row, System.Text.Encoding.UTF8);

        public ReadOnlySpan<byte> GetBytes(int row) => row < 0 || row >= RowCount ? throw new IndexOutOfRangeException() : bytes.AsSpan(offsets[row], offsets[row + 1] - offsets[row]);

        public string GetString(int row, System.Text.Encoding encoding) => encoding.GetString(GetBytes(row));

        public object GetValue(int row) => this[row];

        public void Dispose()
        {
        }
    }

    // The expected text of the cases: characters below 0x100 are the bytes (so "\xff" is the byte 0xff).
    private static byte[] Bytes(string s) => s.Any(c => c > 0x7f && c < 0x100)
        ? s.Select(c => (byte)c).ToArray()
        : System.Text.Encoding.UTF8.GetBytes(s);

    private static readonly object[] FormatCases =
    [
        new object[] { "SELECT 1", "SELECT 1", "TabSeparated", true },
        new object[] { "SELECT 1 FORMAT TSV", "SELECT 1 ", "TSV", true },
        new object[] { "SELECT 1 FORMAT TSVWithNames SETTINGS max_threads = 1", "SELECT 1  SETTINGS max_threads = 1", "TSVWithNames", true },
        new object[] { "SELECT 1 FORMAT JSONEachRow", "SELECT 1 ", "JSONEachRow", false },
        new object[] { "SELECT 'FORMAT JSON'", "SELECT 'FORMAT JSON'", "TabSeparated", true },
        new object[] { "SELECT * FROM (SELECT 1 FORMAT JSON)", "SELECT * FROM (SELECT 1 FORMAT JSON)", "TabSeparated", true },
        new object[] { "SELECT t.format FROM t", "SELECT t.format FROM t", "TabSeparated", true },
        new object[] { "SELECT 1 FORMAT null", "SELECT 1 ", "null", true },
        new object[] { "SELECT 1 FORMAT TSVRaw", "SELECT 1 ", "TSVRaw", true },
        new object[] { "SELECT 1 FORMAT `Null`;", "SELECT 1 ;", "Null", true },
        // FORMAT as an identifier, not the clause.
        new object[] { "SELECT format FROM t", "SELECT format FROM t", "TabSeparated", true },
        new object[] { "SELECT x AS format FROM t WHERE format = 1", "SELECT x AS format FROM t WHERE format = 1", "TabSeparated", true },
        new object[] { "SELECT 1 AS format FROM numbers(1)", "SELECT 1 AS format FROM numbers(1)", "TabSeparated", true },
        new object[] { "SELECT format FROM (SELECT 1 AS format)", "SELECT format FROM (SELECT 1 AS format)", "TabSeparated", true },
        new object[] { "SELECT 1 AS format", "SELECT 1 AS format", "TabSeparated", true },
        new object[] { "SELECT 1 AS format ORDER BY format DESC", "SELECT 1 AS format ORDER BY format DESC", "TabSeparated", true },
        new object[] { "SELECT 1 AS format ORDER BY format ASC", "SELECT 1 AS format ORDER BY format ASC", "TabSeparated", true },
        new object[] { "SELECT 1 AS format ORDER BY format DESC FORMAT TSV", "SELECT 1 AS format ORDER BY format DESC ", "TSV", true },
        new object[] { "SELECT 1 FORMAT TSV -- c\n;", "SELECT 1  -- c\n;", "TSV", true },
        // FORMAT where an operand is expected: a table or a column `format` with an implicit alias.
        new object[] { "SELECT * FROM format f", "SELECT * FROM format f", "TabSeparated", true },
        new object[] { "SELECT format TSV", "SELECT format TSV", "TabSeparated", true },
        new object[] { "SELECT 2 * format x", "SELECT 2 * format x", "TabSeparated", true },
        new object[] { "SELECT 1, format x", "SELECT 1, format x", "TabSeparated", true },
        new object[] { "SELECT x FROM t WHERE x = format y", "SELECT x FROM t WHERE x = format y", "TabSeparated", true },
        new object[] { "SELECT * FROM FORMAT FORMAT Values", "SELECT * FROM FORMAT ", "Values", false },
        // After an operand, FORMAT is the clause: a keyword in an operand position is a name, `*` there is the
        // asterisk, and a comment, a parameter or a qualified name ends an operand.
        new object[] { "SELECT number AS mod FORMAT TSV", "SELECT number AS mod ", "TSV", true },
        new object[] { "SELECT number FROM numbers(2) ORDER BY offset FORMAT TSV", "SELECT number FROM numbers(2) ORDER BY offset ", "TSV", true },
        new object[] { "SELECT * FORMAT TSV", "SELECT * ", "TSV", true },
        new object[] { "SELECT t.* FROM t FORMAT TSV", "SELECT t.* FROM t ", "TSV", true },
        new object[] { "SELECT 1 /* c */ FORMAT TSV", "SELECT 1 /* c */ ", "TSV", true },
        new object[] { "SELECT {p:String} FORMAT TSV", "SELECT {p:String} ", "TSV", true },
        new object[] { "SHOW CREATE TABLE db.table FORMAT Null", "SHOW CREATE TABLE db.table ", "Null", true },
        new object[] { "SELECT $$a FROM$$ FORMAT TSV", "SELECT $$a FROM$$ ", "TSV", true },
        new object[] { "SELECT 1 AS $alias$name$ FORMAT TSVWithNames", "SELECT 1 AS $alias$name$ ", "TSVWithNames", true },
        new object[] { "SELECT 1 AS имя FORMAT TSV", "SELECT 1 AS имя ", "TSV", true },
        new object[] { "SELECT count(*) FROM (SELECT number FROM numbers(2)) AS s FORMAT TSV", "SELECT count(*) FROM (SELECT number FROM numbers(2)) AS s ", "TSV", true },
        new object[] { "SELECT 1 WITH TOTALS FORMAT TSV", "SELECT 1 WITH TOTALS ", "TSV", true },
    ];

    [TestCaseSource(nameof(FormatCases))]
    public void SplitOutputFormat_Query_FindsTheFormatClause(string input, string sql, string format, bool supported)
    {
        var (q, f) = OutputFormat.Split(input);
        Assert.That(q, Is.EqualTo(sql));
        Assert.That(f.Name, Is.EqualTo(format));
        Assert.That(f.Supported, Is.EqualTo(supported));
    }

    [TestCase("KILL MUTATION WHERE table = 't'", true)]
    [TestCase("kill query where query_id = 'x' SYNC", true)]
    [TestCase("SELECT 1", true)]
    [TestCase("ALTER TABLE t DELETE WHERE 1", false)]
    [TestCase("INSERT INTO t VALUES (1)", false)]
    public void ReturnsRows_Statement_TellsIfItHasAResultSet(string sql, bool expected) =>
        Assert.That(OutputFormat.ReturnsRows(sql), Is.EqualTo(expected));

    // ParserExplainQuery of ClickHouse: which query a trailing FORMAT belongs to.
    [TestCase("EXPLAIN AST SHOW ROW POLICIES FORMAT TabSeparated;", true)]
    [TestCase("explain ast SELECT 1 FORMAT TSV", true)]
    [TestCase("EXPLAIN SYNTAX INSERT INTO t FROM INFILE 'f' SELECT x FROM input('x UInt32') FORMAT TSV;", true)]
    [TestCase("EXPLAIN SYNTAX INSERT INTO t SELECT x FROM input('x UInt32') FORMAT TSV", true)]
    [TestCase("EXPLAIN SYNTAX INSERT INTO t FORMAT TSV", true)]
    [TestCase("EXPLAIN SYNTAX INSERT INTO t SELECT 1 FORMAT TSV", false)]
    [TestCase("EXPLAIN AST INSERT INTO t (SELECT 1) SETTINGS max_threads = 1 FORMAT TSV;", false)]
    [TestCase("EXPLAIN AST (INSERT INTO t SELECT 1) FORMAT TSV;", false)]
    [TestCase("EXPLAIN AST (INSERT INTO t FROM INFILE 'f') FORMAT TSV;", false)]
    [TestCase("EXPLAIN AST (CREATE TABLE t (x UInt8) ENGINE = Memory) FORMAT TSV;", false)]
    [TestCase("EXPLAIN AST (EXPLAIN SYNTAX INSERT INTO t SELECT 1) FORMAT TSV;", false)]
    [TestCase("EXPLAIN AST ( /* c */ SELECT 1) FORMAT TSV", true)]
    [TestCase("EXPLAIN AST ((SELECT 1) SETTINGS max_threads = 1) FORMAT TSV;", false)]
    [TestCase("EXPLAIN AST (with 1 AS x SELECT x) FORMAT TSV", true)]
    [TestCase("EXPLAIN AST CREATE TABLE t (x UInt8) ENGINE = Memory FORMAT TSV", true)]
    [TestCase("EXPLAIN SYNTAX SYSTEM CLEAR MARK CACHE FORMAT TSV;", false)]
    [TestCase("EXPLAIN SYNTAX INSERT INTO t (SELECT 1) FORMAT TSV", false)]
    [TestCase("EXPLAIN SYNTAX INSERT INTO t SELECT * FROM (SELECT * FROM input('x UInt32')) FORMAT TSV", true)]
    [TestCase("EXPLAIN SYNTAX INSERT INTO t SELECT input FROM s FORMAT TSV", false)]
    [TestCase("EXPLAIN PLAN header = 1 INSERT INTO t SELECT 1 FORMAT TSV", false)]
    [TestCase("EXPLAIN SELECT 1 FORMAT TSV", false)]
    [TestCase("EXPLAIN SYNTAX SELECT insert, input FROM t FORMAT TSV", false)]
    [TestCase("SELECT 1 FORMAT TSV", false)]
    public void ExplainedQueryOwnsFormat_Explain_FollowsTheServerParser(string sql, bool expected) =>
        Assert.That(OutputFormat.ExplainedQueryOwnsFormat(sql), Is.EqualTo(expected));
}
