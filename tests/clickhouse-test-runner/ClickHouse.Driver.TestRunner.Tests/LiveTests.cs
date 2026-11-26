using System.Text;
using NUnit.Framework;

namespace ClickHouse.Driver.TestRunner.Tests;

/// <summary>
/// Tests that need a ClickHouse server: set <c>CLICKHOUSE_TEST_RUNNER_LIVE=1</c>. The server is
/// <c>CLICKHOUSE_HOST</c> (default localhost), on <c>CLICKHOUSE_PORT_TCP</c> (9000) and
/// <c>CLICKHOUSE_PORT_HTTP</c> (8123).
/// </summary>
[Category("Live")]
[NonParallelizable]
public class LiveTests
{
    // Expressions whose text the shim must write exactly as the server does, over both protocols. A client
    // bug of one protocol is not here; see the known failures in allowlist.txt.
    private static readonly string[] Expressions =
    [
        "toInt8(-1), toUInt8(255), toInt64(-9223372036854775808), toUInt64(18446744073709551615)",
        "toInt128('-170141183460469231731687303715884105728'), toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935')",
        "toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968'), toUInt128('340282366920938463463374607431768211455')",
        "arrayJoin([1e15, 1e21, 1.5e-7, 0.1 + 0.2, nan, inf, -inf, -0., 5e-324, 1.7976931348623157e308])",
        "toFloat32(0.1), toFloat32(3.4028235e38), toFloat32(-1.5e-7), toFloat32(nan)",
        "toBFloat16(1.5), toBFloat16(0.1), toBFloat16(-3.140625)",
        "toDecimal32(1.50, 2), toDecimal64(-0.001, 5), toDecimal128(100, 3), toDecimal256('1.5', 10), toDecimal32(0, 3)",
        "toDecimal128('12345678901234567890.123456789', 9), toDecimal256('-0.0000000000000000000000000000000000001', 40)",
        "'a\\tb\\\\c\\nd''e\\0\\b\\f\\v\\x01', toFixedString('ab', 4), 'ünïcødé ☃'",
        "true, false, NULL, CAST(NULL AS Nullable(String)), CAST(1 AS Nullable(UInt8))",
        "toDate('2020-01-02'), toDate32('1900-01-01'), toDate32('2299-12-31'), toDateTime('2020-01-01 10:00:00', 'UTC')",
        "toDateTime('2020-01-01 10:00:00', 'Asia/Tokyo'), toDateTime64('2020-01-01 00:00:00.120', 3), toDateTime64('2020-01-01 00:00:00.1234567', 7, 'Asia/Tokyo')",
        "toDateTime64('1900-01-01 00:00:00.5', 1, 'UTC'), toDateTime64('2299-12-31 23:59:59.99', 2, 'UTC'), toDateTime64('1969-12-31 23:59:59.999', 3, 'UTC')",
        "toDateTime('2020-07-01 12:00:00', 'Europe/Amsterdam'), toDateTime('2020-01-01 12:00:00', 'America/New_York')",
        "toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toIPv4('1.2.3.4'), toIPv6('::ffff:1.2.3.4'), toIPv6('::1.2.3.4'), toIPv6('2001:db8::1'), toIPv6('::')",
        "[1, 2], ['a''b', NULL], [[1], []], [toDate('2020-01-01')], [toDateTime64('2020-01-01 00:00:00.5', 2)], []",
        "(1, 'x', NULL), CAST((1, 'x') AS Tuple(a Int32, b String)), [(1, 'a')], ((1, 'a'), 2), (toInt128(1), 'a')",
        "tuple(), toNullable(tuple(1::UInt8)), CAST(tuple(NULL) AS Tuple(x LowCardinality(Nullable(String))))",
        "map('k', 1, 'a', 2), map(1, [2]), map('x', map('y', 1, 'z', 2)), map(), mapFromArrays(['a', 'a'], [1, 2])",
        "CAST('a' AS Enum8('a' = 1, 'b' = 2)), [CAST('b' AS Enum16('a' = 1, 'b' = 2))], CAST(NULL AS Nullable(Enum8('x' = -1))), CAST('it''s' AS Enum8('it''s' = 1))",
        "toLowCardinality('x'), toLowCardinality(toNullable('y')), [toLowCardinality('z')], toLowCardinality(toNullable(toDateTime('2020-01-01 00:00:00', 'UTC')))",
        "toTime('12:34:56'), toTime('-1:02:03'), toTime64('12:00:00.5', 3), toTime64('-100:00:00.1234567', 7)",
        "(1.5, 2.5)::Point, [(1, 2), (3, 4)]::Ring, [[(1, 2), (3, 4)]]::Polygon",
        "number, toString(number) FROM numbers(3)",
        "if(number % 5, NULL, toInt8OrZero(toString(number))) FROM numbers(6)",
        "CAST(1 AS SimpleAggregateFunction(sum, UInt64))",
        // Variant, Geometry and Dynamic: TCP takes the type of each value from the column, also inside Array, Tuple
        // and Map; HTTP from the .NET value (alternatives with one text for it, or a .NET type with one text).
        "CAST('a\\tb' AS Variant(String, UInt64)), CAST(7::UInt64 AS Variant(String, UInt64)), CAST(NULL AS Variant(String, UInt64))",
        "[CAST('a' AS Variant(String, UInt8)), CAST(1 AS Variant(String, UInt8)), NULL], (CAST(1 AS Variant(UInt8, String)), 'x'), map('k', CAST('v' AS Variant(String, UInt8)), 'l', CAST(2 AS Variant(String, UInt8)))",
        "CAST([1, 2]::Array(UInt64) AS Variant(Array(UInt64), String)), CAST((1, 'x')::Tuple(UInt8, String) AS Variant(Tuple(UInt8, String), UInt8)), CAST(map('a', 1::UInt8) AS Variant(Map(String, UInt8), UInt8)), CAST('b'::Enum8('b' = 1) AS Variant(Enum8('b' = 1), Array(UInt8)))",
        "[CAST(toDate('2020-01-01') AS Variant(Date, String)), CAST('x' AS Variant(Date, String))], (CAST(toDateTime64('2020-01-01 00:00:00.5', 1, 'UTC') AS Variant(DateTime64(1, 'UTC'), String)), 1)",
        "(1.5, 2.5)::Point::Geometry, [(1, 2), (3, 4)]::Ring::Geometry, [(1, 2), (3, 4)]::LineString::Geometry, [[(1, 2), (3, 4)]]::Polygon::Geometry, [[[(1, 2)]]]::MultiPolygon::Geometry",
        "[(1, 2)::Point::Geometry, [[(0, 0)]]::Polygon::Geometry], ((1, 2)::Point::Geometry, 1), map('g', [(1, 2)]::Ring::Geometry)",
        "1::Dynamic, 'a''b'::Dynamic, NULL::Dynamic, [1, 2]::Dynamic, (1, 'x')::Dynamic, map('a', 1, 'b', 2)::Dynamic",
        "1.5::Dynamic, toFloat32(0.1)::Dynamic, true::Dynamic, toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')::Dynamic, toDecimal64(1.50, 2)::Dynamic, toIPv4('1.2.3.4')::Dynamic, toInt128(-1)::Dynamic",
        "[1::Dynamic, 'a'::Dynamic, NULL::Dynamic], map('k', [1]::Dynamic), (2::Dynamic, 3), [[1::Dynamic], ['b'::Dynamic]]",
        "x::Dynamic FROM (SELECT arrayJoin([1, 2, 3]) AS x)",
        // A column or alias named `format` is not the FORMAT clause.
        "1 AS format ORDER BY format DESC",
        "1 AS format ORDER BY format ASC",
        "format FROM (SELECT 1 AS format)",
        "1 AS format FROM numbers(1)",
    ];

    private static readonly string Host = Environment.GetEnvironmentVariable("CLICKHOUSE_HOST") is { Length: > 0 } h ? h : "localhost";

    [SetUp]
    public void RequireServer()
    {
        if (Environment.GetEnvironmentVariable("CLICKHOUSE_TEST_RUNNER_LIVE") != "1")
        {
            Assert.Ignore("Set CLICKHOUSE_TEST_RUNNER_LIVE=1 to run the tests against a ClickHouse server.");
        }
    }

    public static IEnumerable<TestCaseData> RenderCases() =>
        from protocol in new[] { Protocol.Http, Protocol.Tcp }
        from expr in Expressions
        select new TestCaseData(protocol, expr).SetName($"Render_{protocol}({expr[..Math.Min(60, expr.Length)]})");

    [TestCaseSource(nameof(RenderCases))]
    public async Task Render_Expression_MatchesTheServerTsv(Protocol protocol, string expr)
    {
        var query = "SELECT " + expr;
        var expected = await ServerTsvAsync(query);
        var (stdout, stderr, code) = await RunShimAsync(protocol, query);
        Assert.That(code, Is.Zero, stderr);
        Assert.That(stdout, Is.EqualTo(expected));
    }

    [TestCase(Protocol.Http)]
    [TestCase(Protocol.Tcp)]
    public async Task Run_SessionState_LastsForTheInvocation(Protocol protocol)
    {
        var script = string.Join('\n',
            "SET max_threads = 3;",
            "SELECT getSetting('max_threads');",
            "CREATE TEMPORARY TABLE t (x UInt8);",
            "INSERT INTO t VALUES (1), (2);",
            "INSERT INTO t FORMAT TSV",
            "3",
            string.Empty,
            "SELECT sum(x) FROM t;");
        var (stdout, stderr, code) = await RunShimAsync(protocol, script);
        Assert.That(code, Is.Zero, stderr);
        Assert.That(stdout, Is.EqualTo("3\n6\n"));
    }

    [TestCase(Protocol.Http)]
    [TestCase(Protocol.Tcp)]
    public async Task Run_LexerDetails_AreAsInClickHouseClient(Protocol protocol)
    {
        // The text that clickhouse-client 26.10 writes for this script (checked with the real client). (1) A
        // SYNTAX_ERROR that the server finds at run time (hint serverError) is echoed; a statement that the client
        // cannot parse (hint clientError) is not. (2) Unicode whitespace before the keyword. (3) A `//` comment
        // after the `;` belongs to the statement.
        var script = string.Join('\n',
            "-- { echo }",
            "SELECT formatQuery('SELECT'); -- { serverError SYNTAX_ERROR }",
            "SELECT 1 FROM; -- { clientError SYNTAX_ERROR }",
            "\u00A0\u2003\uFEFFSELECT 2; // c",
            "SELECT 3;");
        var (stdout, stderr, code) = await RunShimAsync(protocol, script);
        Assert.That(code, Is.Zero, stderr);
        Assert.That(stdout, Is.EqualTo(
            "-- { echo }\nSELECT formatQuery('SELECT'); -- { serverError SYNTAX_ERROR }\n" +
            "\u00A0\u2003\uFEFFSELECT 2; // c\n2\n" +
            "SELECT 3;\n3\n"));
    }

    // The trailing FORMAT of EXPLAIN AST and of an explained INSERT with its own data belongs to the explained
    // query. Contrasts: the FORMAT after a parenthesized INSERT, after the SETTINGS of an INSERT SELECT, and of
    // EXPLAIN SYNTAX SELECT is the output format of the EXPLAIN (TSVWithNames shows which one it is).
    [TestCase(Protocol.Http, "EXPLAIN AST SHOW ROW POLICIES FORMAT TabSeparated")]
    [TestCase(Protocol.Tcp, "EXPLAIN AST SHOW ROW POLICIES FORMAT TabSeparated")]
    [TestCase(Protocol.Http, "EXPLAIN SYNTAX INSERT INTO t SELECT x FROM input('x UInt32') FORMAT TSV")]
    [TestCase(Protocol.Tcp, "EXPLAIN SYNTAX INSERT INTO t SELECT x FROM input('x UInt32') FORMAT TSV")]
    [TestCase(Protocol.Http, "EXPLAIN AST (SELECT 1) FORMAT TabSeparated")]
    [TestCase(Protocol.Tcp, "EXPLAIN AST (SELECT 1) FORMAT TabSeparated")]
    [TestCase(Protocol.Http, "EXPLAIN AST ((SELECT 1) SETTINGS max_threads = 1) FORMAT TSVWithNames")]
    [TestCase(Protocol.Tcp, "EXPLAIN AST ((SELECT 1) SETTINGS max_threads = 1) FORMAT TSVWithNames")]
    [TestCase(Protocol.Http, "EXPLAIN AST INSERT INTO t (SELECT 1) SETTINGS max_threads = 1 FORMAT TSV")]
    [TestCase(Protocol.Tcp, "EXPLAIN AST INSERT INTO t (SELECT 1) SETTINGS max_threads = 1 FORMAT TSV")]
    [TestCase(Protocol.Http, "EXPLAIN AST (INSERT INTO t SELECT 1) FORMAT TSVWithNames")]
    [TestCase(Protocol.Tcp, "EXPLAIN AST (INSERT INTO t SELECT 1) FORMAT TSVWithNames")]
    [TestCase(Protocol.Http, "EXPLAIN SYNTAX SELECT 1 FORMAT TSVWithNames")]
    [TestCase(Protocol.Tcp, "EXPLAIN SYNTAX SELECT 1 FORMAT TSVWithNames")]
    public async Task Run_ExplainFormat_IsAsOnTheServer(Protocol protocol, string query)
    {
        var (stdout, stderr, code) = await RunShimAsync(protocol, query);
        Assert.That(code, Is.Zero, stderr);
        Assert.That(stdout, Is.EqualTo(await ServerTsvAsync(query)));
    }

    // The header of a Raw format has no escapes, as its rows. Contrast: TSVWithNames escapes the header.
    [TestCase(Protocol.Http, "TSVWithNames")]
    [TestCase(Protocol.Tcp, "TSVWithNames")]
    [TestCase(Protocol.Http, "TSVRawWithNames")]
    [TestCase(Protocol.Tcp, "TSVRawWithNames")]
    [TestCase(Protocol.Http, "TabSeparatedRawWithNamesAndTypes")]
    [TestCase(Protocol.Tcp, "TabSeparatedRawWithNamesAndTypes")]
    public async Task Run_HeaderOfFormat_IsAsOnTheServer(Protocol protocol, string format)
    {
        var query = $"SELECT 1 AS `a\\tb\\\\c'd`, 'x' AS `é\\nf` FORMAT {format}";
        var (stdout, stderr, code) = await RunShimAsync(protocol, query);
        Assert.That(code, Is.Zero, stderr);
        Assert.That(stdout, Is.EqualTo(await ServerTsvAsync(query)));
    }

    [TestCase(Protocol.Http)]
    [TestCase(Protocol.Tcp)]
    public async Task Run_KillMutation_WritesTheStoppedMutation(Protocol protocol)
    {
        // A mutation that cannot finish ('x' is not a UInt64) fails the ALTER and waits until KILL MUTATION stops it;
        // KILL returns a row for it, as in clickhouse-client.
        // A name of its own for each run: test runs can share one server.
        var table = $"shim_kill_{protocol.ToString().ToLowerInvariant()}_{Guid.NewGuid():N}";
        var script = string.Join('\n',
            $"DROP TABLE IF EXISTS {table};",
            $"CREATE TABLE {table} (v String) ENGINE = MergeTree ORDER BY tuple();",
            $"INSERT INTO {table} VALUES ('x');",
            $"ALTER TABLE {table} MODIFY COLUMN v UInt64 SETTINGS mutations_sync = 2; -- {{ serverError UNFINISHED }}",
            $"KILL MUTATION WHERE database = currentDatabase() AND table = '{table}' FORMAT TSVWithNames;",
            $"DROP TABLE {table};");
        var (stdout, stderr, code) = await RunShimAsync(protocol, script);
        Assert.That(code, Is.Zero, stderr);
        Assert.That(stdout, Does.Match(
            $"^kill_status\tdatabase\ttable\tmutation_id\tcommand\nwaiting\tdefault\t{table}\tmutation_\\d+\\.txt\t\\(MODIFY COLUMN `v` UInt64\\)\n$"));
    }

    [TestCase(Protocol.Http)]
    [TestCase(Protocol.Tcp)]
    public async Task Run_FeatureTagOverrides_TurnTheFeatureOff(Protocol protocol)
    {
        // `async_insert` is on by default in current servers; a test with the tag no-async-insert needs it off. A
        // SET in the test replaces the command-line value. A randomized setting of the runner is not sent.
        var script = string.Join('\n',
            "SELECT getSetting('async_insert'), getSetting('enable_parallel_replicas'), getSetting('max_block_size');",
            "SET async_insert = 1;",
            "SELECT getSetting('async_insert');");
        var (stdout, stderr, code) = await RunShimAsync(protocol, script,
            "--async_insert", "0", "--enable_parallel_replicas", "0", "--max_block_size", "7");
        Assert.That(code, Is.Zero, stderr);
        var expected = await ServerTsvAsync(
            "SELECT getSetting('async_insert'), getSetting('enable_parallel_replicas'), getSetting('max_block_size') " +
            "SETTINGS async_insert = 0, enable_parallel_replicas = 0")
            + await ServerTsvAsync("SELECT getSetting('async_insert') SETTINGS async_insert = 1");
        Assert.That(expected, Does.StartWith("false\t0\t"));
        Assert.That(stdout, Is.EqualTo(expected));
    }

    [TestCase(Protocol.Http)]
    [TestCase(Protocol.Tcp)]
    public async Task Run_SetAfterExpectedError_IsKept(Protocol protocol)
    {
        // The TCP client closes the session after a server error; the shim opens a new session and runs the
        // SET and USE statements again.
        var script = string.Join('\n',
            "SET max_threads = 3;",
            "SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }",
            "SELECT getSetting('max_threads');");
        var (stdout, stderr, code) = await RunShimAsync(protocol, script);
        Assert.That(code, Is.Zero, stderr);
        Assert.That(stdout, Is.EqualTo("3\n"));
    }

    [TestCase(Protocol.Http)]
    [TestCase(Protocol.Tcp)]
    public async Task Run_ParameterOutsideCode_IsNotSent(Protocol protocol)
    {
        // The server substitutes a placeholder only in code, not in a string literal or a comment. The TCP client
        // rejects a parameter without a placeholder in code, so the shim sends only the parameters in code.
        var (stdout, stderr, code) = await RunShimAsync(protocol, "SELECT '{p:String}' /* {p:String} */, {q:UInt8};", "--param_p=x", "--param_q=7");
        Assert.That(code, Is.Zero, stderr);
        Assert.That(stdout, Is.EqualTo(await ServerTsvAsync("SELECT '{p:String}', 7")));
    }

    [TestCase(Protocol.Http)]
    [TestCase(Protocol.Tcp)]
    public async Task Run_OutputFormats_WriteTheTabSeparatedFamily(Protocol protocol)
    {
        var (stdout, stderr, code) = await RunShimAsync(protocol, string.Join('\n',
            "SELECT 'a\\tb' AS s, 1 AS n FORMAT TSVWithNamesAndTypes;",
            "SELECT 'a\\tb' FORMAT TSVRaw;",
            "SELECT 1 FORMAT Null;",
            "SELECT 2; -- { echo }"));
        Assert.That(code, Is.Zero, stderr);
        Assert.That(stdout, Is.EqualTo("s\tn\nString\tUInt8\na\\tb\t1\na\tb\nSELECT 2; -- { echo }\n2\n"));
    }

    // The HTTP client does not know the Interval types (a client bug; see allowlist.txt).
    [TestCase("toIntervalSecond(-5), INTERVAL 3 DAY, [toIntervalYear(1)]")]
    // DateTime64 counts outside the range of the .NET date types: the HTTP client cannot read them (see
    // allowlist.txt); the TCP client gives the count, and the shim writes the server's clamped text.
    [TestCase("reinterpret(toInt64(-62161776000000000), 'DateTime64(6, ''UTC'')'), reinterpret(toInt64(-9223372036854775808), 'DateTime64(3, ''UTC'')'), reinterpret(toInt64(-62167219200001), 'DateTime64(3, ''UTC'')')")]
    [TestCase("reinterpret(toInt64(9223372036854775616), 'DateTime64(3, ''UTC'')'), reinterpret(toInt64(9223372036854775806), 'DateTime64(7, ''Asia/Istanbul'')'), reinterpret(toInt64(9223372036854775806), 'DateTime64(8, ''Asia/Istanbul'')')")]
    [TestCase("[reinterpret(toInt64(253402387199000), 'DateTime64(3, ''America/New_York'')')], toNullable(reinterpret(toInt64(-62167215600000), 'DateTime64(3, ''America/New_York'')'))")]
    // The HTTP client gives a Dynamic or Variant value without its type, and a DateTime (or a JSON value) has no
    // single text without it (shim-render-dynamic, shim-render-variant, shim-render-json in allowlist.txt).
    [TestCase("[toDateTime64('2020-01-01 00:00:00.5', 2, 'UTC')::Dynamic, toDate('2020-01-01')::Dynamic, toDateTime('2020-01-01 10:00:00', 'Asia/Tokyo')::Dynamic], map('t', toTime64('01:02:03.4', 1)::Dynamic)")]
    [TestCase("CAST('{\"a\":1}' AS SimpleAggregateFunction(anyLast, JSON)), [CAST('{\"b\":\"x\"}'::JSON AS Variant(JSON, String))]")]
    public async Task Render_TcpOnlyExpression_MatchesTheServerTsv(string expr)
    {
        var query = "SELECT " + expr;
        var (stdout, stderr, code) = await RunShimAsync(Protocol.Tcp, query);
        Assert.That(code, Is.Zero, stderr);
        Assert.That(stdout, Is.EqualTo(await ServerTsvAsync(query)));
    }

    // TCP: a String that is not UTF-8 is written from its bytes (IStringColumn.GetBytes), as the server writes
    // it, at the top level and nested. The many-rows case checks the row of each value in its column
    // (LowCardinality keys, Array and Map offsets, Variant and Dynamic local rows).
    private static readonly string[] StringBytesExpressions =
    [
        "unhex('FF41'), unhex('C328'), 'a\\tb', unhex('00FF0A5C27'), ''",
        "[unhex('FF'), 'x', unhex('E282')], [[unhex('FE')], []], []::Array(String)",
        "map(unhex('FE'), unhex('FF'), 'k', 'v'), map('a', [unhex('80')]), map(toLowCardinality(unhex('FD')), 1)",
        "(unhex('FF'), 1), CAST((unhex('FF'), 'b') AS Tuple(x String, y Nullable(String))), [(unhex('FE'), unhex('FD'))]",
        "toNullable(unhex('FF')), CAST(NULL AS Nullable(String)), [toNullable(unhex('FE')), NULL]",
        "toLowCardinality(unhex('FF')), toLowCardinality(toNullable(unhex('FE'))), [toLowCardinality(unhex('80'))], CAST(NULL AS LowCardinality(Nullable(String)))",
        "toFixedString(unhex('FF41'), 3), [toFixedString(unhex('FE'), 2)], toNullable(toFixedString(unhex('FD'), 1))",
        "CAST(unhex('FF') AS Variant(String, UInt64)), [CAST(unhex('FE') AS Variant(String, UInt64)), CAST(1::UInt64 AS Variant(String, UInt64))]",
        // Rows with different bytes that decode to the same text (FF, FE, FD: U+FFFD), so a wrong row is a wrong text.
        "unhex(x), toLowCardinality(unhex(x)), toLowCardinality(toNullable(if(x = '41', NULL, unhex(x)))), [unhex(x), x], map(unhex(x), x), (x, unhex(x)), " +
            "[toLowCardinality(unhex(x))], map(toLowCardinality(unhex(x)), 1), " +
            "if(x = '41', 7::UInt64::Variant(String, UInt64), unhex(x)::Variant(String, UInt64)), if(x = '', 1::Dynamic, unhex(x)::Dynamic) " +
            "FROM values('x String', 'FF', 'FE', '41', 'C328', '', 'FF41FE', 'FD')",
        "CAST(arrayMap(i -> (unhex(concat(x, repeat(toString(i), 2))), i), range(n)) AS Nested(a String, b UInt8)) " +
            "FROM values('x String, n UInt8', ('FF', 2), ('FE', 0), ('FD', 1))",
        "CAST(concat('{\"a\":\"', unhex('FF'), '\"}') AS JSON), CAST(concat('x', unhex('FE')) AS SimpleAggregateFunction(any, String))",
    ];

    [TestCaseSource(nameof(StringBytesExpressions))]
    public async Task Render_TcpStringNotUtf8_IsTheServerBytes(string expr)
    {
        var query = "SELECT " + expr;
        var (stdout, stderr, code) = await RunShimBytesAsync(Protocol.Tcp, query);
        Assert.That(code, Is.Zero, stderr);
        Assert.That(Convert.ToHexString(stdout), Is.EqualTo(Convert.ToHexString(await ServerBytesAsync(query))));
    }

    [TestCase("SELECT unhex('FF09'), toLowCardinality(toNullable(unhex('FE5C'))), toFixedString(unhex('FD0A'), 2), CAST(NULL AS Nullable(String)), toNullable(unhex('FD5C')), toLowCardinality(unhex('FC09')), CAST(unhex('FB09') AS SimpleAggregateFunction(any, String)) FORMAT TSVRaw")]
    [TestCase("SELECT toLowCardinality(unhex(x)), toNullable(unhex(x)), toLowCardinality(toNullable(if(x = '41', NULL, unhex(x)))) FROM values('x String', 'FF09', 'FE5C', '41', 'FD09') FORMAT TSVRaw")]
    [TestCase("SELECT unhex(x) AS s, count() FROM values('x String', 'FF', 'FE', 'FE') GROUP BY s WITH TOTALS ORDER BY s SETTINGS extremes = 1")]
    [TestCase("SELECT unhex('FF')::Dynamic, [unhex('FE')::Dynamic, 1::Dynamic], map('k', unhex('FD')::Dynamic), CAST(unhex('FC') AS Variant(String, Array(UInt8)))")]
    public async Task Run_TcpStringNotUtf8_IsTheServerBytes(string query)
    {
        var (stdout, stderr, code) = await RunShimBytesAsync(Protocol.Tcp, query);
        Assert.That(code, Is.Zero, stderr);
        Assert.That(Convert.ToHexString(stdout), Is.EqualTo(Convert.ToHexString(await ServerBytesAsync(query))));
    }

    // TSVRaw writes a string, an enum label and the value of SimpleAggregateFunction(f, String) without escapes.
    [TestCase(Protocol.Http)]
    [TestCase(Protocol.Tcp)]
    public async Task Run_TsvRawStringTypes_AreNotEscaped(Protocol protocol)
    {
        const string query = "SELECT CAST('a\\tb' AS SimpleAggregateFunction(any, String)), CAST('x\\ty' AS Enum8('x\\ty' = 1)), toLowCardinality(toNullable('c\\\\d')), [CAST('e' AS Enum8('e' = 2))] FORMAT TSVRaw";
        var (stdout, stderr, code) = await RunShimBytesAsync(protocol, query);
        Assert.That(code, Is.Zero, stderr);
        Assert.That(Convert.ToHexString(stdout), Is.EqualTo(Convert.ToHexString(await ServerBytesAsync(query))));
    }

    [Test]
    public async Task Render_HttpStringNotUtf8_IsTheUtf8DecodingOfTheClient()
    {
        // HTTP: the shim reads String with the default mapping of the client (string), which has U+FFFD for a byte
        // that is not UTF-8 (shim-http-string-bytes in allowlist.txt). A FixedString is the same.
        var (stdout, stderr, code) = await RunShimBytesAsync(Protocol.Http, "SELECT unhex('FF41'), [unhex('FE')], toFixedString(unhex('FD'), 1)");
        Assert.That(code, Is.Zero, stderr);
        Assert.That(Convert.ToHexString(stdout), Is.EqualTo("EFBFBD41" + "09" + "5B27EFBFBD275D" + "09" + "EFBFBD" + "0A"));
    }

    // The output settings of the session (SET) and of the query (SETTINGS) change the text of the shim as they
    // change the server's text.
    private const string OutputExpressions =
        "toDateTime('2020-10-15 00:00:00', 'Europe/Moscow'), toDateTime64('2020-10-15 00:00:00.12', 2, 'Europe/Moscow'), " +
        "[toDateTime('2020-10-15 00:00:00', 'UTC')], toDateTime64('1960-01-01 00:00:00.5', 1, 'UTC'), toDate('2020-01-01'), " +
        "CAST(NULL, 'Nullable(DateTime)'), [NULL], (1, NULL), toDecimal32(1.5, 4), [toDecimal64(2, 3)], toDecimal128(0, 2), " +
        "toDateTime64('2024-01-01 00:00:00.1', 1, 'UTC'), toDateTime64('2024-01-01 00:00:00.12', 6, 'UTC'), toDateTime64('2024-01-01 00:00:00', 4, 'UTC')";

    [TestCase(Protocol.Http, "date_time_output_format = 'iso'")]
    [TestCase(Protocol.Tcp, "date_time_output_format = 'iso'")]
    [TestCase(Protocol.Http, "date_time_output_format = 'unix_timestamp'")]
    [TestCase(Protocol.Tcp, "date_time_output_format = 'unix_timestamp'")]
    [TestCase(Protocol.Http, "format_tsv_null_representation = 'a\\tb'")]
    [TestCase(Protocol.Tcp, "format_tsv_null_representation = 'XYZ'")]
    [TestCase(Protocol.Http, "format_tsv_null_representation = 'it''s'")]
    [TestCase(Protocol.Tcp, "format_tsv_null_representation = 'it''s'")]
    [TestCase(Protocol.Http, "output_format_decimal_trailing_zeros = 1")]
    [TestCase(Protocol.Tcp, "output_format_decimal_trailing_zeros = 1")]
    [TestCase(Protocol.Http, "date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 1")]
    [TestCase(Protocol.Tcp, "date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 1")]
    public async Task Run_OutputSettings_ChangeTheTextAsOnTheServer(Protocol protocol, string setting)
    {
        var expected = await ServerTsvAsync($"SELECT {OutputExpressions} SETTINGS {setting}");
        var (stdout, stderr, code) = await RunShimAsync(protocol, $"SET {setting};\nSELECT {OutputExpressions};\nSET date_time_output_format = 'simple';\nSELECT {OutputExpressions} SETTINGS {setting};");
        Assert.That(code, Is.Zero, stderr);
        Assert.That(stdout, Is.EqualTo(expected + expected));
    }

    [Test]
    public async Task Run_HttpSessionTimezone_IsTheZoneOfDateTimeWithoutZone()
    {
        // The HTTP client gives the UTC wall clock and the session time zone (ClickHouseCommand.ServerTimezone).
        const string exprs = "toDateTime('2020-01-01 00:00:00') AS a, toDateTime64('2020-01-01 00:00:00.5', 1), [a], toDateTime64('1789-07-14 00:00:00', 0), toDateTime('2020-01-01 00:00:00', 'UTC'), toString(a)";
        var expected = await ServerTsvAsync($"SELECT {exprs} SETTINGS session_timezone = 'America/New_York'");
        var (stdout, stderr, code) = await RunShimAsync(Protocol.Http, $"SET session_timezone = 'America/New_York';\nSELECT {exprs};\nSET session_timezone = '';\nSELECT {exprs} SETTINGS session_timezone = 'America/New_York';");
        Assert.That(code, Is.Zero, stderr);
        Assert.That(stdout, Is.EqualTo(expected + expected));
    }

    [Test]
    public async Task Run_TotalsAndExtremesOverTcp_FollowTheData()
    {
        var query = "SELECT number % 2 AS k, count() FROM numbers(4) GROUP BY k WITH TOTALS ORDER BY k SETTINGS extremes = 1";
        var (stdout, stderr, code) = await RunShimAsync(Protocol.Tcp, query);
        Assert.That(code, Is.Zero, stderr);
        Assert.That(stdout, Is.EqualTo(await ServerTsvAsync(query)));
    }

    [TestCase(Protocol.Http)]
    [TestCase(Protocol.Tcp)]
    public async Task Run_Errors_StopTheScriptWithTheErrorCode(Protocol protocol)
    {
        // An unexpected error stops the script, writes the error name to stderr, and sets the exit code to
        // the error code.
        var (stdout, stderr, code) = await RunShimAsync(protocol, "SELECT 1; SELECT * FROM no_such_table_xyz; SELECT 2;");
        Assert.That(stdout, Is.EqualTo("1\n"));
        Assert.That(code, Is.EqualTo(60));
        Assert.That(stderr, Does.Contain("UNKNOWN_TABLE"));

        // A query that succeeds although the hint expects an error.
        (_, stderr, code) = await RunShimAsync(protocol, "SELECT 1; -- { serverError UNKNOWN_TABLE }");
        Assert.That(code, Is.Not.Zero);
        Assert.That(stderr, Does.Contain("expected serverError UNKNOWN_TABLE"));

        // A different error than the hint expects.
        (_, stderr, code) = await RunShimAsync(protocol, "SELECT * FROM no_such_table_xyz; -- { serverError SYNTAX_ERROR }");
        Assert.That(code, Is.EqualTo(60), stderr);
    }

    // The server's own TabSeparated bytes of a query.
    private static async Task<byte[]> ServerBytesAsync(string query)
    {
        using var http = new HttpClient();
        var port = Environment.GetEnvironmentVariable("CLICKHOUSE_PORT_HTTP") is { Length: > 0 } p ? p : "8123";
        var uri = new UriBuilder("http", Host, int.Parse(port, System.Globalization.CultureInfo.InvariantCulture)) { Query = "default_format=TabSeparated" }.Uri;
        using var response = await http.PostAsync(uri, new StringContent(query));
        var body = await response.Content.ReadAsByteArrayAsync();
        Assert.That(response.IsSuccessStatusCode, Is.True, Encoding.UTF8.GetString(body));
        return body;
    }

    private static async Task<(byte[] Stdout, string Stderr, int Code)> RunShimBytesAsync(Protocol protocol, string query)
    {
        using var stdout = new MemoryStream();
        using var stderr = new StringWriter();
        var port = Environment.GetEnvironmentVariable("CLICKHOUSE_PORT_TCP") is { Length: > 0 } p ? p : "9000";
        var code = await Program.RunAsync(["--host", Host, "--port", port, "--query", query], Stream.Null, stdout, stderr, protocol);
        return (stdout.ToArray(), stderr.ToString(), code);
    }

    // The server's own TabSeparated text of a query.
    private static async Task<string> ServerTsvAsync(string query)
    {
        using var http = new HttpClient();
        var port = Environment.GetEnvironmentVariable("CLICKHOUSE_PORT_HTTP") is { Length: > 0 } p ? p : "8123";
        var uri = new UriBuilder("http", Host, int.Parse(port, System.Globalization.CultureInfo.InvariantCulture)) { Query = "default_format=TabSeparated" }.Uri;
        using var response = await http.PostAsync(uri, new StringContent(query));
        var body = await response.Content.ReadAsStringAsync();
        Assert.That(response.IsSuccessStatusCode, Is.True, body);
        return body;
    }

    private static async Task<(string Stdout, string Stderr, int Code)> RunShimAsync(Protocol protocol, string query, params string[] extraArgs)
    {
        using var stdout = new MemoryStream();
        using var stderr = new StringWriter();
        var port = Environment.GetEnvironmentVariable("CLICKHOUSE_PORT_TCP") is { Length: > 0 } p ? p : "9000";
        var code = await Program.RunAsync(["--host", Host, "--port", port, "--query", query, .. extraArgs], Stream.Null, stdout, stderr, protocol);
        return (Encoding.UTF8.GetString(stdout.ToArray()), stderr.ToString(), code);
    }
}
