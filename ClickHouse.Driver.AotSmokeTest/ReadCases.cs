namespace ClickHouse.Driver.AotSmokeTest;

/// <summary>
/// SELECT expressions and how each value renders through <see cref="Checks.Show"/>. They cover each family of
/// type, and the composites native AOT reads differently from the JIT (Map, Tuple, arrays of either).
/// </summary>
internal static class ReadCases
{
    public static readonly (string Expression, string Expected)[] All =
    [
        // Scalars
        ("toUInt64(42)", "42"),
        ("toInt8(-8)", "-8"),
        ("toFloat32(1.5)", "1.5"),
        ("true", "True"),
        ("'hi'", "hi"),
        ("toFixedString('ab', 3)", "ab\u0000"),
        ("toLowCardinality('lc')", "lc"),
        ("toDecimal64(1.5, 4)", "1.5000"),
        ("toDecimal256('12345678901234567890.123', 3)", "12345678901234567890.123"),
        ("toInt128('-170141183460469231731687303715884105728')", "-170141183460469231731687303715884105728"),
        ("toUInt256(7)", "7"),
        ("toDate('2026-01-02')", "2026-01-02 00:00:00.0000000 Utc"),
        ("toDate32('1900-01-02')", "1900-01-02 00:00:00.0000000 Utc"),
        ("toDateTime('2026-01-02 03:04:05', 'UTC')", "2026-01-02 03:04:05.0000000 Utc"),
        ("toDateTime64('2026-01-02 03:04:05.123', 3, 'UTC')", "2026-01-02 03:04:05.1230000 Utc"),
        ("toUUID('3f2504e0-4f89-11d3-9a0c-0305e82c3301')", "3f2504e0-4f89-11d3-9a0c-0305e82c3301"),
        ("CAST('b' AS Enum8('a' = 1, 'b' = 2))", "b"),
        ("toIPv4('1.2.3.4')", "1.2.3.4"),
        ("toIPv6('::1')", "::1"),
        ("CAST(5 AS Nullable(Int32))", "5"),
        ("CAST(NULL AS Nullable(Int32))", "NULL"),
        ("CAST(NULL AS Nullable(String))", "NULL"),

        // Arrays
        ("[1, 2, 3]::Array(Int32)", "[1, 2, 3]"),
        ("[1, NULL]::Array(Nullable(Int32))", "[1, NULL]"),
        ("[[1], [2, 3]]::Array(Array(UInt8))", "[0x01, 0x0203]"),
        ("['a', NULL]::Array(Nullable(String))", "[a, NULL]"),
        ("[toInt128(1), NULL]::Array(Nullable(Int128))", "[1, NULL]"),
        ("[toDecimal32(1.25, 2)]::Array(Decimal(9, 2))", "[1.25]"),
        ("[toUUID('3f2504e0-4f89-11d3-9a0c-0305e82c3301')]", "[3f2504e0-4f89-11d3-9a0c-0305e82c3301]"),
        ("[toIPv4('1.2.3.4')]", "[1.2.3.4]"),
        ("[toDateTime64('2026-01-02 03:04:05.123', 3, 'UTC')]", "[2026-01-02 03:04:05.1230000 Utc]"),
        ("[(1, 'a'), (2, 'b')]::Array(Tuple(UInt8, String))", "[(1, a), (2, b)]"),
        ("[map('k', 1)]::Array(Map(String, UInt8))", "[{k: 1}]"),

        // Map
        ("map('a', 'b')::Map(String, String)", "{a: b}"),
        ("map('a', 1, 'b', 2)::Map(String, Int32)", "{a: 1, b: 2}"),
        ("map(1, ['x', 'y'])::Map(UInt8, Array(String))", "{1: [x, y]}"),
        ("map('a', NULL)::Map(String, Nullable(Int32))", "{a: NULL}"),
        ("map('o', map('i', 1))::Map(String, Map(String, UInt8))", "{o: {i: 1}}"),
        ("map(toUUID('3f2504e0-4f89-11d3-9a0c-0305e82c3301'), toDate('2026-01-02'))", "{3f2504e0-4f89-11d3-9a0c-0305e82c3301: 2026-01-02 00:00:00.0000000 Utc}"),

        // Tuple
        ("(1, 'x')::Tuple(Int32, String)", "(1, x)"),
        ("(1, 2.5, toDate('2026-01-02'))::Tuple(UInt8, Float64, Date)", "(1, 2.5, 2026-01-02 00:00:00.0000000 Utc)"),
        ("(1, 'x')::Tuple(a Int32, b String)", "(1, x)"),
        ("(NULL, ['a'])::Tuple(Nullable(Int32), Array(String))", "(NULL, [a])"),
        ("(1, 2, 3, 4, 5, 6, 7)::Tuple(UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8)", "(1, 2, 3, 4, 5, 6, 7)"),
        ("(1, 2, 3, 4, 5, 6, 7, 8, 9)::Tuple(UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8)", "(1, 2, 3, 4, 5, 6, 7, 8, 9)"),
        ("(1, map('k', (2, 'v')))::Tuple(UInt8, Map(String, Tuple(UInt8, String)))", "(1, {k: (2, v)})"),

        // Geo
        ("(1.5, 2.5)::Point", "(1.5, 2.5)"),
        ("[(0, 0), (1, 0), (1, 1)]::Ring", "[(0, 0), (1, 0), (1, 1)]"),
        ("[[(0, 0), (1, 0), (1, 1)]]::Polygon", "[[(0, 0), (1, 0), (1, 1)]]"),

        // Polymorphic
        ("123::Variant(String, UInt64)", "123"),
        ("[1, 2]::Variant(String, Array(UInt8))", "0x0102"),
        ("42::Dynamic", "42"),
        ("'{\"a\": 1, \"b\": {\"c\": [1, 2]}}'::JSON", "{\"a\":1,\"b\":{\"c\":[1,2]}}"),
    ];
}
