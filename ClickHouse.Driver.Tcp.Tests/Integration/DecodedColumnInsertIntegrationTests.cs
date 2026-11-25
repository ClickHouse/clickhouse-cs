using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// A column that a query read, inserted into a table whose column has another scale, timezone, enum definition,
/// precision or width. The server reads back the values of the source: the same instants, durations and labels.
/// </summary>
[TestFixture]
[Category("Integration")]
public class DecodedColumnInsertIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static readonly Dictionary<string, string> TimeSettings = new(StringComparer.Ordinal)
    {
        ["enable_time_time64_type"] = "1",
        ["allow_experimental_time_time64_type"] = "1",
    };

    private static readonly Dictionary<string, string> BFloat16Settings = new(StringComparer.Ordinal)
    {
        ["allow_experimental_bfloat16_type"] = "1",
    };

    private static readonly Dictionary<string, string> LowCardinalitySettings = new(StringComparer.Ordinal)
    {
        ["allow_suspicious_low_cardinality_types"] = "1",
    };

    private static readonly Dictionary<string, string> NullableTupleSettings = new(StringComparer.Ordinal)
    {
        ["enable_nullable_tuple_type"] = "1",
    };

    private static readonly Dictionary<string, string> VariantSettings = new(StringComparer.Ordinal)
    {
        ["allow_experimental_variant_type"] = "1",
        ["allow_suspicious_variant_types"] = "1",
    };

    private static readonly Dictionary<string, string> DynamicSettings = new(StringComparer.Ordinal)
    {
        ["allow_experimental_dynamic_type"] = "1",
    };

    private static readonly Dictionary<string, string> DynamicVariantSettings = new(StringComparer.Ordinal)
    {
        ["allow_experimental_dynamic_type"] = "1",
        ["allow_experimental_variant_type"] = "1",
        ["allow_suspicious_variant_types"] = "1",
        ["output_format_native_use_flattened_dynamic_and_json_serialization"] = "1",
    };

    private static readonly Dictionary<string, string> JsonSettings = new(StringComparer.Ordinal)
    {
        ["allow_experimental_json_type"] = "1",
        ["allow_experimental_dynamic_type"] = "1",
        ["output_format_native_write_json_as_string"] = "1",
        ["output_format_native_use_flattened_dynamic_and_json_serialization"] = "1",
    };

    public static IEnumerable<TestCaseData> Cases()
    {
        yield return Case(
            "DateTime64(3, 'UTC')",
            "DateTime64(6, 'UTC')",
            ("'2024-01-02 03:04:05.678'", "2024-01-02 03:04:05.678000"),
            ("'1969-12-31 23:59:59.999'", "1969-12-31 23:59:59.999000"));
        yield return Case("DateTime64(6, 'UTC')", "DateTime64(3, 'UTC')", ("'2024-01-02 03:04:05.678000'", "2024-01-02 03:04:05.678"));
        yield return Case("DateTime64(3, 'UTC')", "DateTime64(3, 'Asia/Tokyo')", ("'2024-01-02 03:04:05.678'", "2024-01-02 12:04:05.678"));
        yield return Case("DateTime('UTC')", "DateTime('Asia/Tokyo')", ("'2024-01-02 03:04:05'", "2024-01-02 12:04:05"));
        yield return Case("Enum8('a' = 1, 'b' = 2)", "Enum8('a' = 2, 'b' = 1)", ("'a'", "a"), ("'b'", "b"));
        yield return Case("Enum8('a' = 1, 'b' = 2)", "Enum16('b' = 1000, 'a' = -1000)", ("'b'", "b"), ("'a'", "a"));
        yield return Case("Nullable(DateTime64(3, 'UTC'))", "Nullable(DateTime64(6, 'UTC'))", ("'2024-01-02 03:04:05.678'", "2024-01-02 03:04:05.678000"), ("NULL", null));
        yield return Case("Array(DateTime64(3, 'UTC'))", "Array(DateTime64(6, 'UTC'))", ("['2024-01-02 03:04:05.678']", "['2024-01-02 03:04:05.678000']"), ("[]", "[]"));
        yield return Case("Map(String, Enum8('a' = 1, 'b' = 2))", "Map(String, Enum8('a' = 2, 'b' = 1))", ("{'k': 'b'}", "{'k':'b'}"));
        yield return Case(
            "Tuple(DateTime64(3, 'UTC'), Enum8('a' = 1, 'b' = 2))",
            "Tuple(DateTime64(6, 'UTC'), Enum8('b' = 1, 'a' = 2))",
            ("('2024-01-02 03:04:05.678', 'a')", "('2024-01-02 03:04:05.678000','a')"));
        yield return Case("Decimal(9, 2)", "Decimal(18, 4)", ("1.23", "1.23"), ("-4.5", "-4.5"));
    }

    // A part whose values mean other values next to parts that keep their meaning, which hold bytes that are not UTF-8: the
    // SQL literal of the source row, the expression that the target reads back, and its text.
    public static IEnumerable<TestCaseData> PartCases()
    {
        yield return PartCase(
            "Tuple(DateTime64(3, 'UTC'), String)",
            "Tuple(DateTime64(6, 'UTC'), String)",
            "('2024-01-02 03:04:05.678', unhex('FF'))",
            "concat(toString(value.1), ' ', hex(value.2))",
            "2024-01-02 03:04:05.678000 FF");
        yield return PartCase(
            "Map(String, DateTime64(3, 'UTC'))",
            "Map(String, DateTime64(6, 'UTC'))",
            "map(unhex('FF'), toDateTime64('2024-01-02 03:04:05.678', 3, 'UTC'), unhex('C328'), toDateTime64('2024-01-02 03:04:05.679', 3, 'UTC'))",
            "concat(hex(arrayStringConcat(mapKeys(value), ',')), ' ', toString(mapValues(value)))",
            "FF2CC328 ['2024-01-02 03:04:05.678000','2024-01-02 03:04:05.679000']");
        yield return PartCase(
            "Map(Enum8('a' = 1, 'b' = 2), String)",
            "Map(Enum8('a' = 2, 'b' = 1), String)",
            "map('b', unhex('FF'))",
            "concat(toString(mapKeys(value)), ' ', hex(mapValues(value)[1]))",
            "['b'] FF");
        yield return PartCase(
            "Array(Tuple(DateTime64(3, 'UTC'), String))",
            "Array(Tuple(DateTime64(6, 'UTC'), String))",
            "[('2024-01-02 03:04:05.678', unhex('C328')), ('2024-01-02 03:04:05.679', unhex('FF'))]",
            "concat(toString(arrayMap(x -> x.1, value)), ' ', hex(arrayStringConcat(arrayMap(x -> x.2, value), ',')))",
            "['2024-01-02 03:04:05.678000','2024-01-02 03:04:05.679000'] C3282CFF");
        yield return PartCase(
            "Tuple(DateTime64(3, 'UTC'), LowCardinality(String), FixedString(2))",
            "Tuple(DateTime64(6, 'UTC'), LowCardinality(String), FixedString(2))",
            "('2024-01-02 03:04:05.678', unhex('FF'), unhex('C328'))",
            "concat(toString(value.1), ' ', hex(value.2), ' ', hex(value.3))",
            "2024-01-02 03:04:05.678000 FF C328");
    }

    // A String that a query read, into another type of a string: the SQL literal of the source row, the expression that
    // the target reads back, and its text.
    public static IEnumerable<TestCaseData> StringCases()
    {
        yield return PartCase("String", "LowCardinality(String)", "unhex('FF')", "hex(value)", "FF");
        yield return PartCase("String", "LowCardinality(Nullable(String))", "unhex('FF')", "hex(value)", "FF");
        yield return PartCase("String", "Nullable(String)", "unhex('FF')", "hex(value)", "FF");
        yield return PartCase("String", "FixedString(1)", "unhex('FF')", "hex(value)", "FF");
        yield return PartCase("String", "Nullable(FixedString(1))", "unhex('FF')", "hex(value)", "FF");
        yield return PartCase("String", "LowCardinality(FixedString(1))", "unhex('FF')", "hex(value)", "FF");
        yield return PartCase("LowCardinality(String)", "String", "unhex('FF')", "hex(value)", "FF");
        yield return PartCase("Nullable(String)", "LowCardinality(Nullable(String))", "NULL", "ifNull(hex(value), 'NULL')", "NULL");
        yield return PartCase("Array(String)", "Array(LowCardinality(String))", "[unhex('FF'), unhex('C328')]", "hex(arrayStringConcat(value, ','))", "FF2CC328");
        yield return PartCase(
            "Map(String, String)",
            "Map(LowCardinality(String), Nullable(String))",
            "map(unhex('FF'), unhex('C328'))",
            "concat(hex(mapKeys(value)[1]), ' ', hex(mapValues(value)[1]))",
            "FF C328");
        yield return PartCase("Tuple(String, Int32)", "Tuple(FixedString(1), Int32)", "(unhex('FF'), 7)", "concat(hex(value.1), ' ', toString(value.2))", "FF 7");
        yield return PartCase(
            "Tuple(DateTime64(3, 'UTC'), String)",
            "Tuple(DateTime64(6, 'UTC'), LowCardinality(String))",
            "('2024-01-02 03:04:05.678', unhex('FF'))",
            "concat(toString(value.1), ' ', hex(value.2))",
            "2024-01-02 03:04:05.678000 FF");
    }

    // A column that a query read, with a String part next to a part of another type, or under a composite of another
    // type: the source type, the target type, the SQL literals of three source rows, the expression that the target reads
    // back, its text for each row, and the settings of the tables and the insert. Every String value is not UTF-8.
    public static IEnumerable<TestCaseData> SiblingCases()
    {
        const string Enum = "Enum8('a' = 1, 'b' = 2)";
        const string TupleReadBack = "concat(hex(value.1), ' ', toString(value.2))";
        string[] enums = { "(unhex('FF'), 'a')", "(unhex('C328'), 'b')", "(unhex('61C0AF'), 'a')" };
        string[] ordinals = { "FF 1", "C328 2", "61C0AF 1" };
        yield return SiblingCase($"Tuple(String, {Enum})", "Tuple(String, Int8)", enums, TupleReadBack, ordinals);
        yield return SiblingCase($"Tuple(String, {Enum})", "Tuple(s String, i Int8)", enums, TupleReadBack, ordinals);
        yield return SiblingCase(
            "Tuple(String, BFloat16)",
            "Tuple(String, Float32)",
            new[] { "(unhex('FF'), 1.5)", "(unhex('C328'), -2)", "(unhex('61C0AF'), 0.5)" },
            TupleReadBack,
            new[] { "FF 1.5", "C328 -2", "61C0AF 0.5" },
            BFloat16Settings);
        yield return SiblingCase(
            "Tuple(String, DateTime('UTC'))",
            "Tuple(String, UInt32)",
            new[] { "(unhex('FF'), toDateTime(7, 'UTC'))", "(unhex('C328'), toDateTime(8, 'UTC'))", "(unhex('61C0AF'), toDateTime(9, 'UTC'))" },
            TupleReadBack,
            new[] { "FF 7", "C328 8", "61C0AF 9" });
        string[] integers = { "(unhex('FF'), 7)", "(unhex('C328'), -1)", "(unhex('61C0AF'), 7)" };
        string[] integersRead = { "FF 7", "C328 -1", "61C0AF 7" };
        yield return SiblingCase("Tuple(String, Int32)", "Tuple(String, LowCardinality(Int32))", integers, TupleReadBack, integersRead, LowCardinalitySettings);
        yield return SiblingCase("Tuple(String, LowCardinality(Int32))", "Tuple(String, Int32)", integers, TupleReadBack, integersRead, LowCardinalitySettings);
        yield return SiblingCase(
            $"Map(String, {Enum})",
            "Map(String, Int8)",
            new[] { "map(unhex('FF'), 'a', unhex('C328'), 'b')", "map()", "map(unhex('61C0AF'), 'b')" },
            "concat(hex(arrayStringConcat(mapKeys(value), ',')), ' ', toString(mapValues(value)))",
            new[] { "FF2CC328 [1,2]", " []", "61C0AF [2]" });
        yield return SiblingCase(
            $"Array(Tuple(String, {Enum}))",
            "Array(Tuple(String, Int8))",
            new[] { "[(unhex('FF'), 'a'), (unhex('C328'), 'b')]", "[]", "[(unhex('61C0AF'), 'b')]" },
            "concat(hex(arrayStringConcat(arrayMap(x -> x.1, value), ',')), ' ', toString(arrayMap(x -> x.2, value)))",
            new[] { "FF2CC328 [1,2]", " []", "61C0AF [2]" });
        yield return SiblingCase(
            $"Tuple(LowCardinality(String), Tuple(String, {Enum}))",
            "Tuple(String, Tuple(String, Int8))",
            new[] { "(unhex('FE'), (unhex('FF'), 'a'))", "(unhex('C328'), (unhex('80'), 'b'))", "(unhex('61C0AF'), (unhex('C0'), 'a'))" },
            "concat(hex(value.1), ' ', hex(value.2.1), ' ', toString(value.2.2))",
            new[] { "FE FF 1", "C328 80 2", "61C0AF C0 1" });

        // Tuples of 8 and 9 elements, whose CLR type nests the elements after the seventh in TRest: the Enum8 as the
        // second element, and as the ninth (in TRest).
        string int8s6 = string.Join(", ", System.Linq.Enumerable.Repeat("Int8", 6));
        string int8s7 = string.Join(", ", System.Linq.Enumerable.Repeat("Int8", 7));
        string int8s8 = string.Join(", ", System.Linq.Enumerable.Repeat("Int8", 8));
        yield return SiblingCase(
            $"Tuple(String, {Enum}, {int8s6})",
            $"Tuple(String, {int8s7})",
            new[] { "(unhex('FF'), 'a', 1, 2, 3, 4, 5, 6)", "(unhex('C328'), 'b', 1, 2, 3, 4, 5, 7)", "(unhex('61C0AF'), 'a', 1, 2, 3, 4, 5, 8)" },
            "concat(hex(value.1), ' ', toString(value.2), ' ', toString(value.8))",
            new[] { "FF 1 6", "C328 2 7", "61C0AF 1 8" });
        yield return SiblingCase(
            $"Tuple(String, {Enum}, {int8s7})",
            $"Tuple(String, {int8s8})",
            new[] { "(unhex('FF'), 'a', 1, 2, 3, 4, 5, 6, 7)", "(unhex('C328'), 'b', 1, 2, 3, 4, 5, 6, 8)", "(unhex('61C0AF'), 'a', 1, 2, 3, 4, 5, 6, 9)" },
            "concat(hex(value.1), ' ', toString(value.2), ' ', toString(value.9))",
            new[] { "FF 1 7", "C328 2 8", "61C0AF 1 9" });
        yield return SiblingCase(
            $"Tuple(String, {int8s7}, {Enum})",
            $"Tuple(String, {int8s8})",
            new[] { "(unhex('FF'), 1, 2, 3, 4, 5, 6, 7, 'a')", "(unhex('C328'), 2, 2, 3, 4, 5, 6, 7, 'b')", "(unhex('61C0AF'), 3, 2, 3, 4, 5, 6, 7, 'a')" },
            "concat(hex(value.1), ' ', toString(value.2), ' ', toString(value.9))",
            new[] { "FF 1 1", "C328 2 2", "61C0AF 3 1" });
    }

    // As SiblingCases, for a Nullable(Tuple) on either side.
    public static IEnumerable<TestCaseData> NullableTupleSiblingCases()
    {
        const string Enum = "Enum8('a' = 1, 'b' = 2)";
        const string ReadBack = "if(isNull(value), 'NULL', concat(hex(tupleElement(assumeNotNull(value), 1)), ' ', toString(tupleElement(assumeNotNull(value), 2))))";
        yield return SiblingCase(
            $"Nullable(Tuple(String, {Enum}))",
            "Nullable(Tuple(String, Int8))",
            new[] { "(unhex('FF'), 'a')", "(unhex('C328'), 'b')", "(unhex('61C0AF'), 'a')" },
            ReadBack,
            new[] { "FF 1", "C328 2", "61C0AF 1" },
            NullableTupleSettings);
        yield return SiblingCase(
            $"Nullable(Tuple(String, {Enum}))",
            "Nullable(Tuple(String, Int8))",
            new[] { "(unhex('FF'), 'a')", "NULL", "(unhex('61C0AF'), 'b')" },
            ReadBack,
            new[] { "FF 1", "NULL", "61C0AF 2" },
            NullableTupleSettings);
        yield return SiblingCase(
            "Tuple(String)",
            "Nullable(Tuple(String))",
            new[] { "tuple(unhex('FF'))", "tuple(unhex('C328'))", "tuple(unhex('61C0AF'))" },
            "hex(tupleElement(assumeNotNull(value), 1))",
            new[] { "FF", "C328", "61C0AF" },
            NullableTupleSettings);

        // A Tuple of 9 elements, whose CLR type nests the ninth element in TRest, into a Nullable(Tuple).
        string wide = $"Tuple(String, {string.Join(", ", System.Linq.Enumerable.Repeat("Int8", 8))})";
        yield return SiblingCase(
            wide,
            $"Nullable({wide})",
            new[] { "(unhex('FF'), 1, 2, 3, 4, 5, 6, 7, 8)", "(unhex('C328'), 2, 2, 3, 4, 5, 6, 7, 9)", "(unhex('61C0AF'), 3, 2, 3, 4, 5, 6, 7, 10)" },
            "concat(hex(tupleElement(assumeNotNull(value), 1)), ' ', toString(tupleElement(assumeNotNull(value), 2)), ' ', toString(tupleElement(assumeNotNull(value), 9)))",
            new[] { "FF 1 8", "C328 2 9", "61C0AF 3 10" },
            NullableTupleSettings);
    }

    // A String that a query read, into a Variant whose String alternative takes both the text and the bytes of a value,
    // also as an element of an Array or a Tuple and a value of a Map: the alternative of each row and the hex of its String
    // value. Every String value is not UTF-8.
    public static IEnumerable<TestCaseData> VariantCases()
    {
        const string ReadBack = "concat(variantType(value), ' ', ifNull(hex(variantElement(value, 'String')), ''))";
        string[] strings = { "unhex('41FF42')", "unhex('C328')", "unhex('61C0AF')" };
        string[] stored = { "String 41FF42", "String C328", "String 61C0AF" };
        yield return SiblingCase("String", "Variant(String, Int64)", strings, ReadBack, stored, VariantSettings);
        yield return SiblingCase("LowCardinality(String)", "Variant(String, Int64)", strings, ReadBack, stored, VariantSettings);
        yield return SiblingCase(
            "Nullable(String)",
            "Variant(String, Int64)",
            new[] { "unhex('41FF42')", "NULL", "unhex('61C0AF')" },
            ReadBack,
            new[] { "String 41FF42", "None ", "String 61C0AF" },
            VariantSettings);
        yield return SiblingCase(
            "Array(String)",
            "Array(Variant(String, Int64))",
            new[] { "[unhex('41FF42'), unhex('C328')]", "[]", "[unhex('61C0AF')]" },
            "concat(toString(arrayMap(x -> variantType(x), value)), ' ', hex(arrayStringConcat(arrayMap(x -> assumeNotNull(variantElement(x, 'String')), value), ',')))",
            new[] { "['String','String'] 41FF422CC328", "[] ", "['String'] 61C0AF" },
            VariantSettings);
        yield return SiblingCase(
            "Tuple(String, Int32)",
            "Tuple(Variant(String, Int32), Int32)",
            new[] { "(unhex('41FF42'), 1)", "(unhex('C328'), 2)", "(unhex('61C0AF'), 3)" },
            "concat(variantType(value.1), ' ', hex(variantElement(value.1, 'String')), ' ', toString(value.2))",
            new[] { "String 41FF42 1", "String C328 2", "String 61C0AF 3" },
            VariantSettings);
        yield return SiblingCase(
            "Map(String, String)",
            "Map(String, Variant(String, Int64))",
            new[] { "map(unhex('FF'), unhex('41FF42'))", "map()", "map(unhex('C328'), unhex('61C0AF'))" },
            "concat(hex(arrayStringConcat(mapKeys(value), ',')), ' ', toString(arrayMap(x -> variantType(x), mapValues(value))), ' ', hex(arrayStringConcat(arrayMap(x -> assumeNotNull(variantElement(x, 'String')), mapValues(value)), ',')))",
            new[] { "FF ['String'] 41FF42", " [] ", "C328 ['String'] 61C0AF" },
            VariantSettings);
        yield return SiblingCase(
            "String",
            "Variant(LowCardinality(String), UInt64)",
            strings,
            "concat(variantType(value), ' ', ifNull(hex(variantElement(value, 'LowCardinality(String)')), ''))",
            new[] { "LowCardinality(String) 41FF42", "LowCardinality(String) C328", "LowCardinality(String) 61C0AF" },
            VariantSettings);
    }

    // A Variant that a query read, into a Variant that has each of its alternatives and others: the alternative of each
    // row and its value (the hex of a String, the text of an Int32). The String values are not UTF-8. A target with a
    // Float64 alternative moves the discriminators of Int32 and String; a target with a UInt64 alternative does not.
    public static IEnumerable<TestCaseData> VariantIntoVariantCases()
    {
        const string Value = "concat(variantType({0}), ' ', ifNull(hex(variantElement({0}, 'String')), ''), ifNull(toString(variantElement({0}, 'Int32')), ''))";
        string top = string.Format(System.Globalization.CultureInfo.InvariantCulture, Value, "value");
        string[] rows = { "unhex('41FF42')", "toInt32(7)", "NULL", "unhex('C328')" };
        string[] stored = { "String 41FF42", "Int32 7", "None ", "String C328" };
        yield return SiblingCase("Variant(Int32, String)", "Variant(Int32, String, UInt64)", rows, top, stored, VariantSettings);
        yield return SiblingCase("Variant(Int32, String)", "Variant(Float64, Int32, String)", rows, top, stored, VariantSettings);

        const string V = "Variant(Int32, String)";
        yield return SiblingCase(
            $"Array({V})",
            "Array(Variant(Float64, Int32, String))",
            new[] { $"[CAST(unhex('41FF42') AS {V}), CAST(toInt32(7) AS {V}), NULL]", "[]", $"[CAST(unhex('C328') AS {V})]" },
            $"toString(arrayMap(x -> {string.Format(System.Globalization.CultureInfo.InvariantCulture, Value, "x")}, value))",
            new[] { "['String 41FF42','Int32 7','None ']", "[]", "['String C328']" },
            VariantSettings);
        yield return SiblingCase(
            $"Tuple({V}, UInt8)",
            "Tuple(Variant(Float64, Int32, String), UInt8)",
            new[] { $"(CAST(unhex('41FF42') AS {V}), 1)", $"(CAST(toInt32(7) AS {V}), 2)", $"(CAST(NULL AS {V}), 3)" },
            $"concat({string.Format(System.Globalization.CultureInfo.InvariantCulture, Value, "value.1")}, ' ', toString(value.2))",
            new[] { "String 41FF42 1", "Int32 7 2", "None  3" },
            VariantSettings);
        yield return SiblingCase(
            $"Map(String, {V})",
            "Map(String, Variant(Int32, String, UInt64))",
            new[] { $"map('a', CAST(unhex('41FF42') AS {V}), 'b', CAST(toInt32(7) AS {V}))", "map()", $"map('c', CAST(unhex('C328') AS {V}))" },
            $"toString(arrayMap(x -> {string.Format(System.Globalization.CultureInfo.InvariantCulture, Value, "x")}, mapValues(value)))",
            new[] { "['String 41FF42','Int32 7']", "[]", "['String C328']" },
            VariantSettings);
    }

    // A column that a query read, whose type holds a String, into a part that only the converter tree of its CLR type writes
    // (a Dynamic, a Variant whose alternative for the value is not a String that takes its bytes): the source type, the
    // target type and the SQL literal of the source row.
    public static IEnumerable<TestCaseData> StringTextDynamicCases()
    {
        const string V = "Variant(Int32, String)";
        yield return TextCase("String", "Dynamic", "'abc'");
        yield return TextCase("Nullable(String)", "Dynamic", "'abc'");
        yield return TextCase("LowCardinality(String)", "Dynamic", "'abc'");
        yield return TextCase("Tuple(String, Int32)", "Tuple(Dynamic, Int32)", "('abc', 1)");
        yield return TextCase("Tuple(String, Int32)", "Dynamic", "('abc', 1)");
        yield return TextCase("Map(String, Int32)", "Dynamic", "map('a', 1)");
        yield return TextCase("Array(String)", "Dynamic", "['abc']");
        yield return TextCase("Array(String)", "Array(Dynamic)", "['abc']");
        yield return TextCase(V, "Dynamic", "'abc'");
        yield return TextCase($"Array({V})", "Array(Dynamic)", $"[CAST('abc' AS {V})]");
        yield return TextCase("Dynamic", "Variant(String, Int64)", "'abc'");
        yield return TextCase("Tuple(Dynamic, Int32)", "Tuple(Variant(String, Int64), Int32)", "('abc', 1)");
    }

    // A column that a query read as JSON, or as a LowCardinality(String), into a part that only the converter tree writes.
    public static IEnumerable<TestCaseData> StringTextJsonCases()
    {
        yield return TextCase("LowCardinality(String)", "JSON", "'{\"a\":1}'");
        yield return TextCase("LowCardinality(Nullable(String))", "JSON", "'{\"a\":1}'");
        yield return TextCase("JSON", "LowCardinality(String)", "'{\"a\":1}'");
        yield return TextCase("JSON", "Dynamic", "'{\"a\":1}'");
    }

    public static IEnumerable<TestCaseData> StringTextVariantCases()
    {
        yield return TextCase("String", "Variant(FixedString(3), UInt64)", "'abc'");
        yield return TextCase("Tuple(String, Int32)", "Tuple(Variant(FixedString(3), UInt64), Int32)", "('abc', 1)");
        yield return TextCase("String", "Variant(String, Array(UInt8))", "'abc'");
        yield return TextCase("Array(String)", "Array(Variant(String, Array(UInt8)))", "['abc']");
        yield return TextCase("String", "Variant(Enum8('a' = 1, 'b' = 2), Int32)", "'a'");
        yield return TextCase("Tuple(String, Int32)", "Variant(Tuple(String, Int32), UInt64)", "('abc', 1)");
    }

    // A Nullable part that a query read, with a NULL row in the second block, into a Variant or a Dynamic part, which holds
    // NULL with no wrapper: the alternative or the type of each row, and its value.
    public static IEnumerable<TestCaseData> NullIntoNullHoldingCases()
    {
        const string VariantRow = "concat(variantType({0}), ' ', ifNull(toString(variantElement({0}, 'Int32')), ''))";
        const string DynamicRow = "concat(dynamicType({0}), ' ', ifNull(toString({0}), ''))";
        string[] ints = { "1", "2", "NULL", "4" };
        yield return SiblingCase("Nullable(Int32)", "Variant(Int32, String)", ints, Row(VariantRow, "value"), new[] { "Int32 1", "Int32 2", "None ", "Int32 4" }, DynamicVariantSettings);
        yield return SiblingCase("Nullable(Int32)", "Dynamic", ints, Row(DynamicRow, "value"), new[] { "Int32 1", "Int32 2", "None ", "Int32 4" }, DynamicVariantSettings);
        yield return SiblingCase(
            "Nullable(Float64)",
            "Dynamic",
            new[] { "1.5", "-2", "NULL", "0.25" },
            Row(DynamicRow, "value"),
            new[] { "Float64 1.5", "Float64 -2", "None ", "Float64 0.25" },
            DynamicVariantSettings);
        yield return SiblingCase(
            "Array(Nullable(Int32))",
            "Array(Variant(Int32, String))",
            new[] { "[1, NULL]", "[]", "[NULL]", "[4]" },
            $"toString(arrayMap(x -> {Row(VariantRow, "x")}, value))",
            new[] { "['Int32 1','None ']", "[]", "['None ']", "['Int32 4']" },
            DynamicVariantSettings);
        yield return SiblingCase(
            "Tuple(Nullable(Int32), Int8)",
            "Tuple(Variant(Int32, String), Int8)",
            new[] { "(1, 1)", "(2, 2)", "(NULL, 3)", "(4, 4)" },
            $"concat({Row(VariantRow, "value.1")}, ' ', toString(value.2))",
            new[] { "Int32 1 1", "Int32 2 2", "None  3", "Int32 4 4" },
            DynamicVariantSettings);
        yield return SiblingCase(
            "Map(Int32, Nullable(Int32))",
            "Map(Int32, Dynamic)",
            new[] { "map(1, 10)", "map()", "map(3, NULL)", "map(4, 40)" },
            $"toString(arrayMap(x -> {Row(DynamicRow, "x")}, mapValues(value)))",
            new[] { "['Int32 10']", "[]", "['None ']", "['Int32 40']" },
            DynamicVariantSettings);
        yield return SiblingCase("Nullable(Int32)", "Dynamic", new[] { "1", "2", "3" }, Row(DynamicRow, "value"), new[] { "Int32 1", "Int32 2", "Int32 3" }, DynamicVariantSettings);
    }

    // As NullIntoNullHoldingCases, for a Nullable(Tuple) source or target.
    public static IEnumerable<TestCaseData> NullTupleIntoNullHoldingCases()
    {
        var settings = new Dictionary<string, string>(DynamicVariantSettings, StringComparer.Ordinal) { ["enable_nullable_tuple_type"] = "1" };
        yield return SiblingCase(
            "Nullable(Tuple(Int32, Int8))",
            "Dynamic",
            new[] { "(1, 1)", "(2, 2)", "NULL", "(4, 4)" },
            "concat(dynamicType(value), ' ', ifNull(toString(value), ''))",
            new[] { "Tuple(Int32, Int8) (1,1)", "Tuple(Int32, Int8) (2,2)", "None ", "Tuple(Int32, Int8) (4,4)" },
            settings);

        // Row 1 is hidden by the NULL of the enclosing Nullable(Tuple); row 2 holds a NULL in the Nullable(Int32) element.
        yield return SiblingCase(
            "Nullable(Tuple(Nullable(Int32), Int8))",
            "Nullable(Tuple(Variant(Int32, String), Int8))",
            new[] { "(1, 1)", "NULL", "(NULL, 3)", "(4, 4)" },
            "if(isNull(value), 'NULL', concat(variantType(tupleElement(assumeNotNull(value), 1)), ' ', toString(tupleElement(assumeNotNull(value), 2))))",
            new[] { "Int32 1", "NULL", "None 3", "Int32 4" },
            settings);
    }

    private static string Row(string format, string value) => string.Format(System.Globalization.CultureInfo.InvariantCulture, format, value);

    public static IEnumerable<TestCaseData> TimeCases()
    {
        yield return Case("Time64(3)", "Time64(6)", ("'01:02:03.456'", "01:02:03.456000"), ("'-00:00:00.001'", "-00:00:00.001000"));
    }

    // A DateTime64 into a Time64, and the reverse, at the top level, in Nullable, as an Array element, as a Tuple element and
    // as a Map value: the source type, the target type, and the SQL literal of the source row.
    public static IEnumerable<TestCaseData> InstantAndDurationCases()
    {
        const string Instant = "DateTime64(3, 'UTC')";
        const string Duration = "Time64(3)";
        const string Time = "'1970-01-01 00:00:02.345'";
        const string Span = "'00:00:02.345'";
        yield return InstantCase(Instant, "Time64(6)", Time);
        yield return InstantCase(Duration, Instant, Span);
        yield return InstantCase($"Nullable({Instant})", $"Nullable({Duration})", Time);
        yield return InstantCase($"Nullable({Duration})", $"Nullable({Instant})", Span);
        yield return InstantCase($"Array({Instant})", $"Array({Duration})", $"[{Time}]");
        yield return InstantCase($"Array({Duration})", $"Array({Instant})", $"[{Span}]");
        yield return InstantCase($"Tuple({Instant}, Int32)", $"Tuple({Duration}, Int32)", $"({Time}, 7)");
        yield return InstantCase($"Tuple({Duration}, Int32)", $"Tuple({Instant}, Int32)", $"({Span}, 7)");
        yield return InstantCase($"Map(String, {Instant})", $"Map(String, {Duration})", $"{{'k': {Time}}}");
        yield return InstantCase($"Map(String, {Duration})", $"Map(String, {Instant})", $"{{'k': {Span}}}");
    }

    [TestCaseSource(nameof(Cases))]
    public Task InsertAsync_ColumnReadAsARelatedType_StoresTheValuesOfTheSource(string source, string target, string[] literals, string[] expected)
        => AssertStoresTheValuesAsync(source, target, literals, expected, settings: null);

    [TestCaseSource(nameof(TimeCases))]
    [RequiresServerFeature(TcpFeature.Time)]
    public Task InsertAsync_Time64ColumnReadAsAnotherScale_StoresTheValuesOfTheSource(string source, string target, string[] literals, string[] expected)
        => AssertStoresTheValuesAsync(source, target, literals, expected, TimeSettings);

    /// <summary>
    /// A column that a query read, with a part whose values mean other values in the target and parts that hold bytes that
    /// are not UTF-8: the server stores the converted values and the bytes of the other parts.
    /// </summary>
    [TestCaseSource(nameof(PartCases))]
    public Task InsertAsync_ColumnReadAsARelatedComposite_StoresTheBytesOfTheOtherParts(string source, string target, string literal, string readBack, string expected)
        => AssertReadsBackAsync(source, target, literal, readBack, expected);

    /// <summary>
    /// A <c>String</c> column that a query read, or a <c>String</c> part of one, into another type of a string
    /// (<c>LowCardinality</c>, <c>Nullable</c>, <c>FixedString</c>, also in <c>Array</c>, <c>Map</c> and <c>Tuple</c>): the
    /// server stores its bytes, also bytes that are not UTF-8.
    /// </summary>
    [TestCaseSource(nameof(StringCases))]
    public Task InsertAsync_StringReadIntoAnotherTypeOfAString_StoresItsBytes(string source, string target, string literal, string readBack, string expected)
        => AssertReadsBackAsync(source, target, literal, readBack, expected);

    /// <summary>
    /// A column that a query read, with a <c>String</c> part that holds bytes that are not UTF-8, next to a part of
    /// another type (an <c>Enum8</c> into an <c>Int8</c>, a <c>BFloat16</c> into a <c>Float32</c>, a <c>DateTime</c> into
    /// a <c>UInt32</c>, a <c>LowCardinality</c> added or removed), also in a <c>Map</c>, an <c>Array</c> and a nested
    /// <c>Tuple</c>: the server stores the bytes of the <c>String</c> part. The insert sends two rows in each block, so a
    /// block starts after row 0.
    /// </summary>
    [TestCaseSource(nameof(SiblingCases))]
    public Task InsertAsync_ColumnReadWithAStringNextToAPartOfAnotherType_StoresTheStringBytes(
        string source,
        string target,
        string[] literals,
        string readBack,
        string[] expected,
        IReadOnlyDictionary<string, string> settings)
        => AssertStoresInBlocksAsync(source, target, literals, readBack, expected, settings);

    /// <summary>
    /// As the test above, for a <c>Nullable(Tuple)</c> with and without a NULL row, and for a <c>Tuple</c> into a
    /// <c>Nullable(Tuple)</c>.
    /// </summary>
    [TestCaseSource(nameof(NullableTupleSiblingCases))]
    [RequiresServerFeature(TcpFeature.NullableTuple)]
    public Task InsertAsync_NullableTupleReadWithAStringNextToAPartOfAnotherType_StoresTheStringBytes(
        string source,
        string target,
        string[] literals,
        string readBack,
        string[] expected,
        IReadOnlyDictionary<string, string> settings)
        => AssertStoresInBlocksAsync(source, target, literals, readBack, expected, settings);

    /// <summary>
    /// A <c>String</c> column that a query read, also under <c>Nullable</c> and <c>LowCardinality</c> and inside an
    /// <c>Array</c>, a <c>Tuple</c> and a <c>Map</c>, into a <c>Variant</c> whose <c>String</c> alternative takes the text
    /// and the bytes of a value: the server stores the bytes in the <c>String</c> alternative, and a NULL row as the NULL
    /// of the <c>Variant</c>. The insert sends two rows in each block.
    /// </summary>
    [TestCaseSource(nameof(VariantCases))]
    [RequiresServerFeature(TcpFeature.Variant)]
    public Task InsertAsync_StringReadIntoAVariantWithAStringAlternative_StoresItsBytes(
        string source,
        string target,
        string[] literals,
        string readBack,
        string[] expected,
        IReadOnlyDictionary<string, string> settings)
        => AssertStoresInBlocksAsync(source, target, literals, readBack, expected, settings);

    /// <summary>
    /// A column that a query read, whose type holds a <c>String</c>, into a <c>Dynamic</c>, also inside a <c>Tuple</c>
    /// and an <c>Array</c>: only the text of the String values could go there, so the insert refuses the column before it
    /// sends a block, the target stores nothing, and the same connection runs the next query.
    /// </summary>
    [TestCaseSource(nameof(StringTextDynamicCases))]
    [RequiresServerFeature(TcpFeature.Dynamic)]
    public Task InsertAsync_StringReadIntoADynamic_IsRefusedAndKeepsTheConnection(string source, string target, string literal)
        => AssertRefusedAsync(source, target, literal, DynamicVariantSettings, "only through the text of its String values");

    /// <summary>
    /// A column that a query read, whose type holds a <c>String</c>, into a <c>Variant</c> that would take the text of
    /// the String values (a <c>FixedString(N)</c> alternative, an <c>Array(UInt8)</c> alternative for the bytes, an
    /// <c>Enum8</c> alternative, a <c>Tuple</c> alternative): the insert refuses the column before it sends a block.
    /// </summary>
    [TestCaseSource(nameof(StringTextVariantCases))]
    [RequiresServerFeature(TcpFeature.Variant)]
    public Task InsertAsync_StringReadIntoAVariantThatTakesItsText_IsRefusedAndKeepsTheConnection(string source, string target, string literal)
        => AssertRefusedAsync(source, target, literal, VariantSettings, "only through the text of its String values");

    /// <summary>
    /// A column that a query read as a <c>LowCardinality(String)</c> into a <c>JSON</c>, or as a <c>JSON</c> into a
    /// <c>LowCardinality(String)</c> or a <c>Dynamic</c>: only the text of the String values could go there, so the
    /// insert refuses the column before it sends a block.
    /// </summary>
    [TestCaseSource(nameof(StringTextJsonCases))]
    [RequiresServerFeature(TcpFeature.Json | TcpFeature.Dynamic)]
    public Task InsertAsync_StringOrJsonReadIntoAPartThatTakesItsText_IsRefusedAndKeepsTheConnection(string source, string target, string literal)
        => AssertRefusedAsync(source, target, literal, JsonSettings, "only through the text of its String values");

    /// <summary>
    /// A <c>String</c> column that a query read into a <c>JSON</c>, a <c>JSON</c> column into a <c>String</c>, and a
    /// <c>Dynamic</c> column into the same <c>Dynamic</c>: the codec writes each from its storage, as before.
    /// </summary>
    [TestCase("String", "JSON", "'{\"a\":1}'", "toString(value)", "{\"a\":1}")]
    [TestCase("JSON", "String", "'{\"a\":1}'", "value", "{\"a\":1}")]
    [TestCase("Dynamic", "Dynamic", "'abc'", "concat(dynamicType(value), ' ', toString(value))", "String abc")]
    [RequiresServerFeature(TcpFeature.Json | TcpFeature.Dynamic)]
    public Task InsertAsync_StringJsonOrDynamicReadIntoAColumnThatItsCodecWrites_StoresTheValues(string source, string target, string literal, string readBack, string expected)
        => AssertStoresInBlocksAsync(source, target, new[] { literal, literal, literal }, readBack, new[] { expected, expected, expected }, JsonSettings);

    /// <summary>
    /// A <c>Nullable</c> column or part that a query read, into a <c>Variant</c> or a <c>Dynamic</c>, which holds NULL
    /// itself, also as an element of an <c>Array</c> or a <c>Tuple</c> and a value of a <c>Map</c>: each value keeps its
    /// value and a NULL row is the NULL of the target. The insert sends two rows in each block, so the NULL row is in a
    /// later block.
    /// </summary>
    [TestCaseSource(nameof(NullIntoNullHoldingCases))]
    [RequiresServerFeature(TcpFeature.Dynamic)]
    public Task InsertAsync_NullableReadIntoAVariantOrADynamic_StoresTheNullOfTheTarget(
        string source,
        string target,
        string[] literals,
        string readBack,
        string[] expected,
        IReadOnlyDictionary<string, string> settings)
        => AssertStoresInBlocksAsync(source, target, literals, readBack, expected, settings);

    /// <summary>As the test above, for a <c>Nullable(Tuple)</c>, also with a row that an enclosing NULL hides.</summary>
    [TestCaseSource(nameof(NullTupleIntoNullHoldingCases))]
    [RequiresServerFeature(TcpFeature.Dynamic | TcpFeature.NullableTuple)]
    public Task InsertAsync_NullableTupleReadIntoAVariantOrADynamic_StoresTheNullOfTheTarget(
        string source,
        string target,
        string[] literals,
        string readBack,
        string[] expected,
        IReadOnlyDictionary<string, string> settings)
        => AssertStoresInBlocksAsync(source, target, literals, readBack, expected, settings);

    /// <summary>
    /// A <c>Nullable(Int32)</c> column that a query read, with a NULL row, into an <c>Int32</c>, which holds no NULL: the
    /// insert refuses the NULL row, and nothing is stored.
    /// </summary>
    [Test]
    public async Task InsertAsync_NullableReadWithANullRowIntoATypeWithNoNull_IsRefusedWithItsRow()
    {
        string sourceTable = UniqueTableName();
        string targetTable = UniqueTableName();
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        try
        {
            await CreateAsync(connection, sourceTable, targetTable, "Nullable(Int32)", "Int32", new[] { "1", "NULL" }, settings: null);
            Exception refusal = null;
            await using (ClickHouseTcpConnection reader = await TcpServerFixture.ConnectAsync(None))
            {
                await foreach (Block block in reader.QueryAsync($"SELECT id, value FROM {sourceTable} ORDER BY id", cancellationToken: None))
                {
                    refusal = Assert.CatchAsync(
                        async () => await connection.InsertAsync($"INSERT INTO {targetTable} (id, value) VALUES", new[] { block[0], block[1] }, cancellationToken: None));
                }
            }

            // A write that fails inside the block ends its connection, so count on another one.
            await using ClickHouseTcpConnection counter = await TcpServerFixture.ConnectAsync(None);
            List<string> stored = await ReadStringsAsync(counter, targetTable, settings: null);

            Assert.Multiple(() =>
            {
                Assert.That(refusal?.Message, Does.Contain("is null at row 1 of the insert, but it cannot hold null"));
                Assert.That(stored, Is.Empty);
            });
        }
        finally
        {
            await using ClickHouseTcpConnection cleanup = await TcpServerFixture.ConnectAsync(None);
            await ExecuteAsync(cleanup, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(cleanup, $"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    /// <summary>
    /// A column of string values that the caller builds from a <c>String</c> column that a query read goes into a
    /// <c>Dynamic</c> through its text, as any column of string values: a value that is not UTF-8 is stored as U+FFFD.
    /// </summary>
    [Test]
    [RequiresServerFeature(TcpFeature.Dynamic)]
    public async Task InsertAsync_StringValuesBuiltFromAReadColumnIntoADynamic_StoresTheText()
    {
        string sourceTable = UniqueTableName();
        string targetTable = UniqueTableName();
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        try
        {
            await CreateAsync(connection, sourceTable, targetTable, "String", "Dynamic", new[] { "'abc'", "unhex('41FF42')" }, DynamicSettings);
            await using (ClickHouseTcpConnection reader = await TcpServerFixture.ConnectAsync(None))
            {
                await foreach (Block block in reader.QueryAsync($"SELECT id, value FROM {sourceTable} ORDER BY id", settings: DynamicSettings, cancellationToken: None))
                {
                    IColumn<string> text = block.ReadAs<string>(1);
                    var values = new string[block.RowCount];
                    for (int row = 0; row < values.Length; row++)
                    {
                        values[row] = text[row];
                    }

                    IColumn[] columns = { block[0], ClickHouseTcpColumn.Create("value", values) };
                    await connection.InsertAsync($"INSERT INTO {targetTable} (id, value) VALUES", columns, settings: DynamicSettings, cancellationToken: None);
                }
            }

            var stored = new List<string>();
            await foreach (Block block in connection.QueryAsync($"SELECT concat(dynamicType(value), ' ', hex(dynamicElement(value, 'String'))) FROM {targetTable} ORDER BY id", settings: DynamicSettings, cancellationToken: None))
            {
                for (int row = 0; row < block.RowCount; row++)
                {
                    stored.Add((string)block[0].GetValue(row));
                }
            }

            Assert.That(stored, Is.EqualTo(new[] { "String 616263", "String 41EFBFBD42" }));
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    /// <summary>
    /// A <c>String</c> column that a query read, into an <c>Enum8</c>: the text is the label, so the insert writes the
    /// labels; a value that is not a label is refused, and nothing is stored.
    /// </summary>
    [Test]
    public async Task InsertAsync_StringReadIntoAnEnum_WritesTheLabelsAndRefusesAnotherValue()
    {
        await AssertStoresInBlocksAsync(
            "String",
            "Enum8('a' = 1, 'b' = 2)",
            new[] { "'a'", "'b'", "'a'" },
            "concat(toString(value), ' ', toString(CAST(value AS Int8)))",
            new[] { "a 1", "b 2", "a 1" },
            null);

        string sourceTable = UniqueTableName();
        string targetTable = UniqueTableName();
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        try
        {
            await CreateAsync(connection, sourceTable, targetTable, "String", "Enum8('a' = 1, 'b' = 2)", new[] { "'c'" }, settings: null);
            ArgumentException refusal = null;
            await using (ClickHouseTcpConnection reader = await TcpServerFixture.ConnectAsync(None))
            {
                await foreach (Block block in reader.QueryAsync($"SELECT id, value FROM {sourceTable} ORDER BY id", cancellationToken: None))
                {
                    refusal = Assert.ThrowsAsync<ArgumentException>(
                        async () => await connection.InsertAsync($"INSERT INTO {targetTable} (id, value) VALUES", new[] { block[0], block[1] }, cancellationToken: None));
                }
            }

            // A write that fails inside the block ends its connection, so count on another one.
            await using ClickHouseTcpConnection counter = await TcpServerFixture.ConnectAsync(None);
            List<string> stored = await ReadStringsAsync(counter, targetTable, settings: null);

            Assert.Multiple(() =>
            {
                Assert.That(refusal?.Message, Does.Contain("'c' is not a label of 'Enum8('a' = 1, 'b' = 2)'"));
                Assert.That(stored, Is.Empty);
            });
        }
        finally
        {
            await using ClickHouseTcpConnection cleanup = await TcpServerFixture.ConnectAsync(None);
            await ExecuteAsync(cleanup, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(cleanup, $"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    /// <summary>A column that a query read, whose type holds no <c>String</c>, goes into a <c>Dynamic</c> as before.</summary>
    [Test]
    [RequiresServerFeature(TcpFeature.Dynamic)]
    public Task InsertAsync_ReadColumnWithNoStringIntoADynamic_StoresTheValues()
        => AssertStoresInBlocksAsync(
            "Tuple(Int32, Float64)",
            "Dynamic",
            new[] { "(1, 1.5)", "(2, -2.5)", "(3, 0.5)" },
            "concat(dynamicType(value), ' ', toString(value))",
            new[] { "Tuple(Int32, Float64) (1,1.5)", "Tuple(Int32, Float64) (2,-2.5)", "Tuple(Int32, Float64) (3,0.5)" },
            DynamicSettings);

    // Reads the source row through one connection and inserts its block through another: the insert must refuse the column
    // before it sends a block, the target must store nothing, and the connection must run the next query.
    private static async Task AssertRefusedAsync(string source, string target, string literal, IReadOnlyDictionary<string, string> settings, string reason)
    {
        string sourceTable = UniqueTableName();
        string targetTable = UniqueTableName();
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        try
        {
            await CreateAsync(connection, sourceTable, targetTable, source, target, new[] { literal }, settings);

            ArgumentException refusal = null;
            await using (ClickHouseTcpConnection reader = await TcpServerFixture.ConnectAsync(None))
            {
                await foreach (Block block in reader.QueryAsync($"SELECT id, value FROM {sourceTable} ORDER BY id", settings: settings, cancellationToken: None))
                {
                    refusal = Assert.ThrowsAsync<ArgumentException>(
                        async () => await connection.InsertAsync($"INSERT INTO {targetTable} (id, value) VALUES", new[] { block[0], block[1] }, settings: settings, cancellationToken: None));
                }
            }

            var one = new List<string>();
            await foreach (Block block in connection.QueryAsync("SELECT toString(1)", cancellationToken: None))
            {
                for (int row = 0; row < block.RowCount; row++)
                {
                    one.Add((string)block[0].GetValue(row));
                }
            }

            List<string> stored = await ReadStringsAsync(connection, targetTable, settings);

            Assert.Multiple(() =>
            {
                Assert.That(refusal?.Message, Does.Contain($"was given a column that a query read as {source}"));
                Assert.That(refusal?.Message, Does.Contain(reason));
                Assert.That(one, Is.EqualTo(new[] { "1" }), "the connection runs the next query");
                Assert.That(stored, Is.Empty, "nothing may land from a column that could not be written");
            });
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    /// <summary>
    /// A <c>Variant</c> column that a query read, into a <c>Variant</c> that has each of its alternatives and others, also
    /// in an <c>Array</c>, a <c>Tuple</c> and a <c>Map</c>: each value keeps its alternative and its bytes, and a NULL row
    /// stays NULL. The insert sends two rows in each block.
    /// </summary>
    [TestCaseSource(nameof(VariantIntoVariantCases))]
    [RequiresServerFeature(TcpFeature.Variant)]
    public Task InsertAsync_VariantReadIntoAVariantWithItsAlternatives_StoresEachValueInItsAlternative(
        string source,
        string target,
        string[] literals,
        string readBack,
        string[] expected,
        IReadOnlyDictionary<string, string> settings)
        => AssertStoresInBlocksAsync(source, target, literals, readBack, expected, settings);

    /// <summary>
    /// A <c>Variant</c> column that a query read, into a <c>Variant</c> that does not have one of its alternatives: the
    /// insert refuses the column before it sends a block, the target stores nothing, and the same connection runs the next
    /// query.
    /// </summary>
    [TestCase("Variant(String, UInt64)", "Variant(FixedString(3), UInt64)", "unhex('FF')", "String")]
    [TestCase("Variant(Int32, String)", "Variant(Int64, String)", "'abc'", "Int32")]
    [RequiresServerFeature(TcpFeature.Variant)]
    public async Task InsertAsync_VariantReadIntoAVariantWithoutOneOfItsAlternatives_IsRefusedAndKeepsTheConnection(string source, string target, string literal, string missing)
    {
        string sourceTable = UniqueTableName();
        string targetTable = UniqueTableName();
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        try
        {
            await CreateAsync(connection, sourceTable, targetTable, source, target, new[] { literal }, VariantSettings);

            ArgumentException refusal = null;
            await using (ClickHouseTcpConnection reader = await TcpServerFixture.ConnectAsync(None))
            {
                await foreach (Block block in reader.QueryAsync($"SELECT id, value FROM {sourceTable} ORDER BY id", settings: VariantSettings, cancellationToken: None))
                {
                    refusal = Assert.ThrowsAsync<ArgumentException>(
                        async () => await connection.InsertAsync($"INSERT INTO {targetTable} (id, value) VALUES", new[] { block[0], block[1] }, settings: VariantSettings, cancellationToken: None));
                }
            }

            var one = new List<string>();
            await foreach (Block block in connection.QueryAsync("SELECT toString(1)", cancellationToken: None))
            {
                for (int row = 0; row < block.RowCount; row++)
                {
                    one.Add((string)block[0].GetValue(row));
                }
            }

            List<string> stored = await ReadStringsAsync(connection, targetTable, VariantSettings);

            Assert.Multiple(() =>
            {
                Assert.That(refusal?.Message, Does.Contain($"was given a column that a query read as {source}"));
                Assert.That(refusal?.Message, Does.Contain($"has no alternative {missing}"));
                Assert.That(one, Is.EqualTo(new[] { "1" }), "the connection runs the next query");
                Assert.That(stored, Is.Empty, "nothing may land from a column that could not be written");
            });
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    // Reads the source rows through one connection and inserts the block through the client, two rows in each block.
    private static async Task AssertStoresInBlocksAsync(
        string source,
        string target,
        string[] literals,
        string readBack,
        string[] expected,
        IReadOnlyDictionary<string, string> settings)
    {
        TcpServerFixture.SkipIfCloudLocksASetting(settings);
        string sourceTable = UniqueTableName();
        string targetTable = UniqueTableName();
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        try
        {
            await CreateAsync(connection, sourceTable, targetTable, source, target, literals, settings);

            await using (ClickHouseTcpClient client = TcpServerFixture.CreateClient())
            {
                var options = new ClickHouseTcpInsertOptions { Settings = settings, MaxRowsPerBlock = 2 };
                await foreach (Block block in connection.QueryAsync($"SELECT id, value FROM {sourceTable} ORDER BY id", settings: settings, cancellationToken: None))
                {
                    Assert.That(block.RowCount, Is.GreaterThan(2), "the insert splits the block");
                    await client.InsertAsync($"INSERT INTO {targetTable} (id, value) VALUES", new[] { block[0], block[1] }, options, None);
                }
            }

            var stored = new List<string>();
            await foreach (Block block in connection.QueryAsync($"SELECT {readBack} FROM {targetTable} ORDER BY id", settings: settings, cancellationToken: None))
            {
                for (int row = 0; row < block.RowCount; row++)
                {
                    stored.Add((string)block[0].GetValue(row));
                }
            }

            Assert.That(stored, Is.EqualTo(expected));
        }
        finally
        {
            // A failed insert inside the read loop ends the reading connection, so drop the tables on another one.
            await using ClickHouseTcpConnection cleanup = await TcpServerFixture.ConnectAsync(None);
            await ExecuteAsync(cleanup, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(cleanup, $"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    // Reads the source row through one connection, inserts its block through another, and reads back the expression.
    private static async Task AssertReadsBackAsync(string source, string target, string literal, string readBack, string expected)
    {
        string sourceTable = UniqueTableName();
        string targetTable = UniqueTableName();
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        try
        {
            await CreateAsync(connection, sourceTable, targetTable, source, target, new[] { literal }, settings: null);

            await using (ClickHouseTcpConnection reader = await TcpServerFixture.ConnectAsync(None))
            {
                await foreach (Block block in reader.QueryAsync($"SELECT id, value FROM {sourceTable} ORDER BY id", cancellationToken: None))
                {
                    await connection.InsertAsync($"INSERT INTO {targetTable} (id, value) VALUES", new[] { block[0], block[1] }, cancellationToken: None);
                }
            }

            var stored = new List<string>();
            await foreach (Block block in connection.QueryAsync($"SELECT {readBack} FROM {targetTable} ORDER BY id", cancellationToken: None))
            {
                for (int row = 0; row < block.RowCount; row++)
                {
                    stored.Add((string)block[0].GetValue(row));
                }
            }

            Assert.That(stored, Is.EqualTo(new[] { expected }));
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    /// <summary>
    /// A column that a query read, with a NULL row that hides a value that the target cannot hold (<c>nullIf</c> keeps the
    /// value under the NULL): the insert stores the NULL and does not convert or check the hidden value. The source is
    /// checked to hold the hidden value first.
    /// </summary>
    [TestCase(
        "Nullable(Tuple(DateTime64(3, 'UTC'), Decimal(18, 2)))",
        "Nullable(Tuple(DateTime64(6, 'UTC'), Decimal(9, 2)))",
        "tuple(toDateTime64('2024-01-02 03:04:05.678', 3, 'UTC'), toDecimal64('1234567890.12', 2))",
        "tuple(toDateTime64('2024-01-02 03:04:05.679', 3, 'UTC'), toDecimal64('1.23', 2))",
        "('2024-01-02 03:04:05.679000',1.23)")]
    [TestCase("Nullable(Decimal(18, 2))", "Nullable(Decimal(9, 2))", "toDecimal64('1234567890.12', 2)", "toDecimal64('1.23', 2)", "1.23")]
    public async Task InsertAsync_ColumnReadWithAValueHiddenUnderNull_StoresTheNull(string source, string target, string hidden, string value, string expected)
    {
        var settings = new Dictionary<string, string>(StringComparer.Ordinal) { ["allow_experimental_nullable_tuple_type"] = "1" };
        string sourceTable = UniqueTableName();
        string targetTable = UniqueTableName();
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {sourceTable} (id UInt32, value {source}) ENGINE = Memory", settings);
            await ExecuteAsync(connection, $"CREATE TABLE {targetTable} (id UInt32, value {target}) ENGINE = Memory", settings);
            await ExecuteAsync(connection, $"INSERT INTO {sourceTable} (id, value) SELECT 0, nullIf({hidden}, {hidden}) UNION ALL SELECT 1, {value}", settings);

            var held = new List<string>();
            await foreach (Block block in connection.QueryAsync($"SELECT toString(assumeNotNull(value)) FROM {sourceTable} WHERE id = 0", settings: settings, cancellationToken: None))
            {
                held.Add((string)block[0].GetValue(0));
            }

            await using (ClickHouseTcpConnection reader = await TcpServerFixture.ConnectAsync(None))
            {
                await foreach (Block block in reader.QueryAsync($"SELECT id, value FROM {sourceTable} ORDER BY id", settings: settings, cancellationToken: None))
                {
                    await connection.InsertAsync($"INSERT INTO {targetTable} (id, value) VALUES", new[] { block[0], block[1] }, settings: settings, cancellationToken: None);
                }
            }

            var stored = new List<string>();
            await foreach (Block block in connection.QueryAsync($"SELECT ifNull(toString(value), 'NULL') FROM {targetTable} ORDER BY id", settings: settings, cancellationToken: None))
            {
                for (int row = 0; row < block.RowCount; row++)
                {
                    stored.Add((string)block[0].GetValue(row));
                }
            }

            Assert.Multiple(() =>
            {
                Assert.That(held, Has.Count.EqualTo(1).And.Some.Contains("1234567890.12"), "the NULL row of the source hides the value");
                Assert.That(stored, Is.EqualTo(new[] { "NULL", expected }));
            });
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    /// <summary>
    /// A value that the target cannot hold (a label that it does not declare, an instant finer than its scale), a column
    /// that no CLR type converts (a scale above 7), and an <c>Array(UInt32)</c> into an <c>Array(Int32)</c> (also as a part
    /// next to a part of another scale), whose elements no write of <c>Int32</c> takes, are refused, and nothing is stored.
    /// </summary>
    [TestCase("Enum8('a' = 1, 'b' = 2)", "Enum8('a' = 1)", "'b'", "'b' is not a label of 'Enum8('a' = 1)'")]
    [TestCase("DateTime64(6, 'UTC')", "DateTime64(3, 'UTC')", "'2024-01-02 03:04:05.678901'", "without losing precision")]
    [TestCase("DateTime64(9, 'UTC')", "DateTime64(6, 'UTC')", "'2024-01-02 03:04:05.678901234'", "is written only into a column of the same scale")]
    [TestCase(
        "Array(UInt32)",
        "Array(Int32)",
        "[3000000000, 7]",
        "was given a column that a query read as Array(UInt32), and the insert writes such a column part by part. 'Int32' cannot be written from System.UInt32. It is written from: System.Int32.")]
    [TestCase(
        "Tuple(DateTime64(3, 'UTC'), Array(UInt32))",
        "Tuple(DateTime64(6, 'UTC'), Array(Int32))",
        "('2024-01-02 03:04:05.678', [3000000000, 7])",
        "and the insert writes such a column part by part. 'Int32' cannot be written from System.UInt32. It is written from: System.Int32.")]
    public async Task InsertAsync_ColumnReadAsARelatedTypeThatTheTargetCannotHold_IsRefused(string source, string target, string literal, string message)
    {
        string sourceTable = UniqueTableName();
        string targetTable = UniqueTableName();
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        try
        {
            await CreateAsync(connection, sourceTable, targetTable, source, target, new[] { literal }, settings: null);

            ArgumentException refusal = null;
            await using (ClickHouseTcpConnection reader = await TcpServerFixture.ConnectAsync(None))
            {
                await foreach (Block block in reader.QueryAsync($"SELECT id, value FROM {sourceTable} ORDER BY id", cancellationToken: None))
                {
                    refusal = Assert.ThrowsAsync<ArgumentException>(
                        async () => await connection.InsertAsync($"INSERT INTO {targetTable} (id, value) VALUES", new[] { block[0], block[1] }, cancellationToken: None));
                }
            }

            // A write that fails inside the block ends its connection, so count on another one.
            await using ClickHouseTcpConnection counter = await TcpServerFixture.ConnectAsync(None);
            List<string> stored = await ReadStringsAsync(counter, targetTable, settings: null);

            Assert.Multiple(() =>
            {
                Assert.That(refusal?.Message, Does.Contain(message));
                Assert.That(stored, Is.Empty, "nothing may land from a column that could not be written");
            });
        }
        finally
        {
            await using ClickHouseTcpConnection cleanup = await TcpServerFixture.ConnectAsync(None);
            await ExecuteAsync(cleanup, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(cleanup, $"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    /// <summary>
    /// A column that a query read as a <c>DateTime64</c> (an instant), into a <c>Time64</c> (a duration), or the reverse,
    /// also in <c>Nullable</c>, <c>Array</c>, <c>Tuple</c> and <c>Map</c>: the insert refuses the column before it sends a
    /// block, the target stores nothing, and the same connection runs the next query.
    /// </summary>
    [TestCaseSource(nameof(InstantAndDurationCases))]
    [RequiresServerFeature(TcpFeature.Time)]
    public async Task InsertAsync_ColumnReadAsAnInstantIntoADurationOrTheReverse_IsRefusedAndKeepsTheConnection(string source, string target, string literal)
    {
        TcpServerFixture.SkipIfCloudLocksASetting(TimeSettings);
        string sourceTable = UniqueTableName();
        string targetTable = UniqueTableName();
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        try
        {
            await CreateAsync(connection, sourceTable, targetTable, source, target, new[] { literal }, TimeSettings);

            ArgumentException refusal = null;
            await using (ClickHouseTcpConnection reader = await TcpServerFixture.ConnectAsync(None))
            {
                await foreach (Block block in reader.QueryAsync($"SELECT id, value FROM {sourceTable} ORDER BY id", settings: TimeSettings, cancellationToken: None))
                {
                    refusal = Assert.ThrowsAsync<ArgumentException>(
                        async () => await connection.InsertAsync($"INSERT INTO {targetTable} (id, value) VALUES", new[] { block[0], block[1] }, settings: TimeSettings, cancellationToken: None));
                }
            }

            List<string> stored = await ReadStringsAsync(connection, targetTable, TimeSettings);

            Assert.Multiple(() =>
            {
                Assert.That(refusal?.ParamName, Is.EqualTo("columns"));
                Assert.That(refusal?.Message, Does.Contain($"was given a column that a query read as {source}"));
                Assert.That(refusal?.Message, Does.Contain("is an instant").And.Contain("is a duration"));
                Assert.That(refusal?.Message, Does.Contain("Convert the values in the query, or insert the counts as a column of System.Int64 values."));
                Assert.That(stored, Is.Empty, "nothing may land from a column that could not be written");
            });
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    /// <summary>
    /// Counts that the caller gives as <c>long</c> values go into a <c>Time64</c> and a <c>DateTime64</c> as counts, and a
    /// <c>DateTime64</c> that a query read goes into an <c>Int64</c> as its count.
    /// </summary>
    [Test]
    [RequiresServerFeature(TcpFeature.Time)]
    public async Task InsertAsync_CountsIntoATimeType_StoresTheCounts()
    {
        TcpServerFixture.SkipIfCloudLocksASetting(TimeSettings);
        string sourceTable = UniqueTableName();
        string targetTable = UniqueTableName();
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        try
        {
            await CreateAsync(connection, sourceTable, targetTable, "DateTime64(3, 'UTC')", "Int64", new[] { "'1970-01-01 00:00:02.345'" }, TimeSettings);
            await using (ClickHouseTcpConnection reader = await TcpServerFixture.ConnectAsync(None))
            {
                await foreach (Block block in reader.QueryAsync($"SELECT id, value FROM {sourceTable} ORDER BY id", settings: TimeSettings, cancellationToken: None))
                {
                    await connection.InsertAsync($"INSERT INTO {targetTable} (id, value) VALUES", new[] { block[0], block[1] }, settings: TimeSettings, cancellationToken: None);
                }
            }

            string durations = UniqueTableName();
            string instants = UniqueTableName();
            try
            {
                await ExecuteAsync(connection, $"CREATE TABLE {durations} (id UInt32, value Time64(3)) ENGINE = Memory", TimeSettings);
                await ExecuteAsync(connection, $"CREATE TABLE {instants} (id UInt32, value DateTime64(3, 'UTC')) ENGINE = Memory", TimeSettings);
                IColumn[] counts = { ClickHouseTcpColumn.Create("id", new uint[] { 0 }), ClickHouseTcpColumn.Create("value", new long[] { 2345 }) };
                await connection.InsertAsync($"INSERT INTO {durations} (id, value) VALUES", counts, settings: TimeSettings, cancellationToken: None);
                await connection.InsertAsync($"INSERT INTO {instants} (id, value) VALUES", counts, settings: TimeSettings, cancellationToken: None);

                List<string> int64 = await ReadStringsAsync(connection, targetTable, TimeSettings);
                List<string> time64 = await ReadStringsAsync(connection, durations, TimeSettings);
                List<string> dateTime64 = await ReadStringsAsync(connection, instants, TimeSettings);

                Assert.Multiple(() =>
                {
                    Assert.That(int64, Is.EqualTo(new[] { "2345" }));
                    Assert.That(time64, Is.EqualTo(new[] { "00:00:02.345" }));
                    Assert.That(dateTime64, Is.EqualTo(new[] { "1970-01-01 00:00:02.345" }));
                    Assert.That(ClickHouseTcpTypes.CanWrite("Time64(6)", typeof(long)), Is.True);
                });
            }
            finally
            {
                await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {durations}");
                await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {instants}");
            }
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    private static TestCaseData InstantCase(string source, string target, string literal)
        => new TestCaseData(source, target, literal).SetArgDisplayNames(source, target);

    private static TestCaseData SiblingCase(string source, string target, string[] literals, string readBack, string[] expected, IReadOnlyDictionary<string, string> settings = null)
        => new TestCaseData(source, target, literals, readBack, expected, settings)
            .SetArgDisplayNames(source, target, Array.Exists(literals, literal => literal.Contains("NULL", StringComparison.Ordinal)) ? "a NULL" : "no NULL");

    private static TestCaseData TextCase(string source, string target, string literal)
        => new TestCaseData(source, target, literal).SetArgDisplayNames(source, target);

    private static TestCaseData PartCase(string source, string target, string literal, string readBack, string expected)
        => new TestCaseData(source, target, literal, readBack, expected).SetArgDisplayNames(source, target);

    // One row for each pair: the SQL literal that the source table holds, and the text that the target reads back.
    private static TestCaseData Case(string source, string target, params (string Literal, string Expected)[] rows)
        => new TestCaseData(source, target, Array.ConvertAll(rows, row => row.Literal), Array.ConvertAll(rows, row => row.Expected)).SetArgDisplayNames(source, target);

    // Reads the source table through one connection and inserts each block, as it is decoded, through another.
    private static async Task AssertStoresTheValuesAsync(string source, string target, string[] literals, string[] expected, IReadOnlyDictionary<string, string> settings)
    {
        string sourceTable = UniqueTableName();
        string targetTable = UniqueTableName();
        await using ClickHouseTcpConnection connection = await TcpServerFixture.ConnectAsync(None);
        try
        {
            await CreateAsync(connection, sourceTable, targetTable, source, target, literals, settings);

            await using (ClickHouseTcpConnection reader = await TcpServerFixture.ConnectAsync(None))
            {
                await foreach (Block block in reader.QueryAsync($"SELECT id, value FROM {sourceTable} ORDER BY id", settings: settings, cancellationToken: None))
                {
                    await connection.InsertAsync($"INSERT INTO {targetTable} (id, value) VALUES", new[] { block[0], block[1] }, settings: settings, cancellationToken: None);
                }
            }

            Assert.That(await ReadStringsAsync(connection, targetTable, settings), Is.EqualTo(expected));
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    // The two tables, and the source rows: one row for each literal, in order.
    private static async Task CreateAsync(
        ClickHouseTcpConnection connection,
        string sourceTable,
        string targetTable,
        string source,
        string target,
        string[] literals,
        IReadOnlyDictionary<string, string> settings)
    {
        await ExecuteAsync(connection, $"CREATE TABLE {sourceTable} (id UInt32, value {source}) ENGINE = Memory", settings);
        await ExecuteAsync(connection, $"CREATE TABLE {targetTable} (id UInt32, value {target}) ENGINE = Memory", settings);
        string rows = string.Join(", ", System.Linq.Enumerable.Select(literals, (literal, id) => $"({id}, {literal})"));
        await ExecuteAsync(connection, $"INSERT INTO {sourceTable} (id, value) VALUES {rows}", settings);
    }

    private static async Task<List<string>> ReadStringsAsync(ClickHouseTcpConnection connection, string table, IReadOnlyDictionary<string, string> settings)
    {
        var values = new List<string>();
        await foreach (Block block in connection.QueryAsync($"SELECT toString(value) FROM {table} ORDER BY id", settings: settings, cancellationToken: None))
        {
            for (int row = 0; row < block.RowCount; row++)
            {
                values.Add((string)block[0].GetValue(row));
            }
        }

        return values;
    }

    private static async Task ExecuteAsync(ClickHouseTcpConnection connection, string sql, IReadOnlyDictionary<string, string> settings = null)
    {
        await foreach (Block block in connection.QueryAsync(sql, settings: settings, cancellationToken: None))
        {
            _ = block;
        }
    }

    private static string UniqueTableName() => $"tcp_decoded_insert_test_{Guid.NewGuid():N}";
}
