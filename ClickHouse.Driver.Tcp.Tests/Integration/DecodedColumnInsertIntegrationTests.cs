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
    }

    // A String that a query read, into a Variant whose alternative for text is not a String, or into a Dynamic: these
    // parts go through the text of their values. Every String value is valid UTF-8, so the stored values are the source.
    public static IEnumerable<TestCaseData> TextRouteCases()
    {
        string[] strings = { "'abc'", "'xyz'", "'def'" };
        yield return SiblingCase(
            "String",
            "Variant(FixedString(3), Int64)",
            strings,
            "concat(variantType(value), ' ', hex(variantElement(value, 'FixedString(3)')))",
            new[] { "FixedString(3) 616263", "FixedString(3) 78797A", "FixedString(3) 646566" },
            VariantSettings);
    }

    public static IEnumerable<TestCaseData> DynamicTextRouteCases()
    {
        yield return SiblingCase(
            "String",
            "Dynamic",
            new[] { "'abc'", "'xyz'", "'def'" },
            "concat(dynamicType(value), ' ', hex(dynamicElement(value, 'String')))",
            new[] { "String 616263", "String 78797A", "String 646566" },
            DynamicSettings);
    }

    public static IEnumerable<TestCaseData> TimeCases()
    {
        yield return Case("Time64(3)", "Time64(6)", ("'01:02:03.456'", "01:02:03.456000"), ("'-00:00:00.001'", "-00:00:00.001000"));
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
    /// A <c>String</c> column that a query read, into a <c>Variant</c> whose alternative for text is a
    /// <c>FixedString(N)</c>: the values go through their text into that alternative.
    /// </summary>
    [TestCaseSource(nameof(TextRouteCases))]
    [RequiresServerFeature(TcpFeature.Variant)]
    public Task InsertAsync_StringReadIntoAVariantWithNoStringAlternative_StoresItsText(
        string source,
        string target,
        string[] literals,
        string readBack,
        string[] expected,
        IReadOnlyDictionary<string, string> settings)
        => AssertStoresInBlocksAsync(source, target, literals, readBack, expected, settings);

    /// <summary>A <c>String</c> column that a query read, into a <c>Dynamic</c>: the values go through their text.</summary>
    [TestCaseSource(nameof(DynamicTextRouteCases))]
    [RequiresServerFeature(TcpFeature.Dynamic)]
    public Task InsertAsync_StringReadIntoADynamic_StoresItsText(
        string source,
        string target,
        string[] literals,
        string readBack,
        string[] expected,
        IReadOnlyDictionary<string, string> settings)
        => AssertStoresInBlocksAsync(source, target, literals, readBack, expected, settings);

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
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {sourceTable}");
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {targetTable}");
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
    /// A value that the target cannot hold (a label that it does not declare, an instant finer than its scale), and a
    /// column that no CLR type converts (a scale above 7), are refused, and nothing is stored.
    /// </summary>
    [TestCase("Enum8('a' = 1, 'b' = 2)", "Enum8('a' = 1)", "'b'", "'b' is not a label of 'Enum8('a' = 1)'")]
    [TestCase("DateTime64(6, 'UTC')", "DateTime64(3, 'UTC')", "'2024-01-02 03:04:05.678901'", "without losing precision")]
    [TestCase("DateTime64(9, 'UTC')", "DateTime64(6, 'UTC')", "'2024-01-02 03:04:05.678901234'", "is written only into a column of the same scale")]
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

    private static TestCaseData SiblingCase(string source, string target, string[] literals, string readBack, string[] expected, IReadOnlyDictionary<string, string> settings = null)
        => new TestCaseData(source, target, literals, readBack, expected, settings)
            .SetArgDisplayNames(source, target, Array.IndexOf(literals, "NULL") < 0 ? "no NULL row" : "a NULL row");

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
