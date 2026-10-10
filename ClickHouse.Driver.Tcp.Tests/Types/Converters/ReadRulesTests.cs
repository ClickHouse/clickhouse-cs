using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// The read rules of D6 (<see cref="ReadRules"/>): the derivation accepts exactly the readings that POCO mapping
/// accepts, with the same values, for a matrix of column types and CLR targets that the differential case list does not
/// have (enums, casts, nullable targets, the array casts that read elements as another type).
/// </summary>
[TestFixture]
public class ReadRulesTests
{
    private static readonly string[] ColumnTypes =
    {
        "Int8", "Int32", "UInt32", "Int64", "Enum8('a' = 1, 'b' = 2)", "String", "FixedString(4)", "Date", "DateTime('UTC')", "UUID",
        "Nullable(Int8)", "Nullable(Int32)", "Nullable(UInt32)", "Nullable(String)", "Nullable(Enum8('a' = 1, 'b' = 2))",
        "Nullable(DateTime('UTC'))", "Nullable(Tuple(String, UInt8))",
        "LowCardinality(String)", "LowCardinality(Int32)", "LowCardinality(Nullable(String))", "LowCardinality(Nullable(Int8))",
        "LowCardinality(Nullable(DateTime('UTC')))",
        "Array(Int8)", "Array(Int32)", "Array(UInt32)", "Array(String)", "Array(Array(UInt32))", "Array(Nullable(Int32))",
        "Array(LowCardinality(String))", "Map(String, Int32)", "Tuple(Int32, String)", "Tuple(String, UInt8)",
        "Variant(String, UInt64)", "Dynamic", "Point", "SimpleAggregateFunction(anyLast, Nullable(Int32))",
    };

    private static readonly Type[] Targets =
    {
        typeof(object), typeof(ValueType), typeof(IComparable), typeof(IFormattable), typeof(IEnumerable), typeof(IEnumerable<int>),
        typeof(IReadOnlyList<int>), typeof(IEnumerable<string>), typeof(IReadOnlyList<SByteEnum>), typeof(string), typeof(byte[]),
        typeof(sbyte), typeof(sbyte?), typeof(byte), typeof(int), typeof(int?), typeof(uint), typeof(uint?), typeof(long), typeof(long?),
        typeof(SByteEnum), typeof(SByteEnum?), typeof(ByteEnum), typeof(IntEnum), typeof(IntEnum?), typeof(UIntEnum), typeof(UIntEnum?),
        typeof(SByteEnum[]), typeof(IntEnum[]), typeof(UIntEnum[]), typeof(int[]), typeof(uint[]), typeof(sbyte[]), typeof(int[][]),
        typeof(uint[][]), typeof(object[]), typeof(string[]), typeof(int?[]), typeof(DateTime), typeof(DateTime?), typeof(DateTimeOffset),
        typeof(DateTimeOffset?), typeof(DateOnly?), typeof(Guid?), typeof((int, string)), typeof((int, string)?), typeof((byte[], byte)),
        typeof((byte[], byte)?), typeof((string, byte)?), typeof(KeyValuePair<string, int>[]), typeof(KeyValuePair<byte[], int>[]),
        typeof((double, double)?),
    };

    internal enum SByteEnum : sbyte
    {
        A = 1,
    }

    internal enum ByteEnum : byte
    {
        A = 1,
    }

    internal enum IntEnum
    {
        A = 1,
    }

    internal enum UIntEnum : uint
    {
        A = 1,
    }

    private static IEnumerable<string> Types() => ColumnTypes;

    [TestCaseSource(nameof(Types))]
    public void Derive_EachTarget_AcceptsWhatPocoMappingAcceptsWithTheSameValues(string columnType)
    {
        var differences = new List<string>();
        using Block block = DecodeSample(columnType);
        foreach (Type target in Targets)
        {
            string difference = (string)ConverterHarness.InvokeGeneric(typeof(ReadRulesTests), nameof(Compare), new[] { target }, block);
            if (difference is not null)
            {
                differences.Add($"{TypeNames.Of(target)}: {difference}");
            }
        }

        Assert.That(differences, Is.Empty, string.Join(Environment.NewLine, differences));
    }

    [Test]
    public void ReinterpretsElements_EveryAcceptedArrayCast_IsListed()
    {
        // The canonical array types of the client: an array of each leaf's canonical type, and the jagged forms.
        Type[] elements = LeafTable.All.SelectMany(leaf => leaf.Reads.Where(pair => !pair.IsConversion).Select(pair => pair.ClrType)).Distinct().ToArray();
        Type[] sources = elements.Select(e => e.MakeArrayType()).Concat(elements.Select(e => e.MakeArrayType().MakeArrayType())).Distinct().ToArray();
        Type[] integers = { typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(ulong), typeof(bool), typeof(char) };
        Type[] enums = { typeof(SByteEnum), typeof(ByteEnum), typeof(IntEnum), typeof(UIntEnum) };
        IEnumerable<Type> targetElements = integers.Concat(enums);
        Type[] targets = targetElements.SelectMany(e => new[] { e.MakeArrayType(), e.MakeArrayType().MakeArrayType(), typeof(IReadOnlyList<>).MakeGenericType(e) }).ToArray();

        string[] accepted = sources
            .SelectMany(source => targets.Where(target => ReadRules.CanConvert(source, target) && ReadRules.ReinterpretsElements(source, target))
                .Select(target => $"{TypeNames.Of(source)} as {TypeNames.Of(target)}"))
            .OrderBy(text => text, StringComparer.Ordinal)
            .ToArray();

        string[] expected =
        {
            "byte[] as ByteEnum[]", "byte[] as IReadOnlyList<ByteEnum>", "byte[] as IReadOnlyList<SByteEnum>", "byte[] as IReadOnlyList<sbyte>",
            "byte[] as SByteEnum[]", "byte[] as sbyte[]", "byte[][] as ByteEnum[][]", "byte[][] as SByteEnum[][]", "byte[][] as sbyte[][]",
            "int[] as IReadOnlyList<IntEnum>", "int[] as IReadOnlyList<UIntEnum>", "int[] as IReadOnlyList<uint>", "int[] as IntEnum[]",
            "int[] as UIntEnum[]", "int[] as uint[]", "int[][] as IntEnum[][]", "int[][] as UIntEnum[][]", "int[][] as uint[][]",
            "long[] as IReadOnlyList<ulong>", "long[] as ulong[]", "long[][] as ulong[][]",
            "sbyte[] as ByteEnum[]", "sbyte[] as IReadOnlyList<ByteEnum>", "sbyte[] as IReadOnlyList<SByteEnum>", "sbyte[] as IReadOnlyList<byte>",
            "sbyte[] as SByteEnum[]", "sbyte[] as byte[]", "sbyte[][] as ByteEnum[][]", "sbyte[][] as SByteEnum[][]", "sbyte[][] as byte[][]",
            "short[] as IReadOnlyList<ushort>", "short[] as ushort[]", "short[][] as ushort[][]",
            "uint[] as IReadOnlyList<IntEnum>", "uint[] as IReadOnlyList<UIntEnum>", "uint[] as IReadOnlyList<int>", "uint[] as IntEnum[]",
            "uint[] as UIntEnum[]", "uint[] as int[]", "uint[][] as IntEnum[][]", "uint[][] as UIntEnum[][]", "uint[][] as int[][]",
            "ulong[] as IReadOnlyList<long>", "ulong[] as long[]", "ulong[][] as long[][]",
            "ushort[] as IReadOnlyList<short>", "ushort[] as short[]", "ushort[][] as short[][]",
        };

        Assert.That(accepted, Is.EqualTo(expected), "Accepted:" + Environment.NewLine + string.Join(Environment.NewLine, accepted.Select(a => $"\"{a}\",")));
    }

    [Test]
    public void ReadAs_ArrayCastThatReadsElementsAsAnotherType_KeepsTheArrayAndItsBits()
    {
        using Block block = Decode("Array(UInt32)", new ArrayColumn<uint[]>("value", "Array(UInt32)", new[] { new[] { 3_000_000_000u, 7u } }));

        int[][] values = Fill<int[]>(block);

        Assert.Multiple(() =>
        {
            Assert.That(values[0][0], Is.EqualTo(-1_294_967_296));
            Assert.That(values[0][1], Is.EqualTo(7));
            Assert.That(values[0].GetType(), Is.EqualTo(typeof(uint[])), "The cast keeps the array that the column reads.");
        });
    }

    [TestCase(typeof(string[]), typeof(object[]), false)]
    [TestCase(typeof(uint[]), typeof(object), false)]
    [TestCase(typeof(uint[]), typeof(IEnumerable), false)]
    [TestCase(typeof(uint[]), typeof(uint[]), false)]
    [TestCase(typeof(uint[]), typeof(long[]), false)]
    [TestCase(typeof(uint), typeof(int), false)]
    [TestCase(typeof(uint[]), typeof(int[]), true)]
    [TestCase(typeof(uint[][]), typeof(int[][]), true)]
    [TestCase(typeof(sbyte[]), typeof(IEnumerable<SByteEnum>), true)]
    public void ReinterpretsElements_Cast_IsWhatTheCastDoesToTheElements(Type from, Type to, bool reinterprets)
        => Assert.That(ReadRules.ReinterpretsElements(from, to), Is.EqualTo(reinterprets));

    [TestCase(typeof(sbyte), typeof(SByteEnum), true)]
    [TestCase(typeof(byte), typeof(SByteEnum), false)]
    [TestCase(typeof(int), typeof(IntEnum), true)]
    [TestCase(typeof(int), typeof(UIntEnum), false)]
    [TestCase(typeof(int), typeof(long), false)]
    [TestCase(typeof(int?), typeof(object), true)]
    [TestCase(typeof(int?), typeof(IComparable), false)]
    [TestCase(typeof(int), typeof(IComparable), true)]
    [TestCase(typeof(string), typeof(IEnumerable<char>), true)]
    public void CanConvert_Pair_IsTheEnumOrdinalOrTheClrCast(Type from, Type to, bool converts)
        => Assert.That(ReadRules.CanConvert(from, to), Is.EqualTo(converts));

    /// <summary>A decoded block of the sample column of a type (5 rows, NULL in rows 1 and 4).</summary>
    internal static Block DecodeSample(string columnType) => Decode(columnType, SampleColumns.Build("value", SampleType(columnType)));

    // The type whose sample values the column takes: a geo type or a SimpleAggregateFunction has the values of its structure.
    private static string SampleType(string columnType)
    {
        TypeNode node = TypeParser.Parse(columnType);
        return node.Name switch
        {
            "Point" => "Tuple(Float64, Float64)",
            "SimpleAggregateFunction" => node.Arguments[1].ToString(),
            _ => columnType,
        };
    }

    /// <summary>Writes a column with its codec and decodes it into a block of one column called <c>value</c>.</summary>
    internal static Block Decode(string columnType, IColumn source)
    {
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(columnType, DifferentialEngine.Context);
        byte[] bytes = CodecTestHarness.WriteAsync(w => codec.WriteFull(w, source)).GetAwaiter().GetResult();
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        if (source.RowCount > 0)
        {
            codec.ReadStatePrefixAsync(reader, CodecTestHarness.None).AsTask().GetAwaiter().GetResult();
        }

        IColumn column = codec.ReadColumnAsync(reader, "value", columnType, source.RowCount, CodecTestHarness.None).AsTask().GetAwaiter().GetResult();
        source.Dispose();
        return new Block(string.Empty, BlockInfo.Default, column.RowCount, new[] { column }, ColumnCodecRegistry.Default, DifferentialEngine.Context);
    }

    /// <summary>All the rows of the block's column through the derived reader's bulk read.</summary>
    internal static T[] Fill<T>(Block block)
    {
        ColumnReader<T> reader = ConverterDerivation.Default.Reader<T>(block[0].TypeName, block.Context);
        var values = new T[block.RowCount];
        reader.Bind(block[0]).Fill(0, values);
        return values;
    }

    // The difference between the derivation and POCO mapping for one target, or null.
    private static string Compare<T>(Block block)
    {
        Derivation derivation = ConverterDerivation.Default.Derive(block[0].TypeName, block.Context, typeof(T), ConversionDirection.Read);
        RowReader<T> poco;
        try
        {
            poco = ClientArms.Poco.Bind<T>(block);
        }
        catch (InvalidOperationException) when (!derivation.Succeeded)
        {
            return null;
        }
        catch (InvalidOperationException e)
        {
            return $"POCO mapping refuses ({e.Message}), and the derivation accepts.";
        }

        if (!derivation.Succeeded)
        {
            return $"POCO mapping accepts, and the derivation refuses: {derivation.Refusal}";
        }

        (T[] Values, Exception Failure) expected = Run(() => poco(0, block.RowCount));
        (T[] Values, Exception Failure) fill = Run(() => Fill<T>(block));
        (T[] Values, Exception Failure) emit = Run(() => ConverterHarness.ReadEmit((ColumnReader<T>)derivation.Converter, block[0], 0, block.RowCount));
        return Difference(expected, fill, "Fill") ?? Difference(expected, emit, "Emit");
    }

    private static (T[] Values, Exception Failure) Run<T>(Func<T[]> read)
    {
        try
        {
            return (read(), null);
        }
        catch (Exception e)
        {
            return (null, e);
        }
    }

    // A NULL fails POCO mapping with its own message, which names the row of the result; the reader names the row.
    private static string Difference<T>((T[] Values, Exception Failure) expected, (T[] Values, Exception Failure) actual, string path)
    {
        if (expected.Failure is not null || actual.Failure is not null)
        {
            if (expected.Failure is InvalidOperationException poco && actual.Failure is NullValueException nullValue)
            {
                return poco.Message.Contains($"is NULL at row {nullValue.Row} of the result", StringComparison.Ordinal)
                    ? null
                    : $"{path}: POCO mapping fails with \"{poco.Message}\", and the reader finds NULL at row {nullValue.Row}.";
            }

            return expected.Failure?.GetType() == actual.Failure?.GetType() && expected.Failure?.Message == actual.Failure?.Message
                ? null
                : $"{path}: POCO mapping gives {Describe(expected)}, and the reader gives {Describe(actual)}.";
        }

        for (int i = 0; i < expected.Values.Length; i++)
        {
            string difference = ValueComparer.Difference(expected.Values[i], actual.Values[i]);
            if (difference is not null)
            {
                return $"{path}: row {i}: {difference}";
            }
        }

        return null;
    }

    private static string Describe<T>((T[] Values, Exception Failure) outcome)
        => outcome.Failure is not null ? $"{outcome.Failure.GetType().Name}: {outcome.Failure.Message}" : "values";
}
