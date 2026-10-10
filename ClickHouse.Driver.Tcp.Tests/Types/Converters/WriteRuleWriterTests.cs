using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;
using static ClickHouse.Driver.Tcp.Tests.Types.Converters.WriteRulesTests;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// The writers of the D6 write rules (<see cref="WriteRules"/>) on the sources that the differential tests do not give
/// them: segments, marked positions, a start above row 0, the inner writers of each kind, and the first NULL.
/// </summary>
[TestFixture]
public class WriteRuleWriterTests
{
    private static readonly ConverterDerivation Derivation = ConverterDerivation.Default;

    private static readonly Type[] TypesThatNoWriterTakes = { typeof(int).MakePointerType(), typeof(int).MakeByRefType(), typeof(Span<int>), typeof(Nullable<>), typeof(void) };

    [TestCase("Int32", typeof(IntEnum), typeof(EnumWriter<IntEnum, int>))]
    [TestCase("Nullable(Int32)", typeof(IntEnum?), typeof(NullableEnumWriter<IntEnum, int>))]
    [TestCase("Int32", typeof(int?), typeof(NonNullWriter<int>))]
    [TestCase("Int32", typeof(IntEnum?), typeof(NonNullWriter<IntEnum>))]
    [TestCase("Variant(Int32, String)", typeof(string), typeof(AssignWriter<string, object>))]
    [TestCase("Dynamic", typeof(int?), typeof(AssignWriter<int?, object>))]
    [TestCase("String", typeof(ByteEnum[]), typeof(AssignWriter<ByteEnum[], byte[]>))]
    [TestCase("Nullable(Int32)", typeof(int), typeof(LiftWriter<int>))]
    [TestCase("Nullable(Int32)", typeof(IntEnum), typeof(EnumWriter<IntEnum, int>))]
    [TestCase("LowCardinality(Nullable(Int32))", typeof(int), typeof(LiftWriter<int>))]
    public void Derive_SourceThatOnlyARuleWrites_GivesTheWriterOfTheRule(string type, Type source, Type writer)
    {
        Derivation derived = Derivation.Derive(type, ConverterHarness.Context, source, ConversionDirection.Write);

        Assert.That(derived.Converter, Is.InstanceOf(writer));
    }

    [TestCase("Int32", typeof(object), "'Int32' cannot be written from System.Object. It is written from: System.Int32.")]
    [TestCase("Int64", typeof(int), "'Int64' cannot be written from System.Int32. It is written from: System.Int64.")]
    [TestCase("Int32", typeof(LongEnum), "'Int32' cannot be written from ClickHouse.Driver.Tcp.Tests.Types.Converters.WriteRulesTests+LongEnum. It is written from: System.Int32.")]
    [TestCase("String", typeof(sbyte[]), "'String' cannot be written from System.SByte[]. It is written from: System.String, System.Byte[].")]
    [TestCase("Array(Nullable(Int32))", typeof(int[]), "'Nullable(Int32)' cannot be written from System.Int32, which cannot hold NULL. It is written from a nullable type. It is inside the column type 'Array(Nullable(Int32))'.")]
    public void Derive_SourceThatNoRuleWrites_GivesTheRefusalOfTheColumnTypesOwnWrites(string type, Type source, string refusal)
    {
        Derivation derived = Derivation.Derive(type, ConverterHarness.Context, source, ConversionDirection.Write);

        Assert.That(derived.Refusal, Is.EqualTo(refusal));
    }

    [TestCaseSource(nameof(TypesThatNoWriterTakes))]
    public void Derive_TypeThatNoWriterTakes_RefusesWithNoException(Type source)
    {
        Assert.That(Derivation.Derive("Int32", ConverterHarness.Context, source, ConversionDirection.Write).Succeeded, Is.False);
    }

    [Test]
    public async Task Write_EnumFromSegments_GivesTheBytesOfTheOrdinals()
    {
        var enums = new[] { new[] { (IntEnum)1, (IntEnum)(-2) }, Array.Empty<IntEnum>(), new[] { (IntEnum)300 } };
        var ordinals = new[] { new[] { 1, -2 }, Array.Empty<int>(), new[] { 300 } };

        byte[] expected = await ConverterHarness.WriteSegmentsAsync(Derivation.Writer<int>("Int32", ConverterHarness.Context), ordinals);
        byte[] actual = await ConverterHarness.WriteSegmentsAsync(Derivation.Writer<IntEnum>("Int32", ConverterHarness.Context), enums);

        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public async Task Write_NullableEnumFromRowTwo_GivesTheBytesOfTheNullableOrdinals()
    {
        IntEnum?[] enums = { (IntEnum)1, null, (IntEnum)7, null, (IntEnum)9 };
        int?[] ordinals = { 1, null, 7, null, 9 };

        byte[] expected = await ConverterHarness.WriteNewAsync(Derivation.Writer<int?>("Nullable(Int32)", ConverterHarness.Context), ordinals, 2, 3);
        byte[] actual = await ConverterHarness.WriteNewAsync(Derivation.Writer<IntEnum?>("Nullable(Int32)", ConverterHarness.Context), enums, 2, 3);

        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public async Task Write_NullableEnumFromSegments_GivesTheBytesOfTheNullableOrdinals()
    {
        var enums = new[] { new IntEnum?[] { (IntEnum)1, null }, new IntEnum?[] { (IntEnum)3 } };
        var ordinals = new[] { new int?[] { 1, null }, new int?[] { 3 } };

        byte[] expected = await ConverterHarness.WriteSegmentsAsync(Derivation.Writer<int?>("Nullable(Int32)", ConverterHarness.Context), ordinals);
        byte[] actual = await ConverterHarness.WriteSegmentsAsync(Derivation.Writer<IntEnum?>("Nullable(Int32)", ConverterHarness.Context), enums);

        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public async Task Write_CastOfReferencesFromRowOne_GivesTheBytesOfTheTargetType()
    {
        string[] texts = { "skip", "a", "b" };
        object[] objects = { "skip", "a", "b" };

        byte[] expected = await ConverterHarness.WriteNewAsync(Derivation.Writer<object>("Variant(Int32, String)", ConverterHarness.Context), objects, 1, 2);
        byte[] actual = await ConverterHarness.WriteNewAsync(Derivation.Writer<string>("Variant(Int32, String)", ConverterHarness.Context), texts, 1, 2);

        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public async Task Write_CastOfReferencesFromSegments_GivesTheBytesOfTheTargetType()
    {
        var bytes = new[] { new ByteEnum[][] { new[] { (ByteEnum)255, (ByteEnum)2 } }, new ByteEnum[][] { new[] { (ByteEnum)3 } } };
        var expected = new[] { new byte[][] { new byte[] { 255, 2 } }, new byte[][] { new byte[] { 3 } } };

        byte[] old = await ConverterHarness.WriteSegmentsAsync(Derivation.Writer<byte[]>("String", ConverterHarness.Context), expected);
        byte[] actual = await ConverterHarness.WriteSegmentsAsync(Derivation.Writer<ByteEnum[]>("String", ConverterHarness.Context), bytes);

        Assert.That(actual, Is.EqualTo(old));
    }

    [Test]
    public async Task Write_CastOfANullableValueType_BoxesEachValueAndKeepsTheNull()
    {
        int?[] values = { 1, null, 3 };
        object[] boxed = { 1, null, 3 };

        byte[] expected = await ConverterHarness.WriteNewAsync(Derivation.Writer<object>("Variant(Int32, String)", ConverterHarness.Context), boxed, 0, 3);
        byte[] actual = await ConverterHarness.WriteNewAsync(Derivation.Writer<int?>("Variant(Int32, String)", ConverterHarness.Context), values, 0, 3);

        Assert.That(actual, Is.EqualTo(expected));
    }

    [TestCase("Nullable(Int32)")]
    [TestCase("LowCardinality(Nullable(Int32))")]
    [TestCase("SimpleAggregateFunction(anyLast, Nullable(Int32))")]
    public async Task Write_ValueTypeIntoATypeWrittenFromItsNullableType_GivesTheBytesOfValuesThatAreNotNull(string type)
    {
        int[] values = { 5, 6, 6, 8 };
        int?[] nullables = { 5, 6, 6, 8 };

        byte[] expected = await ConverterHarness.WriteNewAsync(Derivation.Writer<int?>(type, ConverterHarness.Context), nullables, 1, 3);
        byte[] actual = await ConverterHarness.WriteNewAsync(Derivation.Writer<int>(type, ConverterHarness.Context), values, 1, 3);

        Assert.That(actual, Is.EqualTo(expected));
    }

    [TestCase("Nullable(Int32)")]
    [TestCase("LowCardinality(Nullable(Int32))")]
    public void Write_ValueTypeIntoANullableTypeUnderMarks_IsNullAtEachMarkedPosition(string type)
    {
        int[] values = { 5, 6, 7 };
        int?[] nullables = { 5, null, 7 };
        byte[] marks = { 0, 1, 0 };

        byte[] expected = Write(Derivation.Writer<int?>(type, ConverterHarness.Context), nullables, absent: null);
        byte[] actual = Write(Derivation.Writer<int>(type, ConverterHarness.Context), values, marks);

        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public async Task Write_NullableValueTypeWithNoNull_GivesTheBytesOfTheValueType()
    {
        int?[] nullables = { 1, 2, 3 };
        int[] values = { 1, 2, 3 };

        byte[] expected = await ConverterHarness.WriteNewAsync(Derivation.Writer<int>("LowCardinality(Int32)", ConverterHarness.Context), values, 1, 2);
        byte[] actual = await ConverterHarness.WriteNewAsync(Derivation.Writer<int?>("LowCardinality(Int32)", ConverterHarness.Context), nullables, 1, 2);

        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public void Write_NullableValueTypeIntoATypeWithNoNull_ThrowsAtTheFirstNullBeforeAnyByte()
    {
        ColumnWriter<SByteEnum?> writer = Derivation.Writer<SByteEnum?>("Enum8('a' = 1)", ConverterHarness.Context);
        SByteEnum?[] values = { SByteEnum.A, SByteEnum.A, null, SByteEnum.A, null };
        using var stream = new System.IO.MemoryStream();
        using var output = new ClickHouseBinaryWriter(stream);

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => ConverterHarness.WriteAll(writer, output, ValueSource<SByteEnum?>.Of(values.AsSpan(1), firstRow: 1, column: "level")));

        Assert.Multiple(() =>
        {
            Assert.That(error.Message, Is.EqualTo("Column 'level' (Enum8('a' = 1)) is null at row 2 of the insert, but it cannot hold null. Make the column Nullable(...), or leave out the rows with no value."));
            Assert.That(output.BufferedBytes, Is.Zero, "the first NULL stops the write before the writer of the values starts");
        });
    }

    [Test]
    public void Write_NullableValueTypeWithANullAtAMarkedPosition_WritesThePlaceholderThere()
    {
        int?[] nullables = { 1, null, 3 };
        int[] values = { 1, 0, 3 };
        byte[] marks = { 0, 1, 0 };

        byte[] expected = Write(Derivation.Writer<int>("Int32", ConverterHarness.Context), values, marks);
        byte[] actual = Write(Derivation.Writer<int?>("Int32", ConverterHarness.Context), nullables, marks);

        Assert.That(actual, Is.EqualTo(expected));
    }

    // Begin, prefix and body of the values, with the marks when given.
    private static byte[] Write<T>(ColumnWriter<T> writer, T[] values, byte[] absent)
    {
        using var stream = new System.IO.MemoryStream();
        using (var output = new ClickHouseBinaryWriter(stream))
        {
            ValueSource<T> source = ValueSource<T>.Of(values, 0, "c");
            ConverterHarness.WriteAll(writer, output, absent is null ? source : source.WithAbsent(absent));
            output.FlushAsync(System.Threading.CancellationToken.None).AsTask().GetAwaiter().GetResult();
        }

        return stream.ToArray();
    }
}
