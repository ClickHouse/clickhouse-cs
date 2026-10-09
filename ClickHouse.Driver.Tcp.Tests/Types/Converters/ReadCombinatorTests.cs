using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// What the differential tests cannot reach for the read combinators: values under a NULL that a conversion refuses,
/// the order of a NULL and a failing row, zero rows, columns without the decoded shape, the readings that only some
/// tiers of POCO mapping lift, the types that read only as their canonical type, and the refusal texts. Each reading
/// runs through <see cref="BoundReader{T}.Fill"/> and through a compiled <see cref="ColumnReader.Emit"/>.
/// </summary>
[TestFixture]
public class ReadCombinatorTests
{
    private const string OneMemberEnum = "Enum8('a' = 1)";

    private static readonly ResolveContext Context = ConverterHarness.Context;

    // Rows of Nullable(X): the null map, then the inner values of every row, the NULL rows too.
    private static async Task<IColumn> NullableAsync(string inner, byte[] nulls, Action<ClickHouseBinaryWriter> values)
    {
        string type = $"Nullable({inner})";
        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            w.WriteBytes(nulls);
            values(w);
        });
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        return await ConverterHarness.Codec(type).ReadColumnAsync(reader, "c", type, nulls.Length, CodecTestHarness.None);
    }

    // Rows of LowCardinality(Nullable(X)) with a dictionary that the test gives: slot 0 is the NULL slot.
    private static async Task<IColumn> NullableDictionaryAsync(string inner, int entries, Action<ClickHouseBinaryWriter> dictionary, byte[] keys)
    {
        string type = $"LowCardinality(Nullable({inner}))";
        IColumnCodec codec = ConverterHarness.Codec(type);
        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            w.WriteInt64(LowCardinalityWire.StatePrefixVersion);
            w.WriteUInt64(LowCardinalityWire.NativeFlags | (ulong)LowCardinalityWire.KeyUInt8);
            w.WriteUInt64((ulong)entries);
            dictionary(w);
            w.WriteUInt64((ulong)keys.Length);
            w.WriteBytes(keys);
        });
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        return await codec.ReadColumnAsync(reader, "c", type, keys.Length, CodecTestHarness.None);
    }

    private static ColumnReader<T> Reader<T>(string type) => ConverterDerivation.Default.Reader<T>(type, Context);

    private static void AssertBothPaths<T>(ColumnReader<T> reader, IColumn column, T[] expected)
    {
        ConverterHarness.AssertSameValues(expected, ConverterHarness.ReadFill(reader, column, 0, column.RowCount), "Fill");
        ConverterHarness.AssertSameValues(expected, ConverterHarness.ReadEmit(reader, column, 0, column.RowCount), "Emit");
    }

    [Test]
    public async Task Read_NullableEnumWithAnUndeclaredOrdinalUnderANull_ConvertsOnlyTheRowsThatAreNotNull()
    {
        using IColumn column = await NullableAsync(OneMemberEnum, new byte[] { 0, 1, 0 }, w =>
        {
            w.WriteInt8(1);
            w.WriteInt8(0);
            w.WriteInt8(1);
        });

        AssertBothPaths(Reader<string>($"Nullable({OneMemberEnum})"), column, new[] { "a", null, "a" });
    }

    [Test]
    public async Task Read_NullableTimeWithAValueUnderANullThatIsNoTimeOfDay_ConvertsOnlyTheRowsThatAreNotNull()
    {
        using IColumn column = await NullableAsync("Time", new byte[] { 1, 0, 1 }, w =>
        {
            w.WriteInt32(90_000);
            w.WriteInt32(3_661);
            w.WriteInt32(-1);
        });

        AssertBothPaths(Reader<TimeOnly?>("Nullable(Time)"), column, new TimeOnly?[] { null, new TimeOnly(1, 1, 1), null });
    }

    [Test]
    public async Task Read_NullableDictionaryWhoseNullSlotHoldsAnUndeclaredOrdinal_DoesNotConvertTheNullSlot()
    {
        using IColumn column = await NullableDictionaryAsync(OneMemberEnum, 2, w =>
        {
            w.WriteInt8(0);
            w.WriteInt8(1);
        }, keys: new byte[] { 1, 0, 1 });

        AssertBothPaths(Reader<string>($"LowCardinality(Nullable({OneMemberEnum}))"), column, new[] { "a", null, "a" });
    }

    [Test]
    public async Task Read_NullableDictionaryAsValueTypeWhoseNullSlotIsNoTimeOfDay_DoesNotConvertTheNullSlot()
    {
        using IColumn column = await NullableDictionaryAsync("Time", 2, w =>
        {
            w.WriteInt32(90_000);
            w.WriteInt32(1);
        }, keys: new byte[] { 0, 1 });

        AssertBothPaths(Reader<TimeOnly?>("LowCardinality(Nullable(Time))"), column, new TimeOnly?[] { null, new TimeOnly(0, 0, 1) });
    }

    [Test]
    public async Task Read_NullableAsValueTypeWithANullBeforeAFailingRow_FailsAtTheNull()
    {
        using IColumn column = await NullableAsync("Time", new byte[] { 0, 1, 0 }, w =>
        {
            w.WriteInt32(3_661);
            w.WriteInt32(0);
            w.WriteInt32(90_000);
        });
        ColumnReader<TimeOnly> reader = Reader<TimeOnly>("Nullable(Time)");

        Exception fill = ConverterHarness.Catch(() => ConverterHarness.ReadFill(reader, column, 0, 3));
        Exception emit = ConverterHarness.Catch(() => ConverterHarness.ReadEmit(reader, column, 0, 3));
        Exception afterTheNull = ConverterHarness.Catch(() => ConverterHarness.ReadFill(reader, column, 2, 1));

        Assert.Multiple(() =>
        {
            Assert.That(fill, Is.TypeOf<NullValueException>().With.Property(nameof(NullValueException.Row)).EqualTo(1));
            Assert.That(emit, Is.TypeOf<NullValueException>().With.Property(nameof(NullValueException.Row)).EqualTo(1));
            Assert.That(afterTheNull, Is.TypeOf<InvalidOperationException>().With.Message.Contains("is not a time of day"));
        });
    }

    [Test]
    public async Task Read_NullableDictionaryAsValueTypeWithANull_FailsAtTheFirstNullOfTheRange()
    {
        using IColumn column = await NullableDictionaryAsync("Time", 2, w =>
        {
            w.WriteInt32(0);
            w.WriteInt32(1);
        }, keys: new byte[] { 1, 1, 0, 1, 0 });
        ColumnReader<TimeOnly> reader = Reader<TimeOnly>("LowCardinality(Nullable(Time))");

        Assert.Multiple(() =>
        {
            Assert.That(ConverterHarness.Catch(() => ConverterHarness.ReadFill(reader, column, 0, 5)), Is.TypeOf<NullValueException>().With.Property("Row").EqualTo(2));
            Assert.That(ConverterHarness.Catch(() => ConverterHarness.ReadFill(reader, column, 3, 2)), Is.TypeOf<NullValueException>().With.Property("Row").EqualTo(4));
            Assert.That(ConverterHarness.ReadFill(reader, column, 0, 2), Is.EqualTo(new[] { new TimeOnly(0, 0, 1), new TimeOnly(0, 0, 1) }));
            Assert.That(ConverterHarness.ReadEmit(reader, column, 3, 1), Is.EqualTo(new[] { new TimeOnly(0, 0, 1) }));
        });
    }

    // The dictionary converts every entry on the first read, so an entry that a conversion refuses fails the read,
    // also when the first NULL of the range comes before the row that refers to the entry.
    [Test]
    public async Task Read_NullableDictionaryAsValueTypeWithAFailingEntry_FailsOnTheEntryBeforeTheNull()
    {
        using IColumn column = await NullableDictionaryAsync("Time", 3, w =>
        {
            w.WriteInt32(0);
            w.WriteInt32(1);
            w.WriteInt32(90_000);
        }, keys: new byte[] { 1, 0, 2 });

        Exception thrown = ConverterHarness.Catch(() => ConverterHarness.ReadFill(Reader<TimeOnly>("LowCardinality(Nullable(Time))"), column, 0, 3));

        Assert.That(thrown, Is.TypeOf<InvalidOperationException>().With.Message.Contains("is not a time of day"));
    }

    [TestCase("Nullable(Int32)", typeof(int?))]
    [TestCase("Nullable(String)", typeof(string))]
    [TestCase("Array(Int32)", typeof(int[]))]
    [TestCase("LowCardinality(String)", typeof(byte[]))]
    [TestCase("LowCardinality(Nullable(UInt32))", typeof(uint?))]
    [TestCase("Map(String, Int32)", typeof(KeyValuePair<byte[], int>[]))]
    [TestCase("Tuple(Int32, String)", typeof((int, byte[])))]
    [TestCase("Nullable(Int32)", typeof(int))]
    [TestCase("Int8", typeof(ReadRulesTests.SByteEnum?))]
    [TestCase("Array(String)", typeof(object))]
    public async Task Read_ZeroRows_GivesNoValue(string type, Type target)
    {
        using IColumn column = await ConverterHarness.DecodeAsync(type, EmptyColumn(type));

        object values = ConverterHarness.InvokeGeneric(typeof(ReadCombinatorTests), nameof(ReadBothPaths), new[] { target }, type, column);

        Assert.That(values, Is.Empty);
    }

    [TestCase("Nullable(Int32)", typeof(int?), "INullableColumn")]
    [TestCase("Array(Int32)", typeof(int[]), "IArrayColumn")]
    [TestCase("LowCardinality(String)", typeof(byte[]), "ILowCardinalityColumn")]
    [TestCase("Map(String, Int32)", typeof(KeyValuePair<byte[], int>[]), "IMapColumn")]
    [TestCase("Tuple(Int32, String)", typeof((int, byte[])), "ITupleColumn")]
    public void Bind_CallerBuiltColumnWithoutTheDecodedShape_ThrowsNamingTheShape(string type, Type target, string surface)
    {
        IColumn column = EmptyColumn(type);

        var thrown = (Exception)ConverterHarness.InvokeGeneric(typeof(ReadCombinatorTests), nameof(BindFailure), new[] { target }, type, column);

        Assert.That(thrown, Is.TypeOf<InvalidOperationException>().With.Message.Contains($"does not expose {surface}"));
    }

    [Test]
    public void Bind_TupleColumnWithFewerChildrenThanItsType_ThrowsNamingBothCounts()
    {
        using var narrower = new TupleColumn<byte>(
            "c",
            "Tuple(UInt8, String)",
            new IColumn[] { PrimitiveColumn<byte>.FromValues("1", "UInt8", new byte[] { 7 }) },
            fieldNames: null,
            ownsChildren: false);

        Exception thrown = ConverterHarness.Catch(() => Reader<(byte, byte[])>("Tuple(UInt8, String)").Bind(narrower));

        Assert.That(thrown, Is.TypeOf<InvalidOperationException>().With.Message.Contains("1 children").And.Message.Contains("resolved to 2"));
    }

    // POCO mapping lifts a reading to a nullable value type only when it converts each value alone, so a reading that
    // needs the column (String as byte[]) is not lifted. The derivation keeps that rule.
    [TestCase("Tuple(String, UInt8)", typeof((byte[], byte)), true)]
    [TestCase("Tuple(String, UInt8)", typeof((byte[], byte)?), false)]
    [TestCase("Tuple(String, UInt8)", typeof((string, byte)?), true)]
    [TestCase("Nullable(Tuple(String, UInt8))", typeof((byte[], byte)?), false)]
    [TestCase("Nullable(Tuple(String, UInt8))", typeof((string, byte)?), true)]
    [TestCase("LowCardinality(Nullable(String))", typeof(byte[]), true)]
    [TestCase("Array(Nullable(String))", typeof(byte[][]), true)]
    public void Derive_ReadingThatNeedsTheColumn_IsLiftedOnlyWhereTheRulesLiftIt(string type, Type target, bool succeeds)
        => Assert.That(ConverterDerivation.Default.Derive(type, Context, target, ConversionDirection.Read).Succeeded, Is.EqualTo(succeeds));

    [TestCase("Variant(String, UInt64)", typeof(object))]
    [TestCase("Dynamic", typeof(object))]
    [TestCase("Nested(a UInt8, b String)", typeof(object[][]))]
    [TestCase("QBit(Float32, 4)", typeof(float[]))]
    [TestCase("Point", typeof((double, double)))]
    [TestCase("Ring", typeof((double, double)[]))]
    [TestCase("MultiPolygon", typeof((double, double)[][][]))]
    [TestCase("Geometry", typeof(object))]
    [TestCase("Tuple()", typeof(ValueTuple))]
    public void Derive_TypeThatReadsOnlyAsItsCanonicalType_AcceptsThatTypeAndRefusesAnother(string type, Type canonical)
    {
        Derivation other = ConverterDerivation.Default.Derive(type, Context, typeof(DateTimeOffset), ConversionDirection.Read);

        Assert.Multiple(() =>
        {
            Assert.That(ConverterDerivation.Default.Derive(type, Context, canonical, ConversionDirection.Read).Succeeded, Is.True);
            Assert.That(other.Succeeded, Is.False);
            Assert.That(other.Refusal, Is.EqualTo($"'{type}' cannot be read as System.DateTimeOffset. It reads as: {canonical}."));
        });
    }

    [TestCase("Array(Int32)", typeof(int), "'Array(Int32)' cannot be read as System.Int32. It reads as an array.")]
    [TestCase("Map(String, Int32)", typeof(string[]), "'Map(String, Int32)' cannot be read as System.String[]. It reads as an array of KeyValuePair<TKey, TValue>.")]
    [TestCase("Tuple(Int32, String)", typeof(ValueTuple<int>), "'Tuple(Int32, String)' cannot be read as System.ValueTuple`1[System.Int32]. It reads as a ValueTuple of 2 element(s).")]
    [TestCase("Nullable(Int32)", typeof(long), "'Nullable(Int32)' cannot be read as System.Int64, which cannot hold NULL.")]
    [TestCase("LowCardinality(Nullable(String))", typeof(int), "'LowCardinality(Nullable(String))' cannot be read as System.Int32, which cannot hold NULL.")]
    [TestCase(
        "Nullable(Tuple(String, UInt8))",
        typeof((byte[], byte)?),
        "'Nullable(Tuple(String, UInt8))' cannot be read as System.Nullable`1[System.ValueTuple`2[System.Byte[],System.Byte]]: 'Tuple(String, UInt8)' reads as System.ValueTuple`2[System.Byte[],System.Byte] only from its column, and a nullable value type converts each value alone.")]
    [TestCase("Array(Nullable(Int32))", typeof(long?[]), "'Int32' cannot be read as System.Int64. It reads as: System.Int32. It is inside the column type 'Array(Nullable(Int32))'.")]
    [TestCase("Map(String, Variant(String, UInt64))", typeof(KeyValuePair<string, string>[]), "'Variant(String, UInt64)' cannot be read as System.String. It reads as: System.Object. It is inside the column type 'Map(String, Variant(String, UInt64))'.")]
    public void Derive_CompositeThatCannotBeReadAsTheType_RefusesWithTheReason(string type, Type target, string refusal)
    {
        Derivation derivation = ConverterDerivation.Default.Derive(type, Context, target, ConversionDirection.Read);

        Assert.Multiple(() =>
        {
            Assert.That(derivation.Succeeded, Is.False);
            Assert.That(derivation.Refusal, Is.EqualTo(refusal));
        });
    }

    [TestCase("Int32")]
    [TestCase("Nullable(Int32)")]
    [TestCase("Array(Int32)")]
    [TestCase("LowCardinality(Nullable(String))")]
    public void Derive_ClrTypeThatCannotBeAValue_RefusesWithoutAnException(string type)
    {
        Type[] types = { typeof(int).MakePointerType(), typeof(int).MakeByRefType(), typeof(Span<int>), typeof(List<>), typeof(void), typeof(int).MakePointerType().MakeArrayType() };

        Assert.That(types.Select(t => ConverterDerivation.Default.Derive(type, Context, t, ConversionDirection.Read).Succeeded), Has.All.False);
    }

    [Test]
    public async Task Read_DictionaryAsBytes_GivesOneArrayToTheRowsOfOneKey()
    {
        using IColumn column = await ConverterHarness.DecodeAsync("LowCardinality(String)", new ArrayColumn<string>("c", "LowCardinality(String)", new[] { "x", "y", "x" }));

        byte[][] values = ConverterHarness.ReadFill(Reader<byte[]>("LowCardinality(String)"), column, 0, 3);

        Assert.That(values[2], Is.SameAs(values[0]));
    }

    [Test]
    public async Task Fill_OneBoundDictionaryReadByManyThreads_GivesTheSameValues()
    {
        string[] text = Enumerable.Range(0, 2_000).Select(i => $"v{i % 37}").ToArray();
        using IColumn column = await ConverterHarness.DecodeAsync("LowCardinality(String)", new ArrayColumn<string>("c", "LowCardinality(String)", text));
        BoundReader<byte[]> bound = Reader<byte[]>("LowCardinality(String)").Bind(column);

        byte[][][] results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            var values = new byte[text.Length][];
            bound.Fill(0, values);
            return values;
        })));

        Assert.That(results.Select(values => values.Select(System.Text.Encoding.UTF8.GetString)), Has.All.EqualTo(text));
    }

    private static T[] ReadBothPaths<T>(string type, IColumn column)
    {
        ColumnReader<T> reader = Reader<T>(type);
        T[] fill = ConverterHarness.ReadFill(reader, column, 0, column.RowCount);
        T[] emit = ConverterHarness.ReadEmit(reader, column, 0, column.RowCount);
        Assert.That(emit, Has.Length.EqualTo(fill.Length));
        return fill;
    }

    private static Exception BindFailure<T>(string type, IColumn column)
    {
        ColumnReader<T> reader = Reader<T>(type);
        Exception bind = ConverterHarness.Catch(() => reader.Bind(column));
        Exception emit = ConverterHarness.Catch(() => ConverterHarness.ReadEmit(reader, column, 0, 0));
        Assert.That(emit?.Message, Is.EqualTo(bind?.Message), "Emit and Bind refuse the column alike.");
        return bind;
    }

    // A column of the type's canonical CLR type with no rows, as a caller builds it.
    private static IColumn EmptyColumn(string type)
    {
        Type element = ConverterHarness.Codec(type).ElementType;
        return (IColumn)Activator.CreateInstance(typeof(ArrayColumn<>).MakeGenericType(element), "c", type, Array.CreateInstance(element, 0));
    }
}
