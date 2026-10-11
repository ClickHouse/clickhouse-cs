using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// What the differential tests cannot reach for the read combinators: values under a NULL that a conversion refuses,
/// columns with a NULL and a failing value or with several failing values, dictionary entries that fail, zero rows,
/// columns without the decoded shape, the readings that only some tiers of POCO mapping lift, the types that read only
/// as their canonical type, and the refusal texts. Each reading runs through <see cref="BoundReader{T}.Fill"/> and
/// through a compiled <see cref="ColumnReader.Emit"/>.
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
        return await ConverterHarness.Codec(type).ReadColumnAsync(reader, "value", type, nulls.Length, CodecTestHarness.None);
    }

    // Rows of LowCardinality(Nullable(X)) with a dictionary that the test gives: slot 0 is the NULL slot.
    private static Task<IColumn> NullableDictionaryAsync(string inner, int entries, Action<ClickHouseBinaryWriter> dictionary, byte[] keys)
        => DictionaryAsync($"LowCardinality(Nullable({inner}))", entries, dictionary, keys);

    // Rows of a LowCardinality type with a dictionary that the test gives, in the order that the test gives.
    private static async Task<IColumn> DictionaryAsync(string type, int entries, Action<ClickHouseBinaryWriter> dictionary, byte[] keys)
    {
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
        return await codec.ReadColumnAsync(reader, "value", type, keys.Length, CodecTestHarness.None);
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

    // The dictionary holds only the NULL slot, so every row is NULL.
    [Test]
    public async Task Read_NullableDictionaryWithOnlyTheNullSlotAsNullable_GivesNull()
    {
        using IColumn column = await NullableDictionaryAsync("Time", 1, w => w.WriteInt32(0), keys: new byte[] { 0, 0 });
        var expected = new TimeOnly?[] { null, null };

        AssertBothPaths(Reader<TimeOnly?>("LowCardinality(Nullable(Time))"), column, expected);
        Assert.That(ConverterHarness.ReadAs<TimeOnly?>(column, 0, 2), Is.EqualTo(expected), "Block.ReadAs");
    }

    [Test]
    public async Task Read_NullableDictionaryWithOnlyTheNullSlotAsValueType_FailsAtTheFirstNull()
    {
        using IColumn column = await NullableDictionaryAsync("Int32", 1, w => w.WriteInt32(0), keys: new byte[] { 0, 0 });
        ColumnReader<int> reader = Reader<int>("LowCardinality(Nullable(Int32))");

        Assert.Multiple(() =>
        {
            Assert.That(ConverterHarness.Catch(() => ConverterHarness.ReadFill(reader, column, 0, 2)), Is.TypeOf<NullValueException>().With.Property("Row").EqualTo(0), "Fill");
            Assert.That(ConverterHarness.Catch(() => ConverterHarness.ReadEmit(reader, column, 0, 2)), Is.TypeOf<NullValueException>().With.Property("Row").EqualTo(0), "Emit");
        });
    }

    // The NULL of row 1 and the value of row 2, which is no time of day, fail the read: each path fails on one of them.
    // A read of row 2 alone fails on its value.
    [Test]
    public async Task Read_NullableAsValueTypeWithANullAndAFailingRow_FailsOnOneOfThemInEveryPath()
    {
        using IColumn column = await NullableAsync("Time", new byte[] { 0, 1, 0 }, w =>
        {
            w.WriteInt32(3_661);
            w.WriteInt32(0);
            w.WriteInt32(90_000);
        });

        AssertEveryPathFailsOnOneOf<TimeOnly>(column, Failing.Null(1), Failing.Value("1.01:00:00"));
        Assert.That(
            ConverterHarness.Catch(() => ConverterHarness.ReadFill(Reader<TimeOnly>("Nullable(Time)"), column, 2, 1)),
            Is.TypeOf<InvalidOperationException>().With.Message.Contains("value of 1.01:00:00 is not a time of day"));
    }

    // Rows 2 and 4 are NULL: a read of all rows fails at one of them, and a read from row 3 at row 4.
    [Test]
    public async Task Read_NullableDictionaryAsValueTypeWithANull_FailsAtANullOfTheRange()
    {
        using IColumn column = await NullableDictionaryAsync("Time", 2, w =>
        {
            w.WriteInt32(0);
            w.WriteInt32(1);
        }, keys: new byte[] { 1, 1, 0, 1, 0 });
        ColumnReader<TimeOnly> reader = Reader<TimeOnly>("LowCardinality(Nullable(Time))");

        Assert.Multiple(() =>
        {
            Assert.That(ConverterHarness.Catch(() => ConverterHarness.ReadFill(reader, column, 0, 5)), Is.TypeOf<NullValueException>().With.Property("Row").AnyOf(2, 4));
            Assert.That(ConverterHarness.Catch(() => ConverterHarness.ReadFill(reader, column, 3, 2)), Is.TypeOf<NullValueException>().With.Property("Row").EqualTo(4));
            Assert.That(ConverterHarness.ReadFill(reader, column, 0, 2), Is.EqualTo(new[] { new TimeOnly(0, 0, 1), new TimeOnly(0, 0, 1) }));
            Assert.That(ConverterHarness.ReadEmit(reader, column, 3, 1), Is.EqualTo(new[] { new TimeOnly(0, 0, 1) }));
        });
    }

    // Row 1 is NULL, and row 2 uses an entry that is no time of day: each path fails on one of them.
    [Test]
    public async Task Read_NullableDictionaryAsValueTypeWithANullAndAFailingEntry_FailsOnOneOfThemInEveryPath()
    {
        using IColumn column = await NullableDictionaryAsync("Time", 3, w =>
        {
            w.WriteInt32(0);
            w.WriteInt32(1);
            w.WriteInt32(90_000);
        }, keys: new byte[] { 1, 0, 2 });

        AssertEveryPathFailsOnOneOf<TimeOnly>(column, Failing.Null(1), Failing.Value("1.01:00:00"));
    }

    // Row 0 uses slot 2 and row 1 uses slot 1, and both entries are no time of day: each path fails on one of them, also
    // the readings of the read rules (here as TimeOnly?).
    [Test]
    public async Task Read_DictionaryWithTwoFailingEntriesThatRowsUse_FailsOnOneOfThemInEveryPath()
    {
        using IColumn column = await TimeDictionaryAsync("LowCardinality(Time)", keys: new byte[] { 2, 1 });

        AssertEveryPathFailsOnOneOf<TimeOnly>(column, Failing.Value("-00:00:01"), Failing.Value("1.01:00:00"));
        AssertEveryPathFailsOnOneOf<TimeOnly?>(column, Failing.Value("-00:00:01"), Failing.Value("1.01:00:00"));
    }

    // Slots 1 and 2 are no time of day, and no row uses them: a read gives the values of the rows in every path.
    [TestCase("LowCardinality(Time)", new byte[] { 3, 0, 3 })]
    [TestCase("LowCardinality(Nullable(Time))", new byte[] { 3, 0, 3 })]
    public async Task Read_DictionaryWithFailingEntriesThatNoRowUses_GivesTheValuesInEveryPath(string type, byte[] keys)
    {
        using IColumn column = await TimeDictionaryAsync(type, keys);
        bool nullable = type.Contains("Nullable", StringComparison.Ordinal);

        AssertEveryPathReads(column, new TimeOnly?[] { new TimeOnly(0, 0, 1), nullable ? null : TimeOnly.MinValue, new TimeOnly(0, 0, 1) });
        if (!nullable)
        {
            AssertEveryPathReads(column, new[] { new TimeOnly(0, 0, 1), TimeOnly.MinValue, new TimeOnly(0, 0, 1) });
        }
    }

    // Only row 1 uses a failing entry (slot 1): a read of all rows fails on it in every path, with the same failure, and
    // a read of row 0 alone gives its value.
    [Test]
    public async Task Read_DictionaryWithAFailingEntryThatOneRowUses_FailsOnlyAReadOfThatRow()
    {
        using IColumn column = await TimeDictionaryAsync("LowCardinality(Time)", keys: new byte[] { 3, 1 });
        ColumnReader<TimeOnly> reader = Reader<TimeOnly>("LowCardinality(Time)");

        AssertEveryPathFailsOn<TimeOnly>(column, Failing.Value("1.01:00:00"));
        Assert.Multiple(() =>
        {
            Assert.That(ConverterHarness.ReadFill(reader, column, 0, 1), Is.EqualTo(new[] { new TimeOnly(0, 0, 1) }), "Fill of row 0");
            Assert.That(ConverterHarness.ReadEmit(reader, column, 0, 1), Is.EqualTo(new[] { new TimeOnly(0, 0, 1) }), "Emit of row 0");
            Assert.That(Poco<TimeOnly>(column, tier: null)(0, 1), Is.EqualTo(new[] { new TimeOnly(0, 0, 1) }), "POCO mapping of row 0");
            Assert.That(Poco<TimeOnly>(column, PocoScatterTier.Fill)(0, 1), Is.EqualTo(new[] { new TimeOnly(0, 0, 1) }), "POCO mapping of row 0, Fill tier");
        });
    }

    /// <summary>
    /// CLR types that have no row shape of the column type, or a child that the child type does not read as: a composite
    /// reads as a one-dimension zero-based array of a type that its child reads as, an array of <c>KeyValuePair</c>, or a
    /// <c>ValueTuple</c> of its arity, and a type that erases its children reads only as its canonical type.
    /// </summary>
    public static IEnumerable<TestCaseData> ShapesTheTypeDoesNotHave()
    {
        yield return new TestCaseData("Array(DateTime('UTC'))", typeof(string[])).SetName("{m}(Array, an element the child refuses)");
        yield return new TestCaseData("Array(DateTime('UTC'))", typeof(DateTime[,])).SetName("{m}(Array, a two-dimension array)");
        yield return new TestCaseData("Array(DateTime('UTC'))", typeof(DateTime).MakeArrayType(1)).SetName("{m}(Array, an array that need not start at zero)");
        yield return new TestCaseData("Array(DateTime('UTC'))", typeof(DateTime)).SetName("{m}(Array, no array)");
        yield return new TestCaseData("Map(String, DateTime('UTC'))", typeof(KeyValuePair<string, DateTime>).MakeArrayType(1)).SetName("{m}(Map, a pair array that need not start at zero)");
        yield return new TestCaseData("Map(String, DateTime('UTC'))", typeof(Tuple<string, DateTime>[])).SetName("{m}(Map, an array of other pairs)");
        yield return new TestCaseData("Map(String, DateTime('UTC'))", typeof(KeyValuePair<string, Guid>[])).SetName("{m}(Map, a value the child refuses)");
        yield return new TestCaseData("Tuple(DateTime('UTC'), String)", typeof(ValueTuple<DateTime>)).SetName("{m}(Tuple, another arity)");
        yield return new TestCaseData("Tuple(DateTime('UTC'), String)", typeof(Tuple<DateTime, string>)).SetName("{m}(Tuple, a reference tuple)");
        yield return new TestCaseData("Tuple(DateTime('UTC'), String)", typeof((Guid, string))).SetName("{m}(Tuple, a field the child refuses)");
        yield return new TestCaseData("Variant(String, UInt64)", typeof(DateTime[])).SetName("{m}(Variant)");
        yield return new TestCaseData("Dynamic", typeof(DateTime[])).SetName("{m}(Dynamic)");
        yield return new TestCaseData("Nested(a DateTime('UTC'))", typeof(DateTime[])).SetName("{m}(Nested)");
    }

    [TestCaseSource(nameof(ShapesTheTypeDoesNotHave))]
    public void CanRead_CompositeAskedForAShapeItDoesNotHave_IsFalse(string type, Type target)
        => Assert.That(ClickHouseTcpTypes.CanRead(type, target), Is.False);

    // Two values of the read are no time of day, 90000 (1.01:00:00) and -1: each path fails on one of them.
    [TestCase("Map(Time, Time)", typeof(KeyValuePair<TimeOnly, TimeOnly>[]))]
    [TestCase("Tuple(Time, Time)", typeof((TimeOnly, TimeOnly)))]
    [TestCase("Array(Tuple(Time, Time))", typeof((TimeOnly, TimeOnly)[]))]
    [TestCase("Array(Map(Time, Time))", typeof(KeyValuePair<TimeOnly, TimeOnly>[][]))]
    [TestCase("Nullable(Tuple(Time, Time))", typeof((TimeOnly, TimeOnly)?))]
    [TestCase("Array(Time)", typeof(TimeOnly[]))]
    [TestCase("Nullable(Time)", typeof(TimeOnly?))]
    [TestCase("Tuple(Time)", typeof(ValueTuple<TimeOnly>))]
    [TestCase("Tuple(Time, Time, Time)", typeof((TimeOnly, TimeOnly, TimeOnly)))]
    [TestCase("Tuple(Time, Time, Time, Time)", typeof((TimeOnly, TimeOnly, TimeOnly, TimeOnly)))]
    [TestCase("Tuple(Time, Time, Time, Time, Time)", typeof((TimeOnly, TimeOnly, TimeOnly, TimeOnly, TimeOnly)))]
    [TestCase("Tuple(Time, Time, Time, Time, Time, Time)", typeof((TimeOnly, TimeOnly, TimeOnly, TimeOnly, TimeOnly, TimeOnly)))]
    [TestCase("Tuple(Time, Time, Time, Time, Time, Time, Time)", typeof((TimeOnly, TimeOnly, TimeOnly, TimeOnly, TimeOnly, TimeOnly, TimeOnly)))]
    public async Task Read_CompositeWithTwoFailingValues_FailsOnOneOfThemInEveryPath(string type, Type target)
    {
        using IColumn column = await ConverterHarness.DecodeAsync(type, TwoFailingValues(type));

        ConverterHarness.InvokeGeneric(
            typeof(ReadCombinatorTests),
            nameof(AssertEveryPathFailsOnOneOf),
            new[] { target },
            column,
            new[] { Failing.Value("1.01:00:00"), Failing.Value("-00:00:01") });
    }

    // Row 0 has a field that is no time of day (90000), and row 1 a dictionary field whose entry is none (100000): each
    // path fails on one of them.
    [Test]
    public async Task Read_TupleWithAFailingDictionaryFieldAndAFailingField_FailsOnOneOfThemInEveryPath()
    {
        const string type = "Tuple(LowCardinality(Time), Time)";
        using IColumn column = await ConverterHarness.DecodeAsync(type, new ArrayColumn<(int, int)>("value", type, new[] { (1, 90_000), (100_000, 1) }));

        AssertEveryPathFailsOnOneOf<(TimeOnly, TimeOnly)>(column, Failing.Value("1.01:00:00"), Failing.Value("1.03:46:40"));
    }

    // Row 1 is NULL, and only row 1 refers to the dictionary entry 90000, which is no time of day. The read does not
    // convert a value under a NULL, so that entry does not fail it.
    [Test]
    public async Task Read_NullableTupleWithAFailingDictionaryEntryOnlyUnderANull_GivesTheValues()
    {
        const string type = "Nullable(Tuple(LowCardinality(Time), UInt8))";
        IColumnCodec codec = ConverterHarness.Codec(type);
        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            w.WriteInt64(LowCardinalityWire.StatePrefixVersion);
            w.WriteBytes(new byte[] { 0, 1 });
            w.WriteUInt64(LowCardinalityWire.NativeFlags | (ulong)LowCardinalityWire.KeyUInt8);
            w.WriteUInt64(3);
            w.WriteInt32(0);
            w.WriteInt32(1);
            w.WriteInt32(90_000);
            w.WriteUInt64(2);
            w.WriteBytes(new byte[] { 1, 2 });
            w.WriteBytes(new byte[] { 0, 0 });
        });
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        await codec.ReadStatePrefixAsync(reader, CodecTestHarness.None);
        using IColumn column = await codec.ReadColumnAsync(reader, "c", type, 2, CodecTestHarness.None);
        var expected = new (TimeOnly, byte)?[] { (new TimeOnly(0, 0, 1), 0), null };

        AssertBothPaths(Reader<(TimeOnly, byte)?>(type), column, expected);
        Assert.That(ConverterHarness.ReadAs<(TimeOnly, byte)?>(column, 0, 2), Is.EqualTo(expected), "Block.ReadAs");
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

    // Two values that are no time of day: 90000 and -1.
    private static IColumn TwoFailingValues(string type) => type switch
    {
        "Map(Time, Time)" => new ArrayColumn<KeyValuePair<int, int>[]>("value", type, new[]
        {
            new[] { new KeyValuePair<int, int>(1, 90_000), new KeyValuePair<int, int>(-1, 1) },
            new[] { new KeyValuePair<int, int>(-1, 1) },
        }),
        "Tuple(Time, Time)" => new ArrayColumn<(int, int)>("value", type, new[] { (1, 90_000), (-1, 1) }),
        "Array(Tuple(Time, Time))" => new ArrayColumn<(int, int)[]>("value", type, new[] { new[] { (1, 90_000), (-1, 1) } }),
        "Array(Map(Time, Time))" => new ArrayColumn<KeyValuePair<int, int>[][]>("value", type, new[]
        {
            new[] { new[] { new KeyValuePair<int, int>(1, 90_000) }, new[] { new KeyValuePair<int, int>(-1, 1) } },
        }),
        "Nullable(Tuple(Time, Time))" => new ArrayColumn<(int, int)?>("value", type, new (int, int)?[] { (1, 90_000), (-1, 1) }),
        "Array(Time)" => new ArrayColumn<int[]>("value", type, new[] { new[] { 1, 90_000 }, new[] { -1 } }),
        "Nullable(Time)" => new ArrayColumn<int?>("value", type, new int?[] { 90_000, null, -1 }),
        _ when type.StartsWith("Tuple(Time", StringComparison.Ordinal) => TupleOfTimes(type),
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, "No column of two failing values."),
    };

    // A Tuple of n Time fields: row 0 fails on its last field (90000), row 1 on its first field (-1).
    private static IColumn TupleOfTimes(string type)
    {
        int arity = TypeParser.Parse(type).Arguments.Count;
        Type tuple = ConverterHarness.Codec(type).ElementType;
        object Row(int first, int last) => Activator.CreateInstance(
            tuple,
            Enumerable.Range(0, arity).Select(i => (object)(i == arity - 1 ? last : i == 0 ? first : 1)).ToArray());
        Array rows = Array.CreateInstance(tuple, 2);
        rows.SetValue(Row(arity == 1 ? 90_000 : 1, 90_000), 0);
        rows.SetValue(Row(-1, arity == 1 ? -1 : 1), 1);
        return (IColumn)Activator.CreateInstance(typeof(ArrayColumn<>).MakeGenericType(tuple), "value", type, rows);
    }

    // Rows of LowCardinality(Time) or LowCardinality(Nullable(Time)) over the dictionary [0, 90000, -1, 1]: slot 0 is a
    // time of day (or the NULL slot), slots 1 and 2 are not, and slot 3 is 00:00:01.
    private static Task<IColumn> TimeDictionaryAsync(string type, byte[] keys)
        => DictionaryAsync(type, 4, w =>
        {
            w.WriteInt32(0);
            w.WriteInt32(90_000);
            w.WriteInt32(-1);
            w.WriteInt32(1);
        }, keys);

    // The reads of every path: Fill, Emit, Block.ReadAs, and the client's POCO plan in its two scatter tiers.
    private static IEnumerable<(string Path, Func<T[]> Read)> Paths<T>(IColumn column)
    {
        ColumnReader<T> reader = Reader<T>(column.TypeName);
        yield return ("Fill", () => ConverterHarness.ReadFill(reader, column, 0, column.RowCount));
        yield return ("Emit", () => ConverterHarness.ReadEmit(reader, column, 0, column.RowCount));
        yield return ("Block.ReadAs", () => ConverterHarness.ReadAs<T>(column, 0, column.RowCount));
        yield return ("POCO mapping", () => Poco<T>(column, tier: null)(0, column.RowCount));
        yield return ("POCO mapping, Fill tier", () => Poco<T>(column, PocoScatterTier.Fill)(0, column.RowCount));
    }

    // The client's POCO plan of Row<T> over a block of the column, which must be called "value".
    private static RowReader<T> Poco<T>(IColumn column, PocoScatterTier? tier)
    {
        var block = new Block(string.Empty, BlockInfo.Default, column.RowCount, new[] { column }, ColumnCodecRegistry.Default, Context);
        return new ClientArms.PocoArm("POCO mapping", tier).Bind<T>(block);
    }

    private static void AssertEveryPathReads<T>(IColumn column, T[] expected)
    {
        foreach ((string path, Func<T[]> read) in Paths<T>(column))
        {
            ConverterHarness.AssertSameValues(expected, read(), path);
        }
    }

    // Each path fails, with the failure of one of the failing values.
    private static void AssertEveryPathFailsOnOneOf<T>(IColumn column, params Failing[] failures)
    {
        Assert.Multiple(() =>
        {
            foreach ((string path, Func<T[]> read) in Paths<T>(column))
            {
                Exception failure = ConverterHarness.Catch(() => read());
                Assert.That(failure, Is.Not.Null, $"{path}: no failure");
                Assert.That(Array.Exists(failures, f => f.Is(failure)), Is.True, $"{path}: {failure?.GetType().Name}: {failure?.Message} is the failure of none of: {string.Join<Failing>("; ", failures)}");
            }
        });
    }

    // Each path fails with the failure of the one failing value, and the paths of one kind fail alike: Fill, Emit and
    // Block.ReadAs with the same failure (a NULL in the terms of each), and the two POCO tiers with the same failure.
    private static void AssertEveryPathFailsOn<T>(IColumn column, Failing failing)
    {
        AssertEveryPathFailsOnOneOf<T>(column, failing);
        Exception[] failures = Paths<T>(column).Select(p => ConverterHarness.Catch(() => p.Read())).ToArray();
        if (failures[0] is not NullValueException)
        {
            ConverterHarness.AssertSameFailure(failures[0], failures[1], "Emit");
            ConverterHarness.AssertSameFailure(failures[0], failures[2], "Block.ReadAs");
        }

        ConverterHarness.AssertSameFailure(failures[3], failures[4], "POCO mapping, Fill tier");
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

    /// <summary>The failure of one failing value of a column: a NULL row, or a value that is no time of day.</summary>
    internal sealed class Failing
    {
        private readonly int? nullRow;
        private readonly string text;

        private Failing(int? nullRow, string text)
        {
            this.nullRow = nullRow;
            this.text = text;
        }

        /// <summary>A NULL at a row, read as a type that cannot hold NULL.</summary>
        public static Failing Null(int row) => new(row, null);

        /// <summary>A value of a <c>Time</c> type that is no time of day, by its <see cref="TimeSpan"/> text.</summary>
        public static Failing Value(string timeSpan) => new(null, $"value of {timeSpan} is not a time of day");

        /// <summary>Whether a failure of a path is this one: the NULL named by its row in the terms of the path, or the value.</summary>
        public bool Is(Exception failure) => nullRow is int row
            ? failure is NullValueException nullValue
                ? nullValue.Row == row
                : failure?.GetType() == typeof(InvalidOperationException)
                    && (failure.Message.Contains($"has NULL at row {row},", StringComparison.Ordinal) || failure.Message.Contains($"is NULL at row {row} of the result", StringComparison.Ordinal))
            : failure?.GetType() == typeof(InvalidOperationException) && failure.Message.Contains(text, StringComparison.Ordinal);

        public override string ToString() => nullRow is int row ? $"NULL at row {row}" : text;
    }
}
