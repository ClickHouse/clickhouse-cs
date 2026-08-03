using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Reflection;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Tests.ADO;

/// <summary>
/// Server-free tests for the typed column slots that replaced <c>ClickHouseDataReader</c>'s shared
/// <c>object[]</c> row buffer.
///
/// <para>The whole design rests on one invariant: a slot must be <i>observationally identical</i> to the
/// boxed path it replaced. <see cref="ColumnSlot.Read"/> must consume exactly the bytes
/// <see cref="ClickHouseType.Read(ExtendedBinaryReader)"/> would have, and <see cref="ColumnSlot.GetBoxed"/>
/// must return exactly the value it would have returned — same CLR type, and <see cref="DBNull.Value"/> for a
/// NULL. <see cref="Parity_SlotMatchesBoxedRead_InValueAndByteCount"/> asserts precisely that, differentially,
/// against the real boxed reader rather than against hand-written expectations.</para>
///
/// <para>The rest pins the things parity alone cannot see: <i>which</i> slot kind a column resolves to (a
/// silent demotion to <see cref="BoxedSlot"/> is invisible — values stay correct, only the allocation
/// disappears), and <see cref="ColumnSlot.IsNull"/>, which has no boxed counterpart to compare against.</para>
/// </summary>
[TestFixture]
public class ColumnSlotTests
{
    // Written after the value under test; a slot that consumes the wrong number of bytes decodes garbage here
    // rather than silently corrupting the next column of a real row.
    private const long Sentinel = 0x1234_5678_09AB_CDEFL;

    private const string Enum8Def = "Enum8('a' = -5, 'b' = 7)";

    private static ClickHouseType Parse(string typeName, TypeSettings? settings = null)
        => TypeConverter.ParseClickHouseType(typeName, settings ?? TypeSettings.Default);

    private static byte[] Write(ClickHouseType type, object value)
    {
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);
        type.Write(writer, value);
        new Int64Type().Write(writer, Sentinel);
        writer.Flush();
        return stream.ToArray();
    }

    private static (object Value, long Trailer) ReadBoxed(ClickHouseType type, byte[] payload)
    {
        using var stream = new MemoryStream(payload);
        using var reader = new ExtendedBinaryReader(stream);
        return (type.Read(reader), reader.ReadInt64());
    }

    private static (object Value, long Trailer, ColumnSlot Slot) ReadSlot(ClickHouseType type, byte[] payload)
    {
        var slot = ColumnSlotFactory.Create(type);
        using var stream = new MemoryStream(payload);
        using var reader = new ExtendedBinaryReader(stream);
        slot.Read(reader);
        return (slot.GetBoxed(), reader.ReadInt64(), slot);
    }

    // ---- Differential parity against the boxed reader ----

    // (type, value) pairs covering every ITypedReader<T> shape a slot can be built over, both bare and under
    // Nullable, plus the composites that must fall back. The value is written by the type's own Write, so the
    // payload is exactly what the server would send.
    private static IEnumerable<TestCaseData> ParityCases()
    {
        var settingsByName = new Dictionary<string, TypeSettings>
        {
            ["ReadAsBytes"] = TypeSettings.Default with { readStringsAsByteArrays = true },
            ["BigDecimal"] = TypeSettings.Default with { useBigDecimal = true },
        };

        foreach (var (typeName, value) in Values())
        {
            yield return Case(typeName, value, TypeSettings.Default, string.Empty);

            // Nullable(X) in both states. Composites are covered bare; Nullable(Array(..)) etc. have no typed
            // slot anyway and the bare case already pins that.
            if (!typeName.StartsWith("Array(", StringComparison.Ordinal) &&
                !typeName.StartsWith("Tuple(", StringComparison.Ordinal) &&
                !typeName.StartsWith("Map(", StringComparison.Ordinal))
            {
                yield return Case($"Nullable({typeName})", value, TypeSettings.Default, "Present");
                yield return Case($"Nullable({typeName})", DBNull.Value, TypeSettings.Default, "Null");
            }
        }

        // The two settings that change FrameworkType, and so which typed reader a slot binds to.
        yield return Case("String", "abc", settingsByName["ReadAsBytes"], "ReadAsBytes");
        yield return Case("FixedString(5)", "abcde", settingsByName["ReadAsBytes"], "ReadAsBytes");
        yield return Case("Nullable(String)", "abc", settingsByName["ReadAsBytes"], "ReadAsBytes");
        yield return Case("Nullable(String)", DBNull.Value, settingsByName["ReadAsBytes"], "ReadAsBytesNull");
        yield return Case("Decimal(10, 2)", 12.34m, settingsByName["BigDecimal"], "BigDecimal");
        yield return Case("Nullable(Decimal(10, 2))", DBNull.Value, settingsByName["BigDecimal"], "BigDecimalNull");

        // Wire-transparent wrappers must behave exactly like the type they wrap.
        yield return Case("LowCardinality(String)", "abc", TypeSettings.Default, string.Empty);
        yield return Case("LowCardinality(Nullable(String))", "abc", TypeSettings.Default, "Present");
        yield return Case("LowCardinality(Nullable(String))", DBNull.Value, TypeSettings.Default, "Null");
        yield return Case("SimpleAggregateFunction(sum, UInt64)", 42ul, TypeSettings.Default, string.Empty);
        yield return Case("SimpleAggregateFunction(any, Nullable(Int32))", DBNull.Value, TypeSettings.Default, "Null");

        // Passed as a closure rather than as arguments because TypeSettings is internal, and an internal
        // parameter type on a public test method does not compile.
        static TestCaseData Case(string typeName, object value, TypeSettings settings, string suffix)
            => new TestCaseData((Action)(() => AssertParity(typeName, value, settings)))
                .SetName($"Parity_{TestUtilities.SanitizeTableName(typeName)}{suffix}_MatchesBoxedRead");

        static IEnumerable<(string TypeName, object Value)> Values()
        {
            yield return ("Int8", (sbyte)-8);
            yield return ("Int16", (short)-16);
            yield return ("Int32", -32);
            yield return ("Int64", -64L);
            yield return ("UInt8", (byte)8);
            yield return ("UInt16", (ushort)16);
            yield return ("UInt32", 32u);
            yield return ("UInt64", 64ul);
            yield return ("Int128", (BigInteger)(-128));
            yield return ("UInt128", (BigInteger)128);
            yield return ("Int256", (BigInteger)(-256));
            yield return ("UInt256", (BigInteger)256);
            yield return ("Float32", 1.5f);
            yield return ("Float64", 2.5d);
            yield return ("BFloat16", 1.25f);
            yield return ("Bool", true);
            yield return ("String", "abc");
            yield return ("FixedString(5)", "abcde");
            yield return ("UUID", Guid.Parse("11223344-5566-7788-99aa-bbccddeeff00"));
            yield return ("IPv4", IPAddress.Parse("10.0.0.1"));
            yield return ("IPv6", IPAddress.Parse("2001:db8::1"));
            yield return ("Date", new DateTime(2021, 3, 4, 0, 0, 0, DateTimeKind.Utc));
            yield return ("Date32", new DateTime(2021, 3, 4, 0, 0, 0, DateTimeKind.Utc));
            yield return ("DateTime('UTC')", new DateTime(2021, 3, 4, 5, 6, 7, DateTimeKind.Utc));
            yield return ("DateTime64(3, 'UTC')", new DateTime(2021, 3, 4, 5, 6, 7, 123, DateTimeKind.Utc));
            yield return ("Time", TimeSpan.FromSeconds(3661));
            yield return ("Time64(3)", TimeSpan.FromMilliseconds(3661123));
            yield return ("Decimal(10, 2)", 12.34m);
            yield return ("Decimal(30, 4)", 1234.5678m);
            yield return (Enum8Def, "b");

            // No typed reader: these must fall back and still round-trip byte-for-byte.
            yield return ("Array(Int32)", new[] { 1, 2, 3 });
            yield return ("Tuple(Int32, String)", Tuple.Create(1, "a"));
            yield return ("Map(String, Int32)", new Dictionary<string, int> { ["a"] = 1 });
        }
    }

    [TestCaseSource(nameof(ParityCases))]
    public void Parity_SlotMatchesBoxedRead_InValueAndByteCount(Action assertion) => assertion();

    private static void AssertParity(string typeName, object value, TypeSettings settings)
    {
        var type = Parse(typeName, settings);
        var payload = Write(type, value);

        var boxed = ReadBoxed(type, payload);
        var slot = ReadSlot(type, payload);

        Assert.Multiple(() =>
        {
            Assert.That(slot.Trailer, Is.EqualTo(Sentinel),
                $"{typeName}: the slot consumed the wrong number of bytes");
            Assert.That(boxed.Trailer, Is.EqualTo(Sentinel),
                $"{typeName}: the boxed reader consumed the wrong number of bytes (bad test payload)");
            Assert.That(slot.Value, Is.EqualTo(boxed.Value),
                $"{typeName}: slot value differs from the boxed read");
            Assert.That(slot.Value?.GetType(), Is.EqualTo(boxed.Value?.GetType()),
                $"{typeName}: slot boxed the value as a different CLR type than the boxed read");
            Assert.That(slot.Slot.IsNull, Is.EqualTo(boxed.Value is null or DBNull),
                $"{typeName}: IsNull disagrees with the boxed read's null representation");
        });
    }

    // ---- Slot selection: a silent demotion to BoxedSlot costs the allocation win but changes no value ----

    [TestCase("Int32", typeof(ValueSlot<int>))]
    [TestCase("UInt64", typeof(ValueSlot<ulong>))]
    [TestCase("Float64", typeof(ValueSlot<double>))]
    [TestCase("Bool", typeof(ValueSlot<bool>))]
    [TestCase("String", typeof(ValueSlot<string>))]
    [TestCase("FixedString(3)", typeof(ValueSlot<string>))]
    [TestCase("UUID", typeof(ValueSlot<Guid>))]
    [TestCase("IPv6", typeof(ValueSlot<IPAddress>))]
    [TestCase("Date", typeof(ValueSlot<DateTime>))]
    [TestCase("DateTime64(3, 'UTC')", typeof(ValueSlot<DateTime>))]
    [TestCase("Time64(3)", typeof(ValueSlot<TimeSpan>))]
    [TestCase("Decimal(10, 2)", typeof(ValueSlot<ClickHouseDecimal>))] // TypeSettings.Default is useBigDecimal
    [TestCase("Int256", typeof(ValueSlot<BigInteger>))]
    [TestCase(Enum8Def, typeof(ValueSlot<string>))]
    [TestCase("LowCardinality(String)", typeof(ValueSlot<string>))]
    [TestCase("SimpleAggregateFunction(sum, UInt64)", typeof(ValueSlot<ulong>))]
    [TestCase("Nullable(Int32)", typeof(NullableSlot<int>))]
    [TestCase("Nullable(String)", typeof(NullableSlot<string>))]
    [TestCase("Nullable(UUID)", typeof(NullableSlot<Guid>))]
    [TestCase("LowCardinality(Nullable(String))", typeof(NullableSlot<string>))]
    [TestCase("SimpleAggregateFunction(any, Nullable(Int32))", typeof(NullableSlot<int>))]
    // No ITypedReader for the column's own FrameworkType: composites, polymorphic and geo types.
    [TestCase("Array(Int32)", typeof(BoxedSlot))]
    [TestCase("Tuple(Int32, String)", typeof(BoxedSlot))]
    [TestCase("Map(String, Int32)", typeof(BoxedSlot))]
    [TestCase("Nullable(Array(Int32))", typeof(BoxedSlot))]
    [TestCase("Variant(Int64, String)", typeof(BoxedSlot))]
    [TestCase("Point", typeof(BoxedSlot))]
    [TestCase("Nothing", typeof(BoxedSlot))]
    public void Create_Column_ResolvesToExpectedSlotKind(string typeName, Type expected)
        => Assert.That(ColumnSlotFactory.Create(Parse(typeName)), Is.TypeOf(expected));

    // FrameworkType is instance state, not class state, so the factory has to read it off the resolved
    // instance. Caching by ClickHouseType class alone would bind every String column to one representation.
    [TestCase(false, typeof(ValueSlot<string>))]
    [TestCase(true, typeof(ValueSlot<byte[]>))]
    public void Create_StringColumn_BindsToTheRepresentationTheSettingsSelect(bool asByteArray, Type expected)
    {
        var settings = TypeSettings.Default with { readStringsAsByteArrays = asByteArray };
        Assert.That(ColumnSlotFactory.Create(Parse("String", settings)), Is.TypeOf(expected));
    }

    [TestCase(false, typeof(ValueSlot<decimal>))]
    [TestCase(true, typeof(ValueSlot<ClickHouseDecimal>))]
    public void Create_DecimalColumn_BindsToTheRepresentationTheSettingsSelect(bool useBigDecimal, Type expected)
    {
        var settings = TypeSettings.Default with { useBigDecimal = useBigDecimal };
        Assert.That(ColumnSlotFactory.Create(Parse("Decimal(10, 2)", settings)), Is.TypeOf(expected));
    }

    // Object(...) is wire-transparent too, but the grammar never yields an ObjectType instance —
    // ObjectType.Parse returns a SimpleAggregateFunctionType — so the only way to reach that unwrap branch is
    // to construct one. Worth pinning: if Parse is ever corrected, this is already covered.
    [Test]
    public void Create_ObjectWrappedColumn_ResolvesToWrappedTypedSlot()
    {
        var type = new ObjectType { UnderlyingType = new Int64Type() };
        Assert.That(ColumnSlotFactory.Create(type), Is.TypeOf<ValueSlot<long>>());
    }

    // The factory dispatches through a hand-written table of ValueSlot<T>/NullableSlot<T> constructors rather
    // than MakeGenericMethod, so that NativeAOT and trimming can see every instantiation the reader needs.
    // The cost of giving up runtime generic construction is that the table no longer maintains itself: adding
    // an ITypedReader<T> for a new T and forgetting the entry would silently demote that column to the boxed
    // path — values still correct, allocation quietly back. Nothing else would catch it, so this does.
    [Test]
    public void Binders_CoverEveryTypedReadTarget()
    {
        var declared = typeof(ClickHouseType).Assembly
            .GetTypes()
            .Where(t => typeof(ClickHouseType).IsAssignableFrom(t))
            .SelectMany(t => t.GetInterfaces())
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ITypedReader<>))
            .Select(i => i.GetGenericArguments()[0])
            .Distinct()
            .ToArray();

        Assert.That(declared, Is.Not.Empty, "found no ITypedReader<T> implementations at all — check the query");

        var bound = (IDictionary)typeof(ColumnSlotFactory)
            .GetField("Binders", BindingFlags.NonPublic | BindingFlags.Static)
            .GetValue(null);

        Assert.That(declared.Where(t => !bound.Contains(t)), Is.Empty,
            "every CLR type some ClickHouseType can read box-free needs a ColumnSlotFactory.Binders entry");
    }

    // AggregateFunctionType throws from FrameworkType (and from Read and ToString) so that you learn you need
    // xMerge() when you read the value. Slots are built for every column of the row on the reader's first
    // Read(), so the factory must reach its no-typed-reader bail-out without ever evaluating FrameworkType —
    // otherwise building the slots would throw before a single column had been decoded. Guards the ordering
    // in TryCreateTyped, which is otherwise easy to "tidy up" into a regression.
    //
    // This is the only test that can guard it. End to end the two orderings are indistinguishable: both raise
    // AggregateFunctionException, with the same message, from the same Read() call.
    [Test]
    public void Create_AggregateFunctionColumn_FallsBackToBoxedWithoutEvaluatingFrameworkType()
    {
        var type = Parse("AggregateFunction(quantile(0.5), UInt64)");
        Assert.That(() => ColumnSlotFactory.Create(type), Throws.Nothing);
        Assert.That(ColumnSlotFactory.Create(type), Is.TypeOf<BoxedSlot>());
    }

    // The factory's pre-filter finds the ITypedReader<> interfaces a class implements; the binding rule is
    // narrower than that — the reader has to be for the column's own FrameworkType, or the slot's GetBoxed()
    // would hand back a different CLR type than the boxed Read did. No shipped type is shaped this way, so
    // only a purpose-built one can prove the factory declines rather than mis-binding.
    [Test]
    public void Create_TypeWhoseTypedReaderIsNotItsFrameworkType_FallsBackToBoxed()
        => Assert.That(ColumnSlotFactory.Create(new MismatchedRepresentationType()), Is.TypeOf<BoxedSlot>());

    private sealed class MismatchedRepresentationType : ClickHouseType, ITypedReader<int>
    {
        public override Type FrameworkType => typeof(string);

        public override object Read(ExtendedBinaryReader reader) => reader.ReadString();

        public int ReadValue(ExtendedBinaryReader reader) => reader.ReadInt32();

        public override void Write(ExtendedBinaryWriter writer, object value) => throw new NotSupportedException();

        public override string ToString() => nameof(MismatchedRepresentationType);
    }

    // ---- IsNull: no boxed counterpart, so parity cannot cover it ----

    [Test]
    public void NullableSlot_AfterNullThenValue_TracksPresencePerRow()
    {
        var type = Parse("Nullable(Int32)");
        var slot = ColumnSlotFactory.Create(type);

        using var stream = new MemoryStream(Concat(Write(type, DBNull.Value), Write(type, 7)));
        using var reader = new ExtendedBinaryReader(stream);

        slot.Read(reader);
        Assert.That(slot.IsNull, Is.True);
        Assert.That(slot.GetBoxed(), Is.EqualTo(DBNull.Value));
        Assert.That(reader.ReadInt64(), Is.EqualTo(Sentinel));

        slot.Read(reader);
        Assert.That(slot.IsNull, Is.False);
        Assert.That(slot.GetBoxed(), Is.EqualTo(7));
        Assert.That(reader.ReadInt64(), Is.EqualTo(Sentinel));
    }

    // A slot is reused across rows, so a null must clear the previous row's value rather than leaving it
    // reachable — otherwise a single null cell pins the last string/byte[] for the life of the reader.
    [Test]
    public void NullableSlot_ValueThenNull_ClearsPreviousRowsReference()
    {
        var type = Parse("Nullable(String)");
        var slot = (NullableSlot<string>)ColumnSlotFactory.Create(type);

        using var stream = new MemoryStream(Concat(Write(type, "kept-alive"), Write(type, DBNull.Value)));
        using var reader = new ExtendedBinaryReader(stream);

        slot.Read(reader);
        Assert.That(slot.Value, Is.EqualTo("kept-alive"));
        reader.ReadInt64();

        slot.Read(reader);
        Assert.That(slot.Value, Is.Null, "a null cell must release the previous row's value");
    }

    // A non-nullable column has no null marker on the wire, so nothing it decodes is ever null.
    [TestCase("Int32", 5)]
    [TestCase("String", "")]
    public void ValueSlot_AfterRead_IsNeverNull(string typeName, object value)
    {
        var type = Parse(typeName);
        var (_, _, slot) = ReadSlot(type, Write(type, value));
        Assert.That(slot.IsNull, Is.False);
    }

    private static byte[] Concat(byte[] first, byte[] second) => first.Concat(second).ToArray();
}
