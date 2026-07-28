using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class TupleColumnCodecTests
{
    private static IColumnCodec Resolve(string type) => ColumnCodecRegistry.Default.Resolve(type, default);

    [Test]
    public async Task Values_EveryArity_MaterializesEveryRow()
    {
        // TupleColumn has a separate lazily-built Values cache per arity — seven independent materialization
        // loops. The integration comparison only ever calls GetValue(row), which takes the uncached branch, so
        // none of those loops runs anywhere else. Row values per element type are covered against a real server
        // by the Tuple cases in InsertRoundTripCase; this exists to touch all seven Values implementations, and
        // the read column's stamped TypeName, which the integration comparison never inspects.
        const string t1 = "Tuple(Int32)";
        const string t2 = "Tuple(Int32, String)";
        const string t3 = "Tuple(Int32, String, Float64)";
        const string t4 = "Tuple(Int32, String, Float64, UInt8)";
        const string t5 = "Tuple(Int32, String, Float64, UInt8, Int16)";
        const string t6 = "Tuple(Int32, String, Float64, UInt8, Int16, Bool)";
        const string t7 = "Tuple(Int32, String, Float64, UInt8, Int16, Bool, UInt32)";

        var a1 = new[] { new ValueTuple<int>(1), new ValueTuple<int>(int.MinValue) };
        var a2 = new (int, string)[] { (1, "a"), (int.MaxValue, "héllo✓") };
        var a3 = new (int, string, double)[] { (1, "a", 1.5), (-2, string.Empty, -1.5e100) };
        var a4 = new (int, string, double, byte)[] { (1, "a", 1.5, 7), (-2, "bb", -3.5, 255) };
        var a5 = new (int, string, double, byte, short)[] { (1, "a", 1.5, 7, -3), (-2, "bb", -3.5, 255, short.MaxValue) };
        var a6 = new (int, string, double, byte, short, bool)[] { (1, "a", 1.5, 7, -3, true), (-2, "bb", -3.5, 255, short.MaxValue, false) };
        var a7 = new (int, string, double, byte, short, bool, uint)[] { (1, "a", 1.5, 7, -3, true, 9u), (-2, "bb", -3.5, 255, short.MaxValue, false, uint.MaxValue) };

        using IColumn r1 = await CodecTestHarness.RoundTripAsync(Resolve(t1), new TupleColumn<int>("c", t1, a1), t1, a1.Length);
        using IColumn r2 = await CodecTestHarness.RoundTripAsync(Resolve(t2), new TupleColumn<int, string>("c", t2, a2), t2, a2.Length);
        using IColumn r3 = await CodecTestHarness.RoundTripAsync(Resolve(t3), new TupleColumn<int, string, double>("c", t3, a3), t3, a3.Length);
        using IColumn r4 = await CodecTestHarness.RoundTripAsync(Resolve(t4), new TupleColumn<int, string, double, byte>("c", t4, a4), t4, a4.Length);
        using IColumn r5 = await CodecTestHarness.RoundTripAsync(Resolve(t5), new TupleColumn<int, string, double, byte, short>("c", t5, a5), t5, a5.Length);
        using IColumn r6 = await CodecTestHarness.RoundTripAsync(Resolve(t6), new TupleColumn<int, string, double, byte, short, bool>("c", t6, a6), t6, a6.Length);
        using IColumn r7 = await CodecTestHarness.RoundTripAsync(Resolve(t7), new TupleColumn<int, string, double, byte, short, bool, uint>("c", t7, a7), t7, a7.Length);

        Assert.Multiple(() =>
        {
            Assert.That(((IColumn<ValueTuple<int>>)r1).Values.ToArray(), Is.EqualTo(a1), "arity 1");
            Assert.That(((IColumn<(int, string)>)r2).Values.ToArray(), Is.EqualTo(a2), "arity 2");
            Assert.That(((IColumn<(int, string, double)>)r3).Values.ToArray(), Is.EqualTo(a3), "arity 3");
            Assert.That(((IColumn<(int, string, double, byte)>)r4).Values.ToArray(), Is.EqualTo(a4), "arity 4");
            Assert.That(((IColumn<(int, string, double, byte, short)>)r5).Values.ToArray(), Is.EqualTo(a5), "arity 5");
            Assert.That(((IColumn<(int, string, double, byte, short, bool)>)r6).Values.ToArray(), Is.EqualTo(a6), "arity 6");
            Assert.That(((IColumn<(int, string, double, byte, short, bool, uint)>)r7).Values.ToArray(), Is.EqualTo(a7), "arity 7");
            Assert.That(r2.TypeName, Is.EqualTo(t2));
        });
    }

    [Test]
    public async Task ReadColumn_EmptyColumn_ReadsZeroRowsWithoutConsumingBytes()
    {
        IColumnCodec codec = Resolve("Tuple(Int32, String)");
        var column = new TupleColumn<int, string>("c", "Tuple(Int32, String)", Array.Empty<(int, string)>());

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column, 0, 0));
        Assert.That(bytes, Is.Empty, "a zero-row tuple writes no child values");

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "Tuple(Int32, String)", 0, CodecTestHarness.None);
        Assert.That(read.RowCount, Is.Zero);
    }

    [Test]
    public async Task WriteColumn_SlicedRange_WritesOnlyThatSliceOfEachChild()
    {
        IColumnCodec codec = Resolve("Tuple(Int32, String)");
        var full = new TupleColumn<int, string>("c", "Tuple(Int32, String)", new (int, string)[]
        {
            (1, "a"),
            (2, "bb"),
            (3, "ccc"),
            (4, "d"),
        });

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, full, start: 1, length: 2));
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "Tuple(Int32, String)", 2, CodecTestHarness.None);

        Assert.That(((IColumn<(int, string)>)read).Values.ToArray(), Is.EqualTo(new (int, string)[] { (2, "bb"), (3, "ccc") }));
    }

    [Test]
    public void CanWrite_AcceptsDenseAndFlatMatchingTupleColumnsOnly()
    {
        IColumnCodec codec = Resolve("Tuple(Int32, String)");

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(new TupleColumn<int, string>("c", "Tuple(Int32, String)", new (int, string)[] { (1, "a") })), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<(int, string)>("c", "Tuple(Int32, String)", new (int, string)[] { (1, "a") })), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<(long, string)>("c", "Tuple(Int64, String)", new (long, string)[] { (1L, "a") })), Is.False);
            Assert.That(codec.CanWrite(PrimitiveColumn<int>.FromValues("c", "Int32", new[] { 1 })), Is.False);
        });
    }

    [Test]
    public void ElementType_IsTheValueTupleOfElementTypes()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Resolve("Tuple(Int32)").ElementType, Is.EqualTo(typeof(ValueTuple<int>)));
            Assert.That(Resolve("Tuple(Int32, String)").ElementType, Is.EqualTo(typeof((int, string))));
            Assert.That(Resolve("Tuple(a Int32, b String)").ElementType, Is.EqualTo(typeof((int, string))));
            Assert.That(Resolve("Tuple(Int32, Tuple(String, Float64))").ElementType, Is.EqualTo(typeof((int, (string, double)))));
            Assert.That(Resolve("Tuple(Array(UInt8), Nullable(Int32))").ElementType, Is.EqualTo(typeof((byte[], int?))));
        });
    }

    [Test]
    public void NullPlaceholder_IsTheDefaultValueTuple()
        => Assert.That(Resolve("Tuple(Int32, String)").NullPlaceholder, Is.EqualTo((0, (string)null)));

    [Test]
    public void Resolve_UnnamedTuple_StampsFullTypeName()
        => Assert.That(Resolve("Tuple(Int32, String)").TypeName, Is.EqualTo("Tuple(Int32, String)"));

    [Test]
    public void Resolve_NamedTuple_PreservesElementNamesInTypeName()
        => Assert.That(Resolve("Tuple(a Int32, b String)").TypeName, Is.EqualTo("Tuple(a Int32, b String)"));

    [Test]
    public async Task ReadColumn_NamedTuple_CarriesElementNamesAsMetadata()
    {
        IColumnCodec codec = Resolve("Tuple(a Int32, b String)");
        var column = new TupleColumn<int, string>("c", "Tuple(a Int32, b String)", new (int, string)[] { (1, "a") });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Tuple(a Int32, b String)", 1);

        Assert.That(((TupleColumnBase)read).FieldNames, Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task ReadColumn_NamedParametricElements_ResolveTypesAndRoundTrip()
    {
        // The element name is split off at the first space, so a named element whose type is itself parametric
        // (`a Array(Int32)`, `b Nullable(String)`) must resolve the base type plus its arguments correctly.
        const string type = "Tuple(a Array(Int32), b Nullable(String))";
        IColumnCodec codec = Resolve(type);
        var expected = new (int[], string)[] { (new[] { 1, 2, 3 }, "x"), (Array.Empty<int>(), null), (new[] { -1 }, string.Empty) };
        var column = new TupleColumn<int[], string>("c", type, expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, type, column.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(codec.ElementType, Is.EqualTo(typeof((int[], string))));
            Assert.That(codec.TypeName, Is.EqualTo(type));
            Assert.That(((TupleColumnBase)read).FieldNames, Is.EqualTo(new[] { "a", "b" }));
        });
    }

    [Test]
    public void Resolve_NamedElementWithExtraWhitespace_ResolvesElementTypes()
    {
        // The name is split off at the first whitespace and the run before the type is skipped, so a hand-written
        // type with extra spaces or a tab between name and type ("a  Int32", "b\tString") still resolves the base
        // types instead of failing with a NotSupportedException on a base name carrying leading whitespace.
        IColumnCodec codec = Resolve("Tuple(a  Int32, b\tString)");
        Assert.That(codec.ElementType, Is.EqualTo(typeof((int, string))));
    }

    [Test]
    public void CanWrite_NonWritableElement_IsFalse()
    {
        // A tuple over a non-writable element (Nothing) resolves for reads but must report it cannot be written.
        IColumnCodec codec = Resolve("Tuple(Int32, Nothing)");
        Assert.That(codec.CanWrite(PrimitiveColumn<int>.FromValues("c", "Int32", new[] { 1 })), Is.False);
    }

    [Test]
    public void Resolve_NoElements_ThrowsFormat()
        => Assert.Throws<FormatException>(() => Resolve("Tuple"));

    [Test]
    public void Resolve_TooManyElements_ThrowsNotSupported()
        => Assert.Throws<NotSupportedException>(() => Resolve("Tuple(Int32, Int32, Int32, Int32, Int32, Int32, Int32, Int32)"));

    [Test]
    public void Resolve_UnsupportedElement_ThrowsNotSupported()
        => Assert.Throws<NotSupportedException>(() => Resolve("Tuple(Int32, NoSuchType)"));

    [Test]
    public void RestrictOwnership_DisposesOnlyFlaggedChildren()
    {
        // The mechanism the partial densify rebuild relies on: after RestrictOwnership, Dispose frees exactly the
        // children flagged true (the freshly built ones) and leaves the rest (borrowed) untouched.
        var owned = new DisposeSpyColumn<int>("c", "Int32", new[] { 1 });
        var borrowed = new DisposeSpyColumn<int>("c", "Int32", new[] { 2 });
        var tuple = new TupleColumn<int, int>("c", "Tuple(Int32, Int32)", new IColumn[] { owned, borrowed }, null, rowCount: 1, ownsChildren: false);

        tuple.RestrictOwnership(new[] { true, false });
        tuple.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(owned.DisposeCount, Is.EqualTo(1), "a flagged (freshly built) child must be disposed exactly once");
            Assert.That(borrowed.DisposeCount, Is.EqualTo(0), "an unflagged (borrowed) child must not be disposed");
        });
    }
}
