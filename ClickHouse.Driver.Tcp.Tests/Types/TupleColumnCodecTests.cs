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
    public async Task ReadColumn_WriteThenRead_FixedAndVariableElementsRoundTrip()
    {
        IColumnCodec codec = Resolve("Tuple(Int32, String)");
        var expected = new (int, string)[] { (1, "a"), (-5, string.Empty), (int.MaxValue, "héllo✓") };
        var column = new TupleColumn<int, string>("c", "Tuple(Int32, String)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Tuple(Int32, String)", column.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(read.TypeName, Is.EqualTo("Tuple(Int32, String)"));
            Assert.That(read.RowCount, Is.EqualTo(3));
            Assert.That(((IColumn<(int, string)>)read).Values.ToArray(), Is.EqualTo(expected));
            Assert.That(read.GetValue(2), Is.EqualTo((int.MaxValue, "héllo✓")));
        });
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_SingleElementTupleRoundTrips()
    {
        IColumnCodec codec = Resolve("Tuple(Int32)");
        var expected = new[] { new ValueTuple<int>(1), new ValueTuple<int>(-2), new ValueTuple<int>(int.MinValue) };
        var column = new TupleColumn<int>("c", "Tuple(Int32)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Tuple(Int32)", column.RowCount);

        Assert.That(((IColumn<ValueTuple<int>>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_NullableElementRoundTrips()
    {
        IColumnCodec codec = Resolve("Tuple(Nullable(UInt32), String)");
        var expected = new (uint?, string)[] { (1u, "a"), (null, "b"), (3u, "c") };
        var column = new TupleColumn<uint?, string>("c", "Tuple(Nullable(UInt32), String)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Tuple(Nullable(UInt32), String)", column.RowCount);

        Assert.That(((IColumn<(uint?, string)>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_NestedTupleRoundTrips()
    {
        IColumnCodec codec = Resolve("Tuple(Int32, Tuple(String, Float64))");
        var expected = new (int, (string, double))[] { (1, ("a", 1.5)), (2, (string.Empty, -1.5e100)) };
        var column = new TupleColumn<int, (string, double)>("c", "Tuple(Int32, Tuple(String, Float64))", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Tuple(Int32, Tuple(String, Float64))", column.RowCount);

        Assert.That(((IColumn<(int, (string, double))>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_WriteThenRead_ArrayElementRoundTrips()
    {
        IColumnCodec codec = Resolve("Tuple(Array(UInt8), String)");
        var expected = new (byte[], string)[] { (new byte[] { 1, 2 }, "a"), (Array.Empty<byte>(), "b") };
        var column = new TupleColumn<byte[], string>("c", "Tuple(Array(UInt8), String)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Tuple(Array(UInt8), String)", column.RowCount);

        Assert.That(((IColumn<(byte[], string)>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_ArrayOfTuple_RoundTripsThroughFlatWritePath()
    {
        // Array(Tuple(...)) flattens the jagged tuple arrays into one flat ValueTuple column and hands it to the
        // tuple codec, exercising the boxed per-element projection rather than the dense child-column path.
        IColumnCodec codec = Resolve("Array(Tuple(Int32, String))");
        var expected = new[]
        {
            new[] { (1, "a"), (2, "b") },
            Array.Empty<(int, string)>(),
            new[] { (3, "c") },
        };
        var column = new ArrayColumn<(int, string)[]>("c", "Array(Tuple(Int32, String))", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Array(Tuple(Int32, String))", column.RowCount);

        Assert.That(((IColumn<(int, string)[]>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task WriteColumn_FlatValueTupleColumn_RoundTripsViaProjection()
    {
        // A flat ArrayColumn<ValueTuple> is not an ITupleColumn, so it takes the ergonomic (boxed) write path.
        IColumnCodec codec = Resolve("Tuple(Int32, String)");
        var rows = new (int, string)[] { (1, "a"), (2, "bb"), (3, "ccc") };
        var flat = new ArrayColumn<(int, string)>("c", "Tuple(Int32, String)", rows);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, flat, "Tuple(Int32, String)", rows.Length);

        Assert.That(((IColumn<(int, string)>)read).Values.ToArray(), Is.EqualTo(rows));
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
    public void MeasureRowBytes_AllFixedElements_IsConstantSumOfWidths()
    {
        IColumnCodec codec = Resolve("Tuple(Int32, UInt8)");
        var column = new TupleColumn<int, byte>("c", "Tuple(Int32, UInt8)", new (int, byte)[] { (1, 2), (3, 4) });

        Assert.Multiple(() =>
        {
            Assert.That(codec.FixedRowByteSize, Is.EqualTo(5)); // 4 (Int32) + 1 (UInt8)
            Assert.That(codec.MeasureRowBytes(column, 0), Is.EqualTo(5));
        });
    }

    [Test]
    public void MeasureRowBytes_VariableElement_SumsPerChild()
    {
        IColumnCodec codec = Resolve("Tuple(Int32, String)");
        var dense = new TupleColumn<int, string>("c", "Tuple(Int32, String)", new (int, string)[] { (1, "a"), (2, "bbbb") });
        var flat = new ArrayColumn<(int, string)>("c", "Tuple(Int32, String)", new (int, string)[] { (1, "a"), (2, "bbbb") });

        Assert.Multiple(() =>
        {
            Assert.That(codec.FixedRowByteSize, Is.Null);
            Assert.That(codec.MeasureRowBytes(dense, 0), Is.EqualTo(4 + (1 + 1))); // Int32 + (varint len + "a")
            Assert.That(codec.MeasureRowBytes(dense, 1), Is.EqualTo(4 + (1 + 4)));
            Assert.That(codec.MeasureRowBytes(codec.TryDensify(flat, out _), 1), Is.EqualTo(4 + (1 + 4))); // flat densifies to the same
        });
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
            Assert.That(((IColumn<(int[], string)>)read).Values.ToArray(), Is.EqualTo(expected));
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
    public async Task ReadColumn_Arity3_RoundTripsViaValues()
    {
        IColumnCodec codec = Resolve("Tuple(Int32, String, Float64)");
        var expected = new (int, string, double)[] { (1, "a", 1.5), (-2, string.Empty, -1.5e100) };
        var column = new TupleColumn<int, string, double>("c", "Tuple(Int32, String, Float64)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Tuple(Int32, String, Float64)", column.RowCount);

        Assert.That(((IColumn<(int, string, double)>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_Arity4_RoundTripsViaValues()
    {
        IColumnCodec codec = Resolve("Tuple(Int32, String, Float64, UInt8)");
        var expected = new (int, string, double, byte)[] { (1, "a", 1.5, 7), (-2, "bb", -3.5, 255) };
        var column = new TupleColumn<int, string, double, byte>("c", "Tuple(Int32, String, Float64, UInt8)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Tuple(Int32, String, Float64, UInt8)", column.RowCount);

        Assert.That(((IColumn<(int, string, double, byte)>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_Arity5_RoundTripsViaValues()
    {
        IColumnCodec codec = Resolve("Tuple(Int32, String, Float64, UInt8, Int16)");
        var expected = new (int, string, double, byte, short)[] { (1, "a", 1.5, 7, -3), (-2, "bb", -3.5, 255, short.MaxValue) };
        var column = new TupleColumn<int, string, double, byte, short>("c", "Tuple(Int32, String, Float64, UInt8, Int16)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Tuple(Int32, String, Float64, UInt8, Int16)", column.RowCount);

        Assert.That(((IColumn<(int, string, double, byte, short)>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_Arity6_RoundTripsViaValues()
    {
        IColumnCodec codec = Resolve("Tuple(Int32, String, Float64, UInt8, Int16, Bool)");
        var expected = new (int, string, double, byte, short, bool)[] { (1, "a", 1.5, 7, -3, true), (-2, "bb", -3.5, 255, short.MaxValue, false) };
        var column = new TupleColumn<int, string, double, byte, short, bool>("c", "Tuple(Int32, String, Float64, UInt8, Int16, Bool)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Tuple(Int32, String, Float64, UInt8, Int16, Bool)", column.RowCount);

        Assert.That(((IColumn<(int, string, double, byte, short, bool)>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task ReadColumn_Arity7_RoundTripsViaValues()
    {
        IColumnCodec codec = Resolve("Tuple(Int32, String, Float64, UInt8, Int16, Bool, UInt32)");
        var expected = new (int, string, double, byte, short, bool, uint)[] { (1, "a", 1.5, 7, -3, true, 9u), (-2, "bb", -3.5, 255, short.MaxValue, false, uint.MaxValue) };
        var column = new TupleColumn<int, string, double, byte, short, bool, uint>("c", "Tuple(Int32, String, Float64, UInt8, Int16, Bool, UInt32)", expected);

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Tuple(Int32, String, Float64, UInt8, Int16, Bool, UInt32)", column.RowCount);

        Assert.That(((IColumn<(int, string, double, byte, short, bool, uint)>)read).Values.ToArray(), Is.EqualTo(expected));
    }

    [Test]
    public async Task TryDensify_FlatValueTupleColumn_ProducesDenseColumnThatRoundTrips()
    {
        // A flat ArrayColumn<ValueTuple> is un-transposed into one child column per element, so a later
        // measure/write drives the children directly. It must surface the same rows and round-trip.
        IColumnCodec codec = Resolve("Tuple(Int32, String)");
        var rows = new (int, string)[] { (1, "a"), (2, "b"), (3, "c") };
        var flat = new ArrayColumn<(int, string)>("c", "Tuple(Int32, String)", rows);

        IColumn densified = codec.TryDensify(flat, out _);

        Assert.That(densified, Is.InstanceOf<ITupleColumn>());
        var dense = (ITupleColumn)densified;
        Assert.Multiple(() =>
        {
            Assert.That(dense.Children.Count, Is.EqualTo(2));
            Assert.That(((IColumn<int>)dense.Children[0]).Values.ToArray(), Is.EqualTo(new[] { 1, 2, 3 }));
            Assert.That(((IColumn<string>)dense.Children[1]).Values.ToArray(), Is.EqualTo(new[] { "a", "b", "c" }));
            Assert.That(((IColumn<(int, string)>)densified).Values.ToArray(), Is.EqualTo(rows));
        });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, densified, "Tuple(Int32, String)", rows.Length);
        Assert.That(((IColumn<(int, string)>)read).Values.ToArray(), Is.EqualTo(rows));
    }

    [Test]
    public void TryDensify_FlatTupleWithArrayElement_DensifiesChildToDenseArray()
    {
        // A flat Tuple(Array(UInt8), String): densify un-transposes into per-child columns AND recurses, so the
        // Array child becomes a dense ArrayValueColumn rather than a jagged ArrayColumn<byte[]>.
        IColumnCodec codec = Resolve("Tuple(Array(UInt8), String)");
        var rows = new (byte[], string)[] { (new byte[] { 1, 2 }, "x"), (new byte[] { 3 }, "y") };
        var flat = new ArrayColumn<(byte[], string)>("c", "Tuple(Array(UInt8), String)", rows);

        var dense = (ITupleColumn)codec.TryDensify(flat, out _);
        var arrayChild = (IColumn<byte[]>)dense.Children[0];

        Assert.Multiple(() =>
        {
            Assert.That(dense.Children[0], Is.InstanceOf<ArrayValueColumn<byte>>());
            Assert.That(arrayChild[0], Is.EqualTo(new byte[] { 1, 2 }));
            Assert.That(arrayChild[1], Is.EqualTo(new byte[] { 3 }));
            Assert.That(((IColumn<string>)dense.Children[1]).Values.ToArray(), Is.EqualTo(new[] { "x", "y" }));
        });
    }

    [Test]
    public void TryDensify_AlreadyDenseColumn_ReturnsSameInstanceNotBuilt()
    {
        // A dense TupleColumn whose children are already dense (leaf columns) has nothing to rebuild, so it is
        // returned by reference with built = false.
        IColumnCodec codec = Resolve("Tuple(Int32, String)");
        var dense = new TupleColumn<int, string>("c", "Tuple(Int32, String)", new (int, string)[] { (1, "a"), (2, "b") });

        IColumn result = codec.TryDensify(dense, out bool built);
        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(dense));
            Assert.That(built, Is.False, "an all-dense tuple is borrowed, not rebuilt");
        });
    }

    [Test]
    public void TryDensify_DenseTupleWithOneErgonomicChild_DisposesOnlyRebuiltChildNotBorrowed()
    {
        // A dense TupleColumn whose child 0 is still ergonomic (a jagged ArrayColumn) while child 1 is already dense
        // (a leaf): TryDensify rebuilds only child 0 into a fresh dense column and keeps child 1 by reference. Disposing
        // the rebuilt wrapper must free the freshly built child 0 but leave the borrowed child 1 alone — it is still
        // owned by the source column, so disposing it here would double-dispose it (returning pooled buffers twice).
        IColumnCodec codec = Resolve("Tuple(Array(Int32), Int32)");
        var ergonomicArray = new ArrayColumn<int[]>("c", "Array(Int32)", new[] { new[] { 1, 2 }, new[] { 3 } });
        var borrowedLeaf = new DisposeSpyColumn<int>("c", "Int32", new[] { 9, 8 });
        // The source borrows both children (this test owns them), like a caller-assembled dense tuple.
        var source = new TupleColumn<int[], int>("c", "Tuple(Array(Int32), Int32)", new IColumn[] { ergonomicArray, borrowedLeaf }, null, rowCount: 2, ownsChildren: false);

        IColumn densified = codec.TryDensify(source, out bool built);
        Assert.That(built, Is.True, "child 0 was ergonomic, so a rebuild is expected");
        Assert.That(densified, Is.Not.SameAs(source));
        densified.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(borrowedLeaf.DisposeCount, Is.EqualTo(0), "the borrowed, already-dense child must not be disposed by the rebuilt wrapper");
            Assert.That(borrowedLeaf.Values.ToArray(), Is.EqualTo(new[] { 9, 8 }), "the borrowed child is still readable after disposing the wrapper");
        });

        ergonomicArray.Dispose();
        borrowedLeaf.Dispose();
    }

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
