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

        byte[] bytes = await CodecTestHarness.WriteSliceAsync(codec, column, 0, 0);
        Assert.That(bytes, Is.Empty, "a zero-row tuple writes no child values");

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "Tuple(Int32, String)", 0, CodecTestHarness.None);
        Assert.That(read.RowCount, Is.Zero);
    }

    [Test]
    public async Task WriteColumn_SlicedRange_WritesOnlyThatSliceOfEachChild()
    {
        IColumnCodec codec = Resolve("Tuple(Int32, String)");
        using IColumn full = DecodedColumns.Of("c", "Tuple(Int32, String)", new (int, string)[]
        {
            (1, "a"),
            (2, "bb"),
            (3, "ccc"),
            (4, "d"),
        });

        byte[] bytes = await CodecTestHarness.WriteStoredAsync(codec, full, start: 1, length: 2);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", "Tuple(Int32, String)", 2, CodecTestHarness.None);

        Assert.That(((IColumn<(int, string)>)read).Values.ToArray(), Is.EqualTo(new (int, string)[] { (2, "bb"), (3, "ccc") }));
    }

    [Test]
    public void CanWrite_TupleColumnWithCallerBuiltChildren_ReturnsFalse()
    {
        // A tuple built from rows has caller-built child columns, so the converter layer writes it.
        const string type = "Tuple(Int32, String)";
        IColumnCodec codec = Resolve(type);
        var rows = new (int, string)[] { (1, "a") };
        using IColumn decoded = DecodedColumns.Of("c", type, rows);
        using var fromRows = new TupleColumn<int, string>("c", type, rows);
        var flat = new ArrayColumn<(int, string)>("c", type, rows);

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(decoded), Is.True, "the column that a query of the type reads");
            Assert.That(codec.CanWrite(fromRows), Is.False, "a tuple built from rows");
            Assert.That(codec.CanWrite(flat), Is.False, "a column of tuple values");
        });
    }

    [Test]
    public async Task CanWrite_NestedChildWithoutDenseNamedFieldColumn_ReturnsFalse()
    {
        // The converter layer writes no Nested column. Thus an insert writes the tuple only through the codec, and
        // only when the Nested child is the NestedColumn that a query reads.
        const string type = "Tuple(Nested(a UInt8), String)";
        const string nestedType = "Nested(a UInt8)";
        IColumnCodec codec = Resolve(type);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(Array.Empty<byte>());
        using IColumn decoded = await codec.ReadColumnAsync(reader, "c", type, 0, CodecTestHarness.None);
        var ergonomic = new ArrayColumn<(object[][], string)>("c", type, Array.Empty<(object[][], string)>());
        var wrongDense = new TupleColumn<object[][], string>(
            "c",
            type,
            new IColumn[]
            {
                new ArrayColumn<object[][]>("c", nestedType, Array.Empty<object[][]>()),
                DecodedColumns.Of<string>("c", "String"),
            },
            fieldNames: null,
            ownsChildren: false);

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(decoded), Is.True, "the column that a query of the type reads");
            Assert.That(codec.CanWrite(ergonomic), Is.False, "a column of tuple values has no named field columns");
            Assert.That(codec.CanWrite(wrongDense), Is.False, "the Nested child is not a NestedColumn");
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
    public async Task ReadColumn_UnnamedTuple_ReportsEmptyFieldNamesRatherThanNull()
    {
        // The interface promises an empty list over a null so a caller can enumerate it unguarded. The codec builds
        // the unnamed shape by passing no names at all, so the normalization has to happen in the column.
        IColumnCodec codec = Resolve("Tuple(Int32, String)");
        var column = new TupleColumn<int, string>("c", "Tuple(Int32, String)", new (int, string)[] { (1, "a") });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "Tuple(Int32, String)", 1);

        Assert.Multiple(() =>
        {
            Assert.That(((ITupleColumn)read).FieldNames, Is.Not.Null);
            Assert.That(((ITupleColumn)read).FieldNames, Is.Empty);
            Assert.That(((ITupleColumn)column).FieldNames, Is.Empty, "a column built from rows names nothing either");
        });
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

    [TestCase("Tuple(`a b` Int32, `c,d` String)", new[] { "a b", "c,d" })]
    [TestCase(@"Tuple(`a\`b` Int32, `x\ny` String)", new[] { "a`b", "x\ny" })]
    public async Task ReadColumn_BacktickedElementNames_DecodeToTheNameTheServerHolds(string type, string[] expectedNames)
    {
        // A quoted field name is split off at the closing backtick, not at the first space inside it, and its
        // escapes are decoded: on 26.6 a column created as Tuple(`a``b` Int8) reports Tuple(`a\`b` Int8). The type
        // name keeps the wire spelling, since it is what an insert header echoes.
        IColumnCodec codec = Resolve(type);
        var column = new TupleColumn<int, string>("c", type, new (int, string)[] { (1, "a") });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, type, 1);

        Assert.Multiple(() =>
        {
            Assert.That(codec.TypeName, Is.EqualTo(type));
            Assert.That(((TupleColumnBase)read).FieldNames, Is.EqualTo(expectedNames));
        });
    }

    [Test]
    public async Task CanWrite_NonWritableElement_IsFalse()
    {
        // The Nothing codec writes no column. Thus the codec of a tuple with a Nothing element does not write the
        // column that a query of the type reads.
        const string type = "Tuple(Int32, Nothing)";
        IColumnCodec codec = Resolve(type);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(Array.Empty<byte>());
        using IColumn decoded = await codec.ReadColumnAsync(reader, "c", type, 0, CodecTestHarness.None);

        Assert.Multiple(() =>
        {
            Assert.That(Resolve("Int32").CanWrite(((ITupleColumn)decoded).Children[0]), Is.True, "the Int32 child is the column that a query reads");
            Assert.That(codec.CanWrite(decoded), Is.False);
        });
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
    public void Constructor_ChildrenDisagreeingOnRowCount_Throws()
    {
        // A tuple stores one value per element per row, so its children are its height and no separate row count is
        // accepted — those two can no longer disagree. What a caller can still get wrong is handing over children of
        // different heights, which would otherwise surface much later as one child's out-of-range read partway
        // through materializing a row.
        var two = new ArrayColumn<int>("c", "Int32", new[] { 1, 2 });
        var three = new ArrayColumn<int>("c", "Int32", new[] { 1, 2, 3 });

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new TupleColumn<int, int>("c", "Tuple(Int32, Int32)", new IColumn[] { two, three }, null, ownsChildren: false),
                Throws.ArgumentException.With.Message.Contains("disagree on their row count"));
            Assert.That(
                () => new TupleColumn<int, int>("c", "Tuple(Int32, Int32)", Array.Empty<IColumn>(), null, ownsChildren: false),
                Throws.ArgumentException.With.Message.Contains("at least one child"));
            Assert.That(
                new TupleColumn<int, int>("c", "Tuple(Int32, Int32)", new IColumn[] { two, two }, null, ownsChildren: false).RowCount,
                Is.EqualTo(2),
                "the row count comes from the children");
        });
    }

    [Test]
    public void RestrictOwnership_DisposesOnlyFlaggedChildren()
    {
        // The mechanism the partial densify rebuild relies on: after RestrictOwnership, Dispose frees exactly the
        // children flagged true (the freshly built ones) and leaves the rest (borrowed) untouched.
        var owned = new DisposeSpyColumn<int>("c", "Int32", new[] { 1 });
        var borrowed = new DisposeSpyColumn<int>("c", "Int32", new[] { 2 });
        var tuple = new TupleColumn<int, int>("c", "Tuple(Int32, Int32)", new IColumn[] { owned, borrowed }, null, ownsChildren: false);

        tuple.RestrictOwnership(new[] { true, false });
        tuple.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(owned.DisposeCount, Is.EqualTo(1), "a flagged (freshly built) child must be disposed exactly once");
            Assert.That(borrowed.DisposeCount, Is.EqualTo(0), "an unflagged (borrowed) child must not be disposed");
        });
    }

    [Test]
    public void Resolve_EmptyTuple_IsTheEmptyTupleCodec_AndABareTupleIsRejected()
    {
        // Tuple() and Tuple both parse to zero arguments, so only the argument list separates the legal
        // zero-element tuple from a type naming no elements at all. Neither branch is reachable from a round-trip.
        Assert.Multiple(() =>
        {
            IColumnCodec codec = Resolve("Tuple()");
            Assert.That(codec.TypeName, Is.EqualTo("Tuple()"));
            Assert.That(codec.ElementType, Is.EqualTo(typeof(ValueTuple)));
            Assert.That(
                () => Resolve("Tuple"),
                Throws.TypeOf<FormatException>().With.Message.Contains("at least one element"));
        });
    }

    [Test]
    public void Resolve_NamedEmptyTupleElement_KeepsItsArgumentList()
    {
        // A named element arrives as one token ("y Tuple") that NamedElementParser has to split and rebuild into a
        // node. Rebuilding must carry the argument list over rather than re-derive it from the argument count,
        // which is zero here — otherwise the element becomes a bare, malformed Tuple and resolution throws.
        Assert.Multiple(() =>
        {
            Assert.That(Resolve("Tuple(x Int32, y Tuple())").ElementType, Is.EqualTo(typeof((int, ValueTuple))));
            Assert.That(Resolve("Tuple(x Int32, y Tuple())").TypeName, Is.EqualTo("Tuple(x Int32, y Tuple())"));
            Assert.That(Resolve("Array(Tuple(y Tuple()))").ElementType, Is.EqualTo(typeof(ValueTuple<ValueTuple>[])));
        });
    }

    [Test]
    public async Task EmptyTuple_WritesOnePlaceholderBytePerRow_WithNoStatePrefix()
    {
        // The wire shape a round-trip cannot observe: the server ignores the placeholder byte's value, so only a
        // byte-level assertion pins that the client emits one per row, emits the same ASCII '0' the server does,
        // and writes no serialization state prefix.
        IColumnCodec codec = Resolve("Tuple()");
        var column = new ArrayColumn<ValueTuple>("c", "Tuple()", new ValueTuple[3]);

        byte[] withPrefix = await CodecTestHarness.WriteStoredAsync(codec, column, 0, 3, prefix: true);
        byte[] body = await CodecTestHarness.WriteStoredAsync(codec, column, 0, 3);
        byte[] slice = await CodecTestHarness.WriteStoredAsync(codec, column, 1, 2);
        byte[] empty = await CodecTestHarness.WriteStoredAsync(codec, column, 0, 0);

        Assert.Multiple(() =>
        {
            Assert.That(withPrefix, Is.EqualTo(body), "the empty tuple has no state prefix");
            Assert.That(body, Is.EqualTo(new byte[] { (byte)'0', (byte)'0', (byte)'0' }));
            Assert.That(slice, Is.EqualTo(new byte[] { (byte)'0', (byte)'0' }), "a slice writes one byte per row written");
            Assert.That(empty, Is.Empty, "a zero-row write emits nothing");
        });
    }

    [Test]
    public async Task EmptyTuple_ReadColumn_ConsumesOneBytePerRow_AndLeavesTheStreamAligned()
    {
        // The read discards the placeholder bytes, so nothing downstream of it can prove they were consumed. The
        // sentinel does: if the read stopped short the stream would be misaligned and the next column would be
        // decoded from the wrong offset.
        IColumnCodec codec = Resolve("Tuple()");
        byte[] bytes = await CodecTestHarness.WriteAsync(w =>
        {
            w.WriteByte((byte)'0');
            w.WriteByte((byte)'0');
            w.WriteByte(0x7F);
        });

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn column = await codec.ReadColumnAsync(reader, "c", "Tuple()", 2, CodecTestHarness.None);
        byte sentinel = await reader.ReadByteAsync(CodecTestHarness.None);

        Assert.Multiple(() =>
        {
            Assert.That(column.RowCount, Is.EqualTo(2));
            Assert.That(column.TypeName, Is.EqualTo("Tuple()"));
            Assert.That(column.GetValue(0), Is.EqualTo(default(ValueTuple)));
            Assert.That(((IColumn<ValueTuple>)column).Values.ToArray(), Is.EqualTo(new ValueTuple[2]));
            Assert.That(sentinel, Is.EqualTo(0x7F), "the placeholder bytes must all be consumed");
        });
    }

    [Test]
    public void CanWrite_DenseTupleColumnOfADifferentArity_ReturnsFalse()
    {
        IColumnCodec codec = Resolve("Tuple(Int32, String)");
        var narrower = new TupleColumn<int>(
            "c",
            "Tuple(Int32)",
            new IColumn[] { PrimitiveColumn<int>.FromValues("c", "Int32", new[] { 1 }) },
            fieldNames: null,
            ownsChildren: false);

        Assert.Multiple(() =>
        {
            Assert.That(Resolve("Tuple(Int32)").CanWrite(narrower), Is.True, "the codec of its own arity writes it");
            Assert.That(codec.CanWrite(narrower), Is.False);
        });
    }

    /// <summary>
    /// Verifies the error when a caller-built tuple has fewer children than its type declares.
    /// </summary>
    [Test]
    public void ReadAs_TupleColumnWithFewerChildrenThanItsType_ThrowsNamingBothCounts()
    {
        using var narrower = new TupleColumn<byte>(
            "c",
            "Tuple(UInt8, String)",
            new IColumn[] { PrimitiveColumn<byte>.FromValues("1", "UInt8", new byte[] { 7 }) },
            fieldNames: null,
            ownsChildren: false);

        var thrown = Assert.Throws<InvalidOperationException>(
            () => ColumnCodecRegistry.Default.Projections.ReadAs<(byte, byte[])>(narrower, default));

        Assert.That(thrown.Message, Does.Contain("Tuple(UInt8, String)").And.Contain("1 children").And.Contain("resolved to 2"));
    }
}
