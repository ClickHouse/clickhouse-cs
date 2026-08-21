using System;
using System.IO;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Unit coverage for <c>QBit(T, N)</c>, limited to what a server round-trip cannot observe: the exact plane
/// layout, the significance ordering <see cref="IQBitColumn.GetPlane(int)"/> imposes on top of it, the type-resolution
/// and write error paths, and the pooled <c>Values</c> cache. Per-type values are covered by
/// <see cref="InsertRoundTripCase"/> against a real server.
/// </summary>
[TestFixture]
public class QBitColumnCodecTests
{
    private const string Float32X4 = "QBit(Float32, 4)";

    // Captured from a ClickHouse 26.6 `SELECT v FROM t FORMAT Native` where v is QBit(Float32, 4) holding one row
    // of [1.0, 2.0, 3.0, 4.0] — the example documented on QBitColumnCodec. 32 planes, one byte per plane (one row
    // of ceil(4/8) = 1 byte), most significant bit first.
    private static readonly byte[] DocumentedBytes =
    {
        0x00, // bit 31 (sign): none of the four is negative
        0x0E, // bit 30: set for 2.0, 3.0, 4.0 -> elements 1, 2, 3 -> 0b1110
        0x01, // bit 29: only 1.0 (0x3F800000)
        0x01, // bit 28
        0x01, // bit 27
        0x01, // bit 26
        0x01, // bit 25
        0x01, // bit 24
        0x09, // bit 23: 1.0 and 4.0 -> elements 0 and 3 -> 0b1001
        0x04, // bit 22: only 3.0 -> element 2 -> 0b0100
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };

    // Captured the same way, for QBit(Float32, 16) holding one row of [1, -2, 3, -4, ... 15, -16]. Two bytes per
    // row per plane, so this is the only fixture that spans more than one 8-element group — the unit the vector
    // write path works in. The signs alternate, which makes the sign plane 0xAA 0xAA.
    private static readonly byte[] DocumentedBytes16 =
    {
        0xAA, 0xAA, 0xFF, 0xFE, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01,
        0x00, 0x01, 0xFF, 0x81, 0x80, 0x79, 0x78, 0x64, 0x66, 0x50, 0x55, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    };

    // Captured from a ClickHouse 26.7 `SELECT v FROM t FORMAT Native` where v is QBit(Int8, 16) holding one row of
    // [1, -2, 3, -4, ... 15, -16] — the same input values as DocumentedBytes16, over a two's-complement encoding
    // rather than IEEE-754. 8 planes of two bytes, most significant (the sign) first. Two bytes per row is what
    // makes the reversed byte order within a bitmap observable at all.
    private static readonly byte[] DocumentedInt8Bytes16 =
    {
        0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
        0x55, 0xAA, 0x5A, 0x5A, 0x66, 0x66, 0x55, 0x55,
    };

    private static readonly sbyte[] DocumentedInt8Vector =
    {
        1, -2, 3, -4, 5, -6, 7, -8, 9, -10, 11, -12, 13, -14, 15, -16,
    };

    private static IColumnCodec Codec(string type) => ColumnCodecRegistry.Default.Resolve(type, ResolveContext.ForWrite);

    [Test]
    public async Task WriteColumn_DocumentedExample_ProducesTheServersOwnBytes()
    {
        IColumnCodec codec = Codec(Float32X4);
        using var column = new ArrayColumn<float[]>("v", Float32X4, new[] { new[] { 1f, 2f, 3f, 4f } });

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column));

        CollectionAssert.AreEqual(DocumentedBytes, bytes);
    }

    [Test]
    public async Task ReadColumnAsync_TheServersOwnBytes_DecodesTheDocumentedVector()
    {
        IColumnCodec codec = Codec(Float32X4);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);

        using IColumn read = await codec.ReadColumnAsync(reader, "v", Float32X4, 1, CodecTestHarness.None);

        CollectionAssert.AreEqual(new[] { 1f, 2f, 3f, 4f }, (float[])read.GetValue(0));
    }

    [Test]
    public async Task WriteColumn_SpanningSeveralGroupsOfEight_ProducesTheServersOwnBytes()
    {
        // 16 elements is two whole 8-element groups, which is the unit the vector write path works in; the
        // dimension-4 fixture above never leaves the scalar tail. Pins the group loop against real server bytes.
        const string Type = "QBit(Float32, 16)";
        IColumnCodec codec = Codec(Type);
        using var column = new ArrayColumn<float[]>("v", Type, new[]
        {
            new[] { 1f, -2f, 3f, -4f, 5f, -6f, 7f, -8f, 9f, -10f, 11f, -12f, 13f, -14f, 15f, -16f },
        });

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column));

        CollectionAssert.AreEqual(DocumentedBytes16, bytes);
    }

    [Test]
    public async Task WriteColumn_RowSlice_EmitsEachPlaneStridedByTheSourceRowCount()
    {
        // The body is plane-major, so a row range is contiguous within a plane but the planes are strided by the
        // *source* column's row count. Slicing the middle row of three is the shape that catches a write which
        // strides by the slice length instead. Whole-column re-inserts never reach it.
        IColumnCodec codec = Codec(Float32X4);
        using var source = new ArrayColumn<float[]>("v", Float32X4, new[]
        {
            new[] { 0f, 0f, 0f, 0f },
            new[] { 1f, 2f, 3f, 4f },
            new[] { 0f, 0f, 0f, 0f },
        });

        byte[] dense = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, source));
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(dense);
        using IColumn read = await codec.ReadColumnAsync(reader, "v", Float32X4, 3, CodecTestHarness.None);

        byte[] sliced = await CodecTestHarness.WriteSliceAsync(codec, read, start: 1, length: 1);

        CollectionAssert.AreEqual(DocumentedBytes, sliced);
    }

    [Test]
    public async Task GetPlane_ReadColumn_IndexesPlanesBySignificanceNotWireOrder()
    {
        // The wire stores planes most significant first; GetPlane takes the bit's significance, so bit 30 is the
        // second plane on the wire. Nothing about this ordering is observable through a round-trip.
        IColumnCodec codec = Codec(Float32X4);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "v", Float32X4, 1, CodecTestHarness.None);

        var qbit = (IQBitColumn)read;

        Assert.Multiple(() =>
        {
            Assert.That(qbit.Dimension, Is.EqualTo(4));
            Assert.That(qbit.BitWidth, Is.EqualTo(32));
            Assert.That(qbit.BytesPerRow, Is.EqualTo(1));
            Assert.That(qbit.GetPlane(31)[0], Is.EqualTo(0x00), "sign plane");
            Assert.That(qbit.GetPlane(30)[0], Is.EqualTo(0x0E), "bit 30: 2.0, 3.0, 4.0");
            Assert.That(qbit.GetPlane(23)[0], Is.EqualTo(0x09), "bit 23: 1.0 and 4.0");
            Assert.That(qbit.GetPlane(22)[0], Is.EqualTo(0x04), "bit 22: 3.0");
            Assert.That(qbit.GetPlane(0)[0], Is.EqualTo(0x00), "no value has a bit that low set");
        });
    }

    [Test]
    public async Task GetPlane_MultipleRows_ReturnsEveryRowsBitmapForThatPlane()
    {
        // Rows are contiguous within a plane, so one plane spans the whole column. -0.0 sets only the sign bit,
        // which makes the sign plane the one place the three rows differ.
        IColumnCodec codec = Codec("QBit(Float32, 8)");
        using var column = new ArrayColumn<float[]>("v", "QBit(Float32, 8)", new[]
        {
            new float[8],
            new[] { 1f, 1f, 1f, 1f, 1f, 1f, 1f, 1f },
            new[] { -0f, -0f, -0f, -0f, -0f, -0f, -0f, -0f },
        });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, "QBit(Float32, 8)", 3);
        var qbit = (IQBitColumn)read;

        CollectionAssert.AreEqual(new byte[] { 0x00, 0x00, 0xFF }, qbit.GetPlane(31).ToArray());
    }

    [TestCase(-1)]
    [TestCase(32)]
    public async Task GetPlane_BitOutsideTheWidth_Throws(int bit)
    {
        IColumnCodec codec = Codec(Float32X4);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "v", Float32X4, 1, CodecTestHarness.None);

        Assert.That(() => ((IQBitColumn)read).GetPlane(bit), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task Values_ReadColumn_MaterializesEveryRowThroughThePooledCache()
    {
        // GetValue delegates to the same de-transpose, but Values is a separately materialized pooled cache that
        // the round-trip's per-row comparison never touches.
        IColumnCodec codec = Codec(Float32X4);
        using var column = new ArrayColumn<float[]>("v", Float32X4, new[]
        {
            new[] { 1f, 2f, 3f, 4f },
            new[] { -1f, -2f, -3f, -4f },
        });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, Float32X4, 2);
        ReadOnlySpan<float[]> values = ((IColumn<float[]>)read).Values;

        Assert.That(values.Length, Is.EqualTo(2));
        CollectionAssert.AreEqual(new[] { 1f, 2f, 3f, 4f }, values[0]);
        CollectionAssert.AreEqual(new[] { -1f, -2f, -3f, -4f }, values[1]);
    }

    [Test]
    public async Task Values_ReadTwice_ReturnsTheSameCachedArrays()
    {
        IColumnCodec codec = Codec(Float32X4);
        using var column = new ArrayColumn<float[]>("v", Float32X4, new[] { new[] { 1f, 2f, 3f, 4f } });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, Float32X4, 1);
        var typed = (IColumn<float[]>)read;

        Assert.That(typed.Values[0], Is.SameAs(typed.Values[0]));
    }

    [Test]
    public async Task ReadColumnAsync_ZeroRows_ReadsNoBytesAndDecodesAnEmptyColumn()
    {
        IColumnCodec codec = Codec(Float32X4);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(Array.Empty<byte>());

        using IColumn read = await codec.ReadColumnAsync(reader, "v", Float32X4, 0, CodecTestHarness.None);

        Assert.That(read.RowCount, Is.Zero);
        Assert.That(((IColumn<float[]>)read).Values.Length, Is.Zero);
    }

    [Test]
    public async Task GetValue_RowPastTheRowCount_Throws()
    {
        IColumnCodec codec = Codec(Float32X4);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "v", Float32X4, 1, CodecTestHarness.None);

        Assert.That(() => read.GetValue(1), Throws.InstanceOf<IndexOutOfRangeException>());
    }

    [Test]
    public void Resolve_Float64_SurfacesDoubleVectorsAndSixtyFourPlanes()
    {
        IColumnCodec codec = Codec("QBit(Float64, 3)");

        Assert.Multiple(() =>
        {
            Assert.That(codec.ElementType, Is.EqualTo(typeof(double[])));
            Assert.That(codec.TypeName, Is.EqualTo("QBit(Float64, 3)"));
            Assert.That(codec.NullPlaceholder, Is.EqualTo(new double[3]));
        });
    }

    [Test]
    public void Resolve_BFloat16_SurfacesWidenedFloatVectors()
    {
        IColumnCodec codec = Codec("QBit(BFloat16, 4)");

        Assert.Multiple(() =>
        {
            Assert.That(codec.ElementType, Is.EqualTo(typeof(float[])));
            Assert.That(codec.NullPlaceholder, Is.EqualTo(new float[4]));
        });
    }

    [Test]
    public async Task WriteThenRead_BFloat16_DropsTheLowMantissaBits()
    {
        // A brain-float keeps only the float's high 16 bits, so a value needing the low half comes back narrowed.
        // The server normalizes nothing here — this is the client's own lossy narrowing, so no round-trip shows it.
        const string Type = "QBit(BFloat16, 2)";
        IColumnCodec codec = Codec(Type);
        using var column = new ArrayColumn<float[]>("v", Type, new[] { new[] { 1.0001f, 2f } });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, Type, 1);
        var value = (float[])read.GetValue(0);

        Assert.Multiple(() =>
        {
            Assert.That(value[0], Is.EqualTo(1f), "1.0001f narrows to 1f in a brain-float");
            Assert.That(value[1], Is.EqualTo(2f), "2f is exactly representable");
        });
    }

    [Test]
    public void CanWrite_ColumnOfAnotherElementType_IsRefused()
    {
        IColumnCodec codec = Codec(Float32X4);

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(new ArrayColumn<float[]>("v", Float32X4, new[] { new[] { 1f, 2f, 3f, 4f } })), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<double[]>("v", Float32X4, new[] { new[] { 1d } })), Is.False);
            Assert.That(codec.CanWrite(PrimitiveColumn<float>.FromValues("v", "Float32", new[] { 1f })), Is.False);
            Assert.That(codec.CanWriteElementType(typeof(float[])), Is.True);
            Assert.That(codec.CanWriteElementType(typeof(double[])), Is.False);
        });
    }

    [Test]
    public void WriteColumn_VectorOfTheWrongLength_ThrowsNamingTheRow()
    {
        IColumnCodec codec = Codec(Float32X4);
        using var column = new ArrayColumn<float[]>("v", Float32X4, new[]
        {
            new[] { 1f, 2f, 3f, 4f },
            new[] { 1f, 2f },
        });

        Assert.That(
            async () => await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column)),
            Throws.ArgumentException.With.Message.Contains("row 1").And.Message.Contains("exactly 4"));
    }

    [Test]
    public void WriteColumn_NullVector_ThrowsPointingAtNullable()
    {
        IColumnCodec codec = Codec(Float32X4);
        using var column = new ArrayColumn<float[]>("v", Float32X4, new float[][] { null });

        Assert.That(
            async () => await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column)),
            Throws.ArgumentException.With.Message.Contains("Nullable"));
    }

    [Test]
    public async Task WriteColumn_Int8Vector_ProducesTheServersOwnBytes()
    {
        const string Type = "QBit(Int8, 16)";
        IColumnCodec codec = Codec(Type);
        using var column = new ArrayColumn<sbyte[]>("v", Type, new[] { DocumentedInt8Vector });

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column));

        CollectionAssert.AreEqual(DocumentedInt8Bytes16, bytes);
    }

    [Test]
    public async Task ReadColumnAsync_Int8ServerBytes_DecodesTheTwosComplementVector()
    {
        // The negative values are what a de-transpose that rebuilt the byte through a signed accumulator would
        // get wrong; the sign is just plane 0's bit, with no widening involved.
        const string Type = "QBit(Int8, 16)";
        IColumnCodec codec = Codec(Type);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedInt8Bytes16);

        using IColumn read = await codec.ReadColumnAsync(reader, "v", Type, 1, CodecTestHarness.None);

        CollectionAssert.AreEqual(DocumentedInt8Vector, (sbyte[])read.GetValue(0));
        Assert.That(((IQBitColumn)read).BitWidth, Is.EqualTo(8));
    }

    [Test]
    public void CanWrite_Int8Codec_AcceptsOnlySByteVectors()
    {
        IColumnCodec codec = Codec("QBit(Int8, 4)");

        Assert.Multiple(() =>
        {
            Assert.That(codec.ElementType, Is.EqualTo(typeof(sbyte[])));
            Assert.That(codec.CanWrite(new ArrayColumn<sbyte[]>("v", "QBit(Int8, 4)", new[] { new sbyte[4] })), Is.True);
            Assert.That(codec.CanWrite(new ArrayColumn<float[]>("v", "QBit(Int8, 4)", new[] { new float[4] })), Is.False);
            Assert.That(codec.NullPlaceholder, Is.EqualTo(new sbyte[4]));
        });
    }

    [Test]
    public async Task GetPlane_UnstridedColumn_ReportsOneGroupAndAgreesWithTheGroupOverload()
    {
        // Stride and GroupCount describe the strided QBit(T, N, stride) layout 26.7 added, which is not decoded
        // yet — so every column reports a single group, and GetPlane(bit) is GetPlane(bit, 0). Pinning that keeps
        // the two accessors from drifting apart when the strided layout does land.
        IColumnCodec codec = Codec(Float32X4);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "v", Float32X4, 1, CodecTestHarness.None);
        var qbit = (IQBitColumn)read;

        Assert.Multiple(() =>
        {
            Assert.That(qbit.Stride, Is.EqualTo(qbit.Dimension));
            Assert.That(qbit.GroupCount, Is.EqualTo(1));
            Assert.That(qbit.GetPlane(30, 0).ToArray(), Is.EqualTo(qbit.GetPlane(30).ToArray()));
        });
    }

    [Test]
    public async Task GetPlane_GroupPastTheOnlyGroup_ThrowsArgumentOutOfRange()
    {
        IColumnCodec codec = Codec(Float32X4);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "v", Float32X4, 1, CodecTestHarness.None);
        var qbit = (IQBitColumn)read;

        Assert.Multiple(() =>
        {
            Assert.That(() => qbit.GetPlane(0, 1).ToArray(), Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => qbit.GetPlane(0, -1).ToArray(), Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [TestCase(1, 3, TestName = "length runs past the last row")]
    [TestCase(3, 1, TestName = "start is past the last row")]
    [TestCase(0, -1, TestName = "negative length")]
    public async Task WriteColumn_DenseSliceOutsideTheColumn_ThrowsArgumentOutOfRange(int start, int length)
    {
        // The dense path slices the blob per plane. The blob is rented and may be longer than the column, so an
        // over-long range has to be bounded against RowCount rather than against the array — otherwise it would
        // quietly emit stale pooled bytes instead of failing. Only the dense path can reach this.
        IColumnCodec codec = Codec(Float32X4);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        using IColumn dense = await codec.ReadColumnAsync(reader, "v", Float32X4, 1, CodecTestHarness.None);

        Assert.That(
            async () => await CodecTestHarness.WriteSliceAsync(codec, dense, start, length),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ReadColumnAsync_TruncatedPlaneBody_ThrowsEndOfStream()
    {
        // A body shorter than BitWidth * rows * BytesPerRow must fail rather than decode whatever the rented blob
        // happened to contain. This also drives the catch that hands the rent back before rethrowing — that half
        // is not observable from here, since ArrayPool gives no way to ask whether an array came home.
        IColumnCodec codec = Codec(Float32X4);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(new byte[] { 0x00, 0x0E });

        Assert.That(
            async () => await codec.ReadColumnAsync(reader, "v", Float32X4, 1, CodecTestHarness.None),
            Throws.InstanceOf<EndOfStreamException>());
    }

    [TestCase("QBit(Float32)", TestName = "one argument")]
    [TestCase("QBit(Float32, 4, 2, 1)", TestName = "four arguments")]
    public void Resolve_WrongArgumentCount_ThrowsFormatException(string type)
    {
        Assert.That(() => Codec(type), Throws.InstanceOf<FormatException>().With.Message.Contains("exactly two"));
    }

    [Test]
    public void Resolve_TheStrideFormAddedIn267_ThrowsNotSupportedException()
    {
        // 26.7 added QBit(T, N, stride), whose body is N / stride groups each carrying a full set of planes. The
        // server prints the third argument only when stride != N, so this is always a genuinely strided column.
        // Not decoded yet, and the error has to say which of the two it is rather than "wrong argument count".
        Assert.That(
            () => Codec("QBit(Float32, 16, 8)"),
            Throws.InstanceOf<NotSupportedException>().With.Message.Contains("strided"));
    }

    [TestCase("QBit(Float32, 0)")]
    [TestCase("QBit(Float32, -1)")]
    [TestCase("QBit(Float32, x)")]
    public void Resolve_InvalidDimension_ThrowsFormatException(string type)
    {
        Assert.That(() => Codec(type), Throws.InstanceOf<FormatException>().With.Message.Contains("vector length"));
    }

    // Int16 and UInt8 are the near misses: 26.7 widened the element type to Int8 only, so the neighbouring integer
    // widths stay rejected and a codec that matched on "any integer" would let them through.
    [TestCase("QBit(Int16, 4)")]
    [TestCase("QBit(UInt8, 4)")]
    [TestCase("QBit(Int32, 4)")]
    [TestCase("QBit(String, 4)")]
    [TestCase("QBit(Float16, 4)")]
    public void Resolve_ElementTypeTheServerRejects_ThrowsNotSupportedException(string type)
    {
        Assert.That(
            () => Codec(type),
            Throws.InstanceOf<NotSupportedException>().With.Message.Contains("Int8, BFloat16, Float32 and Float64"));
    }
}
