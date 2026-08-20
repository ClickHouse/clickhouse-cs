using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Unit coverage for <c>QBit(T, N)</c>, limited to what a server round-trip cannot observe: the exact plane
/// layout, the significance ordering <see cref="IQBitColumn.GetPlane"/> imposes on top of it, the type-resolution
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

    [TestCase("QBit(Float32)", TestName = "one argument")]
    [TestCase("QBit(Float32, 4, 2)", TestName = "the stride form no server accepts")]
    public void Resolve_WrongArgumentCount_ThrowsFormatException(string type)
    {
        Assert.That(() => Codec(type), Throws.InstanceOf<FormatException>().With.Message.Contains("exactly two"));
    }

    [TestCase("QBit(Float32, 0)")]
    [TestCase("QBit(Float32, -1)")]
    [TestCase("QBit(Float32, x)")]
    public void Resolve_InvalidDimension_ThrowsFormatException(string type)
    {
        Assert.That(() => Codec(type), Throws.InstanceOf<FormatException>().With.Message.Contains("vector length"));
    }

    [TestCase("QBit(Int32, 4)")]
    [TestCase("QBit(String, 4)")]
    [TestCase("QBit(Float16, 4)")]
    public void Resolve_ElementTypeTheServerRejects_ThrowsNotSupportedException(string type)
    {
        Assert.That(
            () => Codec(type),
            Throws.InstanceOf<NotSupportedException>().With.Message.Contains("BFloat16, Float32 and Float64"));
    }
}
