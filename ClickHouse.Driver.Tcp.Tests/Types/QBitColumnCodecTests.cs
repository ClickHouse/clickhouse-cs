using System;
using System.IO;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class QBitColumnCodecTests
{
    private const string Float32X4 = "QBit(Float32, 4)";

    // Server-produced Native body for one QBit(Float32, 4) row containing [1, 2, 3, 4]. Planes are
    // ordered most-significant first.
    private static readonly byte[] DocumentedBytes =
    {
        0x00, // Bit 31: no elements set.
        0x0E, // Bit 30: elements 1, 2, and 3.
        0x01, // Bits 29 through 24: element 0.
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x09, // Bit 23: elements 0 and 3.
        0x04, // Bit 22: element 2.
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };

    // Server-produced Native body for one QBit(Float32, 16) row containing [1, -2, ..., 15, -16]. Each plane
    // contains a two-byte row bitmap.
    private static readonly byte[] DocumentedBytes16 =
    {
        0xAA, 0xAA, 0xFF, 0xFE, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01,
        0x00, 0x01, 0xFF, 0x81, 0x80, 0x79, 0x78, 0x64, 0x66, 0x50, 0x55, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    };

    // Server-produced Native body for one QBit(Int8, 16) row containing [1, -2, ..., 15, -16]. The eight planes
    // encode two's-complement bytes, most-significant plane first.
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
    public async Task WriteColumn_DenseRowSlice_ProducesTheServersOwnBytes()
    {
        const string Type = "QBit(Float32, 16)";
        IColumnCodec codec = Codec(Type);
        using var source = new ArrayColumn<float[]>("v", Type, new[]
        {
            new float[16],
            new[] { 1f, -2f, 3f, -4f, 5f, -6f, 7f, -8f, 9f, -10f, 11f, -12f, 13f, -14f, 15f, -16f },
            new float[16],
        });

        byte[] dense = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, source));
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(dense);
        using IColumn read = await codec.ReadColumnAsync(reader, "v", Type, 3, CodecTestHarness.None);

        byte[] sliced = await CodecTestHarness.WriteSliceAsync(codec, read, start: 1, length: 1);

        CollectionAssert.AreEqual(DocumentedBytes16, sliced);
    }

    [Test]
    public async Task GetPlane_ReadColumn_IndexesPlanesBySignificanceNotWireOrder()
    {
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
        float[] first = typed.Values[0];
        float[] second = typed.Values[0];

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.SameAs(first));
            Assert.That(read.GetValue(0), Is.SameAs(first));
        });
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

    [TestCase(-1, 1, TestName = "negative start")]
    [TestCase(0, -1, TestName = "negative length")]
    [TestCase(0, 2, TestName = "length runs past the last row")]
    public async Task WriteColumn_DenseSliceOutsideTheColumn_ThrowsArgumentOutOfRange(int start, int length)
    {
        IColumnCodec codec = Codec(Float32X4);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        using IColumn dense = await codec.ReadColumnAsync(reader, "v", Float32X4, 1, CodecTestHarness.None);

        Assert.That(
            async () => await CodecTestHarness.WriteSliceAsync(codec, dense, start, length),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task WriteColumn_EmptySliceAtTheEnd_WritesNoBytes()
    {
        IColumnCodec codec = Codec(Float32X4);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        using IColumn dense = await codec.ReadColumnAsync(reader, "v", Float32X4, 1, CodecTestHarness.None);

        byte[] bytes = await CodecTestHarness.WriteSliceAsync(codec, dense, start: 1, length: 0);

        Assert.That(bytes, Is.Empty);
    }

    [Test]
    public void ReadColumnAsync_TruncatedPlaneBody_ThrowsEndOfStream()
    {
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

    [TestCase("QBit(Int16, 4)")]
    [TestCase("QBit(UInt8, 4)")]
    [TestCase("QBit(Float16, 4)")]
    public void Resolve_ElementTypeTheServerRejects_ThrowsNotSupportedException(string type)
    {
        Assert.That(
            () => Codec(type),
            Throws.InstanceOf<NotSupportedException>().With.Message.Contains("Int8, BFloat16, Float32 and Float64"));
    }
}
