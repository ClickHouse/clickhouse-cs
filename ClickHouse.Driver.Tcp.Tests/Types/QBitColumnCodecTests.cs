using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

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

    // The column that a query of the type reads, for one row whose bits are all zero. The buffer holds one row of 64
    // planes of 32 elements, and the read takes only the bytes of one row of the type.
    private static async Task<IColumn> DecodedRowAsync(string type)
    {
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(new byte[256]);
        return await Codec(type).ReadColumnAsync(reader, "v", type, 1, CodecTestHarness.None);
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
        using IColumn dense = DecodedColumns.Of("v", Type, new[]
        {
            new float[16],
            new[] { 1f, -2f, 3f, -4f, 5f, -6f, 7f, -8f, 9f, -10f, 11f, -12f, 13f, -14f, 15f, -16f },
            new float[16],
        });

        byte[] sliced = await CodecTestHarness.WriteStoredAsync(codec, dense, start: 1, length: 1);

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

    // Int8 gives sbyte vectors. BFloat16 gives float vectors, which hold each BFloat16 value. Float64 gives double
    // vectors. The codec has one plane for each bit of the element type.
    [TestCase("QBit(Int8, 4)", typeof(sbyte[]), 8)]
    [TestCase("QBit(BFloat16, 4)", typeof(float[]), 16)]
    [TestCase("QBit(Float64, 3)", typeof(double[]), 64)]
    public void Resolve_ElementType_SurfacesItsVectorTypeAndOnePlanePerBit(string type, Type elementType, int planes)
    {
        IColumnCodec codec = Codec(type);

        Assert.Multiple(() =>
        {
            Assert.That(codec.ElementType, Is.EqualTo(elementType));
            Assert.That(codec.TypeName, Is.EqualTo(type));
            Assert.That(((QBitColumnCodec)codec).BitWidth, Is.EqualTo(planes));
        });
    }

    // The codec writes a decoded column of its own layout only: the same element type, dimension and bit width. Equal
    // body sizes do not make equal layouts: one row of QBit(Float32, 4) and one row of QBit(Float32, 8) are both 32
    // bytes.
    [TestCase(Float32X4, Float32X4, true)]
    [TestCase(Float32X4, "QBit(Float32, 8)", false)]
    [TestCase(Float32X4, "QBit(BFloat16, 4)", false)]
    [TestCase(Float32X4, "QBit(Float64, 4)", false)]
    [TestCase("QBit(Int8, 4)", "QBit(Int8, 4)", true)]
    [TestCase("QBit(Int8, 4)", Float32X4, false)]
    public async Task CanWrite_DecodedColumn_IsTrueOnlyForTheSameLayout(string codecType, string columnType, bool expected)
    {
        using IColumn decoded = await DecodedRowAsync(columnType);

        Assert.That(Codec(codecType).CanWrite(decoded), Is.EqualTo(expected));
    }

    // An insert gives the vectors that a caller builds to the converter layer.
    [Test]
    public void CanWrite_ColumnThatTheCallerBuilt_IsFalse()
    {
        using var floats = new ArrayColumn<float[]>("v", Float32X4, new[] { new[] { 1f, 2f, 3f, 4f } });
        using var bytes = new ArrayColumn<sbyte[]>("v", "QBit(Int8, 4)", new[] { new sbyte[4] });
        using IColumn scalars = PrimitiveColumn<float>.FromValues("v", "Float32", new[] { 1f });

        Assert.Multiple(() =>
        {
            Assert.That(Codec(Float32X4).CanWrite(floats), Is.False);
            Assert.That(Codec("QBit(Int8, 4)").CanWrite(bytes), Is.False);
            Assert.That(Codec(Float32X4).CanWrite(scalars), Is.False);
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
            async () => await CodecTestHarness.WriteSliceAsync(codec, column, 0, column.RowCount),
            Throws.ArgumentException.With.Message.Contains("row 1").And.Message.Contains("exactly 4"));
    }

    [Test]
    public void WriteColumn_NullVector_ThrowsPointingAtNullable()
    {
        IColumnCodec codec = Codec(Float32X4);
        using var column = new ArrayColumn<float[]>("v", Float32X4, new float[][] { null });

        Assert.That(
            async () => await CodecTestHarness.WriteSliceAsync(codec, column, 0, column.RowCount),
            Throws.ArgumentException.With.Message.Contains("Nullable"));
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
            async () => await CodecTestHarness.WriteStoredAsync(codec, dense, start, length),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task WriteColumn_EmptySliceAtTheEnd_WritesNoBytes()
    {
        IColumnCodec codec = Codec(Float32X4);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(DocumentedBytes);
        using IColumn dense = await codec.ReadColumnAsync(reader, "v", Float32X4, 1, CodecTestHarness.None);

        byte[] bytes = await CodecTestHarness.WriteStoredAsync(codec, dense, start: 1, length: 0);

        Assert.That(bytes, Is.Empty);
    }

    [Test]
    public void ReadColumnAsync_TruncatedPlaneBody_ThrowsConnectionException()
    {
        IColumnCodec codec = Codec(Float32X4);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(new byte[] { 0x00, 0x0E });

        Assert.That(
            async () => await codec.ReadColumnAsync(reader, "v", Float32X4, 1, CodecTestHarness.None),
            Throws.InstanceOf<ClickHouseTcpConnectionException>());
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
