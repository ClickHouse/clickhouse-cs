using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
    public async Task WriteColumn_EmptySliceOfRowVectors_WritesNoBytes()
    {
        IColumnCodec codec = Codec(Float32X4);
        using var column = new ArrayColumn<float[]>("v", Float32X4, new[] { new[] { 1f, 2f, 3f, 4f } });

        byte[] bytes = await CodecTestHarness.WriteSliceAsync(codec, column, start: 1, length: 0);

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

    // The transpose works on 8-element groups and 32-element blocks: these widths cover a lone partial group,
    // whole groups, whole blocks, and whole blocks followed by each kind of partial block. The last three cases
    // write several blocks of rows, the last of them partly filled.
    private static IEnumerable<TestCaseData> BlockShapes()
    {
        foreach (string element in new[] { "Int8", "BFloat16", "Float32", "Float64" })
        {
            foreach (int dimension in new[] { 1, 7, 8, 9, 31, 32, 33, 40, 63, 64, 65, 100, 256 })
            {
                yield return new TestCaseData(element, dimension, 3).SetArgDisplayNames(element, $"{dimension}", "3 rows");
            }
        }

        yield return new TestCaseData("Float32", 1024, 40).SetArgDisplayNames("Float32", "1024", "40 rows");
        yield return new TestCaseData("Float64", 100, 401).SetArgDisplayNames("Float64", "100", "401 rows");
        yield return new TestCaseData("Int8", 8, 1000).SetArgDisplayNames("Int8", "8", "1000 rows");
    }

    [TestCaseSource(nameof(BlockShapes))]
    public async Task WriteColumn_SliceOfVectorsOfEveryBlockShape_ProducesTheReferenceLayout(string element, int dimension, int rows)
    {
        string type = $"QBit({element}, {dimension})";
        ulong[][] patterns = RandomPatterns(element, rows + 1, dimension);
        using IColumn column = VectorColumn(element, type, patterns);

        byte[] bytes = await CodecTestHarness.WriteSliceAsync(Codec(type), column, start: 1, length: rows);

        Assert.That(bytes, Is.EqualTo(ReferenceBody(element, patterns.Skip(1).ToArray(), dimension)));
    }

    [TestCaseSource(nameof(BlockShapes))]
    public void TransposeScalar_VectorsOfEveryBlockShape_WritesEveryByteOfTheReferenceLayout(string element, int dimension, int rows)
    {
        string type = $"QBit({element}, {dimension})";
        var codec = (QBitColumnCodec)Codec(type);
        ulong[][] patterns = RandomPatterns(element, rows, dimension);
        int bytesPerRow = (dimension + 7) / 8;
        int planeStride = rows * bytesPerRow;

        // Every byte starts dirty, so a byte the transpose skips shows up as a mismatch.
        byte[] body = Enumerable.Repeat((byte)0xA5, PlaneCount(element) * planeStride).ToArray();
        for (int row = 0; row < rows; row++)
        {
            Span<byte> destination = body.AsSpan(row * bytesPerRow);
            switch (element)
            {
                case "Int8":
                    codec.TransposeScalar(patterns[row].Select(b => (byte)b).ToArray().AsSpan(), destination, planeStride);
                    break;
                case "Float64":
                    codec.TransposeScalar(patterns[row].AsSpan(), destination, planeStride);
                    break;
                default:
                    codec.TransposeScalar(patterns[row].Select(b => (uint)b).ToArray().AsSpan(), destination, planeStride);
                    break;
            }
        }

        Assert.That(body, Is.EqualTo(ReferenceBody(element, patterns, dimension)));
    }

    [TestCaseSource(nameof(BlockShapes))]
    public async Task GetValue_ReferenceLayoutOfEveryBlockShape_DecodesEveryVector(string element, int dimension, int rows)
    {
        string type = $"QBit({element}, {dimension})";
        ulong[][] patterns = RandomPatterns(element, rows, dimension);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(ReferenceBody(element, patterns, dimension));

        using IColumn read = await Codec(type).ReadColumnAsync(reader, "v", type, rows, CodecTestHarness.None);

        for (int row = 0; row < rows; row++)
        {
            Assert.That(Patterns(read.GetValue(row)), Is.EqualTo(Decoded(element, patterns[row])), $"row {row}");
        }
    }

    [TestCaseSource(nameof(BlockShapes))]
    public async Task DetransposeScalar_ReferenceLayoutOfEveryBlockShape_DecodesEveryVector(string element, int dimension, int rows)
    {
        string type = $"QBit({element}, {dimension})";
        ulong[][] patterns = RandomPatterns(element, rows, dimension);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(ReferenceBody(element, patterns, dimension));
        using IColumn read = await Codec(type).ReadColumnAsync(reader, "v", type, rows, CodecTestHarness.None);
        var column = (QBitColumn)read;

        for (int row = 0; row < rows; row++)
        {
            ulong[] decoded;
            switch (element)
            {
                case "Int8":
                    var bytes = new byte[dimension];
                    column.DetransposeScalar(column.RowPlanes(row), bytes.AsSpan());
                    decoded = bytes.Select(b => (ulong)b).ToArray();
                    break;
                case "Float64":
                    decoded = new ulong[dimension];
                    column.DetransposeScalar(column.RowPlanes(row), decoded.AsSpan());
                    break;
                default:
                    var words = new uint[dimension];
                    column.DetransposeScalar(column.RowPlanes(row), words.AsSpan());
                    decoded = words.Select(w => (ulong)w).ToArray();
                    break;
            }

            Assert.That(decoded, Is.EqualTo(Decoded(element, patterns[row])), $"row {row}");
        }
    }

    private static int ElementBits(string element) => element switch
    {
        "Int8" => 8,
        "Float64" => 64,
        _ => 32,
    };

    private static int PlaneCount(string element) => element == "BFloat16" ? 16 : ElementBits(element);

    /// <summary>Returns random element bit patterns, including NaN payloads, in the low bits of each value.</summary>
    private static ulong[][] RandomPatterns(string element, int rows, int dimension)
    {
        var random = new Random(dimension * 31 + rows);
        int bits = ElementBits(element);
        ulong mask = bits == 64 ? ulong.MaxValue : (1UL << bits) - 1;
        return Enumerable.Range(0, rows)
            .Select(_ => Enumerable.Range(0, dimension).Select(_ => (ulong)random.NextInt64() ^ ((ulong)random.Next() << 63)).Select(v => v & mask).ToArray())
            .ToArray();
    }

    /// <summary>Builds the Native body bit by bit from the layout in <see cref="QBitColumnCodec"/>'s summary.</summary>
    private static byte[] ReferenceBody(string element, ulong[][] patterns, int dimension)
    {
        int elementBits = ElementBits(element);
        int planes = PlaneCount(element);
        int bytesPerRow = (dimension + 7) / 8;
        var body = new byte[planes * patterns.Length * bytesPerRow];
        for (int plane = 0; plane < planes; plane++)
        {
            for (int row = 0; row < patterns.Length; row++)
            {
                for (int i = 0; i < dimension; i++)
                {
                    if (((patterns[row][i] >> (elementBits - 1 - plane)) & 1) != 0)
                    {
                        body[(((plane * patterns.Length) + row) * bytesPerRow) + bytesPerRow - 1 - (i / 8)] |= (byte)(1 << (i % 8));
                    }
                }
            }
        }

        return body;
    }

    private static IColumn VectorColumn(string element, string type, ulong[][] patterns) => element switch
    {
        "Int8" => new ArrayColumn<sbyte[]>("v", type, patterns.Select(r => r.Select(b => unchecked((sbyte)b)).ToArray()).ToArray()),
        "Float64" => new ArrayColumn<double[]>("v", type, patterns.Select(r => r.Select(BitConverter.UInt64BitsToDouble).ToArray()).ToArray()),
        _ => new ArrayColumn<float[]>("v", type, patterns.Select(r => r.Select(b => BitConverter.UInt32BitsToSingle((uint)b)).ToArray()).ToArray()),
    };

    /// <summary>Returns the patterns a decoder yields: BFloat16 keeps only the high 16 bits of each float.</summary>
    private static ulong[] Decoded(string element, ulong[] patterns)
        => element == "BFloat16" ? patterns.Select(b => b & 0xFFFF0000UL).ToArray() : patterns;

    private static ulong[] Patterns(object vector) => vector switch
    {
        sbyte[] v => v.Select(x => (ulong)unchecked((byte)x)).ToArray(),
        float[] v => v.Select(x => (ulong)BitConverter.SingleToUInt32Bits(x)).ToArray(),
        double[] v => v.Select(BitConverter.DoubleToUInt64Bits).ToArray(),
        _ => throw new ArgumentException($"Unexpected vector type {vector?.GetType()}.", nameof(vector)),
    };
}
