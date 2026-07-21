using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class NestedColumnCodecTests
{
    private static IColumnCodec Resolve(string type) => ColumnCodecRegistry.Default.Resolve(type, default);

    // Builds a dense NestedColumn (the wire's own columnar shape) from field columns and per-row offsets.
    private static NestedColumn Nested(string typeName, string[] fieldNames, IColumn[] fields, int[] offsets)
        => new("c", typeName, fieldNames, fields, offsets, rowCount: offsets.Length - 1, pooledOffsets: false, ownsFields: false);

    private static IColumn Field<T>(string typeName, params T[] values) => new ArrayColumn<T>("c", typeName, values);

    [Test]
    public async Task ReadColumn_WriteThenRead_FixedAndStringFieldsRoundTripWithEmptyRows()
    {
        // Rows: [(1,'a'),(2,'b')], [], [(3,'c')] — three rows, the middle one empty.
        const string type = "Nested(a UInt8, b String)";
        IColumnCodec codec = Resolve(type);
        var column = Nested(
            type,
            new[] { "a", "b" },
            new[] { Field<byte>("UInt8", 1, 2, 3), Field<string>("String", "a", "b", "c") },
            new[] { 0, 2, 2, 3 });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, type, column.RowCount);
        var nested = (NestedColumn)read;

        Assert.Multiple(() =>
        {
            Assert.That(nested.TypeName, Is.EqualTo(type));
            Assert.That(nested.RowCount, Is.EqualTo(3));
            Assert.That(nested.FieldNames, Is.EqualTo(new[] { "a", "b" }));
            Assert.That(nested.Offsets.ToArray(), Is.EqualTo(new[] { 0, 2, 2, 3 }));

            // Columnar access: each field is a flat column of every row's elements concatenated.
            Assert.That(((IColumn<byte>)nested.GetField("a")).Values.ToArray(), Is.EqualTo(new byte[] { 1, 2, 3 }));
            Assert.That(((IColumn<string>)nested.GetField("b")).Values.ToArray(), Is.EqualTo(new[] { "a", "b", "c" }));

            // Row (boxed array-of-records) access.
            Assert.That(nested.GetValue(0), Is.EqualTo(new[] { new object[] { (byte)1, "a" }, new object[] { (byte)2, "b" } }));
            Assert.That(nested.GetValue(1), Is.EqualTo(Array.Empty<object[]>()));
            Assert.That(nested.GetValue(2), Is.EqualTo(new[] { new object[] { (byte)3, "c" } }));
        });
    }

    [Test]
    public async Task WriteColumn_Nested_IsByteIdenticalToArrayOfTuple()
    {
        // The flatten_nested = 0 form is byte-for-byte the same as Array(Tuple(...)); only the type-string text
        // (which keeps the field names) differs. Proven by writing the same data through both and comparing bytes.
        var nested = Nested(
            "Nested(a UInt8, b String)",
            new[] { "a", "b" },
            new[] { Field<byte>("UInt8", 10, 20, 30), Field<string>("String", "x", "y", "z") },
            new[] { 0, 2, 3 });
        var arrayOfTuple = new ArrayColumn<(byte, string)[]>("c", "Array(Tuple(a UInt8, b String))", new[]
        {
            new[] { ((byte)10, "x"), ((byte)20, "y") },
            new[] { ((byte)30, "z") },
        });

        byte[] nestedBytes = await CodecTestHarness.WriteAsync(w => Resolve("Nested(a UInt8, b String)").WriteColumn(w, nested));
        byte[] arrayBytes = await CodecTestHarness.WriteAsync(w => Resolve("Array(Tuple(a UInt8, b String))").WriteColumn(w, arrayOfTuple));

        Assert.That(nestedBytes, Is.EqualTo(arrayBytes));
    }

    [Test]
    public async Task ReadColumn_MoreThanSevenFields_RoundTrips()
    {
        // The whole point of the dedicated codec: a Nested is not bound by the tuple's 7-element cap. Eight fields.
        const string type = "Nested(a UInt8, b UInt8, c UInt8, d UInt8, e UInt8, f UInt8, g UInt8, h UInt8)";
        IColumnCodec codec = Resolve(type);
        var names = new[] { "a", "b", "c", "d", "e", "f", "g", "h" };
        var fields = new IColumn[8];
        for (int i = 0; i < 8; i++)
        {
            fields[i] = Field<byte>("UInt8", (byte)(i * 10), (byte)(i * 10 + 1), (byte)(i * 10 + 2));
        }

        var column = Nested(type, names, fields, new[] { 0, 2, 3 });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, type, column.RowCount);
        var nested = (NestedColumn)read;

        Assert.Multiple(() =>
        {
            Assert.That(nested.FieldCount, Is.EqualTo(8));
            Assert.That(nested.RowCount, Is.EqualTo(2));
            Assert.That(((IColumn<byte>)nested.GetField(0)).Values.ToArray(), Is.EqualTo(new byte[] { 0, 1, 2 }));
            Assert.That(((IColumn<byte>)nested.GetField("h")).Values.ToArray(), Is.EqualTo(new byte[] { 70, 71, 72 }));
        });
    }

    [Test]
    public async Task ReadColumn_CompositeFields_RoundTrip()
    {
        // Fields recurse through the registry, so a nullable field, an array field, and a tuple field compose.
        const string type = "Nested(a Nullable(Int32), b Array(String), c Tuple(UInt8, String))";
        IColumnCodec codec = Resolve(type);
        var column = Nested(
            type,
            new[] { "a", "b", "c" },
            new[]
            {
                Field<int?>("Nullable(Int32)", 1, null, -5),
                Field<string[]>("Array(String)", new[] { "x", "y" }, Array.Empty<string>(), new[] { "z" }),
                Field<(byte, string)>("Tuple(UInt8, String)", ((byte)1, "p"), ((byte)2, "q"), ((byte)3, "r")),
            },
            new[] { 0, 2, 3 });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, type, column.RowCount);
        var nested = (NestedColumn)read;

        Assert.Multiple(() =>
        {
            Assert.That(((IColumn<int?>)nested.GetField("a")).Values.ToArray(), Is.EqualTo(new int?[] { 1, null, -5 }));
            Assert.That(((IColumn<string[]>)nested.GetField("b")).Values.ToArray(), Is.EqualTo(new[] { new[] { "x", "y" }, Array.Empty<string>(), new[] { "z" } }));
            Assert.That(((IColumn<(byte, string)>)nested.GetField("c")).Values.ToArray(), Is.EqualTo(new[] { ((byte)1, "p"), ((byte)2, "q"), ((byte)3, "r") }));
        });
    }

    [Test]
    public async Task ReadColumn_EmptyColumn_ReadsZeroRowsWithoutConsumingBytes()
    {
        const string type = "Nested(a UInt8, b String)";
        IColumnCodec codec = Resolve(type);
        var column = Nested(type, new[] { "a", "b" }, new[] { Field<byte>("UInt8"), Field<string>("String") }, new[] { 0 });

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, column, 0, 0));
        Assert.That(bytes, Is.Empty, "an empty Nested column writes no offsets and no field streams");

        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", type, 0, CodecTestHarness.None);
        Assert.That(read.RowCount, Is.Zero);
    }

    [Test]
    public async Task WriteColumn_SlicedRange_WritesOffsetsRelativeToTheSlice()
    {
        // Writing only rows [1, 3) of a four-row column (the insert splitter's per-block path) must emit offsets
        // relative to that block's own field streams, not the full column.
        const string type = "Nested(a UInt8, b String)";
        IColumnCodec codec = Resolve(type);
        var full = Nested(
            type,
            new[] { "a", "b" },
            new[] { Field<byte>("UInt8", 1, 2, 3, 4, 5, 6), Field<string>("String", "a", "b", "c", "d", "e", "f") },
            new[] { 0, 1, 3, 3, 6 }); // rows: [a], [b,c], [], [d,e,f]

        byte[] bytes = await CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, full, start: 1, length: 2));
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        using IColumn read = await codec.ReadColumnAsync(reader, "c", type, 2, CodecTestHarness.None);
        var nested = (NestedColumn)read;

        Assert.Multiple(() =>
        {
            Assert.That(nested.Offsets.ToArray(), Is.EqualTo(new[] { 0, 2, 2 }));
            Assert.That(((IColumn<byte>)nested.GetField("a")).Values.ToArray(), Is.EqualTo(new byte[] { 2, 3 }));
            Assert.That(((IColumn<string>)nested.GetField("b")).Values.ToArray(), Is.EqualTo(new[] { "b", "c" }));
        });
    }

    [Test]
    public void MeasureRowBytes_CountsOneOffsetPlusEachFieldsElements()
    {
        const string type = "Nested(a UInt8, b String)";
        IColumnCodec codec = Resolve(type);
        var column = Nested(
            type,
            new[] { "a", "b" },
            new[] { Field<byte>("UInt8", 1, 2, 3), Field<string>("String", "x", "yy", "z") },
            new[] { 0, 2, 3 });

        Assert.Multiple(() =>
        {
            // row 0 has 2 elements: offset(8) + 2 UInt8 + ("x"=1+1) + ("yy"=1+2)
            Assert.That(codec.MeasureRowBytes(column, 0), Is.EqualTo(8 + 2 + (1 + 1) + (1 + 2)));
            // row 1 has 1 element: offset(8) + 1 UInt8 + ("z"=1+1)
            Assert.That(codec.MeasureRowBytes(column, 1), Is.EqualTo(8 + 1 + (1 + 1)));
            Assert.That(codec.FixedRowByteSize, Is.Null);
        });
    }

    [Test]
    public void CanWrite_AcceptsMatchingNestedColumnOnly()
    {
        IColumnCodec codec = Resolve("Nested(a UInt8, b String)");
        var matching = Nested("Nested(a UInt8, b String)", new[] { "a", "b" }, new[] { Field<byte>("UInt8", 1), Field<string>("String", "x") }, new[] { 0, 1 });
        var wrongFieldType = Nested("Nested(a UInt8, b UInt8)", new[] { "a", "b" }, new[] { Field<byte>("UInt8", 1), Field<byte>("UInt8", 2) }, new[] { 0, 1 });
        var wrongFieldCount = Nested("Nested(a UInt8)", new[] { "a" }, new[] { Field<byte>("UInt8", 1) }, new[] { 0, 1 });

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(matching), Is.True);
            Assert.That(codec.CanWrite(wrongFieldType), Is.False);
            Assert.That(codec.CanWrite(wrongFieldCount), Is.False);
            Assert.That(codec.CanWrite(new ArrayColumn<byte>("c", "UInt8", new byte[] { 1 })), Is.False);
        });
    }

    [Test]
    public async Task Values_MaterializesEveryRowAsArrayOfRecords()
    {
        const string type = "Nested(a UInt8, b String)";
        IColumnCodec codec = Resolve(type);
        var column = Nested(
            type,
            new[] { "a", "b" },
            new[] { Field<byte>("UInt8", 1, 2, 3), Field<string>("String", "a", "b", "c") },
            new[] { 0, 2, 2, 3 });

        using IColumn read = await CodecTestHarness.RoundTripAsync(codec, column, type, column.RowCount);

        Assert.That(((IColumn<object[][]>)read).Values.ToArray(), Is.EqualTo(new[]
        {
            new[] { new object[] { (byte)1, "a" }, new object[] { (byte)2, "b" } },
            Array.Empty<object[]>(),
            new[] { new object[] { (byte)3, "c" } },
        }));
    }

    [Test]
    public void Constructor_EmptyFields_ThrowsArgument()
        => Assert.Throws<ArgumentException>(() => new NestedColumn("c", "Nested()", Array.Empty<string>(), Array.Empty<IColumn>(), new[] { 0 }, 0, false, false));

    [Test]
    public void Constructor_FieldNameCountMismatch_ThrowsArgument()
        => Assert.Throws<ArgumentException>(() => new NestedColumn("c", "Nested(a UInt8)", new[] { "a", "b" }, new[] { Field<byte>("UInt8", 1) }, new[] { 0, 1 }, 1, false, false));

    [Test]
    public void WriteColumn_NonNestedColumn_ThrowsArgument()
    {
        IColumnCodec codec = Resolve("Nested(a UInt8, b String)");
        var wrong = new ArrayColumn<byte>("c", "UInt8", new byte[] { 1 });
        Assert.ThrowsAsync<ArgumentException>(() => CodecTestHarness.WriteAsync(w => codec.WriteColumn(w, wrong, 0, 1)));
    }

    [Test]
    public void ReadColumn_TruncatedFieldStream_ThrowsAndDisposesAlreadyReadFields()
    {
        // Valid offsets declaring one element and a full first field, but the second field's stream is truncated:
        // the read fails after field 'a' is already read, exercising the cleanup path (dispose the fields read so
        // far, then the caller returns the pooled offsets — the offsets must be returned exactly once).
        IColumnCodec codec = Resolve("Nested(a UInt8, b UInt8)");
        byte[] wire = new byte[9];
        BitConverter.TryWriteBytes(wire.AsSpan(0, 8), 1UL); // offsets[0] = 1 → one element expected per field
        wire[8] = 42;                                        // field 'a' value; field 'b' stream is missing
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(wire);

        Assert.CatchAsync(async () => await codec.ReadColumnAsync(reader, "c", "Nested(a UInt8, b UInt8)", 1, CodecTestHarness.None));
    }

    [Test]
    public void ReadColumn_OffsetBeyondInt32_ThrowsProtocol()
    {
        // An offset larger than int.MaxValue cannot be addressed by this client and is rejected up front.
        IColumnCodec codec = Resolve("Nested(a UInt8)");
        byte[] wire = new byte[8];
        BitConverter.TryWriteBytes(wire.AsSpan(0, 8), (ulong)int.MaxValue + 1);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(wire);

        Assert.ThrowsAsync<ClickHouseProtocolException>(async () =>
            await codec.ReadColumnAsync(reader, "c", "Nested(a UInt8)", 1, CodecTestHarness.None));
    }

    [Test]
    public void ElementType_IsArrayOfRecords()
        => Assert.That(Resolve("Nested(a UInt8, b String)").ElementType, Is.EqualTo(typeof(object[][])));

    [Test]
    public void NullPlaceholder_IsEmptyRow()
        => Assert.That(Resolve("Nested(a UInt8, b String)").NullPlaceholder, Is.EqualTo(Array.Empty<object[]>()));

    [Test]
    public void GetField_UnknownName_ThrowsKeyNotFound()
    {
        var column = Nested("Nested(a UInt8, b String)", new[] { "a", "b" }, new[] { Field<byte>("UInt8", 1), Field<string>("String", "x") }, new[] { 0, 1 });
        Assert.Throws<KeyNotFoundException>(() => column.GetField("nope"));
    }

    [Test]
    public void Resolve_Nested_StampsFullTypeNameWithFieldNames()
        => Assert.That(Resolve("Nested(a UInt8, b String)").TypeName, Is.EqualTo("Nested(a UInt8, b String)"));

    [TestCase("Nested")]  // no parens: zero fields reaches the codec's own guard
    [TestCase("Nested()")] // empty parens: rejected by the parser before the codec
    public void Resolve_NoFields_ThrowsFormat(string type)
        => Assert.Throws<FormatException>(() => Resolve(type));

    [Test]
    public void Resolve_UnnamedField_ThrowsFormat()
    {
        // Every Nested field must be named; an unnamed field is malformed (unlike a Tuple, where names are optional).
        Assert.Multiple(() =>
        {
            Assert.Throws<FormatException>(() => Resolve("Nested(UInt8, b String)"));
            Assert.Throws<FormatException>(() => Resolve("Nested(a UInt8, String)"));
        });
    }

    [Test]
    public void Resolve_UnsupportedField_ThrowsNotSupported()
        => Assert.Throws<NotSupportedException>(() => Resolve("Nested(a NoSuchType, b String)"));

    [Test]
    public async Task ReadColumn_NonMonotonicOffsets_ThrowsProtocol()
    {
        // Offsets must be non-decreasing; a decrease is corruption. Wire: two UInt64 offsets [2, 1].
        IColumnCodec codec = Resolve("Nested(a UInt8, b UInt8)");
        byte[] wire = new byte[16];
        BitConverter.TryWriteBytes(wire.AsSpan(0, 8), 2UL);
        BitConverter.TryWriteBytes(wire.AsSpan(8, 8), 1UL);
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(wire);

        Assert.ThrowsAsync<ClickHouseProtocolException>(async () =>
            await codec.ReadColumnAsync(reader, "c", "Nested(a UInt8, b UInt8)", 2, CodecTestHarness.None));
    }
}
