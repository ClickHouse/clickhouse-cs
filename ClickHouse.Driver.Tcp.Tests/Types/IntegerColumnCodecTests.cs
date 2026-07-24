using System;
using System.IO;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Numerics;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class IntegerColumnCodecTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task WriteColumn_Int32_IsLittleEndianAndContiguous()
    {
        var codec = new IntegerColumnCodec<int>("Int32");
        IColumn column = PrimitiveColumn<int>.FromValues("c", "Int32", new[] { 1, -1 });

        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, column));

        CollectionAssert.AreEqual(new byte[] { 0x01, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF }, bytes);
    }

    [Test]
    public async Task RoundTrip_UInt8() => await AssertRoundTripAsync(new IntegerColumnCodec<byte>("UInt8"), "UInt8", new byte[] { 0, 1, 128, 255 });

    [Test]
    public async Task RoundTrip_Int8() => await AssertRoundTripAsync(new IntegerColumnCodec<sbyte>("Int8"), "Int8", new sbyte[] { -128, -1, 0, 127 });

    [Test]
    public async Task RoundTrip_UInt16() => await AssertRoundTripAsync(new IntegerColumnCodec<ushort>("UInt16"), "UInt16", new ushort[] { 0, 258, ushort.MaxValue });

    [Test]
    public async Task RoundTrip_Int16() => await AssertRoundTripAsync(new IntegerColumnCodec<short>("Int16"), "Int16", new short[] { short.MinValue, -1, 0, short.MaxValue });

    [Test]
    public async Task RoundTrip_UInt32() => await AssertRoundTripAsync(new IntegerColumnCodec<uint>("UInt32"), "UInt32", new uint[] { 0, 1, uint.MaxValue });

    [Test]
    public async Task RoundTrip_Int32() => await AssertRoundTripAsync(new IntegerColumnCodec<int>("Int32"), "Int32", new[] { int.MinValue, -1, 0, int.MaxValue });

    [Test]
    public async Task RoundTrip_UInt64() => await AssertRoundTripAsync(new IntegerColumnCodec<ulong>("UInt64"), "UInt64", new ulong[] { 0, 1, ulong.MaxValue });

    [Test]
    public async Task RoundTrip_Int64() => await AssertRoundTripAsync(new IntegerColumnCodec<long>("Int64"), "Int64", new[] { long.MinValue, -1, 0, long.MaxValue });

    [Test]
    public async Task RoundTrip_UInt128() => await AssertRoundTripAsync(new IntegerColumnCodec<UInt128>("UInt128"), "UInt128", new[] { UInt128.Zero, UInt128.One, UInt128.MaxValue });

    [Test]
    public async Task RoundTrip_Int128() => await AssertRoundTripAsync(new IntegerColumnCodec<Int128>("Int128"), "Int128", new[] { Int128.MinValue, -Int128.One, Int128.Zero, Int128.MaxValue });

    [Test]
    public async Task RoundTrip_UInt256() => await AssertRoundTripAsync(
        new IntegerColumnCodec<UInt256>("UInt256"),
        "UInt256",
        new[] { UInt256.Zero, UInt256.FromBigInteger(BigInteger.One), UInt256.FromBigInteger(BigInteger.Pow(2, 200)) });

    [Test]
    public async Task RoundTrip_Int256() => await AssertRoundTripAsync(
        new IntegerColumnCodec<Int256>("Int256"),
        "Int256",
        new[] { Int256.Zero, Int256.FromBigInteger(-1), Int256.FromBigInteger(BigInteger.Pow(2, 200)), Int256.FromBigInteger(-BigInteger.Pow(2, 200)) });

    [Test]
    public async Task ReadColumn_ZeroRows_ReturnsEmptyColumnReadingNoBytes()
    {
        var codec = new IntegerColumnCodec<int>("Int32");
        using var reader = ReaderOver(Array.Empty<byte>());

        using var column = (IColumn<int>)await codec.ReadColumnAsync(reader, "c", "Int32", 0, None);

        Assert.Multiple(() =>
        {
            Assert.That(column.RowCount, Is.EqualTo(0));
            Assert.That(column.Values.Length, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task ReadColumn_StampsNameAndType()
    {
        var codec = new IntegerColumnCodec<int>("Int32");
        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, PrimitiveColumn<int>.FromValues("ignored", "Int32", new[] { 7 })));
        using var reader = ReaderOver(bytes);

        using IColumn column = await codec.ReadColumnAsync(reader, "the_name", "Int32", 1, None);

        Assert.Multiple(() =>
        {
            Assert.That(column.Name, Is.EqualTo("the_name"));
            Assert.That(column.TypeName, Is.EqualTo("Int32"));
            Assert.That(column.GetValue(0), Is.EqualTo(7));
        });
    }

    [Test]
    public void CanWrite_MatchesElementType_RejectsMismatch()
    {
        var codec = new IntegerColumnCodec<int>("Int32");

        Assert.Multiple(() =>
        {
            Assert.That(codec.CanWrite(PrimitiveColumn<int>.FromValues("c", "Int32", new[] { 1 })), Is.True);
            Assert.That(codec.CanWrite(PrimitiveColumn<long>.FromValues("c", "Int64", new[] { 1L })), Is.False);
            Assert.That(codec.CanWrite(new ArrayColumn<string>("c", "String", new[] { "x" })), Is.False);
        });
    }

    private static async Task AssertRoundTripAsync<T>(IColumnCodec codec, string type, T[] values)
        where T : unmanaged
    {
        byte[] bytes = await WriteAsync(w => codec.WriteColumn(w, PrimitiveColumn<T>.FromValues("c", type, values)));
        using var reader = ReaderOver(bytes);

        using var column = (IColumn<T>)await codec.ReadColumnAsync(reader, "c", type, values.Length, None);

        CollectionAssert.AreEqual(values, column.Values.ToArray());
    }

    private static async Task<byte[]> WriteAsync(Action<ClickHouseBinaryWriter> write)
    {
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms))
        {
            write(writer);
            await writer.FlushAsync(None);
        }

        return ms.ToArray();
    }

    private static ClickHouseBinaryReader ReaderOver(byte[] bytes) => new(new MemoryStream(bytes));
}
