using System;
using System.IO;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Numerics;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Protocol;

[TestFixture]
public class ClickHouseBinaryReaderWriterTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    // Writes via the writer into a MemoryStream and returns the produced bytes.
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

    private static ClickHouseBinaryReader ReaderOver(byte[] bytes, int bufferSize = 16384)
        => new(new MemoryStream(bytes), bufferSize);

    [Test]
    public async Task WriteVarUInt_KnownValues_ProducesSpecBytes()
    {
        // Native-format spec: 300 encodes as AC 02.
        var bytes = await WriteAsync(w => w.WriteVarUInt(300));
        CollectionAssert.AreEqual(new byte[] { 0xAC, 0x02 }, bytes);
    }

    [TestCase(0UL)]
    [TestCase(1UL)]
    [TestCase(127UL)]
    [TestCase(128UL)]
    [TestCase(300UL)]
    [TestCase(16383UL)]
    [TestCase(16384UL)]
    [TestCase(2097151UL)]
    [TestCase(ulong.MaxValue)]
    public async Task VarUInt_RoundTrips(ulong value)
    {
        var bytes = await WriteAsync(w => w.WriteVarUInt(value));
        using var reader = ReaderOver(bytes);
        Assert.That(await reader.ReadVarUIntAsync(None), Is.EqualTo(value));
    }

    [Test]
    public async Task WriteUInt32_One_IsLittleEndian()
    {
        var bytes = await WriteAsync(w => w.WriteUInt32(1));
        CollectionAssert.AreEqual(new byte[] { 0x01, 0x00, 0x00, 0x00 }, bytes);
    }

    [Test]
    public async Task WriteInt32_MinusOne_IsAllOnes()
    {
        var bytes = await WriteAsync(w => w.WriteInt32(-1));
        CollectionAssert.AreEqual(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF }, bytes);
    }

    [Test]
    public async Task WriteUInt32_Sequence_MatchesSpecExample()
    {
        // Native-format spec example: a UInt32 column holding [1, 256, 65536].
        var bytes = await WriteAsync(w =>
        {
            w.WriteUInt32(1);
            w.WriteUInt32(256);
            w.WriteUInt32(65536);
        });
        CollectionAssert.AreEqual(
            new byte[] { 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00 },
            bytes);
    }

    [Test]
    public async Task String_Ab_MatchesSpecBytes()
    {
        var bytes = await WriteAsync(w => w.WriteString("ab"));
        CollectionAssert.AreEqual(new byte[] { 0x02, 0x61, 0x62 }, bytes);
    }

    [Test]
    public async Task String_Empty_IsSingleZeroByte()
    {
        var bytes = await WriteAsync(w => w.WriteString(string.Empty));
        CollectionAssert.AreEqual(new byte[] { 0x00 }, bytes);
        using var reader = ReaderOver(bytes);
        Assert.That(await reader.ReadStringAsync(None), Is.EqualTo(string.Empty));
    }

    [TestCase("")]
    [TestCase("ab")]
    [TestCase("héllo ☃ world")]
    public async Task String_RoundTrips(string value)
    {
        var bytes = await WriteAsync(w => w.WriteString(value));
        using var reader = ReaderOver(bytes);
        Assert.That(await reader.ReadStringAsync(None), Is.EqualTo(value));
    }

    [Test]
    public async Task StringBytes_PreservesNonUtf8AndEmbeddedNul()
    {
        byte[] raw = { 0x00, 0xFF, 0x10, 0x00 };
        var bytes = await WriteAsync(w => w.WriteString(raw));
        using var reader = ReaderOver(bytes);
        CollectionAssert.AreEqual(raw, await reader.ReadStringBytesAsync(None));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Bool_RoundTrips(bool value)
    {
        var bytes = await WriteAsync(w => w.WriteBool(value));
        Assert.That(bytes, Is.EqualTo(new[] { value ? (byte)1 : (byte)0 }));
        using var reader = ReaderOver(bytes);
        Assert.That(await reader.ReadBoolAsync(None), Is.EqualTo(value));
    }

    [Test]
    public async Task FixedWidth_RoundTripAcrossAllWidths()
    {
        var bytes = await WriteAsync(w =>
        {
            w.WriteUInt8(200);
            w.WriteInt8(-100);
            w.WriteUInt16(60000);
            w.WriteInt16(-30000);
            w.WriteUInt32(4_000_000_000);
            w.WriteInt32(-2_000_000_000);
            w.WriteUInt64(ulong.MaxValue);
            w.WriteInt64(long.MinValue);
            w.WriteFloat32(3.5f);
            w.WriteFloat64(-1.25);
        });

        using var reader = ReaderOver(bytes);
        Assert.That(await reader.ReadUInt8Async(None), Is.EqualTo(200));
        Assert.That(await reader.ReadInt8Async(None), Is.EqualTo(-100));
        Assert.That(await reader.ReadUInt16Async(None), Is.EqualTo(60000));
        Assert.That(await reader.ReadInt16Async(None), Is.EqualTo(-30000));
        Assert.That(await reader.ReadUInt32Async(None), Is.EqualTo(4_000_000_000));
        Assert.That(await reader.ReadInt32Async(None), Is.EqualTo(-2_000_000_000));
        Assert.That(await reader.ReadUInt64Async(None), Is.EqualTo(ulong.MaxValue));
        Assert.That(await reader.ReadInt64Async(None), Is.EqualTo(long.MinValue));
        Assert.That(await reader.ReadFloat32Async(None), Is.EqualTo(3.5f));
        Assert.That(await reader.ReadFloat64Async(None), Is.EqualTo(-1.25));
    }

    [Test]
    public async Task WriteUInt128_One_IsLittleEndian()
    {
        var bytes = await WriteAsync(w => w.WriteUInt128(1));
        var expected = new byte[16];
        expected[0] = 1;
        CollectionAssert.AreEqual(expected, bytes);
    }

    [Test]
    public async Task WriteInt128_MinusOne_IsAllOnes()
    {
        var bytes = await WriteAsync(w => w.WriteInt128(-1));
        var expected = new byte[16];
        Array.Fill(expected, (byte)0xFF);
        CollectionAssert.AreEqual(expected, bytes);
    }

    [Test]
    public async Task WriteUInt128_HighHalf_LandsInUpperBytes()
    {
        // 2^64 → byte 8 = 0x01, everything else 0 (proves low/high ulong ordering independently of the reader).
        var bytes = await WriteAsync(w => w.WriteUInt128((UInt128)1 << 64));
        var expected = new byte[16];
        expected[8] = 1;
        CollectionAssert.AreEqual(expected, bytes);
    }

    [Test]
    public async Task Writer_WriteBytes_LargerThanBuffer_GrowsAndPreservesData()
    {
        byte[] payload = new byte[1000];
        for (int i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)(i * 17);
        }

        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms, bufferSize: 32))
        {
            writer.WriteBytes(payload); // exceeds the 32-byte buffer, forcing growth
            await writer.FlushAsync(None);
        }

        CollectionAssert.AreEqual(payload, ms.ToArray());
    }

    [Test]
    public async Task Writer_ManySmallWrites_ExceedingBuffer_GrowAndRoundTrip()
    {
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms, bufferSize: 32))
        {
            for (ulong i = 0; i < 100; i++)
            {
                writer.WriteUInt64(i);
            }

            await writer.FlushAsync(None);
        }

        using var reader = ReaderOver(ms.ToArray());
        for (ulong i = 0; i < 100; i++)
        {
            Assert.That(await reader.ReadUInt64Async(None), Is.EqualTo(i));
        }
    }

    [Test]
    public async Task ReadStringAsync_InvalidUtf8_DecodesToReplacementCharacter()
    {
        byte[] invalid = { 0xFF, 0xFE, 0x80 };
        var bytes = await WriteAsync(w => w.WriteString(invalid));
        using var reader = ReaderOver(bytes);
        string decoded = await reader.ReadStringAsync(None);
        Assert.That(decoded, Does.Contain("�"), "invalid UTF-8 should surface as the replacement char");
    }

    [Test]
    public async Task Wide_Integers_RoundTrip()
    {
        UInt128 u128 = (UInt128)ulong.MaxValue + 12345;
        Int128 i128 = -((Int128)ulong.MaxValue + 12345);
        var u256 = UInt256.FromBigInteger((BigInteger.One << 200) + 42);
        var i256 = Int256.FromBigInteger(-((BigInteger.One << 200) + 42));

        var bytes = await WriteAsync(w =>
        {
            w.WriteUInt128(u128);
            w.WriteInt128(i128);
            w.WriteUInt256(u256);
            w.WriteInt256(i256);
        });

        using var reader = ReaderOver(bytes);
        Assert.That(await reader.ReadUInt128Async(None), Is.EqualTo(u128));
        Assert.That(await reader.ReadInt128Async(None), Is.EqualTo(i128));
        Assert.That(await reader.ReadUInt256Async(None), Is.EqualTo(u256));
        Assert.That(await reader.ReadInt256Async(None), Is.EqualTo(i256));
    }

    [Test]
    public async Task ReadBytes_SpansBufferBoundary_WithTinyBuffer()
    {
        // A 4 KB payload read through a 64-byte buffer forces many refills.
        byte[] payload = new byte[4096];
        for (int i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)(i * 31);
        }

        var bytes = await WriteAsync(w => w.WriteString(payload));
        using var reader = ReaderOver(bytes, bufferSize: 64);
        CollectionAssert.AreEqual(payload, await reader.ReadStringBytesAsync(None));
    }

    [Test]
    public void ReadPastEnd_Throws()
    {
        using var reader = ReaderOver(new byte[] { 0x01 });
        Assert.ThrowsAsync<EndOfStreamException>(async () =>
        {
            await reader.ReadUInt32Async(None);
        });
    }

    [Test]
    public void VarUInt_Overlong_Throws()
    {
        // 11 continuation bytes: never terminates within the 10-byte limit.
        byte[] overlong = new byte[11];
        for (int i = 0; i < overlong.Length; i++)
        {
            overlong[i] = 0x80;
        }

        using var reader = ReaderOver(overlong);
        Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await reader.ReadVarUIntAsync(None);
        });
    }

    [Test]
    public async Task ReadVarUIntAsync_MaximumUInt64Encoding_ReturnsMaximumValue()
    {
        byte[] maximum = { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01 };

        using var reader = ReaderOver(maximum);
        Assert.That(await reader.ReadVarUIntAsync(None), Is.EqualTo(ulong.MaxValue));
    }

    [Test]
    public async Task ReadVarUIntAsync_TenthByteRequiresRefill_ReturnsMaximumValue()
    {
        byte[] maximum = { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01 };
        var stream = new ChunkedStream(maximum, maxChunk: 9);

        using var reader = new ClickHouseBinaryReader(stream, bufferSize: 32);
        Assert.That(await reader.ReadVarUIntAsync(None), Is.EqualTo(ulong.MaxValue));
        Assert.That(stream.ReadCount, Is.EqualTo(2));
    }

    [Test]
    public async Task ReadVarUIntAsync_NonCanonicalTenByteZero_ReturnsZero()
    {
        byte[] nonCanonicalZero = { 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00 };

        using var reader = ReaderOver(nonCanonicalZero);
        Assert.That(await reader.ReadVarUIntAsync(None), Is.Zero);
    }

    [TestCase(0x02)]
    [TestCase(0x7F)]
    [TestCase(0x80)]
    [TestCase(0x81)]
    public void ReadVarUIntAsync_InvalidTenthByte_ThrowsInvalidDataException(byte tenthByte)
    {
        byte[] overflowing = { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, tenthByte };

        using var reader = ReaderOver(overflowing);
        Assert.ThrowsAsync<InvalidDataException>(async () => await reader.ReadVarUIntAsync(None));
    }

    [Test]
    public async Task ReadStringAsync_LengthExceedsMaximum_ThrowsBeforeAllocating()
    {
        // A length prefix past the cap must be rejected up front, not drive a huge allocation. The body is
        // absent on purpose: the guard has to fire before any attempt to read it.
        byte[] oversizedPrefix = await WriteAsync(w => w.WriteVarUInt((1UL << 30) + 1));

        using var reader = ReaderOver(oversizedPrefix);
        Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await reader.ReadStringAsync(None);
        });
    }

    [Test]
    public void Writer_FlushAsync_AfterDispose_ThrowsObjectDisposed()
    {
        var writer = new ClickHouseBinaryWriter(new MemoryStream());
        writer.Dispose();
        Assert.ThrowsAsync<ObjectDisposedException>(async () => await writer.FlushAsync(None));
    }

    [Test]
    public void Writer_WriteRequiringGrowth_AfterDispose_ThrowsObjectDisposed()
    {
        // After dispose the backing buffer is empty, so any write hits the grow path — which must throw rather
        // than rent a fresh buffer that Dispose would never return to the pool.
        var writer = new ClickHouseBinaryWriter(new MemoryStream());
        writer.Dispose();
        Assert.Throws<ObjectDisposedException>(() => writer.WriteByte(0x01));
    }

    [Test]
    public async Task WriteClientPacketType_EncodesCodeAsVarUInt()
    {
        // Every defined client code is below 128, so its VarUInt is a single byte equal to the numeric code.
        foreach (ClientPacketType type in Enum.GetValues<ClientPacketType>())
        {
            byte[] bytes = await WriteAsync(w => w.WriteClientPacketType(type));
            CollectionAssert.AreEqual(new[] { (byte)(ulong)type }, bytes);
        }
    }

    [Test]
    public async Task ReadServerPacketType_RoundTripsAllDefinedCodes()
    {
        foreach (ServerPacketType type in Enum.GetValues<ServerPacketType>())
        {
            byte[] bytes = await WriteAsync(w => w.WriteVarUInt((ulong)type));
            using var reader = ReaderOver(bytes);
            Assert.That(await reader.ReadServerPacketTypeAsync(None), Is.EqualTo(type));
        }
    }

    [Test]
    public async Task PacketTypeCodes_OverlapAcrossDirections_CarryDifferentMeaning()
    {
        // Code 2 is client Data but server Exception: the same wire byte read as each direction's enum.
        byte[] bytes = await WriteAsync(w => w.WriteClientPacketType(ClientPacketType.Data));
        Assert.That((ulong)ClientPacketType.Data, Is.EqualTo(2UL));

        using var reader = ReaderOver(bytes);
        Assert.That(await reader.ReadServerPacketTypeAsync(None), Is.EqualTo(ServerPacketType.Exception));
    }

    [Test]
    public async Task ReadServerPacketType_UnknownMultiByteCode_PreservesExactValue()
    {
        // The reader does not validate the range (that is the dispatcher's job): a code outside the defined set
        // is returned verbatim, and the ulong backing avoids any narrowing so the true value survives. 300 also
        // exercises multi-byte VarUInt decoding of the type code.
        byte[] bytes = await WriteAsync(w => w.WriteVarUInt(300));
        using var reader = ReaderOver(bytes);

        ServerPacketType type = await reader.ReadServerPacketTypeAsync(None);
        Assert.That((ulong)type, Is.EqualTo(300UL));
        Assert.That(Enum.IsDefined(type), Is.False);
    }

    [Test]
    public async Task ServerPacketEnvelope_TypeThenBody_RoundTrips()
    {
        // Envelope = [VarUInt type][body]. Simulate a server Data packet with a UInt32 body field and read both
        // back in order, confirming the type code and body compose on the same buffered stream.
        byte[] bytes = await WriteAsync(w =>
        {
            w.WriteVarUInt((ulong)ServerPacketType.Data);
            w.WriteUInt32(0xDEADBEEF);
        });
        using var reader = ReaderOver(bytes);

        Assert.That(await reader.ReadServerPacketTypeAsync(None), Is.EqualTo(ServerPacketType.Data));
        Assert.That(await reader.ReadUInt32Async(None), Is.EqualTo(0xDEADBEEF));
    }
}
