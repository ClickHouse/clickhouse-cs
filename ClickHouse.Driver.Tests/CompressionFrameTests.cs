using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ClickHouse.Driver.Compression;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

/// <summary>
/// Unit tests for <see cref="CompressionFrame"/>: the exact wire bytes of a frame, and the error paths a
/// round trip cannot reach. The two spans a frame declares are easy to confuse, so they are asserted
/// separately — the checksum covers the header plus the body, while <c>compressed_size</c> counts the header
/// plus the body and excludes the checksum.
/// </summary>
[TestFixture]
public class CompressionFrameTests
{
    private static IEnumerable<TestCaseData> BlockCodecs()
    {
        yield return new TestCaseData(Lz4Compressor.Default, CompressionFrame.MethodLz4).SetName("{m}(LZ4)");
        yield return new TestCaseData(ZstdCompressor.Default, CompressionFrame.MethodZstd).SetName("{m}(ZSTD)");
    }

    [TestCaseSource(nameof(BlockCodecs))]
    public void Write_ABlockCodec_EmitsTheDocumentedHeader(IClickHouseCompressor codec, byte expectedMethod)
    {
        byte[] plaintext = Payload(500);
        var destination = new byte[CompressionFrame.MaxFrameSize(codec, plaintext.Length)];

        int total = CompressionFrame.Write(plaintext, codec, destination);

        int compressedSize = BinaryPrimitives.ReadInt32LittleEndian(destination.AsSpan(17, 4));
        Assert.Multiple(() =>
        {
            Assert.That(destination[16], Is.EqualTo(expectedMethod), "method byte");
            Assert.That(compressedSize, Is.EqualTo(total - CompressionFrame.ChecksumSize), "compressed_size counts the header but not the checksum");
            Assert.That(BinaryPrimitives.ReadInt32LittleEndian(destination.AsSpan(21, 4)), Is.EqualTo(plaintext.Length), "uncompressed_size");
            Assert.That(compressedSize, Is.GreaterThanOrEqualTo(CompressionFrame.HeaderSize), "compressed_size covers its own header");
        });
    }

    [TestCaseSource(nameof(BlockCodecs))]
    public void Write_ABlockCodec_ChecksumsTheHeaderAndBodyButNotItself(IClickHouseCompressor codec, byte expectedMethod)
    {
        _ = expectedMethod;
        byte[] plaintext = Payload(500);
        var destination = new byte[CompressionFrame.MaxFrameSize(codec, plaintext.Length)];

        int total = CompressionFrame.Write(plaintext, codec, destination);

        // Recompute over exactly the span the format specifies: from the method byte to the end of the body.
        var (low, high) = CityHash102.Hash128(destination.AsSpan(CompressionFrame.ChecksumSize, total - CompressionFrame.ChecksumSize));
        Assert.Multiple(() =>
        {
            Assert.That(BinaryPrimitives.ReadUInt64LittleEndian(destination.AsSpan(0, 8)), Is.EqualTo(low), "low half, little-endian first");
            Assert.That(BinaryPrimitives.ReadUInt64LittleEndian(destination.AsSpan(8, 8)), Is.EqualTo(high), "high half");
        });
    }

    [TestCaseSource(nameof(BlockCodecs))]
    public void WriteThenDecode_ABlockCodec_RecoversThePlaintext(IClickHouseCompressor codec, byte expectedMethod)
    {
        byte[] plaintext = Payload(4096);
        var destination = new byte[CompressionFrame.MaxFrameSize(codec, plaintext.Length)];
        int total = CompressionFrame.Write(plaintext, codec, destination);

        CompressionFrame.ReadHeader(
            destination.AsSpan(CompressionFrame.ChecksumSize, CompressionFrame.HeaderSize),
            out byte method,
            out int bodySize,
            out int plaintextSize);
        CompressionFrame.VerifyChecksum(destination.AsSpan(0, CompressionFrame.ChecksumSize), destination.AsSpan(CompressionFrame.ChecksumSize, total - CompressionFrame.ChecksumSize));
        var decoded = new byte[plaintextSize];
        CompressionFrame.Decode(method, destination.AsSpan(CompressionFrame.PrefixSize, bodySize), decoded);

        Assert.Multiple(() =>
        {
            Assert.That(method, Is.EqualTo(expectedMethod));
            Assert.That(plaintextSize, Is.EqualTo(plaintext.Length));
            Assert.That(decoded, Is.EqualTo(plaintext));
        });
    }

    [Test]
    public void Write_EmptyPlaintext_StillEmitsAFullPrefix()
    {
        var destination = new byte[CompressionFrame.MaxFrameSize(Lz4Compressor.Default, 0)];

        int total = CompressionFrame.Write(ReadOnlySpan<byte>.Empty, Lz4Compressor.Default, destination);

        Assert.Multiple(() =>
        {
            Assert.That(total, Is.GreaterThanOrEqualTo(CompressionFrame.PrefixSize));
            Assert.That(BinaryPrimitives.ReadInt32LittleEndian(destination.AsSpan(21, 4)), Is.Zero, "uncompressed_size");
        });
    }

    [Test]
    public void Write_DestinationBelowTheCodecBound_Throws()
    {
        byte[] plaintext = Payload(100);
        var tooSmall = new byte[CompressionFrame.PrefixSize];

        Assert.That(
            () => CompressionFrame.Write(plaintext, Lz4Compressor.Default, tooSmall),
            Throws.ArgumentException.With.Message.Contains("Destination holds"));
    }

    [Test]
    public void MaxFrameSize_IsThePrefixPlusTheCodecsBound()
    {
        int bound = CompressionFrame.MaxFrameSize(Lz4Compressor.Default, 1000);

        Assert.That(bound, Is.EqualTo(CompressionFrame.PrefixSize + Lz4Compressor.Default.MaxEncodedLength(1000)));
    }

    [Test]
    public void ReadHeader_CompressedSizeBelowTheHeaderItCounts_Throws()
    {
        var header = new byte[CompressionFrame.HeaderSize];
        header[0] = CompressionFrame.MethodLz4;
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(1, 4), CompressionFrame.HeaderSize - 1);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(5, 4), 10);

        Assert.That(
            () => CompressionFrame.ReadHeader(header, out _, out _, out _),
            Throws.TypeOf<InvalidDataException>().With.Message.Contains("does not cover its own"));
    }

    [Test]
    public void ReadHeader_NegativeUncompressedSize_Throws()
    {
        var header = new byte[CompressionFrame.HeaderSize];
        header[0] = CompressionFrame.MethodLz4;
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(1, 4), CompressionFrame.HeaderSize + 4);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(5, 4), -1);

        Assert.That(
            () => CompressionFrame.ReadHeader(header, out _, out _, out _),
            Throws.TypeOf<InvalidDataException>().With.Message.Contains("outside the supported range"));
    }

    [Test]
    public void ReadHeader_ImplausibleBodySize_Throws()
    {
        var header = new byte[CompressionFrame.HeaderSize];
        header[0] = CompressionFrame.MethodLz4;
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(1, 4), int.MaxValue);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(5, 4), 10);

        Assert.That(
            () => CompressionFrame.ReadHeader(header, out _, out _, out _),
            Throws.TypeOf<InvalidDataException>().With.Message.Contains("above the"));
    }

    [Test]
    public void ReadHeader_WrongLength_Throws()
    {
        Assert.That(
            () => CompressionFrame.ReadHeader(new byte[CompressionFrame.HeaderSize - 1], out _, out _, out _),
            Throws.ArgumentException);
    }

    [Test]
    public void VerifyChecksum_ABodyBitFlippedInTransit_Throws()
    {
        byte[] plaintext = Payload(500);
        var frame = new byte[CompressionFrame.MaxFrameSize(Lz4Compressor.Default, plaintext.Length)];
        int total = CompressionFrame.Write(plaintext, Lz4Compressor.Default, frame);
        frame[CompressionFrame.PrefixSize + 3] ^= 0x01;

        Assert.That(
            () => CompressionFrame.VerifyChecksum(frame.AsSpan(0, CompressionFrame.ChecksumSize), frame.AsSpan(CompressionFrame.ChecksumSize, total - CompressionFrame.ChecksumSize)),
            Throws.TypeOf<InvalidDataException>().With.Message.Contains("checksum mismatch"));
    }

    [Test]
    public void VerifyChecksum_AHeaderBitFlippedInTransit_Throws()
    {
        byte[] plaintext = Payload(500);
        var frame = new byte[CompressionFrame.MaxFrameSize(Lz4Compressor.Default, plaintext.Length)];
        int total = CompressionFrame.Write(plaintext, Lz4Compressor.Default, frame);

        // The uncompressed_size field, which the checksum covers even though it is not body content.
        frame[21] ^= 0x02;

        Assert.That(
            () => CompressionFrame.VerifyChecksum(frame.AsSpan(0, CompressionFrame.ChecksumSize), frame.AsSpan(CompressionFrame.ChecksumSize, total - CompressionFrame.ChecksumSize)),
            Throws.TypeOf<InvalidDataException>().With.Message.Contains("checksum mismatch"));
    }

    [Test]
    public void Decode_TheNoneMethod_CopiesTheBodyVerbatim()
    {
        byte[] body = Payload(64);
        var plaintext = new byte[body.Length];

        CompressionFrame.Decode(CompressionFrame.MethodNone, body, plaintext);

        Assert.That(plaintext, Is.EqualTo(body));
    }

    [Test]
    public void Decode_TheNoneMethodWithABodyThatDisagreesWithTheDeclaredSize_Throws()
    {
        Assert.That(
            () => CompressionFrame.Decode(CompressionFrame.MethodNone, Payload(64), new byte[63]),
            Throws.TypeOf<InvalidDataException>().With.Message.Contains("carries a 64-byte body"));
    }

    [Test]
    public void Decode_AMethodByteThisClientCannotDecode_NamesTheSupportedOnes()
    {
        Assert.That(
            () => CompressionFrame.Decode(0x99, Payload(8), new byte[8]),
            Throws.TypeOf<InvalidDataException>().With.Message.Contains("0x99").And.Message.Contains("0x82 LZ4"));
    }

    [Test]
    public void ResolveCodec_TheKnownMethodBytes_ReturnTheMatchingCodec()
    {
        Assert.Multiple(() =>
        {
            Assert.That(CompressionFrame.ResolveCodec(CompressionFrame.MethodLz4).MethodByte, Is.EqualTo(CompressionFrame.MethodLz4));
            Assert.That(CompressionFrame.ResolveCodec(CompressionFrame.MethodZstd).MethodByte, Is.EqualTo(CompressionFrame.MethodZstd));
        });
    }

    [Test]
    public void Decode_AFrameWhoseBodyDecodesShort_Throws()
    {
        // A body encoded from 100 bytes, then decoded as if it held 200: the codec stops early and the
        // declared size is the only thing that catches it.
        byte[] plaintext = Payload(100);
        var frame = new byte[CompressionFrame.MaxFrameSize(Lz4Compressor.Default, plaintext.Length)];
        int total = CompressionFrame.Write(plaintext, Lz4Compressor.Default, frame);
        int bodySize = total - CompressionFrame.PrefixSize;

        Assert.That(
            () => CompressionFrame.Decode(CompressionFrame.MethodLz4, frame.AsSpan(CompressionFrame.PrefixSize, bodySize), new byte[200]),
            Throws.TypeOf<InvalidDataException>());
    }

    /// <summary>Compressible but not trivially so, which keeps the encoded body a realistic size.</summary>
    private static byte[] Payload(int length)
    {
        var text = new StringBuilder(length + 16);
        while (text.Length < length)
        {
            text.Append("the quick brown fox jumps over the lazy dog 0123456789 ");
        }

        return Encoding.UTF8.GetBytes(text.ToString(0, length));
    }
}
