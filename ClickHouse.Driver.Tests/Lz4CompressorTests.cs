using System;
using System.IO;
using System.Text;
using ClickHouse.Driver.Compression;
using K4os.Compression.LZ4;
using K4os.Compression.LZ4.Streams;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

/// <summary>
/// Unit tests (no server) for <see cref="Lz4Compressor"/> from the opt-in <c>ClickHouse.Driver.Lz4</c>
/// package. Uses the underlying K4os library directly as a differential oracle: what our codec encodes
/// must decode with K4os and vice versa, guarding format compatibility on both the HTTP frame path and
/// the native block path.
/// </summary>
[TestFixture]
public class Lz4CompressorTests
{
    private static readonly byte[] Sample = Encoding.UTF8.GetBytes(string.Concat(
        System.Linq.Enumerable.Repeat(
            "event=purchase;status=ok;region=us-east-1;user_id=42;session=deadbeef ", 200)));

    [Test]
    public void Lz4Compressor_ContentEncoding_IsLz4()
    {
        Assert.That(Lz4Compressor.Default.ContentEncoding, Is.EqualTo("lz4"));
    }

    [Test]
    public void Lz4Compressor_MethodByte_IsLz4NativeByte()
    {
        Assert.That(((IClickHouseCompressor)Lz4Compressor.Default).MethodByte, Is.EqualTo((byte)0x82));
        Assert.That(Lz4Compressor.Lz4MethodByte, Is.EqualTo((byte)0x82));
    }

    [Test]
    public void Lz4Compressor_MaxEncodedLength_MatchesK4osCodec()
    {
        Assert.That(Lz4Compressor.Default.MaxEncodedLength(Sample.Length),
            Is.EqualTo(LZ4Codec.MaximumOutputSize(Sample.Length)));
    }

    [Test]
    public void Lz4Compressor_EncodeThenDecode_RoundTripsData()
    {
        var compressor = Lz4Compressor.Default;
        var encoded = new byte[compressor.MaxEncodedLength(Sample.Length)];
        var encodedLength = compressor.Encode(Sample, encoded);
        Assert.That(encodedLength, Is.GreaterThan(0));

        var decoded = new byte[Sample.Length];
        var decodedLength = compressor.Decode(encoded.AsSpan(0, encodedLength), decoded);

        Assert.That(decodedLength, Is.EqualTo(Sample.Length));
        Assert.That(decoded, Is.EqualTo(Sample));
    }

    [Test]
    public void Lz4Compressor_Encode_ProducesK4osDecodableBlock()
    {
        var compressor = Lz4Compressor.Default;
        var encoded = new byte[compressor.MaxEncodedLength(Sample.Length)];
        var encodedLength = compressor.Encode(Sample, encoded);

        // Oracle: K4os decodes what our codec encoded.
        var decoded = new byte[Sample.Length];
        var decodedLength = LZ4Codec.Decode(encoded.AsSpan(0, encodedLength), decoded);

        Assert.That(decodedLength, Is.EqualTo(Sample.Length));
        Assert.That(decoded, Is.EqualTo(Sample));
    }

    [Test]
    public void Lz4Compressor_Decode_OfK4osEncodedBlock_MatchesOriginal()
    {
        // Oracle: our codec decodes what K4os encoded.
        var encoded = new byte[LZ4Codec.MaximumOutputSize(Sample.Length)];
        var encodedLength = LZ4Codec.Encode(Sample, encoded, LZ4Level.L00_FAST);

        var decoded = new byte[Sample.Length];
        var decodedLength = Lz4Compressor.Default.Decode(encoded.AsSpan(0, encodedLength), decoded);

        Assert.That(decodedLength, Is.EqualTo(Sample.Length));
        Assert.That(decoded, Is.EqualTo(Sample));
    }

    [Test]
    public void Lz4Compressor_Encode_EmptySource_ReturnsZero()
    {
        Assert.That(Lz4Compressor.Default.Encode(ReadOnlySpan<byte>.Empty, new byte[16]), Is.EqualTo(0));
    }

    [Test]
    public void Lz4Compressor_Compress_ProducesLz4FrameDecodableStream()
    {
        using var destination = new MemoryStream();
        using (var compressing = Lz4Compressor.Default.Compress(destination, leaveOpen: true))
        {
            compressing.Write(Sample, 0, Sample.Length);
        }

        Assert.That(destination.Length, Is.GreaterThan(0));

        // Oracle: the HTTP frame path must produce a standard LZ4 frame that K4os's frame decoder reads.
        destination.Position = 0;
        using var decoder = LZ4Stream.Decode(destination, leaveOpen: true);
        using var decompressed = new MemoryStream();
        decoder.CopyTo(decompressed);

        Assert.That(decompressed.ToArray(), Is.EqualTo(Sample));
    }

    [Test]
    public void Lz4Compressor_Constructor_WithNonPositiveBufferSize_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new Lz4Compressor(bufferSize: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new Lz4Compressor(3, bufferSize: -1));
    }

    [Test]
    public void Lz4Compressor_Constructor_WithInvalidLevel_Throws()
    {
        // Valid LZ4 levels are 0 (fast) and 3-12 (HC); 1, 2 and >12 are not LZ4Level values.
        Assert.Throws<ArgumentOutOfRangeException>(() => new Lz4Compressor(1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new Lz4Compressor(2));
        Assert.Throws<ArgumentOutOfRangeException>(() => new Lz4Compressor(13));
        Assert.Throws<ArgumentOutOfRangeException>(() => new Lz4Compressor(-1));
    }

    [Test]
    public void Lz4Compressor_Constructor_WithValidHcLevel_Works()
    {
        Assert.That(new Lz4Compressor(3).ContentEncoding, Is.EqualTo("lz4"));
        Assert.That(new Lz4Compressor(12).ContentEncoding, Is.EqualTo("lz4"));
    }
}
