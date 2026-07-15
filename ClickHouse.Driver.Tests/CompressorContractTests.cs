using System;
using System.IO;
using System.IO.Compression;
using ClickHouse.Driver.Compression;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

/// <summary>
/// Unit tests (no server) for the <see cref="IClickHouseCompressor"/> contract as implemented by the
/// built-in HTTP-only codecs. Guards both the HTTP stream path and the default-interface-method behavior
/// of the native block path (which these codecs do not support).
/// </summary>
[TestFixture]
public class CompressorContractTests
{
    private static readonly byte[] Sample = System.Text.Encoding.UTF8.GetBytes(
        "the quick brown fox jumps over the lazy dog, the quick brown fox jumps over the lazy dog");

    [Test]
    public void GZipCompressor_ContentEncoding_IsGzip()
    {
        Assert.That(GZipCompressor.Default.ContentEncoding, Is.EqualTo("gzip"));
    }

    [Test]
    public void BrotliCompressor_ContentEncoding_IsBr()
    {
        Assert.That(BrotliCompressor.Default.ContentEncoding, Is.EqualTo("br"));
    }

    [Test]
    public void GZipCompressor_Compress_ProducesGzipDecodableStream()
    {
        AssertRoundTripsThroughDecoder(GZipCompressor.Default, raw => new GZipStream(raw, CompressionMode.Decompress));
    }

    [Test]
    public void BrotliCompressor_Compress_ProducesBrotliDecodableStream()
    {
        AssertRoundTripsThroughDecoder(BrotliCompressor.Default, raw => new BrotliStream(raw, CompressionMode.Decompress));
    }

    [Test]
    public void GZipCompressor_Compress_WithLeaveOpenTrue_LeavesDestinationOpen()
    {
        using var destination = new MemoryStream();
        using (var compressing = GZipCompressor.Default.Compress(destination, leaveOpen: true))
        {
            compressing.Write(Sample, 0, Sample.Length);
        }

        // Destination must still be usable (seekable/readable) after the compressing stream is disposed.
        Assert.That(destination.CanWrite, Is.True);
        Assert.That(destination.Length, Is.GreaterThan(0));
    }

    [Test]
    public void GZipCompressor_MethodByte_ThrowsNotSupported()
    {
        Assert.Throws<NotSupportedException>(() => _ = ((IClickHouseCompressor)GZipCompressor.Default).MethodByte);
    }

    [Test]
    public void GZipCompressor_MaxEncodedLength_ThrowsNotSupported()
    {
        Assert.Throws<NotSupportedException>(() => ((IClickHouseCompressor)GZipCompressor.Default).MaxEncodedLength(64));
    }

    [Test]
    public void GZipCompressor_Encode_ThrowsNotSupported()
    {
        IClickHouseCompressor compressor = GZipCompressor.Default;
        Assert.Throws<NotSupportedException>(() =>
        {
            var target = new byte[64];
            compressor.Encode(Sample, target);
        });
    }

    [Test]
    public void GZipCompressor_Decode_ThrowsNotSupported()
    {
        IClickHouseCompressor compressor = GZipCompressor.Default;
        Assert.Throws<NotSupportedException>(() =>
        {
            var target = new byte[64];
            compressor.Decode(Sample, target);
        });
    }

    [Test]
    public void BrotliCompressor_MethodByte_ThrowsNotSupported()
    {
        Assert.Throws<NotSupportedException>(() => _ = ((IClickHouseCompressor)BrotliCompressor.Default).MethodByte);
    }

    [Test]
    public void BrotliCompressor_Encode_ThrowsNotSupported()
    {
        IClickHouseCompressor compressor = BrotliCompressor.Default;
        Assert.Throws<NotSupportedException>(() =>
        {
            var target = new byte[64];
            compressor.Encode(Sample, target);
        });
    }

    [Test]
    public void Compressor_Constructor_WithNonPositiveBufferSize_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new GZipCompressor(bufferSize: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new BrotliCompressor(bufferSize: -1));
    }

    private static void AssertRoundTripsThroughDecoder(
        IClickHouseCompressor compressor, Func<Stream, Stream> createDecoder)
    {
        using var destination = new MemoryStream();
        using (var compressing = compressor.Compress(destination, leaveOpen: true))
        {
            compressing.Write(Sample, 0, Sample.Length);
        }

        destination.Position = 0;
        using var decoder = createDecoder(destination);
        using var decompressed = new MemoryStream();
        decoder.CopyTo(decompressed);

        Assert.That(decompressed.ToArray(), Is.EqualTo(Sample));
    }
}
