using System;
using System.Collections.Generic;
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

    // ---------------------------------------------------------------------------------------------
    // Decompress: the HTTP response-body counterpart to Compress.
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// Every built-in codec, so a codec that forgets to override <c>Decompress</c> fails the whole set.
    /// </summary>
    private static IEnumerable<TestCaseData> BuiltInCompressors()
    {
        yield return new TestCaseData(GZipCompressor.Default).SetName("{m}(gzip)");
        yield return new TestCaseData(BrotliCompressor.Default).SetName("{m}(br)");
        yield return new TestCaseData(Lz4Compressor.Default).SetName("{m}(lz4)");
    }

    [TestCaseSource(nameof(BuiltInCompressors))]
    public void Decompress_WithOwnCompressOutput_RoundTripsToOriginalBytes(IClickHouseCompressor compressor)
    {
        using var destination = new MemoryStream();
        using (var compressing = compressor.Compress(destination, leaveOpen: true))
        {
            compressing.Write(Sample, 0, Sample.Length);
        }

        // The compressed form must not accidentally be the plaintext, or the round-trip proves nothing.
        Assert.That(destination.ToArray(), Is.Not.EqualTo(Sample));

        destination.Position = 0;
        using var decompressing = compressor.Decompress(destination, leaveOpen: true);
        using var plaintext = new MemoryStream();
        decompressing.CopyTo(plaintext);

        Assert.That(plaintext.ToArray(), Is.EqualTo(Sample));
    }

    /// <summary>
    /// A verbatim <c>Content-Encoding: lz4</c> response body captured from a ClickHouse server
    /// (<c>SELECT 'the quick brown fox jumps over the lazy dog' FORMAT TSVRaw</c>). It starts with the LZ4
    /// frame magic <c>04 22 4D 18</c>. Pinning real server bytes is what makes this a genuine
    /// cross-implementation check: the encoder is ClickHouse itself, not our vendored codec. There is no
    /// equivalent for gzip/br — those decoders are the BCL's own, so encoding with
    /// <see cref="GZipStream"/>/<see cref="BrotliStream"/> would only test the BCL against itself.
    /// </summary>
    private const string ServerProducedLz4Frame =
        "BCJNGEBQdywAAIB0aGUgcXVpY2sgYnJvd24gZm94IGp1bXBzIG92ZXIgdGhlIGxhenkgZG9nCgAAAAA=";

    [Test]
    public void Decompress_WithServerProducedLz4Frame_DecodesToOriginalBytes()
    {
        var encoded = Convert.FromBase64String(ServerProducedLz4Frame);
        var expected = System.Text.Encoding.UTF8.GetBytes("the quick brown fox jumps over the lazy dog\n");

        Assert.That(encoded[..4], Is.EqualTo(new byte[] { 0x04, 0x22, 0x4D, 0x18 }), "expected an LZ4 frame magic");
        AssertDecodes(Lz4Compressor.Default, encoded, expected);
    }

    [TestCaseSource(nameof(BuiltInCompressors))]
    public void Decompress_WithLeaveOpenTrue_LeavesSourceOpen(IClickHouseCompressor compressor)
    {
        using var source = new DisposeTrackingStream(Compress(compressor, Sample));

        using (var decompressing = compressor.Decompress(source, leaveOpen: true))
        {
            decompressing.CopyTo(Stream.Null);
        }

        Assert.Multiple(() =>
        {
            Assert.That(source.IsDisposed, Is.False);
            Assert.That(source.CanRead, Is.True);
        });
    }

    [TestCaseSource(nameof(BuiltInCompressors))]
    public void Decompress_WithLeaveOpenFalse_DisposesSource(IClickHouseCompressor compressor)
    {
        var source = new DisposeTrackingStream(Compress(compressor, Sample));

        using (var decompressing = compressor.Decompress(source, leaveOpen: false))
        {
            decompressing.CopyTo(Stream.Null);
        }

        Assert.That(source.IsDisposed, Is.True);
    }

    [Test]
    public void Decompress_OnCompressorWithoutOverride_ThrowsNotSupportedException()
    {
        IClickHouseCompressor compressor = new HttpOnlyCompressorWithoutDecompress();

        var ex = Assert.Throws<NotSupportedException>(() => compressor.Decompress(Stream.Null, leaveOpen: false));

        Assert.That(ex.Message, Does.Contain(nameof(HttpOnlyCompressorWithoutDecompress)));
    }

    private static byte[] Compress(IClickHouseCompressor compressor, byte[] plaintext)
    {
        using var destination = new MemoryStream();
        using (var compressing = compressor.Compress(destination, leaveOpen: true))
        {
            compressing.Write(plaintext, 0, plaintext.Length);
        }

        return destination.ToArray();
    }

    private static void AssertDecodes(IClickHouseCompressor compressor, byte[] encoded, byte[] expected)
    {
        using var source = new MemoryStream(encoded);
        using var decompressing = compressor.Decompress(source, leaveOpen: true);
        using var plaintext = new MemoryStream();
        decompressing.CopyTo(plaintext);

        Assert.That(plaintext.ToArray(), Is.EqualTo(expected));
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

    /// <summary>
    /// A codec that implements only the mandatory HTTP request-body members, so the default-interface
    /// implementations of everything else (including <c>Decompress</c>) apply.
    /// </summary>
    private sealed class HttpOnlyCompressorWithoutDecompress : IClickHouseCompressor
    {
        public string ContentEncoding => "x-custom";

        public Stream Compress(Stream destination, bool leaveOpen) => destination;
    }

    private sealed class DisposeTrackingStream : MemoryStream
    {
        public DisposeTrackingStream(byte[] buffer)
            : base(buffer, writable: false)
        {
        }

        public bool IsDisposed { get; private set; }

        protected override void Dispose(bool disposing)
        {
            IsDisposed = true;
            base.Dispose(disposing);
        }
    }
}
