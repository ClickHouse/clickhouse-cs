using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.Compression;
using NUnit.Framework;
using ZstdSharp;

namespace ClickHouse.Driver.Tests;

/// <summary>
/// Unit tests (no server) for the built-in <see cref="ZstdCompressor"/>, whose codec is a vendored copy
/// of ZstdSharp (see <c>ClickHouse.Driver.Common/Vendor/ZstdSharp</c>). Uses the real upstream
/// <c>ZstdSharp.Port</c> package (a test-only dependency) directly as a differential oracle: what our
/// vendored codec encodes must decode with upstream ZstdSharp and vice versa, guarding format
/// compatibility on both the HTTP frame path and the native block path.
/// </summary>
[TestFixture]
public class ZstdCompressorTests
{
    /// <summary>The zstd frame magic every frame starts with (RFC 8878 §3.1.1).</summary>
    private static readonly byte[] ZstdFrameMagic = [0x28, 0xB5, 0x2F, 0xFD];

    private static readonly byte[] Sample = Encoding.UTF8.GetBytes(string.Concat(
        System.Linq.Enumerable.Repeat(
            "event=purchase;status=ok;region=us-east-1;user_id=42;session=deadbeef ", 200)));

    /// <summary>
    /// The oracle is only an oracle if it is a different implementation. <c>using ZstdSharp;</c> binds to
    /// the real package because the vendored copy lives under <c>ClickHouse.Driver.Vendor.ZstdSharp</c> —
    /// but the vendored types are also visible here (Common declares <c>InternalsVisibleTo</c> for this
    /// assembly), so pin it: the oracle must come from the package, at the version the vendored copy was
    /// derived from.
    /// </summary>
    [Test]
    public void Oracle_IsTheRealZstdSharpPackage_AtTheVendoredVersion()
    {
        var oracle = typeof(Compressor).Assembly;

        Assert.Multiple(() =>
        {
            Assert.That(oracle.GetName().Name, Is.EqualTo("ZstdSharp"));
            Assert.That(oracle, Is.Not.SameAs(typeof(ZstdCompressor).Assembly), "that would compare the vendored copy with itself");
            Assert.That(oracle.GetName().Version, Is.EqualTo(new Version(0, 8, 8, 0)));
        });
    }

    [Test]
    public void ZstdCompressor_ContentEncoding_IsZstd()
    {
        Assert.That(ZstdCompressor.Default.ContentEncoding, Is.EqualTo("zstd"));
    }

    [Test]
    public void ZstdCompressor_MethodByte_IsZstdNativeByte()
    {
        Assert.That(((IClickHouseCompressor)ZstdCompressor.Default).MethodByte, Is.EqualTo((byte)0x90));
        Assert.That(ZstdCompressor.ZstdMethodByte, Is.EqualTo((byte)0x90));
    }

    [Test]
    public void ZstdCompressor_MaxEncodedLength_MatchesZstdCompressBound()
    {
        Assert.That(ZstdCompressor.Default.MaxEncodedLength(Sample.Length),
            Is.EqualTo(Compressor.GetCompressBound(Sample.Length)));
    }

    [Test]
    public void ZstdCompressor_DefaultLevel_IsTheCodecsOwnDefault()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ZstdCompressor.DefaultLevel, Is.EqualTo(Compressor.DefaultCompressionLevel));
            Assert.That(ZstdCompressor.MinLevel, Is.EqualTo(Compressor.MinCompressionLevel));
            Assert.That(ZstdCompressor.MaxLevel, Is.EqualTo(Compressor.MaxCompressionLevel));
        });
    }

    // ---------------------------------------------------------------------------------------------
    // Native block path: a ClickHouse ZSTD block is one complete zstd frame.
    // ---------------------------------------------------------------------------------------------

    [Test]
    public void ZstdCompressor_EncodeThenDecode_RoundTripsData()
    {
        var compressor = ZstdCompressor.Default;
        var encoded = new byte[compressor.MaxEncodedLength(Sample.Length)];
        var encodedLength = compressor.Encode(Sample, encoded);
        Assert.That(encodedLength, Is.GreaterThan(0));

        var decoded = new byte[Sample.Length];
        var decodedLength = compressor.Decode(encoded.AsSpan(0, encodedLength), decoded);

        Assert.That(decodedLength, Is.EqualTo(Sample.Length));
        Assert.That(decoded, Is.EqualTo(Sample));
    }

    /// <summary>
    /// ClickHouse's native ZSTD block is a self-contained zstd frame (it compresses each block with
    /// <c>ZSTD_compress</c> and decompresses with <c>ZSTD_decompress</c>), so <see cref="ZstdCompressor.Encode"/>
    /// must emit the frame magic and a decodable content size rather than a raw block.
    /// </summary>
    [Test]
    public void ZstdCompressor_Encode_ProducesACompleteZstdFrame()
    {
        var compressor = ZstdCompressor.Default;
        var encoded = new byte[compressor.MaxEncodedLength(Sample.Length)];
        var encodedLength = compressor.Encode(Sample, encoded);

        Assert.That(encoded[..4], Is.EqualTo(ZstdFrameMagic), "expected a zstd frame magic");

        // The frame header carries the uncompressed size, which is what lets a reader size its target.
        Assert.That(Decompressor.GetDecompressedSize(encoded.AsSpan(0, encodedLength)),
            Is.EqualTo((ulong)Sample.Length));
    }

    [Test]
    public void ZstdCompressor_Encode_ProducesUpstreamDecodableBlock()
    {
        var compressor = ZstdCompressor.Default;
        var encoded = new byte[compressor.MaxEncodedLength(Sample.Length)];
        var encodedLength = compressor.Encode(Sample, encoded);

        // Oracle: upstream ZstdSharp decodes what our vendored codec encoded.
        using var upstream = new Decompressor();
        var decoded = new byte[Sample.Length];
        var decodedLength = upstream.Unwrap(encoded.AsSpan(0, encodedLength), decoded);

        Assert.That(decodedLength, Is.EqualTo(Sample.Length));
        Assert.That(decoded, Is.EqualTo(Sample));
    }

    [Test]
    public void ZstdCompressor_Decode_OfUpstreamEncodedBlock_MatchesOriginal()
    {
        // Oracle: our vendored codec decodes what upstream ZstdSharp encoded.
        using var upstream = new Compressor(ZstdCompressor.DefaultLevel);
        var encoded = new byte[Compressor.GetCompressBound(Sample.Length)];
        var encodedLength = upstream.Wrap(Sample, encoded);

        var decoded = new byte[Sample.Length];
        var decodedLength = ZstdCompressor.Default.Decode(encoded.AsSpan(0, encodedLength), decoded);

        Assert.That(decodedLength, Is.EqualTo(Sample.Length));
        Assert.That(decoded, Is.EqualTo(Sample));
    }

    /// <summary>
    /// Byte-for-byte agreement with upstream at the same level, which is stricter than "both decode":
    /// it would catch a vendored copy that silently changed a codec parameter default.
    /// </summary>
    [TestCase(-5)]
    [TestCase(1)]
    [TestCase(ZstdCompressor.DefaultLevel)]
    [TestCase(9)]
    [TestCase(19)]
    public void ZstdCompressor_Encode_AtEveryLevel_IsByteIdenticalToUpstream(int level)
    {
        var compressor = new ZstdCompressor(level);
        var ours = new byte[compressor.MaxEncodedLength(Sample.Length)];
        var oursLength = compressor.Encode(Sample, ours);

        using var upstream = new Compressor(level);
        var theirs = new byte[Compressor.GetCompressBound(Sample.Length)];
        var theirsLength = upstream.Wrap(Sample, theirs);

        Assert.That(ours[..oursLength], Is.EqualTo(theirs[..theirsLength]));
    }

    [Test]
    public void ZstdCompressor_Encode_EmptySource_ReturnsZero()
    {
        Assert.That(ZstdCompressor.Default.Encode(ReadOnlySpan<byte>.Empty, new byte[16]), Is.EqualTo(0));
    }

    [Test]
    public void ZstdCompressor_Decode_EmptySource_ReturnsZero()
    {
        Assert.That(ZstdCompressor.Default.Decode(ReadOnlySpan<byte>.Empty, new byte[16]), Is.EqualTo(0));
    }

    [Test]
    public void ZstdCompressor_Encode_TargetTooSmall_Throws()
    {
        // A one-byte target cannot hold the encoded frame, so the codec reports failure.
        var ex = Assert.Throws<InvalidOperationException>(() => ZstdCompressor.Default.Encode(Sample, new byte[1]));

        Assert.That(ex.Message, Does.Contain("MaxEncodedLength"));
    }

    [Test]
    public void ZstdCompressor_Decode_TargetTooSmall_Throws()
    {
        var compressor = ZstdCompressor.Default;
        var encoded = new byte[compressor.MaxEncodedLength(Sample.Length)];
        var encodedLength = compressor.Encode(Sample, encoded);

        // Decoding a valid frame into a target smaller than the original length fails.
        Assert.Throws<InvalidOperationException>(
            () => compressor.Decode(encoded.AsSpan(0, encodedLength), new byte[1]));
    }

    /// <summary>
    /// A corrupt block must surface as the driver's own exception type rather than a vendored one the
    /// caller cannot name.
    /// </summary>
    [Test]
    public void ZstdCompressor_Decode_CorruptSource_ThrowsInvalidOperationException()
    {
        var garbage = new byte[64];
        garbage[0] = 0x28; // start of the frame magic, then nonsense

        Assert.Throws<InvalidOperationException>(() => ZstdCompressor.Default.Decode(garbage, new byte[256]));
    }

    [Test]
    public void ZstdCompressor_Encode_UsesTheConfiguredLevel()
    {
        var fast = new ZstdCompressor(1);
        var strong = new ZstdCompressor(19);

        var fastBuffer = new byte[fast.MaxEncodedLength(Sample.Length)];
        var strongBuffer = new byte[strong.MaxEncodedLength(Sample.Length)];

        // Not just "both work": a higher level must actually compress this sample harder, which is
        // what proves the level reaches the codec instead of being dropped.
        Assert.That(strong.Encode(Sample, strongBuffer), Is.LessThan(fast.Encode(Sample, fastBuffer)));
    }

    // ---------------------------------------------------------------------------------------------
    // HTTP frame path.
    // ---------------------------------------------------------------------------------------------

    [Test]
    public void ZstdCompressor_Compress_ProducesUpstreamDecodableFrame()
    {
        using var destination = new MemoryStream();
        using (var compressing = ZstdCompressor.Default.Compress(destination, leaveOpen: true))
        {
            compressing.Write(Sample, 0, Sample.Length);
        }

        Assert.That(destination.Length, Is.GreaterThan(0));
        Assert.That(destination.ToArray()[..4], Is.EqualTo(ZstdFrameMagic), "expected a zstd frame magic");

        // Oracle: the HTTP frame path must produce a frame upstream ZstdSharp's stream decoder reads.
        destination.Position = 0;
        using var decoder = new DecompressionStream(destination, leaveOpen: true);
        using var decompressed = new MemoryStream();
        decoder.CopyTo(decompressed);

        Assert.That(decompressed.ToArray(), Is.EqualTo(Sample));
    }

    [Test]
    public void ZstdCompressor_Decompress_OfUpstreamEncodedFrame_MatchesOriginal()
    {
        // Oracle: our decoder reads what upstream ZstdSharp's stream encoder produced.
        using var encoded = new MemoryStream();
        using (var upstream = new CompressionStream(encoded, ZstdCompressor.DefaultLevel, leaveOpen: true))
        {
            upstream.Write(Sample, 0, Sample.Length);
        }

        encoded.Position = 0;
        using var decompressing = ZstdCompressor.Default.Decompress(encoded, leaveOpen: true);
        using var plaintext = new MemoryStream();
        decompressing.CopyTo(plaintext);

        Assert.That(plaintext.ToArray(), Is.EqualTo(Sample));
    }

    /// <summary>
    /// The read path asks for a whole 64 KiB buffer at a time, so a query that trickles rows must
    /// surface its first row before that much output exists. This pins that: the source hands over
    /// exactly one flushed chunk and then refuses to be read again, so a decoder that waited to fill
    /// the caller's buffer would fail instead of returning the bytes it already has.
    /// </summary>
    [Test]
    public void ZstdCompressor_Decompress_ReturnsDecodedBytesBeforeTheBufferIsFull()
    {
        var firstRow = Encoding.UTF8.GetBytes("1\tfirst\n");

        using var flushed = new MemoryStream();
        using (var compressing = new CompressionStream(flushed, ZstdCompressor.DefaultLevel, leaveOpen: true))
        {
            compressing.Write(firstRow, 0, firstRow.Length);
            compressing.Flush(); // ZSTD_e_flush: the row is decodable, the frame is not finished
            flushed.Position = 0;

            using var source = new SingleReadStream(flushed.ToArray());
            using var decompressing = ZstdCompressor.Default.Decompress(source, leaveOpen: true);

            var buffer = new byte[64 * 1024];
            var read = decompressing.Read(buffer, 0, buffer.Length);

            Assert.That(read, Is.EqualTo(firstRow.Length));
            Assert.That(buffer[..read], Is.EqualTo(firstRow));
        }
    }

    [Test]
    public void ZstdCompressor_Decompress_TruncatedFrame_Throws()
    {
        using var destination = new MemoryStream();
        using (var compressing = ZstdCompressor.Default.Compress(destination, leaveOpen: true))
        {
            compressing.Write(Sample, 0, Sample.Length);
        }

        // Half a frame: a body cut short must be reported, not silently returned as short data.
        var truncated = destination.ToArray()[..(int)(destination.Length / 2)];

        using var source = new MemoryStream(truncated);
        using var decompressing = ZstdCompressor.Default.Decompress(source, leaveOpen: true);

        Assert.Throws<EndOfStreamException>(() => decompressing.CopyTo(Stream.Null));
    }

    /// <summary>
    /// The interface documents that a returned decompression stream tolerates repeated disposal, which
    /// matters here because the vendored stream rents its input buffer from <c>ArrayPool</c> — returning
    /// it twice would hand the same array to two owners.
    /// </summary>
    [Test]
    public void ZstdCompressor_Decompress_RepeatedDisposal_DoesNotThrowOrDoubleReturnItsBuffer()
    {
        using var destination = new MemoryStream();
        using (var compressing = ZstdCompressor.Default.Compress(destination, leaveOpen: true))
        {
            compressing.Write(Sample, 0, Sample.Length);
        }

        destination.Position = 0;
        var decompressing = ZstdCompressor.Default.Decompress(destination, leaveOpen: true);
        decompressing.CopyTo(Stream.Null);

        decompressing.Dispose();
        Assert.DoesNotThrow(decompressing.Dispose);

        // A double-returned pooled array shows up as two rents handing back the same instance; rent
        // twice and assert the two are distinct, which they cannot be if the pool was corrupted.
        var first = System.Buffers.ArrayPool<byte>.Shared.Rent(1024);
        var second = System.Buffers.ArrayPool<byte>.Shared.Rent(1024);
        try
        {
            Assert.That(second, Is.Not.SameAs(first));
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(first);
            System.Buffers.ArrayPool<byte>.Shared.Return(second);
        }
    }

    [Test]
    public void ZstdCompressor_Constructor_WithLevelOutOfRange_Throws()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new ZstdCompressor(ZstdCompressor.MaxLevel + 1));
            Assert.Throws<ArgumentOutOfRangeException>(() => new ZstdCompressor(ZstdCompressor.MinLevel - 1));
        });
    }

    [Test]
    public void ZstdCompressor_Constructor_WithNegativeLevel_IsAccepted()
    {
        // zstd's levels are a signed range, not a ladder: negative levels are legal and trade ratio
        // for speed. This is why the level is an int rather than an enum.
        var compressor = new ZstdCompressor(-3);
        var encoded = new byte[compressor.MaxEncodedLength(Sample.Length)];

        Assert.That(compressor.Encode(Sample, encoded), Is.GreaterThan(0));
    }

    [Test]
    public void ZstdCompressor_Constructor_WithNonPositiveBufferSize_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ZstdCompressor(bufferSize: 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ZstdCompressor(ZstdCompressor.DefaultLevel, bufferSize: -1));
    }

    // ---------------------------------------------------------------------------------------------
    // Concurrency: unlike the stateless LZ4 codec, zstd contexts are stateful, so the shared static
    // Default instance would corrupt output (or crash) if one context were used by two threads.
    // ---------------------------------------------------------------------------------------------

    [Test]
    public void ZstdCompressor_Default_EncodeAndDecodeConcurrently_ProducesCorrectResultsOnEveryThread()
    {
        const int threads = 16;
        const int iterations = 40;

        var failures = new ConcurrentQueue<string>();

        Parallel.For(0, threads, worker =>
        {
            // Per-thread payloads, so a context leaking between threads shows up as wrong data and
            // not merely as a torn buffer.
            var payload = Encoding.UTF8.GetBytes(string.Concat(
                System.Linq.Enumerable.Repeat($"worker={worker};value={worker * 7};padding=xxxxxxxxxxxx ", 64)));

            for (var i = 0; i < iterations; i++)
            {
                try
                {
                    var encoded = new byte[ZstdCompressor.Default.MaxEncodedLength(payload.Length)];
                    var encodedLength = ZstdCompressor.Default.Encode(payload, encoded);

                    var decoded = new byte[payload.Length];
                    var decodedLength = ZstdCompressor.Default.Decode(encoded.AsSpan(0, encodedLength), decoded);

                    if (decodedLength != payload.Length || !decoded.AsSpan().SequenceEqual(payload))
                        failures.Enqueue($"worker {worker} iteration {i}: round-trip mismatch");
                }
                catch (Exception ex)
                {
                    failures.Enqueue($"worker {worker} iteration {i}: {ex.GetType().Name}: {ex.Message}");
                }
            }
        });

        Assert.That(failures, Is.Empty);
    }

    [Test]
    public void ZstdCompressor_Default_CompressAndDecompressStreamsConcurrently_RoundTripsEveryPayload()
    {
        const int threads = 16;

        var failures = new ConcurrentQueue<string>();

        Parallel.For(0, threads, worker =>
        {
            var payload = Encoding.UTF8.GetBytes(string.Concat(
                System.Linq.Enumerable.Repeat($"stream-worker={worker};row={worker};filler=yyyyyyyyyy ", 256)));

            try
            {
                using var buffer = new MemoryStream();
                using (var compressing = ZstdCompressor.Default.Compress(buffer, leaveOpen: true))
                {
                    compressing.Write(payload, 0, payload.Length);
                }

                buffer.Position = 0;
                using var decompressing = ZstdCompressor.Default.Decompress(buffer, leaveOpen: true);
                using var plaintext = new MemoryStream();
                decompressing.CopyTo(plaintext);

                if (!plaintext.ToArray().AsSpan().SequenceEqual(payload))
                    failures.Enqueue($"worker {worker}: stream round-trip mismatch");
            }
            catch (Exception ex)
            {
                failures.Enqueue($"worker {worker}: {ex.GetType().Name}: {ex.Message}");
            }
        });

        Assert.That(failures, Is.Empty);
    }

    /// <summary>
    /// A source that returns its whole payload on the first read and then fails, so a caller that needs
    /// more input than one chunk cannot silently succeed.
    /// </summary>
    private sealed class SingleReadStream : Stream
    {
        private readonly byte[] payload;
        private bool consumed;

        public SingleReadStream(byte[] payload) => this.payload = payload;

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (consumed)
                throw new InvalidOperationException("the decoder asked for more input instead of yielding what it had");

            consumed = true;
            var length = Math.Min(count, payload.Length);
            Array.Copy(payload, 0, buffer, offset, length);
            return length;
        }

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
