using System;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Http;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Http;

/// <summary>
/// The <c>Content-Encoding: deflate</c> decoder, whose job is to accept both encodings that name is used
/// for: the zlib format (RFC 1950) that ClickHouse actually emits, and the raw DEFLATE (RFC 1951) some
/// other peers send. Exercised directly because the interesting paths — a short read while sniffing the
/// two header bytes, the span overloads, disposal before the first read — are hard to provoke through a
/// whole HTTP response.
/// </summary>
[TestFixture]
public class ZLibOrDeflateStreamTests
{
    private static readonly byte[] Payload =
        Encoding.UTF8.GetBytes("deflate is two formats wearing one Content-Encoding token");

    private static byte[] ZLibEncoded()
    {
        using var buffer = new MemoryStream();
        using (var encoder = new ZLibStream(buffer, CompressionLevel.Fastest, leaveOpen: true))
        {
            encoder.Write(Payload, 0, Payload.Length);
        }

        return buffer.ToArray();
    }

    private static byte[] RawDeflateEncoded()
    {
        using var buffer = new MemoryStream();
        using (var encoder = new DeflateStream(buffer, CompressionLevel.Fastest, leaveOpen: true))
        {
            encoder.Write(Payload, 0, Payload.Length);
        }

        return buffer.ToArray();
    }

    private static byte[] Encoded(string format) => format == "zlib" ? ZLibEncoded() : RawDeflateEncoded();

    [Test]
    public void ZLibEncoded_StartsWithTheHeaderClickHouseSends()
    {
        // Guards the premise of the whole class: if this ever stops being a zlib header, the sniff below
        // is testing something other than what the server sends.
        Assert.That(ZLibEncoded()[0], Is.EqualTo(0x78));
    }

    [TestCase("zlib")]
    [TestCase("raw-deflate")]
    public void Read_ByteArrayOverload_DecodesBothFormats(string format)
    {
        using var stream = new ZLibOrDeflateStream(new MemoryStream(Encoded(format)), leaveOpen: false);

        using var decoded = new MemoryStream();
        var buffer = new byte[16];
        int n;
        while ((n = stream.Read(buffer, 0, buffer.Length)) > 0)
            decoded.Write(buffer, 0, n);

        Assert.That(decoded.ToArray(), Is.EqualTo(Payload));
    }

    /// <summary>
    /// The span overload is not decoration: <c>PooledReadBufferStream</c>, which sits directly above this
    /// stream in the reader chain, reads into the caller's span for any read at least as large as its
    /// buffer. If this override forgot to sniff the header first, such a read would return
    /// still-compressed bytes.
    /// </summary>
    [TestCase("zlib")]
    [TestCase("raw-deflate")]
    public void Read_SpanOverload_DecodesBothFormats(string format)
    {
        using var stream = new ZLibOrDeflateStream(new MemoryStream(Encoded(format)), leaveOpen: false);

        using var decoded = new MemoryStream();
        var buffer = new byte[16];
        int n;
        while ((n = stream.Read(buffer.AsSpan())) > 0)
            decoded.Write(buffer, 0, n);

        Assert.That(decoded.ToArray(), Is.EqualTo(Payload));
    }

    [TestCase("zlib")]
    [TestCase("raw-deflate")]
    public async Task ReadAsync_DecodesBothFormats(string format)
    {
        await using var stream = new ZLibOrDeflateStream(new MemoryStream(Encoded(format)), leaveOpen: false);

        using var decoded = new MemoryStream();
        await stream.CopyToAsync(decoded);

        Assert.That(decoded.ToArray(), Is.EqualTo(Payload));
    }

    /// <summary>
    /// The header sniff needs two bytes and a stream is free to return fewer, so the loop must keep
    /// reading. A source that yields one byte per call would, without that loop, see only the first
    /// header byte and misclassify the format — a socket does exactly this under the wrong packet
    /// boundary, which is precisely where it would be hardest to reproduce.
    /// </summary>
    [TestCase("zlib")]
    [TestCase("raw-deflate")]
    public void Read_WhenTheSourceYieldsOneByteAtATime_StillSniffsTheFullHeader(string format)
    {
        using var stream = new ZLibOrDeflateStream(new OneByteAtATimeStream(Encoded(format)), leaveOpen: false);

        using var decoded = new MemoryStream();
        stream.CopyTo(decoded);

        Assert.That(decoded.ToArray(), Is.EqualTo(Payload));
    }

    [TestCase("zlib")]
    [TestCase("raw-deflate")]
    public async Task ReadAsync_WhenTheSourceYieldsOneByteAtATime_StillSniffsTheFullHeader(string format)
    {
        await using var stream = new ZLibOrDeflateStream(new OneByteAtATimeStream(Encoded(format)), leaveOpen: false);

        using var decoded = new MemoryStream();
        await stream.CopyToAsync(decoded);

        Assert.That(decoded.ToArray(), Is.EqualTo(Payload));
    }

    /// <summary>
    /// A body of fewer than two bytes cannot carry a zlib header, so it cannot be classified and falls
    /// through to raw DEFLATE. Both sizes then yield nothing rather than throwing, because that is how the
    /// BCL inflater treats input that ends early — the same for <c>gzip</c> and <c>br</c>, so it is not
    /// specific to this stream. A body cut this short is indistinguishable from an empty response at the
    /// transport layer either way; it is the row parser above that reports the truncation.
    /// </summary>
    [TestCase(new byte[0], TestName = "{m}(empty)")]
    [TestCase(new byte[] { 0x78 }, TestName = "{m}(one byte, half a zlib header)")]
    public void Read_WithABodyTooShortToClassify_YieldsNoBytes(byte[] body)
    {
        using var stream = new ZLibOrDeflateStream(new MemoryStream(body), leaveOpen: false);

        Assert.That(stream.Read(new byte[8], 0, 8), Is.Zero);
    }

    [Test]
    public void Dispose_BeforeAnyRead_DoesNotThrow()
    {
        var source = new DisposeCountingStream(ZLibEncoded());
        var stream = new ZLibOrDeflateStream(source, leaveOpen: false);

        // No read happened, so no decoder was ever built; disposal must still release the source.
        Assert.DoesNotThrow(stream.Dispose);
        Assert.That(source.DisposeCount, Is.EqualTo(1));
    }

    [Test]
    public void Dispose_CalledTwice_ReleasesTheSourceOnlyOnce()
    {
        var source = new DisposeCountingStream(ZLibEncoded());
        var stream = new ZLibOrDeflateStream(source, leaveOpen: false);
        stream.CopyTo(Stream.Null);

        stream.Dispose();
        stream.Dispose();

        Assert.That(source.DisposeCount, Is.EqualTo(1));
    }

    [Test]
    public void Dispose_WithLeaveOpen_DoesNotTouchTheSource()
    {
        var source = new DisposeCountingStream(ZLibEncoded());
        using (var stream = new ZLibOrDeflateStream(source, leaveOpen: true))
        {
            stream.CopyTo(Stream.Null);
        }

        Assert.Multiple(() =>
        {
            Assert.That(source.DisposeCount, Is.Zero);
            Assert.That(source.CanRead, Is.True);
        });
    }

    [Test]
    public void Read_AfterDispose_Throws()
    {
        var stream = new ZLibOrDeflateStream(new MemoryStream(ZLibEncoded()), leaveOpen: false);
        stream.Dispose();

        // Without the guard this would sniff and build a second decoder over a closed source.
        // ReadByte routes through the byte[] overload, so it exercises the same guard.
        Assert.Throws<ObjectDisposedException>(() => stream.ReadByte());
    }

    [Test]
    public void Constructor_WithNullSource_Throws()
        => Assert.Throws<ArgumentNullException>(() => new ZLibOrDeflateStream(null, leaveOpen: false));

    // ---------------------------------------------------------------------------------------------
    // PrefixedStream: replays the sniffed header bytes ahead of the rest of the body.
    // ---------------------------------------------------------------------------------------------

    [Test]
    public void PrefixedStream_ByteArrayOverload_ServesThePrefixThenTheInnerStream()
    {
        using var prefixed = new ZLibOrDeflateStream.PrefixedStream([1, 2], 2, new MemoryStream([3, 4, 5]));

        using var all = new MemoryStream();
        prefixed.CopyTo(all);

        Assert.That(all.ToArray(), Is.EqualTo(new byte[] { 1, 2, 3, 4, 5 }));
    }

    [Test]
    public void PrefixedStream_SpanOverload_ServesThePrefixThenTheInnerStream()
    {
        using var prefixed = new ZLibOrDeflateStream.PrefixedStream([1, 2], 2, new MemoryStream([3, 4, 5]));

        var buffer = new byte[8];
        using var all = new MemoryStream();
        int n;
        while ((n = prefixed.Read(buffer.AsSpan())) > 0)
            all.Write(buffer, 0, n);

        Assert.That(all.ToArray(), Is.EqualTo(new byte[] { 1, 2, 3, 4, 5 }));
    }

    /// <summary>
    /// A prefix read must not top the buffer up from the inner stream: a short read is legal, and mixing
    /// the two would block on a network read the caller may not want yet.
    /// </summary>
    [Test]
    public void PrefixedStream_WhenAskedForMoreThanThePrefix_ReturnsOnlyThePrefixFirst()
    {
        using var prefixed = new ZLibOrDeflateStream.PrefixedStream([1, 2], 2, new MemoryStream([3, 4, 5]));

        var buffer = new byte[8];

        Assert.Multiple(() =>
        {
            Assert.That(prefixed.Read(buffer, 0, buffer.Length), Is.EqualTo(2), "prefix only");
            Assert.That(prefixed.Read(buffer, 0, buffer.Length), Is.EqualTo(3), "then the inner stream");
        });
    }

    [Test]
    public void PrefixedStream_WithAPartiallyFilledPrefix_ServesOnlyTheBytesItHas()
    {
        // Mirrors a short sniff: the buffer is 2 bytes but only 1 was actually read.
        using var prefixed = new ZLibOrDeflateStream.PrefixedStream([9, 0], 1, new MemoryStream([7]));

        using var all = new MemoryStream();
        prefixed.CopyTo(all);

        Assert.That(all.ToArray(), Is.EqualTo(new byte[] { 9, 7 }));
    }

    [Test]
    public void PrefixedStream_WithAZeroLengthRead_ReturnsZeroWithoutConsumingThePrefix()
    {
        using var prefixed = new ZLibOrDeflateStream.PrefixedStream([1, 2], 2, new MemoryStream([3]));

        Assert.Multiple(() =>
        {
            Assert.That(prefixed.Read([], 0, 0), Is.Zero);
            Assert.That(prefixed.Read(Span<byte>.Empty), Is.Zero);
            Assert.That(prefixed.ReadByte(), Is.EqualTo(1), "the prefix must be intact");
        });
    }

    /// <summary>Yields a single byte per read, the legal-but-awkward case a socket can produce.</summary>
    private sealed class OneByteAtATimeStream : Stream
    {
        private readonly byte[] data;
        private int position;

        public OneByteAtATimeStream(byte[] data) => this.data = data;

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            if (buffer.Length == 0 || position >= data.Length)
                return 0;

            buffer[0] = data[position++];
            return 1;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(Read(buffer.Span));

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private sealed class DisposeCountingStream : MemoryStream
    {
        public DisposeCountingStream(byte[] buffer)
            : base(buffer)
        {
        }

        public int DisposeCount { get; private set; }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                DisposeCount++;

            base.Dispose(disposing);
        }
    }
}
