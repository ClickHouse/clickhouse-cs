using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Compression;

/// <summary>
/// Serves a block body that arrives as a stream of compression frames, so the block and column decoders
/// above it read plaintext and never learn that compression is on.
/// <para>
/// Frames are pulled <b>through</b> the connection's raw reader rather than from the socket directly, so
/// there stays exactly one owner of buffered socket bytes. <see cref="Reader"/> is a second reader over the
/// decoded plaintext; the packet envelope (the type code and the table name) is read from the raw one,
/// because the server writes those outside the frames.
/// </para>
/// <para>
/// Read-ahead is deliberately limited to the current frame: a read is served short at a frame boundary and
/// the next frame is pulled only when a caller asks for more. Reading ahead past a block's last frame would
/// consume the next packet's <i>uncompressed</i> envelope as if it were frame bytes.
/// </para>
/// <para>One per connection, reused across the blocks of a query. Not thread-safe.</para>
/// </summary>
internal sealed class CompressedFrameReader : IDisposable
{
    private readonly ClickHouseBinaryReader raw;
    private byte[] prefix;         // the 16-byte checksum plus the 9-byte header
    private byte[] headerAndBody;  // the header and body, contiguous, as the checksum spans them
    private byte[] plaintext;      // the current frame, decoded
    private int position;          // bytes of `plaintext` already served
    private int length;            // bytes of `plaintext` that are valid
    private bool disposed;

    /// <summary>Initializes a frame reader that pulls its frames from <paramref name="raw"/>.</summary>
    /// <param name="raw">The connection's reader, positioned at a frame boundary when a body starts.</param>
    /// <param name="bufferSize">Capacity of the buffer that serves decoded plaintext to <see cref="Reader"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="raw"/> is null.</exception>
    public CompressedFrameReader(ClickHouseBinaryReader raw, int bufferSize = 16384)
    {
        this.raw = raw ?? throw new ArgumentNullException(nameof(raw));
        prefix = ArrayPool<byte>.Shared.Rent(CompressionFrame.PrefixSize);
        headerAndBody = ArrayPool<byte>.Shared.Rent(CompressionFrame.HeaderSize);
        plaintext = ArrayPool<byte>.Shared.Rent(1);
        // The stream is an adapter onto this reader, not the connection. The frame pulls below it read through
        // `raw`, which reports a failed read itself; the codec's own failures are not the transport's.
        var decoded = new ReadBuffer(new PlaintextStream(this), bufferSize, readsFromTransport: false);
        Reader = new ClickHouseBinaryReader(decoded, ownsBuffer: true);
    }

    /// <summary>Reads the decoded block body. Everything after the packet's table name comes from here.</summary>
    public ClickHouseBinaryReader Reader { get; }

    /// <summary>Decoded bytes of the current frame that no caller has taken yet.</summary>
    public int PendingPlaintext => length - position;

    /// <summary>
    /// Asserts that a finished block consumed its frames exactly. The sender flushes at every block end, so
    /// a block's last frame ends where the block does; anything left means the decoders and the peer
    /// disagree about the body's length, and the connection can no longer be trusted.
    /// </summary>
    /// <exception cref="ClickHouseTcpProtocolException">Decoded plaintext was left unread.</exception>
    public void EndBlock()
    {
        int leftover = PendingPlaintext + Reader.BufferedBytes;
        if (leftover != 0)
        {
            throw new ClickHouseTcpProtocolException(
                $"A compressed block left {leftover} decoded byte(s) unread, so its frames carried more than the block declared. The connection is out of step.");
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        disposed = true;
        Reader.Dispose();
        ArrayPool<byte>.Shared.Return(prefix);
        ArrayPool<byte>.Shared.Return(headerAndBody);
        ArrayPool<byte>.Shared.Return(plaintext);
        prefix = Array.Empty<byte>();
        headerAndBody = Array.Empty<byte>();
        plaintext = Array.Empty<byte>();
    }

    /// <summary>
    /// Reads the next frame: its prefix, its body, then verifies the checksum and decodes the body.
    /// </summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <exception cref="ClickHouseTcpProtocolException">The frame is malformed, its checksum fails, or it declares no plaintext.</exception>
    private async ValueTask PullFrameAsync(CancellationToken cancellationToken)
    {
        // CompressionFrame is shared with the HTTP transport and reports a malformed frame as
        // InvalidDataException. Translate it here so one frame layer does not leak a second exception type;
        // the reads keep their own transport exception, which this filter does not match.
        try
        {
            await raw.ReadBytesAsync(prefix.AsMemory(0, CompressionFrame.PrefixSize), cancellationToken).ConfigureAwait(false);
            CompressionFrame.ReadHeader(
                prefix.AsSpan(CompressionFrame.ChecksumSize, CompressionFrame.HeaderSize),
                out byte method,
                out int bodySize,
                out int plaintextSize);

            if (plaintextSize == 0)
            {
                // A block body is never empty (it carries at least the block info and two counts), and serving
                // zero bytes would spin the read loop rather than make progress.
                throw new ClickHouseTcpProtocolException("Compression frame declares no plaintext, which cannot occur inside a block body (corrupt stream).");
            }

            // The checksum covers the header and body as one run, so keep them contiguous.
            int framed = CompressionFrame.HeaderSize + bodySize;
            Grow(ref headerAndBody, framed);
            prefix.AsSpan(CompressionFrame.ChecksumSize, CompressionFrame.HeaderSize).CopyTo(headerAndBody);
            await raw.ReadBytesAsync(headerAndBody.AsMemory(CompressionFrame.HeaderSize, bodySize), cancellationToken).ConfigureAwait(false);

            CompressionFrame.VerifyChecksum(prefix.AsSpan(0, CompressionFrame.ChecksumSize), headerAndBody.AsSpan(0, framed));

            Grow(ref plaintext, plaintextSize);
            CompressionFrame.Decode(method, headerAndBody.AsSpan(CompressionFrame.HeaderSize, bodySize), plaintext.AsSpan(0, plaintextSize));
            position = 0;
            length = plaintextSize;
        }
        catch (InvalidDataException e)
        {
            throw new ClickHouseTcpProtocolException(e.Message, e);
        }
    }

    /// <summary>Replaces <paramref name="buffer"/> with a larger pooled one when it cannot hold <paramref name="needed"/> bytes.</summary>
    private static void Grow(ref byte[] buffer, int needed)
    {
        if (buffer.Length >= needed)
        {
            return;
        }

        // Rent before returning, so a rent that throws cannot leave the field aliasing a pooled array that
        // Dispose would then return a second time.
        byte[] replacement = ArrayPool<byte>.Shared.Rent(needed);
        ArrayPool<byte>.Shared.Return(buffer);
        buffer = replacement;
    }

    /// <summary>
    /// The stream face the plaintext <see cref="ReadBuffer"/> fills from. Each read serves bytes from the
    /// current frame only, pulling the next frame when the current one runs out.
    /// </summary>
    private sealed class PlaintextStream : Stream
    {
        private readonly CompressedFrameReader owner;

        public PlaintextStream(CompressedFrameReader owner) => this.owner = owner;

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (buffer.IsEmpty)
            {
                return 0;
            }

            if (owner.PendingPlaintext == 0)
            {
                await owner.PullFrameAsync(cancellationToken).ConfigureAwait(false);
            }

            // Never crosses a frame boundary: a short read keeps the next frame unread until it is needed.
            int taken = Math.Min(buffer.Length, owner.PendingPlaintext);
            owner.plaintext.AsMemory(owner.position, taken).CopyTo(buffer);
            owner.position += taken;
            return taken;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => ReadAsync(new Memory<byte>(buffer, offset, count), cancellationToken).AsTask();

        public override int Read(byte[] buffer, int offset, int count)
            => throw new NotSupportedException("The frame reader is asynchronous; use ReadAsync.");

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
