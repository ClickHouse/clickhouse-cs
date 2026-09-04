using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Compression;

/// <summary>
/// Frames an outgoing block body, so the block and column encoders above it write plaintext and never learn
/// that compression is on.
/// <para>
/// With compression requested, the server expects the client's own Data blocks framed the same way it frames
/// its own — the end-of-input marker, external tables and INSERT rows alike. Only the body is framed: the
/// packet type code and the table name go to the raw writer, ahead of the frames.
/// </para>
/// <para>
/// Plaintext is cut into frames of at most <see cref="DefaultFrameTargetBytes"/>, matching the server's own
/// buffer, and each finished frame is flushed. A frame boundary inside a body is legal — the reader finds a
/// block's end from its own dimensions, not from the framing — and flushing per frame keeps at most one
/// frame buffered however large the block is.
/// </para>
/// <para>One per connection, reused across the blocks of a query. Not thread-safe.</para>
/// </summary>
internal sealed class CompressedFrameWriter : IDisposable
{
    /// <summary>Plaintext per frame, matching the server's <c>DBMS_DEFAULT_BUFFER_SIZE</c>.</summary>
    public const int DefaultFrameTargetBytes = 1024 * 1024;

    private readonly ClickHouseBinaryWriter raw;
    private readonly IClickHouseCompressor codec;
    private readonly int frameTarget;
    private byte[] frame;
    private bool disposed;

    /// <summary>Initializes a frame writer that emits its frames into <paramref name="raw"/>.</summary>
    /// <param name="raw">The connection's writer, positioned after the packet envelope when a body starts.</param>
    /// <param name="codec">The codec supplying the method byte and body encoding.</param>
    /// <param name="frameTarget">Plaintext bytes per frame; defaults to <see cref="DefaultFrameTargetBytes"/>.</param>
    /// <param name="bufferSize">Initial capacity of the plaintext writer's buffer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="raw"/> or <paramref name="codec"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="frameTarget"/> is not positive.</exception>
    public CompressedFrameWriter(ClickHouseBinaryWriter raw, IClickHouseCompressor codec, int frameTarget = DefaultFrameTargetBytes, int bufferSize = 16384)
    {
        this.raw = raw ?? throw new ArgumentNullException(nameof(raw));
        this.codec = codec ?? throw new ArgumentNullException(nameof(codec));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(frameTarget);

        this.frameTarget = frameTarget;
        frame = ArrayPool<byte>.Shared.Rent(CompressionFrame.MaxFrameSize(codec, frameTarget));
        Writer = new ClickHouseBinaryWriter(new PlaintextSink(this), bufferSize);
    }

    /// <summary>Writes the block body. Everything after the packet's table name goes here.</summary>
    public ClickHouseBinaryWriter Writer { get; }

    /// <summary>
    /// Ends a block: flushes the plaintext writer, so whatever remains becomes the body's final frame. The
    /// reader relies on this — a block end must coincide with a frame boundary.
    /// </summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    public async ValueTask EndBlockAsync(CancellationToken cancellationToken)
    {
        await Writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        Writer.TrimBuffer();
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        disposed = true;
        Writer.Dispose();
        ArrayPool<byte>.Shared.Return(frame);
        frame = Array.Empty<byte>();
    }

    /// <summary>Compresses one frame's worth of plaintext and writes the frame to the raw writer.</summary>
    /// <param name="plaintext">At most <see cref="frameTarget"/> bytes.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    private async ValueTask EmitFrameAsync(ReadOnlyMemory<byte> plaintext, CancellationToken cancellationToken)
    {
        int required = CompressionFrame.MaxFrameSize(codec, plaintext.Length);
        if (frame.Length < required)
        {
            // Rent before returning, so a rent that throws cannot leave the field aliasing a pooled array that
            // Dispose would then return a second time.
            byte[] replacement = ArrayPool<byte>.Shared.Rent(required);
            ArrayPool<byte>.Shared.Return(frame);
            frame = replacement;
        }

        int written = CompressionFrame.Write(plaintext.Span, codec, frame);
        raw.WriteBytes(frame.AsSpan(0, written));
        await raw.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The stream face the plaintext <see cref="ClickHouseBinaryWriter"/> flushes into. Each flush is cut
    /// into frames of at most the frame target.
    /// </summary>
    private sealed class PlaintextSink : Stream
    {
        private readonly CompressedFrameWriter owner;

        public PlaintextSink(CompressedFrameWriter owner) => this.owner = owner;

        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            while (!buffer.IsEmpty)
            {
                int take = Math.Min(owner.frameTarget, buffer.Length);
                await owner.EmitFrameAsync(buffer.Slice(0, take), cancellationToken).ConfigureAwait(false);
                buffer = buffer.Slice(take);
            }
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => WriteAsync(new ReadOnlyMemory<byte>(buffer, offset, count), cancellationToken).AsTask();

        public override void Write(byte[] buffer, int offset, int count)
            => throw new NotSupportedException("The frame writer is asynchronous; use WriteAsync.");

        public override void Flush()
        {
        }

        // The raw writer is flushed as each frame is emitted, so there is nothing buffered here to push.
        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();
    }
}
