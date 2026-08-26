using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// A fixed-capacity read buffer over an underlying <see cref="Stream"/>. Consumed space is reclaimed by
/// advancing the head; the run is only shifted back to the front when a small fixed-width read would otherwise
/// run past the end of the backing array.
///
/// <para>
/// The buffer lets callers read Spans of at most <see cref="MaxContiguous"/> bytes, via
/// <see cref="ReadSpan"/>. Bulk payloads (strings, whole columns) go through <see cref="ReadIntoAsync"/>,
/// which drains the buffered prefix and then reads the remainder straight from the stream into the caller's
/// destination — so <see cref="Capacity"/> bounds the largest contiguous scalar, not the largest payload.
/// </para>
///
/// <para>Not thread-safe. One per connection.</para>
/// </summary>
internal sealed class ReadBuffer : IDisposable
{
    /// <summary>The largest single contiguous value the buffer can serve via <see cref="ReadSpan"/>.</summary>
    public const int MaxContiguous = 32;

    private readonly Stream stream;
    private readonly bool readsFromTransport;
    private readonly IdleReadDeadline deadline;
    private byte[] buffer;
    private int capacity;
    private int head;      // index of the first valid byte
    private int buffered;  // number of valid bytes; the run is [head, head + buffered) and never wraps
    private bool disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="ReadBuffer"/> class.
    /// </summary>
    /// <param name="stream">The source stream (a network stream in production, any stream in tests).</param>
    /// <param name="capacity">Requested capacity in bytes; must be at least <see cref="MaxContiguous"/>.</param>
    /// <param name="readsFromTransport">
    /// Whether <paramref name="stream"/> is the connection itself, so that a failed read is the transport failing.
    /// False for an adapter stream, whose own layer decides what its failures mean.
    /// </param>
    /// <param name="deadline">
    /// Bounds how long <paramref name="stream"/> may stay silent, or null to wait as long as the caller's token
    /// allows. An adapter stream's reads are served from bytes the transport already delivered, so they cannot
    /// stall on the network and are given none.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="stream"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is below <see cref="MaxContiguous"/>.</exception>
    public ReadBuffer(Stream stream, int capacity = 16384, bool readsFromTransport = true, IdleReadDeadline deadline = null)
    {
        this.stream = stream ?? throw new ArgumentNullException(nameof(stream));
        if (capacity < MaxContiguous)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), $"Capacity must be at least {MaxContiguous} bytes.");
        }

        this.readsFromTransport = readsFromTransport;
        this.deadline = deadline;
        buffer = ArrayPool<byte>.Shared.Rent(capacity);
        this.capacity = buffer.Length; // Rent may return a larger array; use all of it.
    }

    /// <summary>The number of bytes currently buffered and unconsumed.</summary>
    public int Buffered => buffered;

    /// <summary>The actual backing capacity in bytes (may exceed the requested size due to pooling).</summary>
    public int Capacity => capacity;

    /// <summary>Ensures at least <paramref name="needed"/> bytes are buffered and contiguous at the head, pulling from the stream as required.</summary>
    /// <param name="needed">The number of contiguous bytes that must be available; must not exceed <see cref="Capacity"/>.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="needed"/> exceeds the buffer capacity.</exception>
    /// <exception cref="TimeoutException">The stream stayed silent past the idle deadline.</exception>
    /// <exception cref="ClickHouseTcpTransportException">The stream ended before enough bytes arrived, or the read failed.</exception>
    public async ValueTask EnsureAsync(int needed, CancellationToken cancellationToken)
    {
        if (needed > capacity)
        {
            throw new ArgumentOutOfRangeException(nameof(needed), $"Cannot buffer {needed} contiguous bytes; capacity is {capacity}.");
        }
        ArgumentOutOfRangeException.ThrowIfNegative(needed);

        if (needed <= buffered)
        {
            return; // Already satisfied — do not touch the head (no read, no shift).
        }

        // Make room for `needed` contiguous bytes at the head before filling. This is the only place bytes move.
        if (buffered == 0)
        {
            head = 0; // Nothing buffered: reset to the front for free (no copy).
        }
        else if (capacity - head < needed)
        {
            // The head is close enough to the end that `needed` bytes can't fit there. The run is small in this
            // case (buffered <= capacity - head < needed), so this copies fewer than `needed` bytes.
            Array.Copy(buffer, head, buffer, 0, buffered);
            head = 0;
        }

        while (buffered < needed)
        {
            await FillOnceAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Consumes one byte. Caller must first ensure at least one byte is buffered.</summary>
    /// <returns>The consumed byte.</returns>
    public byte ReadByte()
    {
        byte value = buffer[head];
        Advance(1);
        return value;
    }

    /// <summary>
    /// Consumes <paramref name="count"/> contiguous bytes (at most <see cref="MaxContiguous"/>) as a direct
    /// slice of the backing array — no copy. The returned span is valid only until the next buffer operation.
    /// Caller must first ensure the bytes are buffered (via <see cref="EnsureAsync"/>).
    /// </summary>
    /// <param name="count">The number of contiguous bytes to consume; must not exceed <see cref="MaxContiguous"/>.</param>
    /// <returns>A span over the consumed bytes, valid only until the next buffer operation.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> exceeds <see cref="MaxContiguous"/>.</exception>
    public ReadOnlySpan<byte> ReadSpan(int count)
    {
        if (count > MaxContiguous)
        {
            throw new ArgumentOutOfRangeException(nameof(count), $"ReadSpan serves at most {MaxContiguous} bytes; use ReadIntoAsync for larger reads.");
        }

        // Relies on the caller's prior EnsureAsync(count). This method is sync (a span can't cross an await), so it cannot
        // buffer on its own.
        ReadOnlySpan<byte> result = buffer.AsSpan(head, count);
        Advance(count);
        return result;
    }

    /// <summary>
    /// Fills <paramref name="destination"/> completely: drains the buffered prefix first, then reads any
    /// remainder straight from the stream into the caller's memory (no intermediate copy).
    /// </summary>
    /// <param name="destination">The region to fill completely with consumed bytes.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <exception cref="TimeoutException">The stream stayed silent past the idle deadline.</exception>
    /// <exception cref="ClickHouseTcpTransportException">The stream ended before the destination was filled, or the read failed.</exception>
    public async ValueTask ReadIntoAsync(Memory<byte> destination, CancellationToken cancellationToken)
    {
        if (buffered > 0 && !destination.IsEmpty)
        {
            int fromBuffer = Math.Min(destination.Length, buffered);
            buffer.AsMemory(head, fromBuffer).CopyTo(destination);
            Advance(fromBuffer);
            destination = destination.Slice(fromBuffer);
        }

        while (!destination.IsEmpty)
        {
            int read = await ReadFromStreamAsync(destination, cancellationToken).ConfigureAwait(false);
            destination = destination.Slice(read);
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
        ArrayPool<byte>.Shared.Return(buffer);
        buffer = Array.Empty<byte>();
        capacity = 0;
    }

    /// <summary>Advances the head past consumed bytes, resetting to the front when the buffer empties.</summary>
    /// <param name="n">The number of bytes just consumed.</param>
    private void Advance(int n)
    {
        head += n;
        buffered -= n;
        if (buffered == 0)
        {
            head = 0; // Empty: reset to the front so the next fill has the whole array as one contiguous run.
        }
    }

    /// <summary>
    /// Reads one chunk from the stream into the free run at the tail and grows the buffered count. EnsureAsync
    /// guarantees there is free tail space (head + buffered &lt; capacity) whenever this is called.
    /// </summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <exception cref="ClickHouseTcpTransportException">The stream ended before any byte arrived, or the read failed.</exception>
    private async ValueTask FillOnceAsync(CancellationToken cancellationToken)
    {
        int writeStart = head + buffered;
        int read = await ReadFromStreamAsync(buffer.AsMemory(writeStart, capacity - writeStart), cancellationToken).ConfigureAwait(false);
        buffered += read;
    }

    /// <summary>
    /// Reads once from the stream, holding the idle deadline open only for as long as that read is pending. The
    /// single point where a byte arrives, so it is also where a silent transport, a closed one and a failed one
    /// are each turned into the exception that describes them.
    /// </summary>
    /// <param name="destination">Where to put the bytes; filled in part or in whole.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The number of bytes read, always at least one.</returns>
    /// <exception cref="TimeoutException">The stream delivered nothing within the deadline.</exception>
    /// <exception cref="ClickHouseTcpTransportException">The stream ended, or the read failed.</exception>
    private async ValueTask<int> ReadFromStreamAsync(Memory<byte> destination, CancellationToken cancellationToken)
    {
        int read;
        deadline?.Arm();
        try
        {
            read = await stream.ReadAsync(destination, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e) when (deadline is { Elapsed: true } && (e is OperationCanceledException || TransportFailure.IsTransportFailure(e)))
        {
            // The deadline's token cancelled the read, not the caller's. Report the silence rather than a
            // cancellation the caller never asked for. A transport failure counts too: SslStream turns a
            // cancelled read into an IOException unless the token matches the one it was given, and a timeout
            // reported as a broken connection names neither the option that caused it nor the fix.
            throw deadline.ToException();
        }
        catch (Exception e) when (readsFromTransport && TransportFailure.IsTransportFailure(e))
        {
            throw TransportFailure.Read(e);
        }
        finally
        {
            deadline?.Disarm();
        }

        if (read == 0)
        {
            throw TransportFailure.EndOfStream();
        }

        return read;
    }
}
