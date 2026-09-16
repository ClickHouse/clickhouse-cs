using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Numerics;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// Decodes ClickHouse native-protocol wire primitives. Buffering and stream I/O are delegated to
/// a <see cref="ReadBuffer"/>; this type only turns buffered bytes into values. Not thread-safe —
/// one per connection, which processes a single query at a time.
/// </summary>
internal sealed class ClickHouseBinaryReader : IDisposable
{
    private const int MaxVarUIntBytes = 10;

    // A corrupt or hostile stream can declare an arbitrary length prefix; cap it so a bad value can't drive a
    // multi-gigabyte allocation/rent. 1 GiB is far larger than any legitimate native-protocol String field.
    private const int MaxStringLength = 1 << 30;

    private readonly ReadBuffer buffer;
    private readonly bool ownsBuffer;
    private bool disposed;

    // Decodes a fixed-width value from a span whose length equals the value's byte width. These are cached
    // static delegates, so ReadFixedAsync costs one indirect call and no per-read allocation.
    private delegate T SpanDecoder<T>(ReadOnlySpan<byte> source);

    private static readonly SpanDecoder<ushort> DecodeUInt16 = BinaryPrimitives.ReadUInt16LittleEndian;
    private static readonly SpanDecoder<short> DecodeInt16 = BinaryPrimitives.ReadInt16LittleEndian;
    private static readonly SpanDecoder<uint> DecodeUInt32 = BinaryPrimitives.ReadUInt32LittleEndian;
    private static readonly SpanDecoder<int> DecodeInt32 = BinaryPrimitives.ReadInt32LittleEndian;
    private static readonly SpanDecoder<ulong> DecodeUInt64 = BinaryPrimitives.ReadUInt64LittleEndian;
    private static readonly SpanDecoder<long> DecodeInt64 = BinaryPrimitives.ReadInt64LittleEndian;
    private static readonly SpanDecoder<float> DecodeFloat32 = BinaryPrimitives.ReadSingleLittleEndian;
    private static readonly SpanDecoder<double> DecodeFloat64 = BinaryPrimitives.ReadDoubleLittleEndian;
    private static readonly SpanDecoder<UInt128> DecodeUInt128 = DecodeUInt128Value;
    private static readonly SpanDecoder<Int128> DecodeInt128 = static source => unchecked((Int128)DecodeUInt128Value(source));
    private static readonly SpanDecoder<UInt256> DecodeUInt256 = UInt256.ReadLittleEndian;
    private static readonly SpanDecoder<Int256> DecodeInt256 = Int256.ReadLittleEndian;

    /// <summary>Initializes a reader over <paramref name="stream"/>, creating and owning its buffer.</summary>
    /// <param name="stream">The source stream to read from.</param>
    /// <param name="bufferSize">Requested read-buffer capacity in bytes.</param>
    public ClickHouseBinaryReader(Stream stream, int bufferSize = 16384)
        : this(new ReadBuffer(stream, bufferSize), ownsBuffer: true)
    {
    }

    /// <summary>Initializes a reader over an existing buffer (which the caller owns unless stated otherwise).</summary>
    /// <param name="buffer">The byte buffer to read from.</param>
    /// <param name="ownsBuffer">When true, disposing the reader disposes the buffer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="buffer"/> is null.</exception>
    public ClickHouseBinaryReader(ReadBuffer buffer, bool ownsBuffer = false)
    {
        this.buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
        this.ownsBuffer = ownsBuffer;
    }

    /// <summary>Reads a single byte.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The byte read.</returns>
    public async ValueTask<byte> ReadByteAsync(CancellationToken cancellationToken)
    {
        await buffer.EnsureAsync(1, cancellationToken).ConfigureAwait(false);
        return buffer.ReadByte();
    }

    /// <summary>Reads exactly <paramref name="destination"/>.Length bytes.</summary>
    /// <param name="destination">The region to fill completely with the bytes read.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>A task that completes when the destination has been filled.</returns>
    public ValueTask ReadBytesAsync(Memory<byte> destination, CancellationToken cancellationToken)
        => buffer.ReadIntoAsync(destination, cancellationToken);

    /// <summary>Reads a LEB-128 variable-length unsigned integer (native-format VarUInt).</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded value.</returns>
    /// <exception cref="InvalidDataException">The encoding exceeds 10 bytes or the range of <see cref="ulong"/>.</exception>
    public async ValueTask<ulong> ReadVarUIntAsync(CancellationToken cancellationToken)
    {
        ulong result = 0;
        for (int i = 0; i < MaxVarUIntBytes - 1; i++)
        {
            // Await only at a buffer boundary; when the bytes are already buffered (the common case) this
            // decodes with zero awaits. VarUInt is on the hottest read path (every length/count/offset).
            if (buffer.Buffered == 0)
            {
                await buffer.EnsureAsync(1, cancellationToken).ConfigureAwait(false);
            }

            byte b = buffer.ReadByte();
            result |= (ulong)(b & 0x7F) << (7 * i);
            if ((b & 0x80) == 0)
            {
                return result;
            }
        }

        if (buffer.Buffered == 0)
        {
            await buffer.EnsureAsync(1, cancellationToken).ConfigureAwait(false);
        }

        byte last = buffer.ReadByte();
        if ((last & 0xFE) != 0)
        {
            throw new InvalidDataException("VarUInt exceeds the UInt64 range (corrupt stream).");
        }

        return result | ((ulong)last << 63);
    }

    /// <summary>
    /// Reads a server→client packet type code (the leading VarUInt of a packet envelope). At protocol
    /// version 54460 there is no chunk wrapping, so this is read straight from the buffered stream.
    /// </summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded packet type code.</returns>
    /// <exception cref="InvalidDataException">The VarUInt encoding exceeds 10 bytes (corrupt stream).</exception>
    public async ValueTask<ServerPacketType> ReadServerPacketTypeAsync(CancellationToken cancellationToken)
        => (ServerPacketType)await ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Reads a native-format String: a VarUInt length prefix followed by that many UTF-8 bytes.
    /// ClickHouse <c>String</c> is byte-oriented and not required to be valid UTF-8; invalid sequences decode
    /// to the Unicode replacement character (U+FFFD). Use <see cref="ReadStringBytesAsync"/> for a lossless
    /// byte-for-byte read.
    /// </summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded string.</returns>
    /// <exception cref="InvalidDataException">The declared length exceeds the supported maximum.</exception>
    public async ValueTask<string> ReadStringAsync(CancellationToken cancellationToken)
    {
        int length = await ReadStringLengthAsync(cancellationToken).ConfigureAwait(false);
        if (length == 0)
        {
            return string.Empty;
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            await buffer.ReadIntoAsync(rented.AsMemory(0, length), cancellationToken).ConfigureAwait(false);
            return Encoding.UTF8.GetString(rented, 0, length);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Reads a native-format String as raw bytes (ClickHouse <c>String</c> is byte-oriented, not necessarily UTF-8).</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The raw string bytes.</returns>
    /// <exception cref="InvalidDataException">The declared length exceeds the supported maximum.</exception>
    public async ValueTask<byte[]> ReadStringBytesAsync(CancellationToken cancellationToken)
    {
        int length = await ReadStringLengthAsync(cancellationToken).ConfigureAwait(false);
        if (length == 0)
        {
            return Array.Empty<byte>();
        }

        byte[] result = new byte[length];
        await buffer.ReadIntoAsync(result, cancellationToken).ConfigureAwait(false);
        return result;
    }

    /// <summary>Reads a single-byte boolean (<c>0x00</c> false, non-zero true).</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded boolean.</returns>
    public async ValueTask<bool> ReadBoolAsync(CancellationToken cancellationToken)
        => await ReadByteAsync(cancellationToken).ConfigureAwait(false) != 0;

    /// <summary>Reads an unsigned 8-bit integer.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The value read.</returns>
    public ValueTask<byte> ReadUInt8Async(CancellationToken cancellationToken) => ReadByteAsync(cancellationToken);

    /// <summary>Reads a signed 8-bit integer.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The value read.</returns>
    public async ValueTask<sbyte> ReadInt8Async(CancellationToken cancellationToken)
        => unchecked((sbyte)await ReadByteAsync(cancellationToken).ConfigureAwait(false));

    /// <summary>Reads a little-endian unsigned 16-bit integer.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The value read.</returns>
    public ValueTask<ushort> ReadUInt16Async(CancellationToken cancellationToken) => ReadFixedAsync(2, DecodeUInt16, cancellationToken);

    /// <summary>Reads a little-endian signed 16-bit integer.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The value read.</returns>
    public ValueTask<short> ReadInt16Async(CancellationToken cancellationToken) => ReadFixedAsync(2, DecodeInt16, cancellationToken);

    /// <summary>Reads a little-endian unsigned 32-bit integer.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The value read.</returns>
    public ValueTask<uint> ReadUInt32Async(CancellationToken cancellationToken) => ReadFixedAsync(4, DecodeUInt32, cancellationToken);

    /// <summary>Reads a little-endian signed 32-bit integer.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The value read.</returns>
    public ValueTask<int> ReadInt32Async(CancellationToken cancellationToken) => ReadFixedAsync(4, DecodeInt32, cancellationToken);

    /// <summary>Reads a little-endian unsigned 64-bit integer.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The value read.</returns>
    public ValueTask<ulong> ReadUInt64Async(CancellationToken cancellationToken) => ReadFixedAsync(8, DecodeUInt64, cancellationToken);

    /// <summary>Reads a little-endian signed 64-bit integer.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The value read.</returns>
    public ValueTask<long> ReadInt64Async(CancellationToken cancellationToken) => ReadFixedAsync(8, DecodeInt64, cancellationToken);

    /// <summary>Reads a little-endian unsigned 128-bit integer.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The value read.</returns>
    public ValueTask<UInt128> ReadUInt128Async(CancellationToken cancellationToken) => ReadFixedAsync(16, DecodeUInt128, cancellationToken);

    /// <summary>Reads a little-endian signed 128-bit integer.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The value read.</returns>
    public ValueTask<Int128> ReadInt128Async(CancellationToken cancellationToken) => ReadFixedAsync(16, DecodeInt128, cancellationToken);

    /// <summary>Reads a little-endian unsigned 256-bit integer.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The value read.</returns>
    public ValueTask<UInt256> ReadUInt256Async(CancellationToken cancellationToken) => ReadFixedAsync(UInt256.Size, DecodeUInt256, cancellationToken);

    /// <summary>Reads a little-endian signed 256-bit integer.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The value read.</returns>
    public ValueTask<Int256> ReadInt256Async(CancellationToken cancellationToken) => ReadFixedAsync(Int256.Size, DecodeInt256, cancellationToken);

    /// <summary>Reads a little-endian IEEE 754 single-precision float.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The value read.</returns>
    public ValueTask<float> ReadFloat32Async(CancellationToken cancellationToken) => ReadFixedAsync(4, DecodeFloat32, cancellationToken);

    /// <summary>Reads a little-endian IEEE 754 double-precision float.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The value read.</returns>
    public ValueTask<double> ReadFloat64Async(CancellationToken cancellationToken) => ReadFixedAsync(8, DecodeFloat64, cancellationToken);

    /// <inheritdoc/>
    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        disposed = true;
        if (ownsBuffer)
        {
            buffer.Dispose();
        }
    }

    /// <summary>Reads a VarUInt string-length prefix and validates it against the supported maximum.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The length in bytes.</returns>
    /// <exception cref="InvalidDataException">The declared length exceeds <see cref="MaxStringLength"/>.</exception>
    private async ValueTask<int> ReadStringLengthAsync(CancellationToken cancellationToken)
    {
        ulong length = await ReadVarUIntAsync(cancellationToken).ConfigureAwait(false);
        if (length > MaxStringLength)
        {
            throw new InvalidDataException($"String length {length} exceeds the supported maximum of {MaxStringLength} bytes (corrupt stream).");
        }

        return (int)length;
    }

    /// <summary>
    /// Ensures <paramref name="size"/> bytes are buffered, then decodes them from a single contiguous span.
    /// Centralizing the ensure-then-read pairing here means each fixed-width reader is a one-liner and can't
    /// get the contract wrong.
    /// </summary>
    /// <typeparam name="T">The value type produced by <paramref name="decode"/>.</typeparam>
    /// <param name="size">The number of bytes the value occupies on the wire.</param>
    /// <param name="decode">Decodes the value from a span of exactly <paramref name="size"/> bytes.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded value.</returns>
    private async ValueTask<T> ReadFixedAsync<T>(int size, SpanDecoder<T> decode, CancellationToken cancellationToken)
    {
        await buffer.EnsureAsync(size, cancellationToken).ConfigureAwait(false);
        return decode(buffer.ReadSpan(size));
    }

    /// <summary>Decodes a little-endian unsigned 128-bit integer from a 16-byte span.</summary>
    /// <param name="source">A span of at least 16 bytes.</param>
    /// <returns>The decoded value.</returns>
    private static UInt128 DecodeUInt128Value(ReadOnlySpan<byte> source)
        => new UInt128(
            BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(8, 8)),
            BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(0, 8)));
}
