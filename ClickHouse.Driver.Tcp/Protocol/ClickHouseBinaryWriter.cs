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
/// Buffered writer for ClickHouse native-protocol wire primitives over a <see cref="Stream"/>.
/// Accumulates into an internal buffer and flushes explicitly at packet boundaries so the caller controls
/// when bytes hit the wire. Not thread-safe — one per connection.
/// </summary>
internal sealed class ClickHouseBinaryWriter : IDisposable
{
    private readonly Stream stream;
    private byte[] buffer;
    private int position;
    private bool disposed;

    /// <summary>Initializes a new writer over <paramref name="stream"/>.</summary>
    /// <param name="stream">The destination stream to write to.</param>
    /// <param name="bufferSize">Initial buffer capacity in bytes; must be at least 32. Buffer can grow if necessary.</param>
    /// <exception cref="ArgumentNullException"><paramref name="stream"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bufferSize"/> is below 32.</exception>
    public ClickHouseBinaryWriter(Stream stream, int bufferSize = 16384)
    {
        this.stream = stream ?? throw new ArgumentNullException(nameof(stream));
        if (bufferSize < 32)
        {
            throw new ArgumentOutOfRangeException(nameof(bufferSize), "Buffer must hold at least one 32-byte value.");
        }

        buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
    }

    /// <summary>The number of bytes buffered and not yet flushed.</summary>
    public int BufferedBytes => position;

    /// <summary>Writes a single byte.</summary>
    /// <param name="value">The byte to write.</param>
    public void WriteByte(byte value)
    {
        EnsureCapacity(1);
        buffer[position++] = value;
    }

    /// <summary>Writes a single-byte boolean (canonically <c>0x01</c> / <c>0x00</c>).</summary>
    /// <param name="value">The boolean to write.</param>
    public void WriteBool(bool value) => WriteByte(value ? (byte)1 : (byte)0);

    /// <summary>Writes raw bytes verbatim (no length prefix).</summary>
    /// <param name="value">The bytes to write.</param>
    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        EnsureCapacity(value.Length);
        value.CopyTo(buffer.AsSpan(position));
        position += value.Length;
    }

    /// <summary>Writes a LEB-128 variable-length unsigned integer (native-format VarUInt).</summary>
    /// <param name="value">The value to write.</param>
    public void WriteVarUInt(ulong value)
    {
        EnsureCapacity(10);
        while (value >= 0x80)
        {
            buffer[position++] = unchecked((byte)(value | 0x80));
            value >>= 7;
        }

        buffer[position++] = unchecked((byte)value);
    }

    /// <summary>
    /// Writes a client→server packet type code as the leading VarUInt of a packet envelope. At protocol
    /// version 54460 there is no chunk wrapping, so this is written straight onto the buffered stream.
    /// </summary>
    /// <param name="type">The packet type to write.</param>
    public void WriteClientPacketType(ClientPacketType type) => WriteVarUInt((ulong)type);

    /// <summary>Writes a native-format String: a VarUInt length prefix followed by the UTF-8 bytes.</summary>
    /// <param name="value">The string to write.</param>
    public void WriteString(string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        // Encode once (the length prefix needs the byte count up front, so writing straight into the buffer
        // would require a second UTF-8 pass). Small strings use the stack; larger ones a pooled scratch.
        int maxBytes = Encoding.UTF8.GetMaxByteCount(value.Length);
        if (maxBytes <= 256)
        {
            Span<byte> scratch = stackalloc byte[256];
            WriteEncodedString(value, scratch);
        }
        else
        {
            byte[] rented = ArrayPool<byte>.Shared.Rent(maxBytes);
            try
            {
                WriteEncodedString(value, rented);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    /// <summary>Writes a native-format String from raw bytes (VarUInt length prefix + bytes).</summary>
    /// <param name="value">The raw string bytes to write.</param>
    public void WriteString(ReadOnlySpan<byte> value)
    {
        WriteVarUInt((ulong)value.Length);
        WriteBytes(value);
    }

    /// <summary>Encodes <paramref name="value"/> as UTF-8 into <paramref name="scratch"/>, then writes a length-prefixed String.</summary>
    /// <param name="value">The string to encode.</param>
    /// <param name="scratch">Scratch space large enough for the UTF-8 encoding of <paramref name="value"/>.</param>
    private void WriteEncodedString(string value, Span<byte> scratch)
    {
        int written = Encoding.UTF8.GetBytes(value, scratch);
        WriteVarUInt((ulong)written);
        WriteBytes(scratch.Slice(0, written));
    }

    /// <summary>Writes an unsigned 8-bit integer.</summary>
    /// <param name="value">The value to write.</param>
    public void WriteUInt8(byte value) => WriteByte(value);

    /// <summary>Writes a signed 8-bit integer.</summary>
    /// <param name="value">The value to write.</param>
    public void WriteInt8(sbyte value) => WriteByte(unchecked((byte)value));

    /// <summary>Writes a little-endian unsigned 16-bit integer.</summary>
    /// <param name="value">The value to write.</param>
    public void WriteUInt16(ushort value)
    {
        EnsureCapacity(2);
        BinaryPrimitives.WriteUInt16LittleEndian(buffer.AsSpan(position, 2), value);
        position += 2;
    }

    /// <summary>Writes a little-endian signed 16-bit integer.</summary>
    /// <param name="value">The value to write.</param>
    public void WriteInt16(short value)
    {
        EnsureCapacity(2);
        BinaryPrimitives.WriteInt16LittleEndian(buffer.AsSpan(position, 2), value);
        position += 2;
    }

    /// <summary>Writes a little-endian unsigned 32-bit integer.</summary>
    /// <param name="value">The value to write.</param>
    public void WriteUInt32(uint value)
    {
        EnsureCapacity(4);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(position, 4), value);
        position += 4;
    }

    /// <summary>Writes a little-endian signed 32-bit integer.</summary>
    /// <param name="value">The value to write.</param>
    public void WriteInt32(int value)
    {
        EnsureCapacity(4);
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(position, 4), value);
        position += 4;
    }

    /// <summary>Writes a little-endian unsigned 64-bit integer.</summary>
    /// <param name="value">The value to write.</param>
    public void WriteUInt64(ulong value)
    {
        EnsureCapacity(8);
        BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(position, 8), value);
        position += 8;
    }

    /// <summary>Writes a little-endian signed 64-bit integer.</summary>
    /// <param name="value">The value to write.</param>
    public void WriteInt64(long value)
    {
        EnsureCapacity(8);
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(position, 8), value);
        position += 8;
    }

    /// <summary>Writes a little-endian unsigned 128-bit integer.</summary>
    /// <param name="value">The value to write.</param>
    public void WriteUInt128(UInt128 value)
    {
        EnsureCapacity(16);
        BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(position, 8), (ulong)value);
        BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(position + 8, 8), (ulong)(value >> 64));
        position += 16;
    }

    /// <summary>Writes a little-endian signed 128-bit integer.</summary>
    /// <param name="value">The value to write.</param>
    public void WriteInt128(Int128 value) => WriteUInt128(unchecked((UInt128)value));

    /// <summary>Writes a little-endian unsigned 256-bit integer.</summary>
    /// <param name="value">The value to write.</param>
    public void WriteUInt256(UInt256 value)
    {
        EnsureCapacity(UInt256.Size);
        value.WriteLittleEndian(buffer.AsSpan(position, UInt256.Size));
        position += UInt256.Size;
    }

    /// <summary>Writes a little-endian signed 256-bit integer.</summary>
    /// <param name="value">The value to write.</param>
    public void WriteInt256(Int256 value)
    {
        EnsureCapacity(Int256.Size);
        value.WriteLittleEndian(buffer.AsSpan(position, Int256.Size));
        position += Int256.Size;
    }

    /// <summary>Writes a little-endian IEEE 754 single-precision float.</summary>
    /// <param name="value">The value to write.</param>
    public void WriteFloat32(float value)
    {
        EnsureCapacity(4);
        BinaryPrimitives.WriteSingleLittleEndian(buffer.AsSpan(position, 4), value);
        position += 4;
    }

    /// <summary>Writes a little-endian IEEE 754 double-precision float.</summary>
    /// <param name="value">The value to write.</param>
    public void WriteFloat64(double value)
    {
        EnsureCapacity(8);
        BinaryPrimitives.WriteDoubleLittleEndian(buffer.AsSpan(position, 8), value);
        position += 8;
    }

    /// <summary>Flushes buffered bytes to the underlying stream and flushes the stream.</summary>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <exception cref="ObjectDisposedException">The writer has been disposed.</exception>
    public async ValueTask FlushAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        if (position > 0)
        {
            await stream.WriteAsync(buffer.AsMemory(0, position), cancellationToken).ConfigureAwait(false);
            position = 0;
        }

        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
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
    }

    /// <summary>Ensures the buffer has room for at least <paramref name="count"/> more bytes, growing it if needed.</summary>
    /// <param name="count">The number of additional bytes about to be written.</param>
    /// <exception cref="ObjectDisposedException">The writer has been disposed (checked only on the grow path).</exception>
    private void EnsureCapacity(int count)
    {
        if (position + count <= buffer.Length)
        {
            return;
        }

        // Slow path only. Guard here (not on the per-write fast path above) so a use-after-dispose can't rent a
        // fresh buffer that Dispose's short-circuit would then never return to the pool.
        ObjectDisposedException.ThrowIf(disposed, this);
        int newSize = Math.Max(buffer.Length * 2, position + count);
        byte[] grown = ArrayPool<byte>.Shared.Rent(newSize);
        Array.Copy(buffer, grown, position);
        ArrayPool<byte>.Shared.Return(buffer);
        buffer = grown;
    }
}
