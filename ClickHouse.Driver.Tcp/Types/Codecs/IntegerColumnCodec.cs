using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for a fixed-width integer column. ClickHouse writes these values little-endian and contiguously, so
/// the whole column is read in one bulk transfer into a byte buffer that becomes the column's storage — no
/// per-element decoding, and the values are reinterpreted from those bytes on access. Covers the 8- through
/// 256-bit signed and unsigned widths.
///
/// <para>
/// Little-endian only: the buffer's bytes are the wire bytes, reinterpreted as <typeparamref name="T"/>. Every
/// runtime .NET targets is little-endian; that invariant is assumed here (not re-checked per column). TODO: a
/// single big-endian guard at the client entry point is not yet in place — until it lands, running on a
/// big-endian host would silently mis-decode rather than fail fast.
/// </para>
/// </summary>
/// <typeparam name="T">The CLR type the ClickHouse integer maps to.</typeparam>
internal sealed class IntegerColumnCodec<T> : IColumnCodec
    where T : unmanaged
{
    /// <summary>Initializes a new instance of the <see cref="IntegerColumnCodec{T}"/> class.</summary>
    /// <param name="typeName">The canonical ClickHouse type name (e.g. <c>Int32</c>).</param>
    public IntegerColumnCodec(string typeName) => TypeName = typeName;

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int? FixedRowByteSize => Unsafe.SizeOf<T>();

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            return new PrimitiveColumn<T>(columnName, columnType, Array.Empty<byte>(), length: 0, pooled: false);
        }

        int byteCount = checked(rowCount * Unsafe.SizeOf<T>());
        byte[] rented = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            await reader.ReadBytesAsync(rented.AsMemory(0, byteCount), cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // The column never took ownership of the rent, so return it rather than leak it on a read failure.
            ArrayPool<byte>.Shared.Return(rented);
            throw;
        }

        return new PrimitiveColumn<T>(columnName, columnType, rented, byteCount, pooled: true);
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<T>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        writer.WriteBytes(MemoryMarshal.AsBytes(((IColumn<T>)column).Values.Slice(start, length)));
    }
}
