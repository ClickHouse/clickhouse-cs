using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for any fixed-width column whose CLR type is a direct little-endian reinterpret of the wire bytes —
/// the integers (8- through 256-bit, signed and unsigned), the IEEE-754 floats (<c>Float32</c>/<c>Float64</c>),
/// <c>Bool</c>, and the <c>Interval*</c> family (a raw <c>Int64</c> count). ClickHouse writes these values
/// little-endian and contiguously, so the whole column is read in one bulk transfer into a byte buffer that
/// becomes the column's storage — no per-element decoding, and the values are reinterpreted from those bytes on
/// access.
///
/// <para>
/// Little-endian only: the buffer's bytes are the wire bytes, reinterpreted as <typeparamref name="T"/>. Every
/// runtime .NET targets is little-endian; the connection refuses to open on a big-endian host, so that invariant
/// holds by the time any column is read and is not re-checked per column.
/// </para>
/// </summary>
/// <typeparam name="T">The CLR type the ClickHouse value maps to.</typeparam>
internal sealed class FixedWidthColumnCodec<T> : IColumnCodec
    where T : unmanaged
{
    /// <summary>Initializes a new instance of the <see cref="FixedWidthColumnCodec{T}"/> class.</summary>
    /// <param name="typeName">The canonical ClickHouse type name (e.g. <c>Int32</c>, <c>Float64</c>).</param>
    public FixedWidthColumnCodec(string typeName) => TypeName = typeName;

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => typeof(T);

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
    // The column that a query of the type reads.
    public bool CanWrite(IColumn column) => column is PrimitiveColumn<T>;

    /// <inheritdoc/>
    // The stored bytes are the wire bytes, so the slice is one copy.
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
        => writer.WriteBytes(MemoryMarshal.AsBytes(((PrimitiveColumn<T>)column).Values.Slice(start, length)));
}
