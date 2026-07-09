using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A fixed-width column backed by the raw little-endian wire bytes, reinterpreted as <typeparamref name="T"/>
/// on access with no copy. ClickHouse writes these values contiguously little-endian — the exact in-memory
/// layout of a <typeparamref name="T"/> span on a little-endian host — so the decoded bytes need no
/// transformation. <see cref="Values"/> is a borrowed view over those bytes.
///
/// <para>
/// The backing buffer is normally rented from the array pool and returned on <see cref="Dispose"/>; the buffer
/// may be larger than the data, so the logical length is tracked separately and <see cref="Values"/> slices to
/// it. Once disposed the buffer must not be read.
/// </para>
/// </summary>
/// <typeparam name="T">The CLR type the ClickHouse integer maps to.</typeparam>
internal sealed class PrimitiveColumn<T> : IColumn<T>
    where T : unmanaged
{
    private readonly int length;
    private readonly bool pooled;
    private byte[] buffer;

    /// <summary>Initializes a column over a backing buffer.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="buffer">The little-endian column bytes (may be longer than <paramref name="length"/>).</param>
    /// <param name="length">The logical byte length; must be a whole multiple of <c>sizeof(T)</c>.</param>
    /// <param name="pooled">Whether <paramref name="buffer"/> was rented and should be returned on dispose.</param>
    /// <exception cref="ArgumentNullException"><paramref name="buffer"/> is null.</exception>
    public PrimitiveColumn(string name, string typeName, byte[] buffer, int length, bool pooled)
    {
        Name = name;
        TypeName = typeName;
        this.buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
        this.length = length;
        this.pooled = pooled;
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => length / Unsafe.SizeOf<T>();

    /// <inheritdoc/>
    public ReadOnlySpan<T> Values => MemoryMarshal.Cast<byte, T>(buffer.AsSpan(0, length));

    /// <inheritdoc/>
    public T this[int row] => Values[row];

    /// <inheritdoc/>
    public object GetValue(int row) => Values[row];

    /// <inheritdoc/>
    public void Dispose()
    {
        if (pooled && buffer.Length != 0)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        buffer = Array.Empty<byte>();
    }

    /// <summary>Builds a self-owned (unpooled) column by copying <paramref name="values"/> into a backing buffer.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The ClickHouse type string.</param>
    /// <param name="values">The values to store.</param>
    /// <returns>The column.</returns>
    public static PrimitiveColumn<T> FromValues(string name, string typeName, ReadOnlySpan<T> values)
    {
        byte[] bytes = MemoryMarshal.AsBytes(values).ToArray();
        return new PrimitiveColumn<T>(name, typeName, bytes, bytes.Length, pooled: false);
    }
}
