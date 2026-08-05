using System;
using System.Buffers;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>FixedString(N)</c> column: every row is exactly <c>N</c> bytes with no
/// length prefix, so the column body is <c>num_rows * N</c> contiguous bytes. The rows are read in one bulk
/// transfer into a pooled blob and surfaced as a <see cref="FixedStringColumn"/> (each row a <see cref="byte"/>
/// array). On write, a row's bytes are emitted verbatim and must be exactly <c>N</c>: a longer value is
/// rejected, matching the server, which errors on an over-length value rather than truncating, and a shorter
/// one is rejected too rather than zero-padded — padding would silently rewrite the caller's data, hiding the
/// bug that produced a wrong-width value. This matches the HTTP path, where
/// <c>ClickHouse.Driver.Types.FixedStringType</c> likewise requires a <see cref="byte"/> array to be exactly
/// <c>N</c> bytes.
/// </summary>
internal sealed class FixedStringColumnCodec : IColumnCodec, ISpanWritableCodec<byte[]>
{
    private readonly int size;

    // N zero bytes, shared: the write path only ever reads it, and it is the exact width a null position must
    // advance the values stream by. Built on first use, not in the constructor: a codec is resolved per column per
    // block, so a pure read of a wide FixedString would otherwise allocate an N-byte buffer per block that only the
    // Nullable write path ever touches. Single-consumer per connection, so the lazy fill needs no synchronization.
    private byte[] nullPlaceholder;

    private FixedStringColumnCodec(int size, string typeName)
    {
        this.size = size;
        TypeName = typeName;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType => typeof(byte[]);

    /// <summary>
    /// The placeholder for a null row is <c>N</c> zero bytes, so the values stream stays aligned at a
    /// <c>Nullable(FixedString(N))</c> null position — the width every row occupies.
    /// </summary>
    public object NullPlaceholder => nullPlaceholder ??= new byte[size];

    /// <summary>Builds a <c>FixedString(N)</c> codec from its type node's single integer length argument.</summary>
    /// <param name="node">The parsed <c>FixedString</c> type node.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The type does not have exactly one positive integer length argument.</exception>
    public static FixedStringColumnCodec Create(TypeNode node)
    {
        if (node.Arguments.Count != 1)
        {
            throw new FormatException($"FixedString type '{node}' must have exactly one length argument.");
        }

        string token = node.Arguments[0].Name.Trim();
        if (!int.TryParse(token, NumberStyles.None, CultureInfo.InvariantCulture, out int size) || size <= 0)
        {
            throw new FormatException($"FixedString type '{node}' has an invalid length '{token}'; expected a positive integer.");
        }

        return new FixedStringColumnCodec(size, node.ToString());
    }

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        if (rowCount == 0)
        {
            return new FixedStringColumn(columnName, columnType, size, Array.Empty<byte>(), rowCount: 0, pooled: false);
        }

        int byteCount = checked(rowCount * size);
        byte[] blob = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            await reader.ReadBytesAsync(blob.AsMemory(0, byteCount), cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // The column never took ownership of the rent, so return it rather than leak it on a read failure.
            ArrayPool<byte>.Shared.Return(blob);
            throw;
        }

        return new FixedStringColumn(columnName, columnType, size, blob, rowCount, pooled: true);
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<byte[]>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        // A dense FixedStringColumn of this width already holds its rows back-to-back at the stride the wire uses,
        // so the whole range is one contiguous blit — no per-row byte[] materialized through the IColumn<byte[]>
        // indexer, and no per-row width check, since every row is N bytes by construction. This is the hot path
        // when re-inserting a value read straight back. A dense column of a *different* width is not a valid body
        // for this type, so it falls through to the per-row path, which rejects each row with the width message
        // rather than silently blitting a mis-strided run. A scattered write-path view (a nullable substitute, a
        // Tuple field) has no contiguous run either, so it reads each row through the indexer.
        if (column is FixedStringColumn dense && dense.Size == size)
        {
            writer.WriteBytes(dense.GetBytes(start, length));
            return;
        }

        var typed = (IColumn<byte[]>)column;
        for (int i = 0; i < length; i++)
        {
            WriteValue(writer, typed[start + i], start + i, "row");
        }
    }

    /// <inheritdoc/>
    // Each value is its own fixed-width byte run, so a run of values is written in order. These are the jagged
    // per-element arrays of one Array(FixedString(N)) row, so there is no contiguous run to blit here.
    public void WriteValues(ClickHouseBinaryWriter writer, ReadOnlySpan<byte[]> values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            WriteValue(writer, values[i], i, "element");
        }
    }

    // Emits one value's bytes verbatim. It must be exactly N bytes — a shorter one is rejected rather than
    // zero-padded, so a wrong-width value surfaces as an error instead of silently reaching the server rewritten.
    // A null is rejected too: a FixedString row is never null, Nullable carries that and substitutes the
    // placeholder at a null position so this never sees one.
    //
    // Rejecting is now the only signal a caller gets, so both messages carry the offending position: its row in a
    // column, or its index within the row's array when the values come from an Array(FixedString(N)). The noun is
    // a literal at each call site, so naming it costs nothing on the path that does not throw.
    private void WriteValue(ClickHouseBinaryWriter writer, byte[] value, int position, string positionNoun)
    {
        if (value is null)
        {
            throw new ArgumentException(
                $"A {TypeName} column cannot hold a null value (at {positionNoun} {position}); wrap the type in Nullable to write nulls.",
                nameof(value));
        }

        if (value.Length != size)
        {
            throw new ArgumentException(
                $"A {TypeName} value at {positionNoun} {position} is {value.Length} bytes; every value must be exactly {size} bytes. Resize it to {size} bytes before writing it — the write path will not pad or truncate, since doing so would silently alter the data.",
                nameof(value));
        }

        writer.WriteBytes(value);
    }
}
