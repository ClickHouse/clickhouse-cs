using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the zero-element ClickHouse <c>Tuple()</c> — a legal column type, produced by expressions such as
/// <c>tuple()</c> or a cast. It has no element streams, so it does not share the <see cref="TupleColumnCodec"/>
/// layout: like <c>Nothing</c> (see <see cref="NothingColumnCodec"/>), it occupies exactly one placeholder byte
/// per row and has no serialization state prefix. The byte carries no value — the server emits ASCII <c>'0'</c>
/// and ignores what it reads back — so it is discarded on read and every row surfaces as the one value the type
/// has, the empty <see cref="ValueTuple"/>.
///
/// <para>
/// Kept separate from <see cref="TupleColumnCodec"/>, whose body is entirely a loop over child codecs: an empty
/// tuple has no children, so the two share no code, and a tuple column could not hold the result anyway —
/// <c>TupleColumnBase</c> takes its row count from its children. The duplication that does remain is the
/// placeholder read, shared with <see cref="NothingColumnCodec"/>; merging those two is a cleanup of its own.
/// </para>
/// </summary>
internal sealed class EmptyTupleColumnCodec : IColumnCodec
{
    /// <summary>The shared, stateless instance.</summary>
    public static readonly EmptyTupleColumnCodec Instance = new();

    // Matches what the server writes. Neither side reads the value back, so this only keeps a capture of the
    // client's bytes indistinguishable from the server's.
    private const byte Placeholder = (byte)'0';

    private EmptyTupleColumnCodec()
    {
    }

    /// <inheritdoc/>
    public string TypeName => "Tuple()";

    /// <inheritdoc/>
    public Type ElementType => typeof(ValueTuple);

    /// <inheritdoc/>
    // The server rejects Nullable(Tuple(...)), so no null map ever selects a placeholder here; this satisfies the
    // interface with the type's only value.
    public object NullPlaceholder => default(ValueTuple);

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        // The placeholder bytes must be consumed to keep the stream aligned, even though they carry no value.
        if (rowCount > 0)
        {
            byte[] rented = ArrayPool<byte>.Shared.Rent(rowCount);
            try
            {
                await reader.ReadBytesAsync(rented.AsMemory(0, rowCount), cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        // Every element of a fresh ValueTuple[] already is the empty tuple, so there is nothing to fill in.
        return new ArrayColumn<ValueTuple>(columnName, columnType, new ValueTuple[rowCount]);
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<ValueTuple>;

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        // The rows hold no data, so the column contributes only its element type: this cast rejects a mismatched
        // one the way every other codec's write does.
        _ = (IColumn<ValueTuple>)column;

        for (int i = 0; i < length; i++)
        {
            writer.WriteByte(Placeholder);
        }
    }
}
