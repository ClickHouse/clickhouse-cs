using System;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// <c>Tuple()</c> from <see cref="ValueTuple"/>: one placeholder byte for each value, as for
/// <see cref="EmptyTupleColumnCodec"/>. The value has no data, so a position that the source marks gets the same byte.
/// </summary>
internal sealed class EmptyTupleWriter : ColumnWriter<ValueTuple>
{
    /// <summary>The writer. It holds no state.</summary>
    public static readonly EmptyTupleWriter Instance = new();

    private EmptyTupleWriter()
    {
    }

    /// <inheritdoc/>
    public override bool IsFlat => true;

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<ValueTuple> values, IColumnWriteState state)
    {
        for (int i = 0; i < values.Count; i++)
        {
            writer.WriteByte(EmptyTupleColumnCodec.Placeholder);
        }
    }
}
