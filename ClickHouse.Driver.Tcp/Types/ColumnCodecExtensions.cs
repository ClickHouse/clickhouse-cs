using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>Convenience helpers over <see cref="IColumnCodec"/>.</summary>
internal static class ColumnCodecExtensions
{
    /// <summary>Writes all of the column's values — rows [0, <see cref="IColumn.RowCount"/>).</summary>
    /// <param name="codec">The codec to write with.</param>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The column whose values to write; must match the codec's element type.</param>
    public static void WriteColumn(this IColumnCodec codec, ClickHouseBinaryWriter writer, IColumn column)
        => codec.WriteColumn(writer, column, 0, column.RowCount);

    /// <summary>Writes the state prefix for all of the column's rows — [0, <see cref="IColumn.RowCount"/>).</summary>
    /// <param name="codec">The codec to write with.</param>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The column whose prefix to write; must match the codec's element type.</param>
    public static void WriteStatePrefix(this IColumnCodec codec, ClickHouseBinaryWriter writer, IColumn column)
        => codec.WriteStatePrefix(writer, column, 0, column.RowCount);
<<<<<<< HEAD

    /// <summary>
    /// Writes the whole column the way the block layer does: compute the per-operation scratch, write the state
    /// prefix and body sharing it, then dispose it. Use this for a codec whose prefix and body must share state
    /// (a data-dependent prefix); the separate <see cref="WriteStatePrefix(IColumnCodec, ClickHouseBinaryWriter,
    /// IColumn)"/> / <see cref="WriteColumn(IColumnCodec, ClickHouseBinaryWriter, IColumn)"/> pair does not.
    /// </summary>
    /// <param name="codec">The codec to write with.</param>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The column to write; must match the codec's element type.</param>
    public static void WriteFull(this IColumnCodec codec, ClickHouseBinaryWriter writer, IColumn column)
    {
        // A zero-row column has no state prefix and no body — skip both, exactly as the block writer does (some
        // codecs, e.g. Nothing, treat an empty range as unwritable).
        if (column.RowCount == 0)
        {
            return;
        }

        IColumnWriteState state = codec.BeginWrite(column, 0, column.RowCount);
        try
        {
            codec.WriteStatePrefix(writer, column, 0, column.RowCount, state);
            codec.WriteColumn(writer, column, 0, column.RowCount, state);
        }
        finally
        {
            state?.Dispose();
        }
    }
=======
>>>>>>> bc9e8fd (Widen IColumnCodec.WriteStatePrefix to receive the sliced column)
}
