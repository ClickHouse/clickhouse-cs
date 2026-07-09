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
}
