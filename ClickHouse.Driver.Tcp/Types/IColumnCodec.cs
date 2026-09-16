using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Reads and writes one ClickHouse column type over the wire. A codec is resolved from a type string by the
/// registry and knows how to turn <c>num_rows</c> wire values into an <see cref="IColumn"/> and back.
///
/// <para>
/// A column may carry a serialization state prefix before its values (used by dictionary-bearing types such
/// as LowCardinality). Most types have none, so the prefix hooks default to no-ops; composite codecs that own
/// child codecs recurse into them. The block layer always calls the prefix hook before the value read/write,
/// so a codec that needs a prefix can rely on the ordering without the block layer knowing which types do.
/// </para>
/// </summary>
internal interface IColumnCodec
{
    /// <summary>The canonical base type name this codec handles (e.g. <c>UInt64</c>, <c>String</c>).</summary>
    string TypeName { get; }

    /// <summary>Reads the column's serialization state prefix, if any. Default: none.</summary>
    /// <param name="reader">The reader positioned at the prefix.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>A task that completes when the prefix has been consumed.</returns>
    ValueTask ReadStatePrefixAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken) => default;

    /// <summary>Reads exactly <paramref name="rowCount"/> values into a column.</summary>
    /// <param name="reader">The reader positioned at the column body.</param>
    /// <param name="columnName">The column name from the block header.</param>
    /// <param name="columnType">The full ClickHouse type string from the block header (stamped onto the column).</param>
    /// <param name="rowCount">The number of values to read.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    /// <returns>The decoded column.</returns>
    ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken);

    /// <summary>Writes the column's serialization state prefix, if any. Default: none.</summary>
    /// <param name="writer">The writer to encode into.</param>
    void WriteStatePrefix(ClickHouseBinaryWriter writer)
    {
    }

    /// <summary>Writes all of the column's values.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The column whose values to write; must match this codec's element type.</param>
    void WriteColumn(ClickHouseBinaryWriter writer, IColumn column);
}
