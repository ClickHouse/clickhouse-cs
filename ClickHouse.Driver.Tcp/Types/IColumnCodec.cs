using System;
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

    /// <summary>
    /// The exact number of value bytes <see cref="WriteColumn"/> emits per row when that size is identical for
    /// every row (fixed-width types), or null when a row's size varies (e.g. <c>String</c>). The insert
    /// splitter uses this to price fixed-width columns in O(1) and walk only the variable-width ones per row.
    /// </summary>
    int? FixedRowByteSize => null;

    /// <summary>
    /// The exact number of value bytes <see cref="WriteColumn"/> emits for row <paramref name="row"/> of
    /// <paramref name="column"/> (state prefix and header excluded). Fixed-width codecs inherit this from
    /// <see cref="FixedRowByteSize"/>; a variable-width codec overrides it to measure the value.
    /// </summary>
    /// <param name="column">The column being measured.</param>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>The encoded byte length of that row's value.</returns>
    long MeasureRowBytes(IColumn column, int row)
        => FixedRowByteSize ?? throw new NotSupportedException(
            $"The '{TypeName}' codec has a variable per-row size and must override {nameof(MeasureRowBytes)}.");

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

    /// <summary>
    /// Whether <see cref="WriteColumn"/> accepts <paramref name="column"/>'s CLR element type. A codec may
    /// accept several (e.g. a date-time codec taking both <see cref="System.DateTime"/> and
    /// <see cref="System.DateTimeOffset"/>), so this is a membership test. Inserts check it up front so a bad
    /// column type is a clear error rather than a mid-write cast failure.
    /// </summary>
    /// <param name="column">The column to test.</param>
    /// <returns><see langword="true"/> if <see cref="WriteColumn"/> accepts <paramref name="column"/>.</returns>
    bool CanWrite(IColumn column);

    /// <summary>Writes the column's serialization state prefix, if any. Default: none.</summary>
    /// <param name="writer">The writer to encode into.</param>
    void WriteStatePrefix(ClickHouseBinaryWriter writer)
    {
    }

    /// <summary>
    /// Writes rows [<paramref name="start"/>, <paramref name="start"/> + <paramref name="length"/>) of the
    /// column, slicing <see cref="IColumn{T}.Values"/> directly so a large insert splits into bounded blocks
    /// with no copying. To write the whole column use the
    /// <see cref="ColumnCodecExtensions.WriteColumn(IColumnCodec, ClickHouseBinaryWriter, IColumn)"/> overload.
    /// </summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The column whose values to write; must match this codec's element type.</param>
    /// <param name="start">The zero-based first row to write.</param>
    /// <param name="length">The number of rows to write.</param>
    void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length);
}
