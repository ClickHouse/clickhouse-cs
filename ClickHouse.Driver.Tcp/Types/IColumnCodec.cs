using System;
using System.Collections.Generic;
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
    /// The CLR element type the decoded column surfaces — the <c>T</c> of the <see cref="IColumn{T}"/> that
    /// <see cref="ReadColumnAsync"/> produces (e.g. <see cref="ulong"/> for <c>UInt64</c>, <see cref="string"/>
    /// for <c>String</c>, <see cref="System.DateTimeOffset"/> for <c>DateTime</c>). Composite codecs consult a
    /// child codec's element type to build the right typed wrapper column (e.g. <c>Nullable(T)</c> surfaces
    /// <c>T?</c> for a value-type inner and the nullable reference for a reference-type inner). A codec may still
    /// <see cref="CanWrite"/> more CLR types than this on the write path (see <see cref="WritableElementTypes"/>);
    /// this is the one it reads back.
    /// </summary>
    Type ElementType { get; }

    /// <summary>
    /// The CLR element types <see cref="WriteColumn"/> accepts, in preference order (the canonical
    /// <see cref="ElementType"/> first). Defaults to just <see cref="ElementType"/>; a codec that also takes
    /// convenience write types (e.g. a date-time codec accepting <see cref="System.DateTime"/> as well as
    /// <see cref="System.DateTimeOffset"/>) lists them all here, so a composite such as <c>Nullable(T)</c> can
    /// re-offer the same write types through its own write path. Every type listed must be answerable by both
    /// <see cref="CanWrite"/> and <see cref="NullPlaceholderAs"/>.
    /// </summary>
    IReadOnlyList<Type> WritableElementTypes => new[] { ElementType };

    /// <summary>
    /// A value of <see cref="ElementType"/> to encode where a row has no value of its own — the placeholder
    /// written at the null positions of a <c>Nullable(T)</c> column's values stream. The server ignores those
    /// bytes, but the codec must still be handed a value it accepts, so this is the type's canonical zero/epoch
    /// (e.g. <c>0</c>, <c>1970-01-01</c>, <c>0.0.0.0</c>, the empty string) rather than the CLR default, which a
    /// range-checked type would reject. A codec whose values cannot be written throws when this is read.
    /// </summary>
    object NullPlaceholder { get; }

    /// <summary>
    /// <see cref="NullPlaceholder"/> expressed in <paramref name="writeType"/> — one of
    /// <see cref="WritableElementTypes"/>. A composite filling a placeholder buffer needs the placeholder in the
    /// same CLR write type as the buffer it materializes (e.g. a <c>Nullable(DateTime)</c> written as
    /// <see cref="System.DateTime"/> needs a <see cref="System.DateTime"/> placeholder, not the canonical
    /// <see cref="System.DateTimeOffset"/> one). Defaults to <see cref="NullPlaceholder"/> for the canonical
    /// <see cref="ElementType"/> and throws for any other write type; a codec advertising extra
    /// <see cref="WritableElementTypes"/> overrides this to answer each of them.
    /// </summary>
    /// <param name="writeType">The CLR write type to express the placeholder in.</param>
    /// <returns>The placeholder value, assignable to <paramref name="writeType"/>.</returns>
    /// <exception cref="NotSupportedException"><paramref name="writeType"/> is not a writable element type.</exception>
    object NullPlaceholderAs(Type writeType) => writeType == ElementType
        ? NullPlaceholder
        : throw new NotSupportedException($"The '{TypeName}' codec has no null placeholder for {writeType}.");

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

    /// <summary>
    /// Computes any per-operation scratch this codec needs to write rows [<paramref name="start"/>,
    /// <paramref name="start"/> + <paramref name="length"/>), shared across the following state-prefix and body
    /// phases so a data-dependent prefix or an element-flattening composite does its discovery/flatten exactly
    /// once. Default: none — the codec's prefix is absent or a fixed constant, so the phases share nothing.
    /// When non-null, the returned state is passed to the state-aware
    /// <see cref="WriteStatePrefix(ClickHouseBinaryWriter, IColumn, int, int, IColumnWriteState)"/> and
    /// <see cref="WriteColumn(ClickHouseBinaryWriter, IColumn, int, int, IColumnWriteState)"/> overloads, and the
    /// caller disposes it after the body.
    /// </summary>
    /// <param name="column">The column about to be written; must match this codec's element type.</param>
    /// <param name="start">The zero-based first row the write will cover.</param>
    /// <param name="length">The number of rows the write will cover.</param>
    /// <returns>The per-operation write state, or <see langword="null"/> when the codec needs none.</returns>
    IColumnWriteState BeginWrite(IColumn column, int start, int length) => null;

    /// <summary>
    /// Computes any per-operation scratch this codec needs to write rows [<paramref name="start"/>,
    /// <paramref name="start"/> + <paramref name="length"/>), shared across the following state-prefix and body
    /// phases so a data-dependent prefix or an element-flattening composite does its discovery/flatten exactly
    /// once. Default: none — the codec's prefix is absent or a fixed constant, so the phases share nothing.
    /// When non-null, the returned state is passed to the state-aware
    /// <see cref="WriteStatePrefix(ClickHouseBinaryWriter, IColumn, int, int, IColumnWriteState)"/> and
    /// <see cref="WriteColumn(ClickHouseBinaryWriter, IColumn, int, int, IColumnWriteState)"/> overloads, and the
    /// caller disposes it after the body.
    /// </summary>
    /// <param name="column">The column about to be written; must match this codec's element type.</param>
    /// <param name="start">The zero-based first row the write will cover.</param>
    /// <param name="length">The number of rows the write will cover.</param>
    /// <returns>The per-operation write state, or <see langword="null"/> when the codec needs none.</returns>
    IColumnWriteState BeginWrite(IColumn column, int start, int length) => null;

    /// <summary>
    /// Writes the column's serialization state prefix for rows [<paramref name="start"/>,
    /// <paramref name="start"/> + <paramref name="length"/>), if any. Default: none. The slice is supplied
    /// because a type whose prefix is data-dependent (its bytes derive from the values themselves) must see the
    /// same rows the following body will write, and each block's prefix reflects only that block's rows. Types
    /// whose prefix is a fixed constant — or absent — ignore these arguments.
    /// </summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The column whose prefix to write; must match this codec's element type.</param>
    /// <param name="start">The zero-based first row the following body will write.</param>
    /// <param name="length">The number of rows the following body will write.</param>
    void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
    }

    /// <summary>
    /// Writes the state prefix reusing the scratch from <see cref="BeginWrite"/>. Defaults to the state-free
    /// <see cref="WriteStatePrefix(ClickHouseBinaryWriter, IColumn, int, int)"/>; a codec that returns non-null
    /// from <see cref="BeginWrite"/> overrides this to emit its data-dependent prefix from the shared state.
    /// </summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The column whose prefix to write; must match this codec's element type.</param>
    /// <param name="start">The zero-based first row the following body will write.</param>
    /// <param name="length">The number of rows the following body will write.</param>
    /// <param name="state">The scratch from <see cref="BeginWrite"/>, or <see langword="null"/>.</param>
    void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
        => WriteStatePrefix(writer, column, start, length);

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

    /// <summary>
    /// Writes the column body reusing the scratch from <see cref="BeginWrite"/>. Defaults to the state-free
    /// <see cref="WriteColumn(ClickHouseBinaryWriter, IColumn, int, int)"/>; a codec that returns non-null from
    /// <see cref="BeginWrite"/> overrides this to write its body (discriminators, flattened elements) from the
    /// shared state instead of recomputing it.
    /// </summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The column whose values to write; must match this codec's element type.</param>
    /// <param name="start">The zero-based first row to write.</param>
    /// <param name="length">The number of rows to write.</param>
    /// <param name="state">The scratch from <see cref="BeginWrite"/>, or <see langword="null"/>.</param>
    void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
        => WriteColumn(writer, column, start, length);
}
