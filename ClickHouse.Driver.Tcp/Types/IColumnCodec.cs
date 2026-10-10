using System;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The wire layer of one ClickHouse column type: it reads a column, and it writes a column from its storage (the
/// column that a query of the type reads). The converter layer (<see cref="Converters.ConverterDerivation"/>) gives
/// every other reading and write.
/// </summary>
internal interface IColumnCodec
{
    /// <summary>The ClickHouse type name handled by this codec.</summary>
    string TypeName { get; }

    /// <summary>The canonical CLR type returned by <see cref="ReadColumnAsync"/>.</summary>
    Type ElementType { get; }

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
    /// Whether <see cref="WriteColumn"/> writes this column: the column that a query of the type reads, or a dense
    /// composite column in the wire layout of the type whose child columns the codecs of the children write (an array
    /// that <see cref="ClickHouseTcpColumn.CreateArray{TElement}"/> builds over a decoded column). The write is from the
    /// storage of the column, with no conversion of its values, so it keeps the bytes that the server sent (the raw
    /// bytes of a <c>String</c>, a LowCardinality dictionary and its keys). The column then has the element type of the
    /// codec. Every other column goes through the converter layer. The default is false.
    /// </summary>
    /// <param name="column">The column to test.</param>
    /// <returns>Whether the codec writes the column.</returns>
    bool CanWrite(IColumn column) => false;

    /// <summary>
    /// Whether <paramref name="value"/> belongs to this type rather than to a sibling that takes the same CLR type.
    /// Asked only to break a tie between <c>Variant</c> alternatives that collide on <see cref="ElementType"/>, or that
    /// are all written from the CLR type of a value whose type is the element type of no alternative, so it is never on
    /// the path of an unambiguous write.
    ///
    /// <para>
    /// Exactly one alternative must claim the value for the tie to resolve; zero or several is a refusal. The
    /// default therefore claims everything, which is the safe answer for a codec with no value-level test: it can
    /// never win a tie on its own, so a genuine ambiguity (<c>Variant(JSON, String)</c> given a string) stays a
    /// refusal rather than becoming a silent pick.
    /// </para>
    ///
    /// <para>
    /// A codec may decline a value here that the converter of its type accepts, because the question is which
    /// alternative the value <em>means</em>, not which one could store it. <c>IPv6</c> is written from an IPv4 address
    /// by mapping it, but declines it beside an <c>IPv4</c> alternative that is the better home for it.
    /// </para>
    /// </summary>
    /// <param name="value">The non-null value being placed: of this codec's element type, or of a CLR type that the
    /// alternative is written from.</param>
    /// <returns>Whether this codec claims the value.</returns>
    bool ClaimsValue(object value) => true;

    /// <summary>Prepares state shared by the prefix and body. The caller disposes it after the body.</summary>
    /// <param name="column">The column about to be written, which <see cref="CanWrite"/> accepts.</param>
    /// <param name="start">The zero-based first row the write will cover.</param>
    /// <param name="length">The number of rows the write will cover.</param>
    /// <returns>The per-operation write state, or <see langword="null"/> when the codec needs none.</returns>
    IColumnWriteState BeginWrite(IColumn column, int start, int length) => null;

    /// <summary>Writes the serialization prefix, with the state from <see cref="BeginWrite"/>. The default writes nothing.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The column whose prefix to write, which <see cref="CanWrite"/> accepts.</param>
    /// <param name="start">The zero-based first row the following body will write.</param>
    /// <param name="length">The number of rows the following body will write.</param>
    /// <param name="state">The state from <see cref="BeginWrite"/>; null only for a codec that returns null from it.</param>
    void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
    }

    /// <summary>Writes the selected rows, with the state from <see cref="BeginWrite"/>.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The column whose values to write, which <see cref="CanWrite"/> accepts.</param>
    /// <param name="start">The zero-based first row to write.</param>
    /// <param name="length">The number of rows to write.</param>
    /// <param name="state">The state from <see cref="BeginWrite"/>; null only for a codec that returns null from it.</param>
    void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state);
}
