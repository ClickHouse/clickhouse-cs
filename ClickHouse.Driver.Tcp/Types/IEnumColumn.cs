using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// The read surface of a decoded <c>Enum8</c> or <c>Enum16</c> column: the values ride the wire as their
/// underlying signed ordinal, and the labels come from the type's declaration. The column's
/// <see cref="IColumn{T}"/> surface is that raw ordinal (<c>IColumn&lt;sbyte&gt;</c> for <c>Enum8</c>,
/// <c>IColumn&lt;short&gt;</c> for <c>Enum16</c>), so this interface is where the labels are, and it is the only
/// route to them that does not re-parse <see cref="IColumn.TypeName"/>.
///
/// <para>
/// The ordinals are widened to <see cref="long"/> here so the interface is not generic over the storage width:
/// which of the two it is says nothing a caller needs. Reading a whole column of labels is
/// <c>block.ReadAs&lt;string&gt;(name)</c>; this interface is for the map itself — labelling one row, filtering
/// rows on the ordinal a label maps to (which touches the label once rather than per row), or printing the
/// declaration.
/// </para>
///
/// <para>
/// Obtain it by pattern-matching a column, e.g. <c>if (column is IEnumColumn labelled)</c>. A
/// <c>Nullable(Enum8)</c> column matches through <see cref="INullableColumn.Inner"/>, and an <c>Array(Enum8)</c>
/// through <see cref="IArrayColumn.Inner"/>.
/// </para>
/// </summary>
public interface IEnumColumn : IColumn
{
    /// <summary>
    /// The declared members, each label paired with its ordinal, in the order the column's type string lists them.
    /// That is not necessarily the order a <c>CREATE TABLE</c> or a <c>CAST</c> wrote: the server canonicalizes an
    /// enum's members, so <c>Enum8('b' = 2, 'a' = 1)</c> arrives as <c>Enum8('a' = 1, 'b' = 2)</c> and reports
    /// <c>a</c> before <c>b</c>. Owned by the client and safe to retain past the block, unlike the column's values.
    /// </summary>
    IReadOnlyList<KeyValuePair<string, long>> Members { get; }

    /// <summary>The label of the value at <paramref name="row"/>.</summary>
    /// <param name="row">The zero-based row index.</param>
    /// <returns>That row's label.</returns>
    /// <exception cref="KeyNotFoundException">
    /// The row holds an ordinal the type declares no member for. Every row of a column read from the server is a
    /// declared ordinal, so this is a guard rather than a case to handle; use <see cref="TryGetLabel"/> to ask
    /// about an ordinal that did not come from a row.
    /// </exception>
    string GetLabel(int row);

    /// <summary>Finds the label an ordinal is declared with.</summary>
    /// <param name="ordinal">The underlying ordinal.</param>
    /// <param name="label">That ordinal's label, or null when the type declares no member for it.</param>
    /// <returns>Whether the type declares a member with that ordinal.</returns>
    bool TryGetLabel(long ordinal, out string label);

    /// <summary>
    /// Finds the ordinal a label is declared with — the value to compare the raw
    /// <see cref="IColumn{T}.Values"/> against when filtering or grouping rows by label.
    /// </summary>
    /// <param name="label">The label, matched ordinally (ClickHouse enum labels are case-sensitive).</param>
    /// <param name="ordinal">That label's ordinal, or 0 when the type declares no member with it.</param>
    /// <returns>Whether the type declares a member with that label.</returns>
    /// <exception cref="System.ArgumentNullException"><paramref name="label"/> is null.</exception>
    bool TryGetOrdinal(string label, out long ordinal);
}
