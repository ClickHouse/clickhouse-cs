using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Provides labels for a decoded <c>Enum8</c> or <c>Enum16</c> column. The ordinary column surface exposes the
/// signed ordinals; this interface widens both storage widths to <see cref="long"/>. Use
/// <c>Block.ReadAs&lt;string&gt;</c> to read every row as a label.
/// </summary>
public interface IEnumColumn : IColumn
{
    /// <summary>
    /// The declared label and ordinal pairs in the server's canonical order. The list is owned and may outlive
    /// the block.
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
