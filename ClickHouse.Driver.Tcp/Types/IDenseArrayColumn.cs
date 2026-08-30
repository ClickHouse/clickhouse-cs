namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Marks an array column in the wire's own layout — the flat elements plus the per-row offsets of
/// <see cref="IArrayColumn"/> — whose invariants this assembly has checked: offsets starting at zero, never
/// decreasing, and ending at the element column's row count. The <c>Array(T)</c> codec re-emits such a column
/// with no rebuilding, whatever CLR element type it carries.
///
/// <para>
/// Internal on purpose. The public <see cref="IArrayColumn"/> says what a column exposes, not where it came
/// from, so a caller could implement it with offsets that address nothing; the zero-copy write path accepts only
/// the shapes built here (a decoded column, or one from
/// <see cref="ClickHouseTcpColumn.CreateArray{TElement}(string, IColumn{TElement}, int[])"/>, which validates).
/// </para>
/// </summary>
internal interface IDenseArrayColumn : IArrayColumn
{
}
