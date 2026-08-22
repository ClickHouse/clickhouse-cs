namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A column whose <see cref="IColumn{T}.Values"/> is a view over storage it already holds, so reading that property
/// materializes nothing and costs nothing. The fixed-width and calendar columns are these: their values <em>are</em>
/// the decode buffer.
///
/// <para>
/// The distinction matters to anything that reads a whole column. A column without this marker builds its values on
/// first access and caches them for the block's lifetime, which pins one object per row: a <c>String</c> column
/// decodes every row's UTF-8, a <c>LowCardinality</c> column allocates one reference per row. Hoisting
/// <see cref="IColumn{T}.Values"/> out of a loop is free for a marked column and materializes the whole block for an
/// unmarked one, so a reader that only wants a window of rows should hoist for the former and index the latter.
/// </para>
///
/// <para>
/// Not the same question as <see cref="ISpanColumn{T}"/>, which asks whether the values are one contiguous run so a
/// codec can blit them. A materializing column's cache is contiguous too; what this marker says is that no cache has
/// to exist.
/// </para>
/// </summary>
internal interface IStoredValuesColumn
{
}
