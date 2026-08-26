namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// How a compiled scatter reads a column value. Both tiers use the same conversion and assignment logic.
/// </summary>
internal enum PocoScatterTier
{
    /// <summary>Hoists <see cref="Types.IColumn{T}.Values"/> and indexes its span.</summary>
    Span,

    /// <summary>
    /// Reads <c>IColumn&lt;T&gt;[row]</c>; used when spans would materialize values or expression trees are interpreted.
    /// </summary>
    Indexer,
}
