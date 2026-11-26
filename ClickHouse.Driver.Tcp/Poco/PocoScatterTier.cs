namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// How a scatter of <see cref="PocoColumnScatterFactory"/> runs the converter tree of a column. Both tiers give the
/// same values. When more than one row fails, the tiers can throw the failures of different rows: a value, a NULL,
/// named by its row, or the exception of a property setter.
/// </summary>
internal enum PocoScatterTier
{
    /// <summary>
    /// One compiled loop for each column, which sets the property of each row to the tree's expression for that row
    /// (<see cref="Types.Converters.ColumnReader.Emit"/>). It needs a runtime that compiles expression trees, because the
    /// loop holds span locals, which the expression interpreter cannot run.
    /// </summary>
    Emit,

    /// <summary>
    /// The tree's bulk read (<see cref="Types.Converters.BoundReader{T}.Fill"/>) into a pooled buffer, then a setter
    /// delegate for each row. It compiles no code, so a runtime without dynamic code uses it.
    /// </summary>
    Fill,
}
