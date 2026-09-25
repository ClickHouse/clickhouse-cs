namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Marks a dense array whose offsets were validated by this assembly. The array codec can re-emit this shape
/// without rebuilding it; arbitrary public <see cref="IArrayColumn"/> implementations are not trusted.
/// </summary>
internal interface IDenseArrayColumn : IArrayColumn
{
}
