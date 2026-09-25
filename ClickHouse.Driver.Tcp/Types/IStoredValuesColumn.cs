namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Marks a column whose <see cref="IColumn{T}.Values"/> already exists as stored data and can be read without
/// materializing or caching the whole column.
/// </summary>
internal interface IStoredValuesColumn
{
}
