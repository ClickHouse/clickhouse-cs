namespace ClickHouse.Driver.Copy;

/// <summary>
/// Cached metadata for a single POCO property used in binary insert operations.
/// </summary>
internal sealed class BinaryInsertPropertyInfo
{
    /// <summary>
    /// Gets the ClickHouse column name this property maps to.
    /// </summary>
    public string ColumnName { get; init; }

    /// <summary>
    /// Gets the explicit ClickHouse type string from <see cref="ClickHouseColumnAttribute.Type"/>, or null if not specified.
    /// </summary>
    public string ExplicitClickHouseType { get; init; }
}
