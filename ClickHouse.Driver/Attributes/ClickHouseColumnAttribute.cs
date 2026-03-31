using System;

namespace ClickHouse.Driver;

/// <summary>
/// Configures how a property maps to a ClickHouse column during binary insert operations.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class ClickHouseColumnAttribute : Attribute
{
    /// <summary>
    /// Gets or sets the ClickHouse column name this property maps to.
    /// When not set, the property name is used as-is (case-sensitive match).
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// Gets or sets the explicit ClickHouse type string for this column (e.g. "Nullable(String)", "UInt64").
    /// When all mapped properties specify a type, the schema probe query is skipped entirely.
    /// </summary>
    public string Type { get; set; }
}
