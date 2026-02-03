namespace ClickHouse.Driver;

/// <summary>
/// Represents a parameter for ClickHouse queries.
/// This is a simpler alternative to <see cref="ADO.ClickHouseDbParameter"/> for use with <see cref="ClickHouseClient"/>.
/// </summary>
public sealed class ClickHouseParameter
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseParameter"/> class.
    /// </summary>
    /// <param name="name">The parameter name (without @ or : prefix).</param>
    /// <param name="value">The parameter value.</param>
    /// <param name="clickHouseType">Optional ClickHouse type hint (e.g., "Int32", "String", "DateTime64(3)").</param>
    public ClickHouseParameter(string name, object? value, string? clickHouseType = null)
    {
        Name = name;
        Value = value;
        ClickHouseType = clickHouseType;
    }

    /// <summary>
    /// Gets the parameter name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the parameter value.
    /// </summary>
    public object? Value { get; }

    /// <summary>
    /// Gets the optional ClickHouse type hint for the parameter.
    /// When specified, this type is used instead of inferring from the .NET type.
    /// </summary>
    public string? ClickHouseType { get; }
}
