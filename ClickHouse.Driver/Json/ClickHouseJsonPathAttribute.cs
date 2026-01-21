using System;

namespace ClickHouse.Driver.Json;

/// <summary>
/// Specifies a custom JSON path for a property when serializing to ClickHouse JSON type.
/// </summary>
/// <remarks>
/// Use this attribute to map a .NET property to a different path in the JSON structure.
/// For example, [ClickHouseJsonPath("user.name")] would serialize the property to the nested path "user.name".
/// </remarks>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public sealed class ClickHouseJsonPathAttribute : Attribute
{
    /// <summary>
    /// Gets the JSON path to use for this property.
    /// </summary>
    public string Path { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseJsonPathAttribute"/> class.
    /// </summary>
    /// <param name="path">The JSON path to use for the property.</param>
    public ClickHouseJsonPathAttribute(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("Path cannot be null or empty.", nameof(path));

        Path = path;
    }
}
