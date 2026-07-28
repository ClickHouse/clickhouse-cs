using System;
using System.Reflection;

namespace ClickHouse.Driver.Poco;

/// <summary>
/// Cached metadata for a single POCO property used in binary insert and read operations.
/// </summary>
internal sealed class PocoPropertyInfo
{
    /// <summary>
    /// Gets the reflected property, used to compile typed property-access expressions
    /// (e.g. the box-free insert write delegates in <see cref="PocoWriteExpressionFactory"/>).
    /// </summary>
    public PropertyInfo Property { get; init; }

    /// <summary>
    /// Gets the ClickHouse column name this property maps to.
    /// </summary>
    public string ColumnName { get; init; }

    /// <summary>
    /// Gets the explicit ClickHouse type string from <see cref="ClickHouseColumnAttribute.Type"/>, or null if not specified.
    /// </summary>
    public string ExplicitClickHouseType { get; init; }

    /// <summary>
    /// Gets the CLR property name (e.g. "UserId").
    /// </summary>
    public string PropertyName { get; init; }

    /// <summary>
    /// Gets the CLR property type (e.g. <see cref="long"/>, <see cref="string"/>, <see cref="Nullable{Int32}"/>).
    /// </summary>
    public Type PropertyType { get; init; }

    /// <summary>
    /// Gets the underlying type for a <see cref="Nullable{T}"/> property, or null when
    /// <see cref="PropertyType"/> is not a nullable value type.
    /// </summary>
    public Type NullableUnderlyingType { get; init; }

    /// <summary>
    /// Gets whether null/<see cref="DBNull"/> can be assigned to this property — true for
    /// reference types and <see cref="Nullable{T}"/> value types, false otherwise.
    /// </summary>
    public bool CanAssignNull { get; init; }
}
