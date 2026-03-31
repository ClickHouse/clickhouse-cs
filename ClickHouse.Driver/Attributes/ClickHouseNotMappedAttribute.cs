using System;

namespace ClickHouse.Driver;

/// <summary>
/// Indicates that a property should be excluded from ClickHouse column mapping.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class ClickHouseNotMappedAttribute : Attribute
{
}
