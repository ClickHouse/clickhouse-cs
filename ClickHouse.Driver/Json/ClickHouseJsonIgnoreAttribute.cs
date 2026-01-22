using System;

namespace ClickHouse.Driver.Json;

/// <summary>
/// Indicates that a property should be ignored when serializing to ClickHouse JSON type.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public sealed class ClickHouseJsonIgnoreAttribute : Attribute
{
}
