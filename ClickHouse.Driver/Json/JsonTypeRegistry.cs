using System;
using System.Collections;
using System.Collections.Generic;
using System.Net;
using System.Numerics;
using System.Reflection;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types;
using NodaTime;

namespace ClickHouse.Driver.Json;

/// <summary>
/// Registry for POCO types used with JSON and Dynamic columns.
/// Types must be registered before they can be serialized.
/// </summary>
internal sealed class JsonTypeRegistry
{
    /// <summary>
    /// Cache for registered POCO types and their property metadata.
    /// </summary>
    private readonly Dictionary<Type, JsonPropertyInfo[]> _registeredTypes = new();

    /// <summary>
    /// Registers a POCO type for JSON/Dynamic serialization.
    /// Validates that all properties can be mapped to ClickHouse types.
    /// </summary>
    /// <typeparam name="T">The POCO type to register.</typeparam>
    /// <exception cref="ClickHouseJsonSerializationException">
    /// Thrown if any property type cannot be mapped to a ClickHouse type.
    /// </exception>
    internal void RegisterType<T>() where T : class
        => RegisterType(typeof(T));

    /// <summary>
    /// Registers a POCO type for JSON/Dynamic serialization.
    /// Validates that all properties can be mapped to ClickHouse types.
    /// </summary>
    /// <param name="type">The POCO type to register.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="type"/> is null.</exception>
    /// <exception cref="ClickHouseJsonSerializationException">
    /// Thrown if any property type cannot be mapped to a ClickHouse type.
    /// </exception>
    internal void RegisterType(Type type)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        if (_registeredTypes.ContainsKey(type))
            return;

        BuildPropertyInfo(type, new HashSet<Type>());
    }

    /// <summary>
    /// Gets the property info for a registered type.
    /// </summary>
    /// <param name="type">The type to get properties for.</param>
    /// <returns>The property info array, or null if the type is not registered.</returns>
    internal JsonPropertyInfo[] GetProperties(Type type)
        => _registeredTypes.TryGetValue(type, out var props) ? props : null;

    /// <summary>
    /// Builds and validates property info for a type.
    /// </summary>
    /// <param name="type">The type to build property info for.</param>
    /// <param name="typesBeingRegistered">Set of types currently being registered in this call chain.</param>
    private void BuildPropertyInfo(Type type, HashSet<Type> typesBeingRegistered)
    {
        // Track this type to prevent infinite recursion from circular references
        if (!typesBeingRegistered.Add(type))
        {
            // Type is already being registered (circular reference) - skip
            return;
        }

        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var result = new List<JsonPropertyInfo>(properties.Length);
        var usedPaths = new HashSet<string>();

        foreach (var property in properties)
        {
            if (!property.CanRead)
                continue;

            // Skip indexers - they have index parameters and can't be serialized as simple properties
            if (property.GetIndexParameters().Length > 0)
                continue;

            var ignoreAttr = property.GetCustomAttribute<ClickHouseJsonIgnoreAttribute>();
            if (ignoreAttr != null)
            {
                result.Add(new JsonPropertyInfo
                {
                    Property = property,
                    JsonPath = property.Name,
                    IsIgnored = true,
                    IsNestedObject = false,
                });
                continue;
            }

            var pathAttr = property.GetCustomAttribute<ClickHouseJsonPathAttribute>();
            var jsonPath = pathAttr?.Path ?? property.Name;

            // Validate path uniqueness
            if (!usedPaths.Add(jsonPath))
            {
                throw new ClickHouseJsonSerializationException(
                    $"Failed to register type '{type.Name}': multiple properties map to JSON path '{jsonPath}'.");
            }

            var propertyType = property.PropertyType;
            var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
            var isNested = IsNestedObject(underlyingType);

            // Validate that non-nested types can be mapped to ClickHouse types
            if (!isNested)
            {
                ValidatePropertyType(type, property.Name, underlyingType);
            }

            result.Add(new JsonPropertyInfo
            {
                Property = property,
                JsonPath = jsonPath,
                IsIgnored = false,
                IsNestedObject = isNested,
            });

            // Recursively register nested object types (skip if already registered or being registered)
            if (isNested && !_registeredTypes.ContainsKey(underlyingType))
            {
                BuildPropertyInfo(underlyingType, typesBeingRegistered);
            }
        }

        // Add to the cache after processing all properties
        _registeredTypes[type] = result.ToArray();
    }

    /// <summary>
    /// Validates that a property type can be mapped to a ClickHouse type.
    /// </summary>
    private static void ValidatePropertyType(Type targetType, string propertyName, Type propertyType)
    {
        try
        {
            // Try to infer the ClickHouse type - this will throw if the type is not supported
            TypeConverter.ToClickHouseType(propertyType);
        }
        catch (ArgumentOutOfRangeException)
        {
            throw new ClickHouseJsonSerializationException(targetType, propertyName, propertyType);
        }
    }

    /// <summary>
    /// Determines if a type should be treated as a nested object to recursively serialize.
    /// </summary>
    internal static bool IsNestedObject(Type type)
    {
        return !type.IsPrimitive
            && type != typeof(string)
            && type != typeof(decimal)
            && type != typeof(DateTime)
            && type != typeof(DateTimeOffset)
            && type != typeof(OffsetDateTime)
            && type != typeof(ZonedDateTime)
            && type != typeof(Instant)
            && type != typeof(TimeSpan)
#if NET6_0_OR_GREATER
            && type != typeof(DateOnly)
#endif
            && type != typeof(Guid)
            && type != typeof(BigInteger)
            && type != typeof(ClickHouseDecimal)
            && type != typeof(IPAddress)
            && !typeof(IEnumerable).IsAssignableFrom(type)
            && !type.IsEnum;
    }
}

/// <summary>
/// Cached property information for JSON serialization.
/// </summary>
internal sealed class JsonPropertyInfo
{
    public PropertyInfo Property { get; init; }

    public string JsonPath { get; init; }

    public bool IsIgnored { get; init; }

    public bool IsNestedObject { get; init; }
}
