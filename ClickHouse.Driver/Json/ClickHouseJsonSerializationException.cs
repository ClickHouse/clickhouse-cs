using System;

namespace ClickHouse.Driver.Json;

/// <summary>
/// Exception thrown when a POCO type cannot be serialized to JSON or Dynamic columns.
/// </summary>
public class ClickHouseJsonSerializationException : Exception
{
    /// <summary>
    /// Gets the target type that failed registration or serialization.
    /// </summary>
    public Type TargetType { get; }

    /// <summary>
    /// Gets the name of the property that caused the failure, if applicable.
    /// </summary>
    public string PropertyName { get; }

    /// <summary>
    /// Gets the type of the property that could not be mapped, if applicable.
    /// </summary>
    public Type PropertyType { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseJsonSerializationException"/> class.
    /// </summary>
    /// <param name="message">The error message.</param>
    public ClickHouseJsonSerializationException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseJsonSerializationException"/> class
    /// for an unsupported property type during registration.
    /// </summary>
    /// <param name="targetType">The POCO type being registered.</param>
    /// <param name="propertyName">The name of the property with an unsupported type.</param>
    /// <param name="propertyType">The unsupported property type.</param>
    public ClickHouseJsonSerializationException(Type targetType, string propertyName, Type propertyType)
        : base(FormatPropertyTypeMessage(targetType, propertyName, propertyType))
    {
        TargetType = targetType;
        PropertyName = propertyName;
        PropertyType = propertyType;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseJsonSerializationException"/> class
    /// for an unregistered type during serialization.
    /// </summary>
    /// <param name="unregisteredType">The type that was not registered.</param>
    public ClickHouseJsonSerializationException(Type unregisteredType)
        : base(FormatUnregisteredTypeMessage(unregisteredType))
    {
        TargetType = unregisteredType;
    }

    private static string FormatPropertyTypeMessage(Type targetType, string propertyName, Type propertyType)
    {
        return $"Failed to register type '{targetType.Name}' for JSON serialization: " +
               $"Property '{propertyName}' has type '{propertyType.FullName}' which cannot be mapped to a ClickHouse type.";
    }

    private static string FormatUnregisteredTypeMessage(Type unregisteredType)
    {
        return $"Type '{unregisteredType.Name}' is not registered for JSON serialization. " +
               $"Call ClickHouseJsonSerializer.RegisterType<{unregisteredType.Name}>() before inserting data.";
    }
}
