#if NET462
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Utility;

/// <summary>
/// Compatibility shim for Tuple in .NET 4.6.2
/// For now, only eight elements are supported, with the eighth element sourced from `Rest`.
/// <see href="https://learn.microsoft.com/en-us/dotnet/api/system.tuple-8.rest?view=netframework-4.6.2"/>
/// </summary>
internal static class TupleHelper
{
    private static readonly ConcurrentDictionary<Type, bool> TypeCache = new();
    private static readonly ConcurrentDictionary<Type, PropertyInfo[]> PropertyCache = new();

    public static bool IsTupleType(Type type)
    {
        if (type == null) return false;

        return TypeCache.GetOrAdd(type, t =>
        {
            if (!t.IsGenericType)
                return false;

            var definition = t.GetGenericTypeDefinition();
            return definition == typeof(Tuple<>) ||
                   definition == typeof(Tuple<,>) ||
                   definition == typeof(Tuple<,,>) ||
                   definition == typeof(Tuple<,,,>) ||
                   definition == typeof(Tuple<,,,,>) ||
                   definition == typeof(Tuple<,,,,,>) ||
                   definition == typeof(Tuple<,,,,,,>) ||
                   definition == typeof(Tuple<,,,,,,,>);
        });
    }

    public static PropertyInfo[] GetTupleProperties(Type type)
    {
        return PropertyCache.GetOrAdd(type, t =>
        {
            var properties = new List<PropertyInfo>();

            for (var i = 1; i <= 8; i++)
            {
                var property = t.GetProperty($"Item{i}");
                if (property == null)
                    break;

                properties.Add(property);
            }
            return properties.ToArray();
        });
    }

    public static PropertyInfo[] GetTuplePropertiesWithRest(Type type)
    {
        return PropertyCache.GetOrAdd(type, t =>
        {
            var properties = new PropertyInfo[8];
            var length = t.GetGenericArguments().Length;

            for (var i = 0; i < Math.Min(length, 8); i++)
            {
                var propertyName = i == 7 && length == 8 ? "Rest" : $"Item{i + 1}";
                properties[i] = t.GetProperty(propertyName);
            }

            return properties;
        });
    }

    public static int GetTupleLength(Type tupleType)
    {
        if (!IsTupleType(tupleType))
            return 0;

        var arguments = tupleType.GetGenericArguments();
        if (arguments.Length == 8 && IsTupleType(arguments[7]))
            return 8;

        return arguments.Length;
    }

    public static string FormatTuple(object value, ClickHouseType[] underlyingTypes, Func<ClickHouseType, object, bool, string> formatter, string nullValue)
    {
        if (value == null)
            return nullValue;

        var type = value.GetType();
        var properties = GetTupleProperties(type);

        var items = new List<string>();
        var count = Math.Min(properties.Length, underlyingTypes.Length);

        for (var i = 0; i < count; i++)
        {
            var itemValue = properties[i].GetValue(value);
            items.Add(formatter(underlyingTypes[i], itemValue, true));
        }

        return $"({string.Join(",", items)})";
    }

    public static object CreateTuple(object[] values, Type frameworkType, ClickHouseType[] underlyingTypes)
    {
        var count = values.Length;
        if (count > 8)
            return values;

        var arguments = frameworkType.GetGenericArguments();
        var typedValues = new object[count];
        for (var i = 0; i < count; i++)
        {
            var expectedType = arguments[i];
            var value = values[i];

            if (i == 7 && count == 8)
                expectedType = expectedType.GetGenericArguments()[0];

            if (value is null or DBNull)
            {
                typedValues[i] = null;
            }
            else if (expectedType.IsInstanceOfType(value))
            {
                typedValues[i] = value;
            }
            else if (expectedType.IsGenericType &&
                     expectedType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var underlyingType = Nullable.GetUnderlyingType(expectedType);
                if (underlyingType != null)
                    typedValues[i] = Convert.ChangeType(value, underlyingType, CultureInfo.InvariantCulture);
                else
                    typedValues[i] = null;
            }
            else if (IsTupleType(expectedType) && value is object[] nested)
            {
                if (underlyingTypes[i] is TupleType nestedTupleType)
                {
                    typedValues[i] = CreateTuple(nested, nestedTupleType.FrameworkType, nestedTupleType.UnderlyingTypes);
                }
                else
                {
                    typedValues[i] = value;
                }
            }
            else
            {
                try
                {
                    typedValues[i] = Convert.ChangeType(value, expectedType, CultureInfo.InvariantCulture);
                }
                catch
                {
                    typedValues[i] = value;
                }
            }
        }

        if (count == 8) // Tuple<T8>
        {
            var wrapped = new object[8];
            Array.Copy(typedValues, 0, wrapped, 0, 7);
            wrapped[7] = Activator.CreateInstance(arguments[7], typedValues[7]);
            return Activator.CreateInstance(frameworkType, wrapped);
        }

        return Activator.CreateInstance(frameworkType, typedValues);
    }

    public static Array ReadArrayWithRuntimeType(ExtendedBinaryReader reader, int length, ClickHouseType elementType, Type fallbackFrameworkType)
    {
        var values = new object[length];
        for (var i = 0; i < length; i++)
        {
            var value = elementType.Read(reader);
            values[i] = value is DBNull ? null : value;
        }

        if (length > 0 && values[0] != null)
        {
            var typedArray = Array.CreateInstance(values[0].GetType(), length);
            for (var i = 0; i < length; i++)
            {
                typedArray.SetValue(values[i], i);
            }
            return typedArray;
        }

        return values;
    }

    public static Array ReadNestedArrayWithRuntimeType(ExtendedBinaryReader reader, int length, TupleType tupleType)
    {
        var values = new object[length];
        for (var i = 0; i < length; i++)
        {
            var value = tupleType.Read(reader);
            values[i] = value is DBNull ? null : value;
        }

        if (length > 0 && values[0] != null)
        {
            var typedArray = Array.CreateInstance(values[0].GetType(), length);
            for (var i = 0; i < length; i++)
            {
                typedArray.SetValue(values[i], i);
            }
            return typedArray;
        }

        return values;
    }
}
#endif
