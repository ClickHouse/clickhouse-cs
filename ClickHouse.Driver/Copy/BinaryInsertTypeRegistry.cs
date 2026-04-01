using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Copy;

/// <summary>
/// Registry for POCO types used with binary insert operations.
/// Types must be registered before they can be inserted via <c>InsertBinaryAsync&lt;T&gt;</c>.
/// </summary>
internal sealed class BinaryInsertTypeRegistry
{
    private readonly ConcurrentDictionary<Type, PocoTypeMapping> registeredTypes = new();

    /// <summary>
    /// Registers a POCO type for binary insert operations.
    /// Validates that all mapped properties have types supported by ClickHouse.
    /// </summary>
    internal void RegisterType<T>()
        where T : class
    {
        if (registeredTypes.ContainsKey(typeof(T)))
            return;

        registeredTypes[typeof(T)] = BuildMapping<T>();
    }

    /// <summary>
    /// Gets the typed mapping for a registered type, or null if not registered.
    /// </summary>
    internal PocoTypeMapping<T> GetMapping<T>()
        where T : class
        => registeredTypes.TryGetValue(typeof(T), out var mapping) ? (PocoTypeMapping<T>)mapping : null;

    private static PocoTypeMapping<T> BuildMapping<T>()
        where T : class
    {
        var type = typeof(T);
        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var propInfos = new List<BinaryInsertPropertyInfo>(properties.Length);
        var getters = new List<Func<T, object>>(properties.Length);
        var usedColumnNames = new HashSet<string>(StringComparer.Ordinal);

        foreach (var property in properties)
        {
            if (!property.CanRead || !property.GetMethod.IsPublic)
                continue;

            // Skip indexers
            if (property.GetIndexParameters().Length > 0)
                continue;

            // Skip [ClickHouseNotMapped]
            if (property.GetCustomAttribute<ClickHouseNotMappedAttribute>() != null)
                continue;

            var columnAttr = property.GetCustomAttribute<ClickHouseColumnAttribute>();
            var columnName = columnAttr?.Name ?? property.Name;

            if (!usedColumnNames.Add(columnName))
            {
                throw new InvalidOperationException(
                    $"Failed to register type '{type.Name}': multiple properties map to column '{columnName}'.");
            }

            propInfos.Add(new BinaryInsertPropertyInfo
            {
                ColumnName = columnName,
                ExplicitClickHouseType = columnAttr?.Type,
            });

            getters.Add(CompileGetter<T>(property));
        }

        if (propInfos.Count == 0)
        {
            throw new InvalidOperationException(
                $"Type '{type.Name}' has no public readable properties that map to ClickHouse columns. " +
                $"Ensure the type has at least one public property with a getter that is not marked with [ClickHouseNotMapped].");
        }

        var props = propInfos.ToArray();

        // Pre-build column types dictionary when ALL properties have explicit types.
        IReadOnlyDictionary<string, string> columnTypes = null;
        if (Array.TrueForAll(props, p => p.ExplicitClickHouseType != null))
        {
            var dict = new Dictionary<string, string>(props.Length, StringComparer.Ordinal);
            foreach (var prop in props)
                dict[prop.ColumnName] = prop.ExplicitClickHouseType;
            columnTypes = dict;
        }

        return new PocoTypeMapping<T>
        {
            Properties = props,
            ColumnTypes = columnTypes,
            Getters = getters.ToArray(),
        };
    }

    private static Func<T, object> CompileGetter<T>(PropertyInfo property)
    {
        var param = Expression.Parameter(typeof(T), "instance");
        var access = Expression.Property(param, property);
        var box = Expression.Convert(access, typeof(object));
        var lambda = Expression.Lambda<Func<T, object>>(box, param);
        return lambda.Compile();
    }
}
