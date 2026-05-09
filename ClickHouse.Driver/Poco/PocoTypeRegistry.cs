using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace ClickHouse.Driver.Poco;

/// <summary>
/// Per-client registry of POCO types used with binary insert and read materialization.
/// A given type can carry the insert capability, the read capability, or both — independently
/// validated. The two kinds of registration share property-metadata fields but compile separate
/// getters/setters, because a type's mappable-for-insert and mappable-for-read property sets can
/// differ (e.g., a class with read-only auto-properties is insert-mappable but not read-mappable).
/// </summary>
internal sealed class PocoTypeRegistry
{
    private readonly ConcurrentDictionary<Type, PocoInsertMapping> insertMappings = new();
    private readonly ConcurrentDictionary<Type, PocoReadMapping> readMappings = new();

    /// <summary>
    /// Registers a POCO type for binary insert. Idempotent and thread-safe.
    /// </summary>
    internal void RegisterForInsert<T>()
        where T : class
        => insertMappings.GetOrAdd(typeof(T), static _ => BuildInsertMapping<T>());

    /// <summary>
    /// Registers a POCO type for read materialization. Idempotent and thread-safe.
    /// </summary>
    internal void RegisterForRead<T>()
        where T : class
        => readMappings.GetOrAdd(typeof(T), static _ => BuildReadMapping<T>());

    /// <summary>
    /// Registers a POCO type for both binary insert and read materialization. Both mappings are
    /// validated up front; if either validation throws, neither mapping is committed, so the
    /// registry is left in its prior state.
    /// </summary>
    internal void RegisterForBoth<T>()
        where T : class
    {
        // Build both before committing either — a validation throw here cannot leave a partial
        // registration behind.
        var read = BuildReadMapping<T>();
        var insert = BuildInsertMapping<T>();
        readMappings.GetOrAdd(typeof(T), read);
        insertMappings.GetOrAdd(typeof(T), insert);
    }

    /// <summary>
    /// Gets the typed insert mapping for a registered type, or null if not registered for insert.
    /// </summary>
    internal PocoInsertMapping<T> GetInsertMapping<T>()
        where T : class
        => insertMappings.TryGetValue(typeof(T), out var mapping) ? (PocoInsertMapping<T>)mapping : null;

    /// <summary>
    /// Gets the typed read mapping for a registered type, or null if not registered for read.
    /// </summary>
    internal PocoReadMapping<T> GetReadMapping<T>()
        where T : class
        => readMappings.TryGetValue(typeof(T), out var mapping) ? (PocoReadMapping<T>)mapping : null;

    private static PocoInsertMapping<T> BuildInsertMapping<T>()
        where T : class
    {
        var type = typeof(T);
        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var propInfos = new List<PocoPropertyInfo>(properties.Length);
        var getters = new List<Func<T, object>>(properties.Length);
        var usedColumnNames = new HashSet<string>(StringComparer.Ordinal);

        foreach (var property in properties)
        {
            if (!property.CanRead || !property.GetMethod.IsPublic)
                continue;

            if (property.GetIndexParameters().Length > 0)
                continue;

            if (property.GetCustomAttribute<ClickHouseNotMappedAttribute>() != null)
                continue;

            var (columnName, explicitType) = ResolveColumnAttributes(type, property, usedColumnNames);

            propInfos.Add(BuildPropertyInfo(property, columnName, explicitType));
            getters.Add(CompileGetter<T>(property));
        }

        if (propInfos.Count == 0)
        {
            throw new InvalidOperationException(
                $"Type '{type.Name}' has no public readable properties that map to ClickHouse columns. " +
                $"Ensure the type has at least one public property with a getter that is not marked with [ClickHouseNotMapped].");
        }

        var props = propInfos.ToArray();

        IReadOnlyDictionary<string, string> columnTypes = null;
        if (Array.TrueForAll(props, p => p.ExplicitClickHouseType != null))
        {
            var dict = new Dictionary<string, string>(props.Length, StringComparer.Ordinal);
            foreach (var prop in props)
                dict[prop.ColumnName] = prop.ExplicitClickHouseType;
            columnTypes = dict;
        }

        return new PocoInsertMapping<T>
        {
            Properties = props,
            ColumnTypes = columnTypes,
            Getters = getters.ToArray(),
        };
    }

    private static PocoReadMapping<T> BuildReadMapping<T>()
        where T : class
    {
        var type = typeof(T);

        var ctor = type.GetConstructor(Type.EmptyTypes);
        if (ctor == null)
        {
            throw new InvalidOperationException(
                $"Failed to register type '{type.Name}' for POCO read: type must have a public parameterless constructor.");
        }

        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var propInfos = new List<PocoPropertyInfo>(properties.Length);
        var setters = new List<Action<T, object>>(properties.Length);
        var usedColumnNames = new HashSet<string>(StringComparer.Ordinal);

        foreach (var property in properties)
        {
            if (property.GetIndexParameters().Length > 0)
                continue;

            if (property.GetCustomAttribute<ClickHouseNotMappedAttribute>() != null)
                continue;

            var setMethod = property.SetMethod;
            if (setMethod is null || !setMethod.IsPublic || IsInitOnly(setMethod))
                continue;

            var (columnName, explicitType) = ResolveColumnAttributes(type, property, usedColumnNames);

            propInfos.Add(BuildPropertyInfo(property, columnName, explicitType));
            setters.Add(CompileSetter<T>(property));
        }

        if (propInfos.Count == 0)
        {
            throw new InvalidOperationException(
                $"Type '{type.Name}' has no public properties with a public non-init setter that map to ClickHouse columns. " +
                $"POCO read registration requires at least one mapped property usable as a setter (init-only and read-only properties are ignored).");
        }

        var props = propInfos.ToArray();
        var nameToIndex = new Dictionary<string, int>(props.Length, StringComparer.Ordinal);
        for (var i = 0; i < props.Length; i++)
            nameToIndex[props[i].ColumnName] = i;

        return new PocoReadMapping<T>
        {
            Properties = props,
            Setters = setters.ToArray(),
            ColumnNameToPropertyIndex = nameToIndex,
            Constructor = CompileConstructor<T>(ctor),
        };
    }

    private static (string ColumnName, string ExplicitType) ResolveColumnAttributes(
        Type type,
        PropertyInfo property,
        HashSet<string> usedColumnNames)
    {
        var columnAttr = property.GetCustomAttribute<ClickHouseColumnAttribute>();
        var columnName = columnAttr?.Name ?? property.Name;

        if (string.IsNullOrWhiteSpace(columnName))
        {
            throw new InvalidOperationException(
                $"Failed to register type '{type.Name}': property '{property.Name}' has an empty or whitespace column name.");
        }

        if (!usedColumnNames.Add(columnName))
        {
            throw new InvalidOperationException(
                $"Failed to register type '{type.Name}': multiple properties map to column '{columnName}'.");
        }

        var explicitType = columnAttr?.Type;
        if (explicitType != null && string.IsNullOrWhiteSpace(explicitType))
        {
            throw new InvalidOperationException(
                $"Failed to register type '{type.Name}': property '{property.Name}' has an empty or whitespace ClickHouse type.");
        }

        return (columnName, explicitType);
    }

    private static PocoPropertyInfo BuildPropertyInfo(PropertyInfo property, string columnName, string explicitType)
    {
        var propertyType = property.PropertyType;
        var nullableUnderlying = Nullable.GetUnderlyingType(propertyType);
        var canAssignNull = !propertyType.IsValueType || nullableUnderlying != null;

        return new PocoPropertyInfo
        {
            ColumnName = columnName,
            ExplicitClickHouseType = explicitType,
            PropertyName = property.Name,
            PropertyType = propertyType,
            NullableUnderlyingType = nullableUnderlying,
            CanAssignNull = canAssignNull,
        };
    }

    private static bool IsInitOnly(MethodInfo setMethod)
    {
        var modifiers = setMethod.ReturnParameter.GetRequiredCustomModifiers();
        for (var i = 0; i < modifiers.Length; i++)
        {
            if (modifiers[i].FullName == "System.Runtime.CompilerServices.IsExternalInit")
                return true;
        }
        return false;
    }

    private static Func<T, object> CompileGetter<T>(PropertyInfo property)
    {
        var param = Expression.Parameter(typeof(T), "instance");
        var access = Expression.Property(param, property);
        var box = Expression.Convert(access, typeof(object));
        var lambda = Expression.Lambda<Func<T, object>>(box, param);
        return lambda.Compile();
    }

    private static Action<T, object> CompileSetter<T>(PropertyInfo property)
    {
        var instance = Expression.Parameter(typeof(T), "instance");
        var value = Expression.Parameter(typeof(object), "value");
        var typedValue = Expression.Convert(value, property.PropertyType);
        var assign = Expression.Assign(Expression.Property(instance, property), typedValue);
        var lambda = Expression.Lambda<Action<T, object>>(assign, instance, value);
        return lambda.Compile();
    }

    private static Func<T> CompileConstructor<T>(ConstructorInfo ctor)
        where T : class
        => Expression.Lambda<Func<T>>(Expression.New(ctor)).Compile();
}
