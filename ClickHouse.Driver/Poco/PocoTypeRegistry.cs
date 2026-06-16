using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Extensions.Logging;

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
    internal void RegisterForInsert<T>(ILogger logger = null)
        where T : class
        => insertMappings.GetOrAdd(typeof(T), _ => BuildInsertMapping<T>(logger));

    /// <summary>
    /// Registers a POCO type for both binary insert and read materialization. Both mappings are
    /// validated up front; if either validation throws, neither mapping is committed, so the
    /// registry is left in its prior state.
    /// </summary>
    internal void RegisterForBoth<T>(ILogger logger = null)
        where T : class
    {
        var type = typeof(T);

        // Already fully registered: nothing to (re)build
        if (readMappings.ContainsKey(type) && insertMappings.ContainsKey(type))
            return;

        // Build both before committing either — a validation throw here cannot leave a partial
        // registration behind.
        var read = BuildReadMapping<T>(logger);
        var insert = BuildInsertMapping<T>(logger);
        readMappings.GetOrAdd(type, read);
        insertMappings.GetOrAdd(type, insert);
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

    private static PocoInsertMapping<T> BuildInsertMapping<T>(ILogger logger)
        where T : class
    {
        var type = typeof(T);
        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var propInfos = new List<PocoPropertyInfo>(properties.Length);
        var getters = new List<Func<T, object>>(properties.Length);
        var usedColumnNames = new HashSet<string>(StringComparer.Ordinal);
        var skipped = IsDebugEnabled(logger) ? new List<string>() : null;

        foreach (var property in properties)
        {
            if (!property.CanRead || !property.GetMethod.IsPublic)
            {
                skipped?.Add($"{property.Name} (no public getter)");
                continue;
            }

            if (property.GetIndexParameters().Length > 0)
            {
                skipped?.Add($"{property.Name} (indexer)");
                continue;
            }

            if (property.GetCustomAttribute<ClickHouseNotMappedAttribute>() != null)
            {
                skipped?.Add($"{property.Name} ([ClickHouseNotMapped])");
                continue;
            }

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

        LogRegistration(logger, type, "insert", props, p => $"{p.PropertyName}->{p.ColumnName} ({p.PropertyType.Name})", skipped);

        return new PocoInsertMapping<T>
        {
            Properties = props,
            ColumnTypes = columnTypes,
            Getters = getters.ToArray(),
        };
    }

    private static PocoReadMapping<T> BuildReadMapping<T>(ILogger logger)
        where T : class
    {
        var type = typeof(T);

        // IsAbstract is true for both abstract classes and interfaces. A public parameterless
        // ctor on an abstract class is still uninstantiable at runtime (Expression.New would
        // succeed at registration but the compiled delegate would throw at first invocation),
        // so we reject these up front instead of leaving a registration that explodes on use.
        if (type.IsAbstract)
        {
            throw new InvalidOperationException(
                $"Failed to register type '{type.Name}' for POCO read: abstract classes and interfaces cannot be instantiated.");
        }

        var ctor = type.GetConstructor(Type.EmptyTypes);
        if (ctor == null)
        {
            throw new InvalidOperationException(
                $"Failed to register type '{type.Name}' for POCO read: type must have a public parameterless constructor.");
        }

        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var bindings = new Dictionary<string, ColumnBinding<T>>(properties.Length, StringComparer.Ordinal);
        var usedColumnNames = new HashSet<string>(StringComparer.Ordinal);
        var skipped = IsDebugEnabled(logger) ? new List<string>() : null;

        foreach (var property in properties)
        {
            if (property.GetIndexParameters().Length > 0)
            {
                skipped?.Add($"{property.Name} (indexer)");
                continue;
            }

            if (property.GetCustomAttribute<ClickHouseNotMappedAttribute>() != null)
            {
                skipped?.Add($"{property.Name} ([ClickHouseNotMapped])");
                continue;
            }

            var setMethod = property.SetMethod;
            if (setMethod is null || !setMethod.IsPublic || IsInitOnly(setMethod))
            {
                skipped?.Add($"{property.Name} ({DescribeUnsettable(setMethod)})");
                continue;
            }

            var (columnName, explicitType) = ResolveColumnAttributes(type, property, usedColumnNames);

            bindings[columnName] = new ColumnBinding<T>
            {
                PropInfo = BuildPropertyInfo(property, columnName, explicitType),
                Setter = CompileSetter<T>(property),
            };
        }

        if (bindings.Count == 0)
        {
            throw new InvalidOperationException(
                $"Type '{type.Name}' has no public properties with a public non-init setter that map to ClickHouse columns. " +
                $"POCO read registration requires at least one mapped property usable as a setter (init-only and read-only properties are ignored).");
        }

        LogRegistration(logger, type, "read", bindings.Values, b => $"{b.PropInfo.PropertyName}->{b.PropInfo.ColumnName} ({b.PropInfo.PropertyType.Name})", skipped);

        return new PocoReadMapping<T>
        {
            Bindings = bindings,
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

    private static bool IsDebugEnabled(ILogger logger) => logger != null && logger.IsEnabled(LogLevel.Debug);

    private static string DescribeUnsettable(MethodInfo setMethod)
    {
        if (setMethod is null)
            return "no setter";
        if (IsInitOnly(setMethod))
            return "init-only setter";
        return "non-public setter";
    }

    // Emits a single Debug line summarizing what a registration mapped and what it skipped. The
    // skipped list makes the otherwise-silent "property quietly left at its CLR default" failure
    // mode (init-only/read-only/non-public-setter properties on the read side) diagnosable.
    private static void LogRegistration<TItem>(
        ILogger logger,
        Type type,
        string kind,
        IEnumerable<TItem> mapped,
        Func<TItem, string> describe,
        List<string> skipped)
    {
        if (!IsDebugEnabled(logger))
            return;

        var mappedItems = mapped.Select(describe).ToArray();
        logger.LogDebug(
            "Registered POCO type '{PocoType}' for {RegistrationKind}: mapped {MappedCount} propert(ies) [{MappedProperties}]; skipped {SkippedCount} [{SkippedProperties}].",
            type.Name,
            kind,
            mappedItems.Length,
            mappedItems.Length == 0 ? "none" : string.Join(", ", mappedItems),
            skipped.Count,
            skipped.Count == 0 ? "none" : string.Join(", ", skipped));
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
