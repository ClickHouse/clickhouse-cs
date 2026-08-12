using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;
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

    // Cache of compiled per-column write delegates. The delegates fuse the property read with the writer
    // call and depend on the resolved ClickHouseType[], which is only known per-insert — so they are built
    // here at plan time, not at registration time. The outer key is the POCO Type itself (identity, not
    // display name — so types that merely share a FullName across assemblies never collide), and its value
    // holds the boxed Action&lt;T, ExtendedBinaryWriter&gt;[] for that exact T; the inner key is the resolved
    // column-type sequence.
    private readonly ConcurrentDictionary<Type, ConcurrentDictionary<string, object>> writerCache = new();

    // Mirrors writerCache for the read path: outer key is the POCO Type identity, inner key is the
    // wire-column signature. The key omits String's byte-array-vs-string mode because a registry belongs to
    // one ClickHouseClient, so every read through it shares that client's ReadStringsAsByteArrays setting.
    // Sharing a registry across clients with differing settings would have to add it to the key.
    private readonly ConcurrentDictionary<Type, ConcurrentDictionary<string, object>> rowReaderCache = new();

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
    /// Gets (building and caching on first use) the per-column box-free write delegates for a POCO insert
    /// into columns of the given resolved types. Each delegate reads one property and writes it directly;
    /// columns without a fast path fall back to a delegate that wraps the boxed
    /// <see cref="ClickHouseType.Write(ExtendedBinaryWriter, object)"/> using the compiled boxed getter.
    /// The result is cached per <c>(T, resolved column-type sequence)</c>: property types are fixed by
    /// <typeparamref name="T"/>, and the type sequence captures the only per-insert variable, so the cached
    /// delegates stay correct even when the same POCO is inserted into different tables.
    /// </summary>
    /// <param name="properties">Mapped properties, ordered to match <paramref name="types"/> and <paramref name="getters"/>.</param>
    /// <param name="getters">Compiled boxed getters, used for the fallback path.</param>
    /// <param name="types">Resolved ClickHouse column types.</param>
    internal Action<T, ExtendedBinaryWriter>[] GetOrBuildWriters<T>(
        PocoPropertyInfo[] properties, Func<T, object>[] getters, ClickHouseType[] types)
        where T : class
    {
        var byType = writerCache.GetOrAdd(typeof(T), _ => new ConcurrentDictionary<string, object>(StringComparer.Ordinal));
        var key = BuildTypeSequenceKey(types);
        return (Action<T, ExtendedBinaryWriter>[])byType.GetOrAdd(
            key, _ => BuildWriters(properties, getters, types));
    }

    // Length-prefixes each resolved column type, so the key stays injective whatever the signatures contain:
    // a JSON path hint is an arbitrary identifier which the server escapes and the client unescapes, so a
    // signature can hold any character, separators included.
    // Keys on ClickHouseType.CacheSignature, not ToString(): a type whose Write consults state its
    // ToString() omits (JSON path hints) must not share a cache entry with another instance of that type.
    private static string BuildTypeSequenceKey(ClickHouseType[] types)
    {
        var builder = new StringBuilder();
        foreach (var type in types)
            builder.AppendLengthPrefixed(type.CacheSignature);

        return builder.ToString();
    }

    private static Action<T, ExtendedBinaryWriter>[] BuildWriters<T>(
        PocoPropertyInfo[] properties, Func<T, object>[] getters, ClickHouseType[] types)
        where T : class
    {
        var writers = new Action<T, ExtendedBinaryWriter>[properties.Length];
        var rowParam = Expression.Parameter(typeof(T), "row");
        var writerParam = Expression.Parameter(typeof(ExtendedBinaryWriter), "writer");

        for (var i = 0; i < properties.Length; i++)
        {
            var propertyAccess = Expression.Property(rowParam, properties[i].Property);
            var body = PocoWriteExpressionFactory.TryBuildWriteBody(types[i], propertyAccess, writerParam);

            if (body != null)
            {
                writers[i] = Expression.Lambda<Action<T, ExtendedBinaryWriter>>(body, rowParam, writerParam).Compile();
            }
            else
            {
                // No fast path for this (property, type) pair: reuse the boxed getter + boxed Write, which
                // is byte-identical to the default path. Copy to locals so the closure captures per-column
                // values rather than the loop variable.
                var type = types[i];
                var getter = getters[i];
                writers[i] = (row, writer) => type.Write(writer, getter(row));
            }
        }

        return writers;
    }

    /// <summary>
    /// Gets the typed read mapping for a registered type, or null if not registered for read.
    /// </summary>
    internal PocoReadMapping<T> GetReadMapping<T>()
        where T : class
        => readMappings.TryGetValue(typeof(T), out var mapping) ? (PocoReadMapping<T>)mapping : null;

    /// <summary>
    /// Gets (building and caching on first use) the per-wire-column read delegates that materialize one row
    /// of the given shape straight from the stream into a <typeparamref name="T"/>. There is one delegate per
    /// wire column, in wire order, and together they consume every column's bytes: a bound column decodes
    /// box-free where it has a typed read, otherwise through the boxed path with the same null/error
    /// semantics as <c>MapTo&lt;T&gt;</c>; an unmapped column reads and discards to keep the stream aligned.
    /// Cached per <c>(T, wire-column signature)</c>, since the same POCO can be read from different
    /// projections. Throws on first use of a shape if a boxed-fallback column is not assignable.
    /// </summary>
    /// <param name="fieldNames">Wire column names, in order.</param>
    /// <param name="types">Resolved ClickHouse column types, in wire order (parallel to <paramref name="fieldNames"/>).</param>
    /// <param name="mapping">The read mapping for <typeparamref name="T"/> (bindings + constructor).</param>
    /// <param name="withConverter">
    /// Whether the delegates should route values through the reader's <see cref="IReadValueConverter"/>.
    /// Part of the cache key, so a converter-free reader keeps delegates that never touch the converter
    /// parameters at all.
    /// </param>
    internal RowColumnReader<T>[] GetOrBuildRowReaders<T>(
        string[] fieldNames, ClickHouseType[] types, PocoReadMapping<T> mapping, bool withConverter)
        where T : class
    {
        var byType = rowReaderCache.GetOrAdd(typeof(T), _ => new ConcurrentDictionary<string, object>(StringComparer.Ordinal));
        var key = BuildRowReaderKey(fieldNames, types, withConverter);
        return (RowColumnReader<T>[])byType.GetOrAdd(key, _ => BuildRowReaders(fieldNames, types, mapping, withConverter));
    }

    // Signature over the wire shape: field name + resolved type per column, in order. '\t' and '\n' are the
    // separators because no ClickHouse type name contains either. Only a backtick-quoted alias holding a
    // literal tab or newline could collide, and the shape comes from the caller's own SQL. The converter flag
    // leads, because it changes the shape of every delegate in the array.
    private static string BuildRowReaderKey(string[] fieldNames, ClickHouseType[] types, bool withConverter)
    {
        var parts = new string[types.Length + 1];
        parts[0] = withConverter ? "conv" : string.Empty;
        for (var i = 0; i < types.Length; i++)
            parts[i + 1] = fieldNames[i] + "\t" + types[i].ToString();
        return string.Join("\n", parts);
    }

    // The generic ConvertValue<T>, picked out from the non-generic overload of the same name.
    private static readonly MethodInfo ConvertValueTypedMethod = typeof(IReadValueConverter)
        .GetMethods()
        .Single(m => m.Name == nameof(IReadValueConverter.ConvertValue) && m.IsGenericMethodDefinition);

    private static RowColumnReader<T>[] BuildRowReaders<T>(
        string[] fieldNames, ClickHouseType[] types, PocoReadMapping<T> mapping, bool withConverter)
        where T : class
    {
        var readers = new RowColumnReader<T>[types.Length];
        var readerParam = Expression.Parameter(typeof(ExtendedBinaryReader), "reader");
        var instanceParam = Expression.Parameter(typeof(T), "instance");
        var converterParam = Expression.Parameter(typeof(IReadValueConverter), "converter");
        var typeNamesParam = Expression.Parameter(typeof(string[]), "columnTypeNames");
        var parameters = new[] { readerParam, instanceParam, converterParam, typeNamesParam };

        for (var i = 0; i < types.Length; i++)
        {
            var type = types[i];

            if (!mapping.Bindings.TryGetValue(fieldNames[i], out var binding))
            {
                // Unmapped column: its bytes must still be consumed so the next column reads from the right
                // offset. Boxes the discarded value, which the POCO does not use anyway. No conversion: the
                // value never reaches the caller. Copy to a local for correct closure capture.
                var discardType = type;
                readers[i] = (r, _, _, _) => discardType.Read(r);
                continue;
            }

            var propertyType = binding.PropInfo.PropertyType;
            var readBody = PocoReadExpressionFactory.TryBuildReadBody(type, readerParam, propertyType);
            if (readBody != null)
            {
                // A typed read yields the property's exact CLR type, so it converts through the typed
                // ConvertValue<T> — the same overload GetFieldValue<T> uses for a typed read. Exact-typed by
                // construction, so no runtime assignability check is needed.
                Expression value = readBody;
                if (withConverter)
                {
                    value = Expression.Call(
                        converterParam,
                        ConvertValueTypedMethod.MakeGenericMethod(propertyType),
                        readBody,
                        Expression.Constant(fieldNames[i]),
                        Expression.ArrayIndex(typeNamesParam, Expression.Constant(i)));
                }

                var assign = Expression.Assign(Expression.Property(instanceParam, binding.PropInfo.Property), value);
                readers[i] = Expression.Lambda<RowColumnReader<T>>(assign, parameters).Compile();
            }
            else
            {
                // No typed read (composite/polymorphic column): boxed read + setter for this column alone,
                // byte-identical to MapTo<T>, and validated up front as MapTo's plan build does.
                if (!PocoColumnAssignment.IsAssignable(binding.PropInfo, type))
                {
                    var colFrameworkType = type.FrameworkType;
                    var unwrapped = Nullable.GetUnderlyingType(colFrameworkType) ?? colFrameworkType;
                    throw new InvalidOperationException(PocoColumnAssignment.BuildAssignmentErrorMessage(
                        typeof(T), binding.PropInfo, fieldNames[i], type.ToString(), unwrapped));
                }

                readers[i] = BuildBoxedColumnReader(type, binding, fieldNames[i], i, withConverter);
            }
        }

        return readers;
    }

    // Boxed fallback for a single column, replicating MapTo<T>'s per-column assignment. The value is boxed
    // either way here, so it converts through the boxed ConvertValue overload, exactly as MapTo<T> does via
    // GetValue — including converting before the null test, so a converter still sees a DBNull cell.
    private static RowColumnReader<T> BuildBoxedColumnReader<T>(
        ClickHouseType type, ColumnBinding<T> binding, string fieldName, int ordinal, bool withConverter)
        where T : class
    {
        var propInfo = binding.PropInfo;
        var setter = binding.Setter;
        var canAssignNull = propInfo.CanAssignNull;

        return (reader, instance, converter, columnTypeNames) =>
        {
            var value = type.Read(reader);
            if (withConverter)
                value = converter.ConvertValue(value, fieldName, columnTypeNames[ordinal]);

            if (value is null || value is DBNull)
            {
                if (canAssignNull)
                {
                    setter(instance, null);
                    return;
                }

                throw new InvalidOperationException(PocoColumnAssignment.BuildAssignmentErrorMessage(
                    typeof(T), propInfo, fieldName, type.ToString(), null));
            }

            try
            {
                setter(instance, value);
            }
            catch (InvalidCastException)
            {
                throw new InvalidOperationException(PocoColumnAssignment.BuildAssignmentErrorMessage(
                    typeof(T), propInfo, fieldName, type.ToString(), value.GetType()));
            }
        };
    }

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
            Property = property,
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
