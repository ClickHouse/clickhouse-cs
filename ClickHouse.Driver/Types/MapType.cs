using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class MapType : ParameterizedType
{
    // Process-wide so each map shape is compiled once, even though the Dynamic read path
    // creates a fresh MapType per value.
    private static readonly ConcurrentDictionary<Type, Func<int, IDictionary>> DictionaryFactoryCache = new();
    private static readonly ConcurrentDictionary<Type, Func<int, IList>> ListFactoryCache = new();
    private static readonly ConcurrentDictionary<Type, Func<object, object, object>> PairFactoryCache = new();
    private static readonly ConcurrentDictionary<Type, (Func<object, object> Key, Func<object, object> Value)> PairAccessorCache = new();
    private static readonly ConcurrentDictionary<Type, bool> EntrySequenceCache = new();

    private readonly MapReadMode readMode;
    private Type frameworkType;
    private ClickHouseType keyType;
    private ClickHouseType valueType;
    private Func<int, IDictionary> dictionaryFactory;
    private Func<int, IList> listFactory;
    private Func<object, object, object> pairFactory;

    public MapType()
        : this(MapReadMode.Dictionary)
    {
    }

    public MapType(MapReadMode readMode) => this.readMode = readMode;

    public Tuple<ClickHouseType, ClickHouseType> UnderlyingTypes
    {
        get => Tuple.Create(keyType, valueType);

        set
        {
            keyType = value.Item1;
            valueType = value.Item2;

            if (readMode == MapReadMode.KeyValuePairs)
            {
                var pairType = typeof(KeyValuePair<,>).MakeGenericType([keyType.FrameworkType, valueType.FrameworkType]);
                frameworkType = typeof(List<>).MakeGenericType(pairType);
                listFactory = ListFactoryCache.GetOrAdd(frameworkType, static type => BuildListFactory(type));
                pairFactory = PairFactoryCache.GetOrAdd(pairType, static type => BuildPairFactory(type));
            }
            else
            {
                var genericType = typeof(Dictionary<,>);
                frameworkType = genericType.MakeGenericType([keyType.FrameworkType, valueType.FrameworkType]);
                dictionaryFactory = DictionaryFactoryCache.GetOrAdd(frameworkType, static type => BuildDictionaryFactory(type));
            }
        }
    }

    // Avoids Activator.CreateInstance(Type, params object[]), which resolves the constructor
    // and boxes the capacity on every call.
    private static Func<int, IDictionary> BuildDictionaryFactory(Type dictionaryType)
    {
        var constructor = dictionaryType.GetConstructor([typeof(int)])
            ?? throw new InvalidOperationException($"{dictionaryType} has no constructor taking a capacity");

        var capacity = Expression.Parameter(typeof(int), "capacity");
        var body = Expression.Convert(Expression.New(constructor, capacity), typeof(IDictionary));
        return Expression.Lambda<Func<int, IDictionary>>(body, capacity).Compile();
    }

    private static Func<int, IList> BuildListFactory(Type listType)
    {
        var constructor = listType.GetConstructor([typeof(int)])
            ?? throw new InvalidOperationException($"{listType} has no constructor taking a capacity");

        var capacity = Expression.Parameter(typeof(int), "capacity");
        var body = Expression.Convert(Expression.New(constructor, capacity), typeof(IList));
        return Expression.Lambda<Func<int, IList>>(body, capacity).Compile();
    }

    private static Func<object, object, object> BuildPairFactory(Type pairType)
    {
        var arguments = pairType.GetGenericArguments();
        var constructor = pairType.GetConstructor(arguments)
            ?? throw new InvalidOperationException($"{pairType} has no key-value constructor");

        var key = Expression.Parameter(typeof(object), "key");
        var value = Expression.Parameter(typeof(object), "value");
        var body = Expression.Convert(
            Expression.New(constructor, Expression.Convert(key, arguments[0]), Expression.Convert(value, arguments[1])),
            typeof(object));
        return Expression.Lambda<Func<object, object, object>>(body, key, value).Compile();
    }

    private static (Func<object, object> Key, Func<object, object> Value) BuildPairAccessors(Type pairType)
    {
        if (!pairType.IsGenericType || pairType.GetGenericTypeDefinition() != typeof(KeyValuePair<,>))
            throw new ArgumentException($"Map entries must be KeyValuePair<,>, got {pairType}", nameof(pairType));

        var pair = Expression.Parameter(typeof(object), "pair");
        var typed = Expression.Convert(pair, pairType);
        return (
            Expression.Lambda<Func<object, object>>(Expression.Convert(Expression.Property(typed, "Key"), typeof(object)), pair).Compile(),
            Expression.Lambda<Func<object, object>>(Expression.Convert(Expression.Property(typed, "Value"), typeof(object)), pair).Compile());
    }

    public ClickHouseType KeyType => keyType;

    public ClickHouseType ValueType => valueType;

    public override Type FrameworkType => frameworkType;

    public override string Name => "Map";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        var types = node.ChildNodes.Select(parseClickHouseTypeFunc).ToArray();
        var result = new MapType(settings.mapReadMode) { UnderlyingTypes = Tuple.Create(types[0], types[1]) };
        return result;
    }

    public override object Read(ExtendedBinaryReader reader)
    {
        var length = reader.Read7BitEncodedInt();

        // The number of key-value pairs is known up front, so size the result to it and
        // avoid repeated rehashing/resizing as entries are inserted (mirrors ArrayType.Read
        // pre-allocating the result array with its length).
        if (readMode == MapReadMode.KeyValuePairs)
        {
            var list = listFactory(length);
            for (var i = 0; i < length; i++)
            {
                var pairKey = KeyType.Read(reader);
                var pairValue = ClearDBNull(ValueType.Read(reader));
                list.Add(pairFactory(pairKey, pairValue));
            }
            return list;
        }

        var dict = dictionaryFactory(length);

        for (var i = 0; i < length; i++)
        {
            var key = KeyType.Read(reader); // null is not supported as dictionary key in C#
            var value = ClearDBNull(ValueType.Read(reader));

            // A ClickHouse map may repeat a key, a Dictionary cannot: the last pair wins.
            // MapReadMode.KeyValuePairs keeps every pair.
            dict[key] = value;
        }
        return dict;
    }

    public override string ToString() => $"{Name}({keyType}, {valueType})";

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        if (value is IDictionary dict)
        {
            writer.Write7BitEncodedInt(dict.Count);
            foreach (DictionaryEntry kvp in dict)
            {
                KeyType.Write(writer, kvp.Key);
                ValueType.Write(writer, kvp.Value);
            }
            return;
        }

        // The entry count precedes the entries, so the sequence is materialized first
        var entries = EnumerateEntries(value).ToList();

        writer.Write7BitEncodedInt(entries.Count);
        foreach (var entry in entries)
        {
            KeyType.Write(writer, entry.Key);
            ValueType.Write(writer, entry.Value);
        }
    }

    /// <summary>
    /// Reports whether a value is a map in one of the supported representations: an
    /// <see cref="IDictionary"/>, or a sequence of <see cref="KeyValuePair{TKey, TValue}"/> as
    /// returned by <see cref="MapReadMode.KeyValuePairs"/>.
    /// </summary>
    internal static bool IsMapValue(object value) =>
        value is IDictionary || (value is not null && EntrySequenceCache.GetOrAdd(value.GetType(), static type => IsEntrySequence(type)));

    private static bool IsEntrySequence(Type type) => type
        .GetInterfaces()
        .Concat([type])
        .Any(candidate => candidate.IsGenericType
            && candidate.GetGenericTypeDefinition() == typeof(IEnumerable<>)
            && IsKeyValuePair(candidate.GetGenericArguments()[0]));

    private static bool IsKeyValuePair(Type type) =>
        type.IsGenericType && type.GetGenericTypeDefinition() == typeof(KeyValuePair<,>);

    /// <summary>
    /// Enumerates the entries of a map value in either supported representation: an
    /// <see cref="IDictionary"/>, or a sequence of <see cref="KeyValuePair{TKey, TValue}"/> as
    /// returned by <see cref="MapReadMode.KeyValuePairs"/>.
    /// </summary>
    internal static IEnumerable<KeyValuePair<object, object>> EnumerateEntries(object value)
    {
        if (value is IDictionary dictionary)
        {
            foreach (DictionaryEntry entry in dictionary)
                yield return new KeyValuePair<object, object>(entry.Key, entry.Value);
            yield break;
        }

        if (!IsMapValue(value))
            throw new ArgumentException($"Cannot read map entries from {value?.GetType().ToString() ?? "null"}", nameof(value));

        foreach (var item in (IEnumerable)value)
        {
            var accessors = PairAccessorCache.GetOrAdd(item.GetType(), static type => BuildPairAccessors(type));
            yield return new KeyValuePair<object, object>(accessors.Key(item), accessors.Value(item));
        }
    }
}
