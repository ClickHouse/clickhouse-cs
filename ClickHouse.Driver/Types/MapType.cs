using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class MapType : ParameterizedType
{
    // Process-wide so each map shape is built once, even though the Dynamic read path
    // creates a fresh MapType per value.
    private static readonly ConcurrentDictionary<(Type Key, Type Value), MapShape> ShapeCache = new();
    private static readonly ConcurrentDictionary<Type, (Func<object, object> Key, Func<object, object> Value)> PairAccessorCache = new();
    private static readonly ConcurrentDictionary<Type, (Type Key, Type Value)[]> EntryTypesCache = new();

    private static readonly MethodInfo BuildShapeMethod =
        typeof(MapType).GetMethod(nameof(BuildShape), BindingFlags.NonPublic | BindingFlags.Static);

    private static readonly MethodInfo BuildPairAccessorsMethod =
        typeof(MapType).GetMethod(nameof(BuildPairAccessors), BindingFlags.NonPublic | BindingFlags.Static);

    private readonly MapReadMode readMode;
    private Type frameworkType;
    private (Type Key, Type Value) entryTypes;
    private ClickHouseType keyType;
    private ClickHouseType valueType;
    private MapShape shape;

    // See EntryTypesIdentifyMap: 0 until first read, then 1 (true) or 2 (false).
    private int entriesIdentifyMap;

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

            // Wrapper types (Array, Nullable) rebuild their FrameworkType per access,
            // and CanWrite is called per value written, so resolve the pair once
            entryTypes = (keyType.FrameworkType, valueType.FrameworkType);
            shape = ShapeCache.GetOrAdd(entryTypes, static types => GetShape(types.Key, types.Value, RuntimeFeature.IsDynamicCodeSupported));
            entriesIdentifyMap = 0;
            frameworkType = readMode == MapReadMode.KeyValuePairs ? shape.ListType : shape.DictionaryType;
        }
    }

    /// <summary>
    /// Gets the CLR representation of a map whose key and value have the given framework types. With
    /// dynamic code it is typed (<c>Dictionary&lt;TKey, TValue&gt;</c>). Native AOT cannot instantiate a
    /// generic type over a value type it did not compile ahead of time, so there it is typed only for the
    /// shapes listed below, and otherwise holds <see cref="object"/> keys and values.
    /// </summary>
    /// <param name="key">The key's framework type.</param>
    /// <param name="value">The value's framework type.</param>
    /// <param name="dynamicCode"><see cref="RuntimeFeature.IsDynamicCodeSupported"/>, a parameter so tests can pass false.</param>
    internal static MapShape GetShape(Type key, Type value, bool dynamicCode)
    {
        if (dynamicCode)
            return BuildTypedShape(key, value);

        if (key == typeof(string) && value == typeof(string))
            return BuildShape<string, string>();

        return BuildShape<object, object>() with { IsObjectFallback = key != typeof(object) || value != typeof(object) };
    }

    [UnconditionalSuppressMessage("AotAnalysis", "IL3050", Justification = "Called only when dynamic code is supported.")]
    [UnconditionalSuppressMessage("Trimming", "IL2060", Justification = "The generic method's type parameters have no DynamicallyAccessedMembers requirements.")]
    private static MapShape BuildTypedShape(Type key, Type value) =>
        (MapShape)BuildShapeMethod.MakeGenericMethod(key, value).Invoke(null, null);

    // Avoids Activator.CreateInstance(Type, params object[]), which resolves the constructor
    // and boxes the capacity on every call.
    private static MapShape BuildShape<TKey, TValue>() => new(
        typeof(Dictionary<TKey, TValue>),
        static capacity => new Dictionary<TKey, TValue>(capacity),
        typeof(List<KeyValuePair<TKey, TValue>>),
        static capacity => new List<KeyValuePair<TKey, TValue>>(capacity),
        static (key, value) => new KeyValuePair<TKey, TValue>((TKey)key, (TValue)value));

    internal static (Func<object, object> Key, Func<object, object> Value) GetPairAccessors(Type pairType, bool dynamicCode)
    {
        if (!IsKeyValuePair(pairType))
            throw new ArgumentException($"Map entries must be KeyValuePair<,>, got {pairType}", nameof(pairType));

        if (pairType == typeof(KeyValuePair<object, object>))
            return BuildPairAccessors<object, object>();

        if (pairType == typeof(KeyValuePair<string, string>))
            return BuildPairAccessors<string, string>();

        if (!dynamicCode)
        {
            throw new NotSupportedException(
                $"Cannot read map entries of type {pairType} under native AOT. Pass the map as an IDictionary instead.");
        }

        return BuildTypedPairAccessors(pairType);
    }

    [UnconditionalSuppressMessage("AotAnalysis", "IL3050", Justification = "Called only when dynamic code is supported.")]
    [UnconditionalSuppressMessage("Trimming", "IL2060", Justification = "The generic method's type parameters have no DynamicallyAccessedMembers requirements.")]
    private static (Func<object, object> Key, Func<object, object> Value) BuildTypedPairAccessors(Type pairType) =>
        ((Func<object, object> Key, Func<object, object> Value))BuildPairAccessorsMethod
            .MakeGenericMethod(pairType.GetGenericArguments())
            .Invoke(null, null);

    private static (Func<object, object> Key, Func<object, object> Value) BuildPairAccessors<TKey, TValue>() => (
        static pair => ((KeyValuePair<TKey, TValue>)pair).Key,
        static pair => ((KeyValuePair<TKey, TValue>)pair).Value);

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
            var list = shape.NewList(length);
            for (var i = 0; i < length; i++)
            {
                var pairKey = KeyType.Read(reader);
                var pairValue = ClearDBNull(ValueType.Read(reader));
                list.Add(shape.NewPair(pairKey, pairValue));
            }
            return list;
        }

        var dict = shape.NewDictionary(length);

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

    /// <summary>
    /// Reads the map into an ordered sequence of <see cref="KeyValuePair{TKey,TValue}"/> for the POCO read
    /// fast path, preserving the on-wire order and any duplicate keys that the <see cref="Read"/> dictionary
    /// collapses. The POCO property type selects this, so it applies per-property and independently of
    /// <see cref="MapReadMode"/>.
    ///
    /// A map is a composite column, so keys and values still go through the boxed
    /// <see cref="ClickHouseType.Read"/>; only the pair itself is built unboxed. <typeparamref name="TKey"/>
    /// and <typeparamref name="TValue"/> must therefore match the key/value framework types exactly, which
    /// <c>PocoReadExpressionFactory</c> enforces.
    /// </summary>
    public List<KeyValuePair<TKey, TValue>> ReadList<TKey, TValue>(ExtendedBinaryReader reader)
    {
        var length = reader.Read7BitEncodedInt();
        var list = new List<KeyValuePair<TKey, TValue>>(length);
        for (var i = 0; i < length; i++)
            list.Add(ReadPair<TKey, TValue>(reader));
        return list;
    }

    /// <inheritdoc cref="ReadList{TKey,TValue}"/>
    public KeyValuePair<TKey, TValue>[] ReadArray<TKey, TValue>(ExtendedBinaryReader reader)
    {
        var length = reader.Read7BitEncodedInt();
        var array = new KeyValuePair<TKey, TValue>[length];
        for (var i = 0; i < length; i++)
            array[i] = ReadPair<TKey, TValue>(reader);
        return array;
    }

    // Keys are never null in a ClickHouse map. A null value (Map(K, Nullable(V))) becomes default(TValue),
    // matching Read's ClearDBNull, since TValue is the nullable or reference framework type.
    private KeyValuePair<TKey, TValue> ReadPair<TKey, TValue>(ExtendedBinaryReader reader)
    {
        var key = (TKey)KeyType.Read(reader);
        var value = ClearDBNull(ValueType.Read(reader));
        return new KeyValuePair<TKey, TValue>(key, value is null ? default : (TValue)value);
    }

    public override string ToString() => $"{Name}({keyType}, {valueType})";

    internal override string CacheSignature =>
        ComposeCacheSignature(children => $"{Name}({children[0]}, {children[1]})", keyType, valueType);

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
        value is IDictionary || (value is not null && GetEntryTypes(value.GetType()).Length > 0);

    /// <summary>
    /// A Variant member is selected by <see cref="CanWrite"/>, so it must accept every
    /// representation <see cref="Write"/> accepts, in either <see cref="MapReadMode"/>. A
    /// dictionary and a key-value-pair sequence both enumerate as
    /// <see cref="KeyValuePair{TKey, TValue}"/>, so the entry types identify the map, unless the key or value
    /// type is one whose CLR type several ClickHouse types share (see <see cref="ValueFit.IdentifiedByFrameworkType"/>):
    /// then the entries also have to fit. Under native AOT a map can read with <see cref="object"/> keys and
    /// values, so one that does also accepts object entries that its key and value types accept.
    /// </summary>
    public override bool CanWrite(object value)
    {
        if (value is null || entryTypes.Key is null)
            return false;

        var valueEntryTypes = GetEntryTypes(value.GetType());
        if (Array.IndexOf(valueEntryTypes, entryTypes) >= 0)
            return EntryTypesIdentifyMap || value is not ICollection || EntriesFit(value);

        return shape.IsObjectFallback
            && value is ICollection
            && Array.IndexOf(valueEntryTypes, (typeof(object), typeof(object))) >= 0
            && EntriesFit(value);
    }

    // Enumerates the value, so callers pass only a collection: Write enumerates it again afterwards, and a
    // one-shot sequence would then be empty.
    private bool EntriesFit(object value) =>
        EnumerateEntries(value).All(entry => ValueFit.Fits(keyType, entry.Key) && ValueFit.Fits(valueType, entry.Value));

    /// <summary>
    /// Whether a map's entry types alone identify this type: its key and value types are identified by their CLR
    /// types (see <see cref="ValueFit.IdentifiedByFrameworkType"/>), and it does not read with the native AOT
    /// <see cref="object"/> fallback.
    /// </summary>
    internal bool EntriesIdentifyMap => EntryTypesIdentifyMap && !shape.IsObjectFallback;

    // Computed on first use, by CanWrite only: reading a Dynamic value builds a new type per value.
    private bool EntryTypesIdentifyMap
    {
        get
        {
            var state = entriesIdentifyMap;
            if (state == 0)
            {
                var identified = ValueFit.IdentifiedByFrameworkType(keyType) && ValueFit.IdentifiedByFrameworkType(valueType);
                entriesIdentifyMap = state = identified ? 1 : 2;
            }

            return state == 1;
        }
    }

    /// <summary>
    /// The key and value types of every <see cref="KeyValuePair{TKey, TValue}"/> sequence the type
    /// enumerates as. Empty when the type is not a map representation.
    /// </summary>
    // The type is a value's runtime type, so its interfaces are those it is cast to and enumerated through.
    [UnconditionalSuppressMessage("Trimming", "IL2070", Justification = "Reads the interfaces of an instantiated type.")]
    private static (Type Key, Type Value)[] GetEntryTypes(Type type) =>
        EntryTypesCache.GetOrAdd(type, static candidate => candidate
            .GetInterfaces()
            .Concat([candidate])
            .Where(iface => iface.IsGenericType
                && iface.GetGenericTypeDefinition() == typeof(IEnumerable<>)
                && IsKeyValuePair(iface.GetGenericArguments()[0]))
            .Select(iface => iface.GetGenericArguments()[0].GetGenericArguments())
            .Select(arguments => (arguments[0], arguments[1]))
            .Distinct()
            .ToArray());

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
            var accessors = PairAccessorCache.GetOrAdd(item.GetType(), static type => GetPairAccessors(type, RuntimeFeature.IsDynamicCodeSupported));
            yield return new KeyValuePair<object, object>(accessors.Key(item), accessors.Value(item));
        }
    }

#pragma warning disable SA1313 // Positional record parameters are properties
    internal sealed record MapShape(
        Type DictionaryType,
        Func<int, IDictionary> NewDictionary,
        Type ListType,
        Func<int, IList> NewList,
        Func<object, object, object> NewPair)
    {
        /// <summary>Gets a value indicating whether the shape holds objects only because the typed one could not be built.</summary>
        public bool IsObjectFallback { get; init; }
    }
#pragma warning restore SA1313
}
