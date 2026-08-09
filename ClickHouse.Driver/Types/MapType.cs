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
    // Compiled "new Dictionary<TKey, TValue>(capacity)" factories, keyed by the concrete dictionary
    // CLR type. Keying on frameworkType (a process-cached constructed generic type) means each
    // distinct map shape is compiled exactly once for the whole process. This matters on the Dynamic
    // read path, where BinaryTypeDecoder instantiates a fresh MapType per value: a per-instance
    // cache would recompile the factory on every row, whereas this shared cache compiles once.
    private static readonly ConcurrentDictionary<Type, Func<int, IDictionary>> DictionaryFactoryCache = new();

    private Type frameworkType;
    private ClickHouseType keyType;
    private ClickHouseType valueType;
    private Func<int, IDictionary> dictionaryFactory;

    public Tuple<ClickHouseType, ClickHouseType> UnderlyingTypes
    {
        get => Tuple.Create(keyType, valueType);

        set
        {
            keyType = value.Item1;
            valueType = value.Item2;

            var genericType = typeof(Dictionary<,>);
            frameworkType = genericType.MakeGenericType([keyType.FrameworkType, valueType.FrameworkType]);
            dictionaryFactory = DictionaryFactoryCache.GetOrAdd(frameworkType, static type => BuildDictionaryFactory(type));
        }
    }

    // Builds a Func<int, IDictionary> constructing the dictionary with a known capacity. Replaces
    // Activator.CreateInstance(Type, params object[]), whose binder-based constructor resolution
    // runs on every call and allocates an object[1] plus a box for the capacity.
    private static Func<int, IDictionary> BuildDictionaryFactory(Type dictionaryType)
    {
        var constructor = dictionaryType.GetConstructor([typeof(int)])
            ?? throw new InvalidOperationException($"{dictionaryType} has no constructor taking a capacity");

        var capacity = Expression.Parameter(typeof(int), "capacity");
        var body = Expression.Convert(Expression.New(constructor, capacity), typeof(IDictionary));
        return Expression.Lambda<Func<int, IDictionary>>(body, capacity).Compile();
    }

    public ClickHouseType KeyType => keyType;

    public ClickHouseType ValueType => valueType;

    public override Type FrameworkType => frameworkType;

    public override string Name => "Map";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        var types = node.ChildNodes.Select(parseClickHouseTypeFunc).ToArray();
        var result = new MapType() { UnderlyingTypes = Tuple.Create(types[0], types[1]) };
        return result;
    }

    public override object Read(ExtendedBinaryReader reader)
    {
        var length = reader.Read7BitEncodedInt();

        // The number of key-value pairs is known up front, so size the dictionary
        // to it and avoid repeated rehashing/resizing as entries are inserted
        // (mirrors ArrayType.Read pre-allocating the result array with its length).
        var dict = dictionaryFactory(length);

        for (var i = 0; i < length; i++)
        {
            var key = KeyType.Read(reader); // null is not supported as dictionary key in C#
            var value = ClearDBNull(ValueType.Read(reader));
            dict[key] = value;
        }
        return dict;
    }

    public override string ToString() => $"{Name}({keyType}, {valueType})";

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        var dict = (IDictionary)value;
        writer.Write7BitEncodedInt(dict.Count);
        foreach (DictionaryEntry kvp in dict)
        {
            KeyType.Write(writer, kvp.Key);
            ValueType.Write(writer, kvp.Value);
        }
    }
}
