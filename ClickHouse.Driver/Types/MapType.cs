using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class MapType : ParameterizedType
{
    private Type frameworkType;
    private ClickHouseType keyType;
    private ClickHouseType valueType;

    public Tuple<ClickHouseType, ClickHouseType> UnderlyingTypes
    {
        get => Tuple.Create(keyType, valueType);

        set
        {
            keyType = value.Item1;
            valueType = value.Item2;
            UpdateFrameworkType();
        }
    }

    internal bool MapAsListOfTuples { get; init; }

    private void UpdateFrameworkType()
    {
        if (MapAsListOfTuples)
        {
            // List<(TKey, TValue)>
            var tupleType = typeof(ValueTuple<,>).MakeGenericType(keyType.FrameworkType, valueType.FrameworkType);
            frameworkType = typeof(List<>).MakeGenericType(tupleType);
        }
        else
        {
            // Dictionary<TKey, TValue>
            frameworkType = typeof(Dictionary<,>).MakeGenericType(keyType.FrameworkType, valueType.FrameworkType);
        }
    }

    public ClickHouseType KeyType => keyType;

    public ClickHouseType ValueType => valueType;

    public override Type FrameworkType => frameworkType;

    public override string Name => "Map";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        var types = node.ChildNodes.Select(parseClickHouseTypeFunc).ToArray();
        var result = new MapType { MapAsListOfTuples = settings.mapAsListOfTuples, UnderlyingTypes = Tuple.Create(types[0], types[1]) };
        return result;
    }

    public override object Read(ExtendedBinaryReader reader)
    {
        var length = reader.Read7BitEncodedInt();

        if (MapAsListOfTuples)
        {
            var list = (IList)Activator.CreateInstance(FrameworkType);
            var tupleType = typeof(ValueTuple<,>).MakeGenericType(keyType.FrameworkType, valueType.FrameworkType);

            for (var i = 0; i < length; i++)
            {
                var key = KeyType.Read(reader);
                var value = ClearDBNull(ValueType.Read(reader));
                var tuple = Activator.CreateInstance(tupleType, key, value);
                list.Add(tuple);
            }
            return list;
        }
        else
        {
            var dict = (IDictionary)Activator.CreateInstance(FrameworkType);

            for (var i = 0; i < length; i++)
            {
                var key = KeyType.Read(reader); // null is not supported as dictionary key in C#
                var value = ClearDBNull(ValueType.Read(reader));
                dict[key] = value;
            }
            return dict;
        }
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
        }
        else if (value is IList list)
        {
            // Handle List<(TKey, TValue)>
            writer.Write7BitEncodedInt(list.Count);
            foreach (var item in list)
            {
                // item is a ValueTuple<TKey, TValue>
                var tupleType = item.GetType();
                var keyField = tupleType.GetField("Item1");
                var valueField = tupleType.GetField("Item2");
                KeyType.Write(writer, keyField.GetValue(item));
                ValueType.Write(writer, valueField.GetValue(item));
            }
        }
        else
        {
            throw new ArgumentException($"Cannot write value of type {value?.GetType().Name ?? "null"} as Map", nameof(value));
        }
    }
}
