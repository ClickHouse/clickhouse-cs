using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
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

            var genericType = typeof(Dictionary<,>);
            frameworkType = genericType.MakeGenericType([keyType.FrameworkType, valueType.FrameworkType]);
        }
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
        var dict = (IDictionary)Activator.CreateInstance(FrameworkType);

        var length = reader.Read7BitEncodedInt();

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

        // Also accept an ordered collection of KeyValuePair<,> (e.g. List<KeyValuePair<K,V>> or an array):
        // the shape a POCO Map property may use instead of Dictionary. Preserves order and duplicate keys.
        if (value is ICollection collection)
        {
            writer.Write7BitEncodedInt(collection.Count);
            PropertyInfo keyProperty = null;
            PropertyInfo valueProperty = null;
            foreach (var entry in collection)
            {
                if (keyProperty is null)
                {
                    var entryType = entry?.GetType();
                    if (entryType is not { IsGenericType: true } || entryType.GetGenericTypeDefinition() != typeof(KeyValuePair<,>))
                        throw new ArgumentException($"Map requires an IDictionary or a collection of KeyValuePair<,>, got element {entry?.GetType().Name ?? "null"}");
                    keyProperty = entryType.GetProperty("Key");
                    valueProperty = entryType.GetProperty("Value");
                }

                KeyType.Write(writer, keyProperty.GetValue(entry));
                ValueType.Write(writer, valueProperty.GetValue(entry));
            }
            return;
        }

        throw new ArgumentException($"Map requires an IDictionary or a collection of KeyValuePair<,>, got {value?.GetType().Name ?? "null"}");
    }
}
