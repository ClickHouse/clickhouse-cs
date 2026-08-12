using System;
using System.Collections.Generic;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Poco;

/// <summary>
/// Reads a ClickHouse <c>Map(K,V)</c> column into an ordered sequence of <see cref="KeyValuePair{TKey,TValue}"/>
/// for the POCO read fast path, preserving the on-wire order and any duplicate keys that the boxed
/// <see cref="MapType.Read"/>'s dictionary collapses. The property type selects this over a dictionary, so it
/// works per-property and independently of the client-level <see cref="MapReadMode"/>.
///
/// A Map is a composite column, so keys and values still go through the boxed
/// <see cref="ClickHouseType.Read"/>; only the pair itself is built unboxed. <typeparamref name="TKey"/> and
/// <typeparamref name="TValue"/> must therefore match the map's framework types exactly, which
/// <c>PocoReadExpressionFactory</c> enforces.
/// </summary>
internal static class MapMaterializer
{
    public static List<KeyValuePair<TKey, TValue>> ReadList<TKey, TValue>(MapType map, ExtendedBinaryReader reader)
    {
        var length = reader.Read7BitEncodedInt();
        var list = new List<KeyValuePair<TKey, TValue>>(length);
        for (var i = 0; i < length; i++)
            list.Add(ReadPair<TKey, TValue>(map, reader));
        return list;
    }

    public static KeyValuePair<TKey, TValue>[] ReadArray<TKey, TValue>(MapType map, ExtendedBinaryReader reader)
    {
        var length = reader.Read7BitEncodedInt();
        var array = new KeyValuePair<TKey, TValue>[length];
        for (var i = 0; i < length; i++)
            array[i] = ReadPair<TKey, TValue>(map, reader);
        return array;
    }

    private static KeyValuePair<TKey, TValue> ReadPair<TKey, TValue>(MapType map, ExtendedBinaryReader reader)
    {
        // Keys are never null in a ClickHouse Map. A null value (Map(K, Nullable(V))) is surfaced as
        // default(TValue), matching the boxed Read's ClearDBNull (TValue is the nullable/reference framework type).
        var key = (TKey)map.KeyType.Read(reader);
        var rawValue = map.ValueType.Read(reader);
        var value = rawValue is DBNull ? default : (TValue)rawValue;
        return new KeyValuePair<TKey, TValue>(key, value);
    }
}
