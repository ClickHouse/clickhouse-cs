using System;
using System.Collections.Generic;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Poco;

/// <summary>
/// Reads a ClickHouse <c>Map(K,V)</c> column into an ordered sequence of <see cref="KeyValuePair{TKey,TValue}"/>
/// for the POCO read fast path, as an alternative to the <see cref="System.Collections.Generic.Dictionary{TKey,TValue}"/>
/// the boxed <see cref="MapType.Read"/> produces. Unlike the dictionary, this preserves the on-wire order and
/// any duplicate keys (a ClickHouse Map is physically an array of key/value tuples).
///
/// Element values are decoded through the key/value types' own (boxed) <see cref="ClickHouseType.Read"/> — a Map
/// is a composite column, not a box-free scalar — so <typeparamref name="TKey"/>/<typeparamref name="TValue"/>
/// must exactly match the map's key/value framework types; the caller (<c>PocoReadExpressionFactory</c>) enforces this.
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
