using System;
using System.Collections.Generic;
using System.Linq;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class VariantType : ParameterizedType
{
    // Below this many underlying types a linear scan beats a dictionary lookup: the hash + probe
    // costs more than a couple of CanWrite calls. Benchmarks (issue #493, worst-case last-element
    // match) put the crossover at 3 types, so 2-type variants (the common Variant(T, String) shape)
    // stay on the linear path and only 3+ build the lookup.
    private const int MinTypesForMap = 3;

    private ClickHouseType[] underlyingTypes;

    // Maps a value's runtime type to the underlying variant candidates that share that FrameworkType,
    // in ascending index order. Lets GetMatchingType do an O(1) hash lookup instead of an O(n) scan.
    // Null for small variants (see MinTypesForMap) — GetMatchingType then uses the linear scan.
    private Dictionary<Type, (int Index, ClickHouseType Type)[]> writeLookup;

    public ClickHouseType[] UnderlyingTypes
    {
        get => underlyingTypes;
        internal set
        {
            underlyingTypes = value;
            writeLookup = BuildWriteLookup(value);
        }
    }

    public override Type FrameworkType => typeof(object);

    public override string Name => "Variant";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return new VariantType
        {
            UnderlyingTypes = node.ChildNodes.Select(parseClickHouseTypeFunc).ToArray(),
        };
    }

    public override string ToString() => $"{Name}({string.Join(",", UnderlyingTypes.Select(t => t.ToString()))})";

    public override object Read(ExtendedBinaryReader reader)
    {
        var typeIndex = reader.ReadByte();
        if (typeIndex == 0xFF)
            return DBNull.Value;

        var type = UnderlyingTypes[typeIndex];

        return type.Read(reader);
    }

    public (int, ClickHouseType) GetMatchingType(object value)
    {
        // Fast path: a ClickHouseType only accepts values whose runtime type equals its FrameworkType
        // (the IPv4/IPv6 pair shares FrameworkType=IPAddress and is disambiguated within its bucket),
        // so every candidate that CanWrite(value) lives in the bucket keyed by value.GetType(). The
        // bucket is ordered by index, so the first match is the same one the linear scan would return.
        if (value != null && writeLookup != null && writeLookup.TryGetValue(value.GetType(), out var candidates))
        {
            foreach (var (index, type) in candidates)
            {
                if (type.CanWrite(value))
                {
                    return (index, type);
                }
            }
        }

        // Small variants skip the lookup (writeLookup is null); this is also the fallback for any
        // type whose CanWrite accepts a value whose runtime type differs from its FrameworkType.
        for (int i = 0; i < UnderlyingTypes.Length; i++)
        {
            if (UnderlyingTypes[i].CanWrite(value))
            {
                return (i, UnderlyingTypes[i]);
            }
        }
        throw new ArgumentException("Could not find matching type for variant", nameof(value));
    }

    private static Dictionary<Type, (int Index, ClickHouseType Type)[]> BuildWriteLookup(ClickHouseType[] types)
    {
        if (types is null || types.Length < MinTypesForMap)
        {
            return null;
        }

        var buckets = new Dictionary<Type, List<(int, ClickHouseType)>>();
        for (int i = 0; i < types.Length; i++)
        {
            var key = types[i].FrameworkType;
            if (key is null)
            {
                continue;
            }

            if (!buckets.TryGetValue(key, out var list))
            {
                list = new List<(int, ClickHouseType)>();
                buckets[key] = list;
            }

            list.Add((i, types[i]));
        }

        return buckets.ToDictionary(kv => kv.Key, kv => kv.Value.ToArray());
    }

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        if (value is null or DBNull)
        {
            writer.Write((byte)0xFF);
            return;
        }

        var (index, type) = GetMatchingType(value);
        writer.Write((byte)index);
        type.Write(writer, value);
    }
}
