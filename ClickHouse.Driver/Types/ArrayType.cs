using System;
using System.Collections;
using System.Collections.Generic;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class ArrayType : ParameterizedType
{
    // Typed readers for common leaf element types (keyed by element CLR type) fill a strongly-typed
    // T[] via the indexer instead of the reflective Array.CreateInstance/SetValue fallback below.
    // Unregistered types (nested/composite, big integers, ClickHouseDecimal, IP) use the fallback.
    private static readonly Dictionary<Type, Func<ClickHouseType, ExtendedBinaryReader, int, Array>> TypedReaders = BuildTypedReaders();

    public ClickHouseType UnderlyingType { get; set; }

    public override Type FrameworkType => UnderlyingType.FrameworkType.MakeArrayType();

    public override string Name => "Array";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return new ArrayType
        {
            UnderlyingType = parseClickHouseTypeFunc(node.SingleChild),
        };
    }

    public override string ToString() => $"{Name}({UnderlyingType})";

    internal override string CacheSignature =>
        ComposeCacheSignature(children => $"{Name}({children[0]})", UnderlyingType);

    public override object Read(ExtendedBinaryReader reader)
    {
        var length = reader.Read7BitEncodedInt();

        // Resolve the element FrameworkType once (wrapper types rebuild it reflectively per access).
        var elementType = UnderlyingType.FrameworkType;

        if (TypedReaders.TryGetValue(elementType, out var typedReader))
            return typedReader(UnderlyingType, reader, length);

        // Fallback: reflection-based read for element types without a typed reader.
        var data = Array.CreateInstance(elementType, length);
        for (var i = 0; i < length; i++)
        {
            data.SetValue(ClearDBNull(UnderlyingType.Read(reader)), i);
        }
        return data;
    }

    private static Array ReadTyped<T>(ClickHouseType elementType, ExtendedBinaryReader reader, int length)
    {
        var data = new T[length];
        for (var i = 0; i < length; i++)
        {
            data[i] = (T)ClearDBNull(elementType.Read(reader));
        }
        return data;
    }

    private static Dictionary<Type, Func<ClickHouseType, ExtendedBinaryReader, int, Array>> BuildTypedReaders()
    {
        var readers = new Dictionary<Type, Func<ClickHouseType, ExtendedBinaryReader, int, Array>>();

        // Register a value type and its Nullable<T> form (ClearDBNull restores the null sentinel).
        void AddValue<T>()
            where T : struct
        {
            readers[typeof(T)] = ReadTyped<T>;
            readers[typeof(T?)] = ReadTyped<T?>;
        }

        AddValue<sbyte>();
        AddValue<byte>();
        AddValue<short>();
        AddValue<ushort>();
        AddValue<int>();
        AddValue<uint>();
        AddValue<long>();
        AddValue<ulong>();
        AddValue<float>();
        AddValue<double>();
        AddValue<decimal>();
        AddValue<bool>();
        AddValue<DateTime>();
        AddValue<Guid>();

        // String and Nullable(String) share the same framework type (string).
        readers[typeof(string)] = ReadTyped<string>;

        return readers;
    }

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        if (value is null || value is DBNull)
        {
            writer.Write7BitEncodedInt(0);
            return;
        }

        // Rank>1 CLR arrays (e.g. byte[,]) iterate flattened via IEnumerable/IList. Walk the
        // axes directly via MultiDimArrayHelper so leaf scalars are written without per-row
        // sub-array allocation. Rank-1 arrays (including jagged outer T[][]) keep the IList
        // path because the outer rank is 1 even though the element type is itself an array.
        if (value is Array multidim && multidim.Rank > 1)
        {
            var leaf = MultiDimArrayHelper.ResolveLeafType(this, multidim.Rank);
            MultiDimArrayHelper.WriteMultidimensional(writer, multidim, leaf);
            return;
        }

        var collection = (IList)value;
        writer.Write7BitEncodedInt(collection.Count);
        for (var i = 0; i < collection.Count; i++)
        {
            UnderlyingType.Write(writer, collection[i]);
        }
    }
}
