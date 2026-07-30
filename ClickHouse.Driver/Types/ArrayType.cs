using System;
using System.Collections;
using System.Collections.Generic;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class ArrayType : ParameterizedType
{
    // Typed readers for common leaf element types, keyed by the element's CLR framework type.
    // These build a strongly-typed T[] and store elements through the array indexer, which
    // avoids the two reflection costs on the generic Read path below:
    //   - Array.CreateInstance(Type, int): a per-array reflection-driven allocation, and
    //   - Array.SetValue(object, int): a per-element reflection store with a type check and
    //     possible widening conversion.
    // Every element type maps to exactly one FrameworkType whose Read() returns exactly that
    // boxed type, so the typed (T) unbox is behaviourally equivalent to SetValue (minus the
    // reflection). Element types without an entry fall through to the reflection path, so
    // correctness is unchanged for everything (nested/composite elements, big integers,
    // decimals read as ClickHouseDecimal, IP addresses, etc.).
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

    public override object Read(ExtendedBinaryReader reader)
    {
        var length = reader.Read7BitEncodedInt();

        // Resolve the element FrameworkType once. Wrapper types build it reflectively on every
        // access (NullableType -> MakeGenericType, ArrayType -> MakeArrayType), so caching avoids
        // a redundant reflective call on the fallback path, which needs it both for the reader
        // lookup and the array allocation.
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

        // Register a value type and its Nullable<T> form (Array(Nullable(T)) reports the
        // Nullable<T> framework type). ClearDBNull maps the DBNull null-sentinel to a real
        // null before the (T?) unbox.
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

        // Reference type: Nullable(String) shares the same framework type (string), so one
        // entry covers both String and Nullable(String).
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
