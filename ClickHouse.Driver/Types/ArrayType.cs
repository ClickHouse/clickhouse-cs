using System;
using System.Collections;
using System.Collections.Generic;
using System.Numerics;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types.Grammar;

namespace ClickHouse.Driver.Types;

internal class ArrayType : ParameterizedType
{
    // Typed readers for common leaf element types (keyed by element CLR type) fill a strongly-typed
    // T[] via the indexer instead of the reflective Array.CreateInstance/SetValue fallback below.
    // Unregistered types (nested/composite, big integers, ClickHouseDecimal, IP) use the fallback.
    private static readonly Dictionary<Type, Func<ClickHouseType, ExtendedBinaryReader, int, Array>> TypedReaders = BuildTypedReaders();

    // Typed writers for the CLR element types some column type can serialize box-free (keyed by the runtime
    // element type of the array being written), so a T[] of a value type is written without boxing every
    // element on the IList path. Every ITypedWriter<T> target of a value type needs an entry here, which
    // ArrayTypeWriteTests.TypedWriters_CoverEveryValueTypeTypedWriteTarget checks.
    private static readonly Dictionary<Type, Func<ClickHouseType, ExtendedBinaryWriter, Array, bool>> TypedWriters = BuildTypedWriters();

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

        // An element type that can decode straight into T does so: the boxed Read would produce the very
        // same value (T is the element's own FrameworkType), only through a box per element.
        if (TransparentWrapper.Unwrap(elementType) is ITypedReader<T> typedReader)
        {
            for (var i = 0; i < length; i++)
            {
                data[i] = typedReader.ReadValue(reader);
            }
            return data;
        }

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

        // Every collection[i] below boxes a value-type element. When the CLR element type is one the column
        // can serialize box-free, write the array through that instead. Declines (writing nothing) unless
        // the element type implements ITypedWriter for this exact CLR type, so a T[] the boxed path would
        // have coerced (a long[] on Array(Int32)) keeps going through Convert as before.
        if (value is Array typedArray
            && TypedWriters.TryGetValue(typedArray.GetType().GetElementType(), out var typedWriter)
            && typedWriter(TransparentWrapper.Unwrap(UnderlyingType), writer, typedArray))
        {
            return;
        }

        var collection = (IList)value;
        writer.Write7BitEncodedInt(collection.Count);
        for (var i = 0; i < collection.Count; i++)
        {
            UnderlyingType.Write(writer, collection[i]);
        }
    }

    // Returns false without writing anything when the column cannot serialize T box-free, leaving the
    // caller's boxed loop to write the whole array.
    private static bool WriteTyped<T>(ClickHouseType elementType, ExtendedBinaryWriter writer, Array array)
    {
        // The T[] test also rejects an array whose element type matches but whose layout does not: a rank-1
        // array with a non-zero lower bound reports the same element type and is not a T[]. Those keep the
        // IList path, which indexes from the array's own lower bound.
        if (elementType is not ITypedWriter<T> typedWriter || array is not T[] values)
            return false;

        writer.Write7BitEncodedInt(values.Length);
        for (var i = 0; i < values.Length; i++)
        {
            typedWriter.WriteValue(writer, values[i]);
        }
        return true;
    }

    private static Dictionary<Type, Func<ClickHouseType, ExtendedBinaryWriter, Array, bool>> BuildTypedWriters() => new()
    {
        [typeof(sbyte)] = WriteTyped<sbyte>,
        [typeof(byte)] = WriteTyped<byte>,
        [typeof(short)] = WriteTyped<short>,
        [typeof(ushort)] = WriteTyped<ushort>,
        [typeof(int)] = WriteTyped<int>,
        [typeof(uint)] = WriteTyped<uint>,
        [typeof(long)] = WriteTyped<long>,
        [typeof(ulong)] = WriteTyped<ulong>,
        [typeof(float)] = WriteTyped<float>,
        [typeof(double)] = WriteTyped<double>,
        [typeof(decimal)] = WriteTyped<decimal>,
        [typeof(ClickHouseDecimal)] = WriteTyped<ClickHouseDecimal>,
        [typeof(bool)] = WriteTyped<bool>,
        [typeof(BigInteger)] = WriteTyped<BigInteger>,
        [typeof(Guid)] = WriteTyped<Guid>,
        [typeof(TimeSpan)] = WriteTyped<TimeSpan>,
        [typeof(DateTime)] = WriteTyped<DateTime>,
        [typeof(DateTimeOffset)] = WriteTyped<DateTimeOffset>,
        [typeof(DateOnly)] = WriteTyped<DateOnly>,
    };
}
