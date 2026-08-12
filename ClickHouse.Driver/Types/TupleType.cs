using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Types;

internal class TupleType : ParameterizedType
{
    // Compiled "new System.Tuple<...>(...)" factories, keyed by the concrete tuple CLR type.
    // Keying on frameworkType (a process-cached constructed generic type) means each distinct
    // tuple shape is compiled exactly once for the whole process. This matters on the Dynamic
    // read path, where BinaryTypeDecoder instantiates a fresh TupleType per value: a per-instance
    // cache would recompile the factory on every row, whereas this shared cache compiles once.
    private static readonly ConcurrentDictionary<Type, Delegate> TupleFactoryCache = new();

    private Type frameworkType;
    private ClickHouseType[] underlyingTypes;
    private Delegate tupleFactory;

    public ClickHouseType[] UnderlyingTypes
    {
        get => underlyingTypes;
        set
        {
            underlyingTypes = value;
            frameworkType = DeviseFrameworkType(underlyingTypes);
            tupleFactory = underlyingTypes.Length >= 1 && underlyingTypes.Length <= 7
                ? TupleFactoryCache.GetOrAdd(frameworkType, static type => BuildTupleFactory(type))
                : null;
        }
    }

    private static Type DeviseFrameworkType(ClickHouseType[] underlyingTypes)
    {
        var count = underlyingTypes.Length;

        if (count > 7)
            return typeof(LargeTuple);

        var typeArgs = new Type[count];
        for (var i = 0; i < count; i++)
        {
            typeArgs[i] = underlyingTypes[i].FrameworkType;
        }
        var genericType = Type.GetType("System.Tuple`" + typeArgs.Length);
        return genericType.MakeGenericType(typeArgs);
    }

    // Builds a Func<object, ..., object, ITuple> that constructs the tuple positionally, unboxing
    // each argument to its element framework type. Replaces the per-row Activator.CreateInstance
    // reflection invoke; combined with the arity-specialised Read path it also removes both the
    // object[] buffers that MakeTuple allocates.
    //
    // The element types are read back off the tuple type itself, so the factory is a pure function
    // of the cache key. The conversions unbox strictly, which relies on each ClickHouseType.Read
    // returning a value of exactly its own FrameworkType — already required by the Array and Map
    // read paths, which store into a typed array/dictionary. A future element type whose Read can
    // return null for a non-nullable value-type FrameworkType would need a null guard here, where
    // the old reflection invoke would silently have substituted default(T).
    private static Delegate BuildTupleFactory(Type tupleType)
    {
        var frameworkTypes = tupleType.GetGenericArguments();
        var count = frameworkTypes.Length;

        var constructor = tupleType.GetConstructor(frameworkTypes)
            ?? throw new InvalidOperationException($"{tupleType} has no constructor matching its element types");

        var parameters = new ParameterExpression[count];
        var arguments = new Expression[count];
        var funcTypeArgs = new Type[count + 1];
        for (var i = 0; i < count; i++)
        {
            parameters[i] = Expression.Parameter(typeof(object), "a" + i);
            arguments[i] = Expression.Convert(parameters[i], frameworkTypes[i]);
            funcTypeArgs[i] = typeof(object);
        }
        funcTypeArgs[count] = typeof(ITuple);

        var body = Expression.Convert(Expression.New(constructor, arguments), typeof(ITuple));
        return Expression.Lambda(Expression.GetFuncType(funcTypeArgs), body, parameters).Compile();
    }

    public ITuple MakeTuple(params object[] values)
    {
        var count = values.Length;
        if (underlyingTypes.Length != count)
            throw new ArgumentException($"Count of tuple type elements ({underlyingTypes.Length}) does not match number of elements ({count})");

        if (count > 7)
            return new LargeTuple(values);

        var valuesCopy = new object[count];

        // Coerce the values into types which can be stored in the tuple
        for (int i = 0; i < count; i++)
        {
            valuesCopy[i] = UnderlyingTypes[i].FrameworkType.IsSubclassOf(typeof(IConvertible)) ? Convert.ChangeType(values[i], UnderlyingTypes[i].FrameworkType, CultureInfo.InvariantCulture) : values[i];
        }

        return (ITuple)Activator.CreateInstance(frameworkType, valuesCopy);
    }

    public override Type FrameworkType => frameworkType;

    public override string Name => "Tuple";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return new TupleType
        {
            UnderlyingTypes = node.ChildNodes.Select(parseClickHouseTypeFunc).ToArray(),
        };
    }

    public override string ToString() => $"{Name}({string.Join(",", UnderlyingTypes.Select(t => t.ToString()))})";

    internal override string CacheSignature => $"{Name}({string.Join(",", UnderlyingTypes.Select(t => t.CacheSignature))})";

    public override object Read(ExtendedBinaryReader reader)
    {
        var count = UnderlyingTypes.Length;

        // Fast path for small tuples: construct the System.Tuple<...> directly through the cached
        // compiled factory, reading each element into a call argument. This avoids the two
        // intermediate object[] allocations and the reflection constructor invoke that the
        // MakeTuple/Activator path performs per row. Arguments are evaluated left to right, so
        // elements are still read from the stream in order.
        switch (count)
        {
            case 1:
                return ((Func<object, ITuple>)tupleFactory)(
                    ReadElement(reader, 0));
            case 2:
                return ((Func<object, object, ITuple>)tupleFactory)(
                    ReadElement(reader, 0), ReadElement(reader, 1));
            case 3:
                return ((Func<object, object, object, ITuple>)tupleFactory)(
                    ReadElement(reader, 0), ReadElement(reader, 1), ReadElement(reader, 2));
            case 4:
                return ((Func<object, object, object, object, ITuple>)tupleFactory)(
                    ReadElement(reader, 0), ReadElement(reader, 1), ReadElement(reader, 2), ReadElement(reader, 3));
            case 5:
                return ((Func<object, object, object, object, object, ITuple>)tupleFactory)(
                    ReadElement(reader, 0), ReadElement(reader, 1), ReadElement(reader, 2), ReadElement(reader, 3), ReadElement(reader, 4));
            case 6:
                return ((Func<object, object, object, object, object, object, ITuple>)tupleFactory)(
                    ReadElement(reader, 0), ReadElement(reader, 1), ReadElement(reader, 2), ReadElement(reader, 3), ReadElement(reader, 4), ReadElement(reader, 5));
            case 7:
                return ((Func<object, object, object, object, object, object, object, ITuple>)tupleFactory)(
                    ReadElement(reader, 0), ReadElement(reader, 1), ReadElement(reader, 2), ReadElement(reader, 3), ReadElement(reader, 4), ReadElement(reader, 5), ReadElement(reader, 6));
        }

        // Large tuples (> 7 elements) have no System.Tuple<...> arity; keep the object[]-backed
        // LargeTuple path unchanged.
        var contents = new object[count];
        for (var i = 0; i < count; i++)
            contents[i] = ClearDBNull(UnderlyingTypes[i].Read(reader));

        return MakeTuple(contents);
    }

    private object ReadElement(ExtendedBinaryReader reader, int index) =>
        ClearDBNull(underlyingTypes[index].Read(reader));

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        if (value is ITuple tuple)
        {
            if (tuple.Length != UnderlyingTypes.Length)
                throw new ArgumentException("Wrong number of elements in Tuple", nameof(value));
            for (var i = 0; i < tuple.Length; i++)
            {
                UnderlyingTypes[i].Write(writer, tuple[i]);
            }
            return;
        }

        if (value is IList list)
        {
            if (list.Count != UnderlyingTypes.Length)
                throw new ArgumentException("Wrong number of elements in Tuple", nameof(value));
            for (var i = 0; i < list.Count; i++)
            {
                UnderlyingTypes[i].Write(writer, list[i]);
            }
            return;
        }
    }
}
