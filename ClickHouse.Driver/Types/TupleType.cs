using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Types;

internal class TupleType : ParameterizedType
{
    // "new System.Tuple<...>(...)" factories, keyed by the concrete tuple CLR type. Keying on
    // frameworkType (a process-cached constructed generic type) means each distinct tuple shape is
    // built exactly once for the whole process. This matters on the Dynamic read path, where
    // BinaryTypeDecoder instantiates a fresh TupleType per value.
    private static readonly ConcurrentDictionary<Type, Delegate> TupleFactoryCache = new();

    // System.Tuple has arities 1 to 7 (Tuple`8 nests the rest), indexed by arity - 1.
    private static readonly Type[] TupleDefinitions =
    [
        typeof(Tuple<>), typeof(Tuple<,>), typeof(Tuple<,,>), typeof(Tuple<,,,>),
        typeof(Tuple<,,,,>), typeof(Tuple<,,,,,>), typeof(Tuple<,,,,,,>),
    ];

    private static readonly MethodInfo[] FactoryMethods =
    [
        typeof(TupleType).GetMethod(nameof(Factory1), BindingFlags.NonPublic | BindingFlags.Static),
        typeof(TupleType).GetMethod(nameof(Factory2), BindingFlags.NonPublic | BindingFlags.Static),
        typeof(TupleType).GetMethod(nameof(Factory3), BindingFlags.NonPublic | BindingFlags.Static),
        typeof(TupleType).GetMethod(nameof(Factory4), BindingFlags.NonPublic | BindingFlags.Static),
        typeof(TupleType).GetMethod(nameof(Factory5), BindingFlags.NonPublic | BindingFlags.Static),
        typeof(TupleType).GetMethod(nameof(Factory6), BindingFlags.NonPublic | BindingFlags.Static),
        typeof(TupleType).GetMethod(nameof(Factory7), BindingFlags.NonPublic | BindingFlags.Static),
    ];

    private Type frameworkType;
    private ClickHouseType[] underlyingTypes;
    private Delegate tupleFactory;

    public ClickHouseType[] UnderlyingTypes
    {
        get => underlyingTypes;
        set
        {
            underlyingTypes = value;
            (frameworkType, tupleFactory) = GetShape(underlyingTypes, RuntimeFeature.IsDynamicCodeSupported);
        }
    }

    /// <summary>
    /// Gets the CLR tuple type for the element types, and the factory that builds it (null for
    /// <see cref="LargeTuple"/>, which <see cref="MakeTuple"/> builds). With dynamic code it is a
    /// <c>System.Tuple&lt;...&gt;</c> of the element framework types for 1 to 7 elements. Native AOT cannot
    /// instantiate a generic type over a value type it did not compile ahead of time, so there it is a
    /// <c>System.Tuple</c> only for the shapes listed below, and a <see cref="LargeTuple"/> otherwise.
    /// </summary>
    /// <param name="underlyingTypes">The element types.</param>
    /// <param name="dynamicCode"><see cref="RuntimeFeature.IsDynamicCodeSupported"/>, a parameter so tests can pass false.</param>
    internal static (Type FrameworkType, Delegate Factory) GetShape(ClickHouseType[] underlyingTypes, bool dynamicCode)
    {
        var count = underlyingTypes.Length;

        // There is no System.Tuple`0, and no System.Tuple arity above 7
        if (count == 0 || count > 7)
            return (typeof(LargeTuple), null);

        var typeArgs = new Type[count];
        for (var i = 0; i < count; i++)
        {
            typeArgs[i] = underlyingTypes[i].FrameworkType;
        }

        if (dynamicCode)
        {
            var tupleType = MakeTupleType(typeArgs);
            return (tupleType, TupleFactoryCache.GetOrAdd(tupleType, static type => BuildTypedFactory(type)));
        }

        // Point, and the geo types built from it
        if (count == 2 && typeArgs[0] == typeof(double) && typeArgs[1] == typeof(double))
            return (typeof(Tuple<double, double>), Factory2<double, double>());

        return (typeof(LargeTuple), null);
    }

    [UnconditionalSuppressMessage("AotAnalysis", "IL3050", Justification = "Called only when dynamic code is supported.")]
    [UnconditionalSuppressMessage("Trimming", "IL2055", Justification = "System.Tuple's type parameters have no DynamicallyAccessedMembers requirements.")]
    private static Type MakeTupleType(Type[] typeArgs) => TupleDefinitions[typeArgs.Length - 1].MakeGenericType(typeArgs);

    [UnconditionalSuppressMessage("AotAnalysis", "IL3050", Justification = "Called only when dynamic code is supported.")]
    [UnconditionalSuppressMessage("Trimming", "IL2060", Justification = "The generic method's type parameters have no DynamicallyAccessedMembers requirements.")]
    private static Delegate BuildTypedFactory(Type tupleType)
    {
        var typeArgs = tupleType.GetGenericArguments();
        return (Delegate)FactoryMethods[typeArgs.Length - 1].MakeGenericMethod(typeArgs).Invoke(null, null);
    }

    // Each factory constructs the tuple positionally, unboxing each argument to its element framework
    // type. With the arity-specialised Read path this avoids the object[] buffers and the reflection
    // constructor invoke that MakeTuple's LargeTuple path would cost per row.
    //
    // The casts unbox strictly, which relies on each ClickHouseType.Read returning a value of exactly its
    // own FrameworkType, as the Array and Map read paths, which store into a typed array or dictionary,
    // already require.
    private static Func<object, ITuple> Factory1<T1>() =>
        static a0 => new Tuple<T1>((T1)a0);

    private static Func<object, object, ITuple> Factory2<T1, T2>() =>
        static (a0, a1) => new Tuple<T1, T2>((T1)a0, (T2)a1);

    private static Func<object, object, object, ITuple> Factory3<T1, T2, T3>() =>
        static (a0, a1, a2) => new Tuple<T1, T2, T3>((T1)a0, (T2)a1, (T3)a2);

    private static Func<object, object, object, object, ITuple> Factory4<T1, T2, T3, T4>() =>
        static (a0, a1, a2, a3) => new Tuple<T1, T2, T3, T4>((T1)a0, (T2)a1, (T3)a2, (T4)a3);

    private static Func<object, object, object, object, object, ITuple> Factory5<T1, T2, T3, T4, T5>() =>
        static (a0, a1, a2, a3, a4) => new Tuple<T1, T2, T3, T4, T5>((T1)a0, (T2)a1, (T3)a2, (T4)a3, (T5)a4);

    private static Func<object, object, object, object, object, object, ITuple> Factory6<T1, T2, T3, T4, T5, T6>() =>
        static (a0, a1, a2, a3, a4, a5) => new Tuple<T1, T2, T3, T4, T5, T6>((T1)a0, (T2)a1, (T3)a2, (T4)a3, (T5)a4, (T6)a5);

    private static Func<object, object, object, object, object, object, object, ITuple> Factory7<T1, T2, T3, T4, T5, T6, T7>() =>
        static (a0, a1, a2, a3, a4, a5, a6) => new Tuple<T1, T2, T3, T4, T5, T6, T7>((T1)a0, (T2)a1, (T3)a2, (T4)a3, (T5)a4, (T6)a5, (T7)a6);

    public ITuple MakeTuple(params object[] values)
    {
        var count = values.Length;
        if (underlyingTypes.Length != count)
            throw new ArgumentException($"Count of tuple type elements ({underlyingTypes.Length}) does not match number of elements ({count})");

        return tupleFactory switch
        {
            Func<object, ITuple> f => f(values[0]),
            Func<object, object, ITuple> f => f(values[0], values[1]),
            Func<object, object, object, ITuple> f => f(values[0], values[1], values[2]),
            Func<object, object, object, object, ITuple> f => f(values[0], values[1], values[2], values[3]),
            Func<object, object, object, object, object, ITuple> f => f(values[0], values[1], values[2], values[3], values[4]),
            Func<object, object, object, object, object, object, ITuple> f => f(values[0], values[1], values[2], values[3], values[4], values[5]),
            Func<object, object, object, object, object, object, object, ITuple> f => f(values[0], values[1], values[2], values[3], values[4], values[5], values[6]),
            _ => new LargeTuple(values),
        };
    }

    public override Type FrameworkType => frameworkType;

    /// <summary>
    /// A Variant member is selected by <see cref="CanWrite"/>. Under native AOT a tuple of 1 to 7 elements can read
    /// as a <see cref="LargeTuple"/>, which callers cannot construct, so such a tuple also accepts any
    /// <see cref="ITuple"/> of its arity whose elements its element types accept, as <see cref="Write"/> does.
    /// </summary>
    public override bool CanWrite(object value)
    {
        if (base.CanWrite(value))
            return true;

        var count = underlyingTypes.Length;
        if (frameworkType != typeof(LargeTuple) || count is 0 or > 7 || value is not ITuple tuple || tuple.Length != count)
            return false;

        for (var i = 0; i < count; i++)
        {
            var element = ClearDBNull(tuple[i]);
            var elementType = underlyingTypes[i] is NullableType nullable ? nullable.UnderlyingType : underlyingTypes[i];
            if (element is null ? underlyingTypes[i] is not NullableType : !elementType.CanWrite(element))
                return false;
        }

        return true;
    }

    public override string Name => "Tuple";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return new TupleType
        {
            UnderlyingTypes = node.ChildNodes.Select(parseClickHouseTypeFunc).ToArray(),
        };
    }

    public override string ToString() => $"{Name}({string.Join(",", UnderlyingTypes.Select(t => t.ToString()))})";

    internal override string CacheSignature =>
        ComposeCacheSignature(children => $"{Name}({string.Join(",", children)})", UnderlyingTypes);

    public override object Read(ExtendedBinaryReader reader)
    {
        var count = tupleFactory is null ? 0 : UnderlyingTypes.Length;

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

        // Large tuples (> 7 elements) have no System.Tuple<...> arity, and under native AOT most
        // small tuples have no compiled one either; both read into the object[]-backed LargeTuple.
        var contents = new object[UnderlyingTypes.Length];
        for (var i = 0; i < contents.Length; i++)
            contents[i] = ClearDBNull(UnderlyingTypes[i].Read(reader));

        return new LargeTuple(contents);
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
