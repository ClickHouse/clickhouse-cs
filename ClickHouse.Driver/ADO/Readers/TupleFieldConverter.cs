using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace ClickHouse.Driver.ADO.Readers;

/// <summary>
/// Converts a materialized tuple value into a caller-requested <see cref="ValueTuple"/> or
/// <see cref="Tuple{T1,T2,T3,T4,T5,T6,T7,TRest}"/> shape for <see cref="ClickHouseDataReader.GetFieldValue{T}"/>.
/// </summary>
/// <remarks>
/// <para>The reader materializes a tuple as <c>System.Tuple</c> (1–7 elements) or <c>LargeTuple</c> (otherwise),
/// and an array of tuples as an array of those. Neither is a <see cref="ValueTuple"/>, and an 8+ element tuple is
/// never a <c>Tuple`8</c>, so a plain cast to such a target always throws. This converter covers exactly those
/// targets and leaves every other <typeparamref name="T"/> on the plain cast.</para>
///
/// <para>The source is read through <see cref="ITuple"/>, whose indexing is flat for both source shapes. In the
/// 8-argument <c>ValueTuple</c> and <c>Tuple</c> forms the last generic argument is <c>TRest</c> by CLR definition
/// and continues that flat indexing from position 7, so a tuple nested as a real element is never confused with
/// <c>TRest</c>. A target holding a tuple type whose <c>TRest</c> is not a tuple of the same kind is not a valid
/// tuple shape (its constructor rejects it), so it throws for any data.</para>
///
/// <para>A part of the target that contains no <see cref="ValueTuple"/> or <c>Tuple`8</c> is assigned with the
/// same strict cast <see cref="ClickHouseDataReader.GetFieldValue{T}"/> applies to a whole column: no numeric
/// widening, no date conversion, and null only into a reference or <see cref="Nullable{T}"/> type. A null nested
/// value converts to null when its target is an array or a <c>System.Tuple</c>; a null column value always throws,
/// as on the plain cast.</para>
///
/// <para>An empty array has no elements to check, so its element type is checked against the target instead: it
/// passes when some value of that type would convert. A <c>Nullable(T)</c> element may be NULL, so it passes for
/// any target that holds null. An element read as a reference type (<c>String</c>, <c>IPv4</c>/<c>IPv6</c> and
/// others) may be NULL too, but its CLR type does not tell <c>Nullable(String)</c> from <c>String</c>, so it is
/// checked by type alone. A <c>LargeTuple</c> element type
/// (an 8+ element tuple or the empty <c>Tuple()</c>) does not record the element count or types, so an empty array
/// of those is checked only for being an array of tuples.</para>
/// </remarks>
internal static class TupleFieldConverter
{
    private static readonly PropertyInfo TupleIndexer = typeof(ITuple).GetProperty("Item");

    // The open System.Tuple definitions indexed by arity (index 0 unused).
    private static readonly Type[] SystemTupleDefinitions =
    {
        null,
        typeof(Tuple<>),
        typeof(Tuple<,>),
        typeof(Tuple<,,>),
        typeof(Tuple<,,,>),
        typeof(Tuple<,,,,>),
        typeof(Tuple<,,,,,>),
        typeof(Tuple<,,,,,,>),
    };

    private static readonly MethodInfo AsTupleMethod =
        typeof(TupleFieldConverter).GetMethod(nameof(AsTuple), BindingFlags.NonPublic | BindingFlags.Static);

    private static readonly MethodInfo CastElementMethod =
        typeof(TupleFieldConverter).GetMethod(nameof(CastElement), BindingFlags.NonPublic | BindingFlags.Static);

    private static readonly MethodInfo ConvertArrayMethod =
        typeof(TupleFieldConverter).GetMethod(nameof(ConvertArray), BindingFlags.NonPublic | BindingFlags.Static);

    // Whether an empty source array with a given element type may convert to a given target element type.
    private static readonly ConcurrentDictionary<(Type Source, Type Target), bool> EmptyArrayCompatibility = new();

    /// <summary>
    /// True when <paramref name="type"/> contains a <see cref="ValueTuple"/> (of any arity, the empty one included) or a
    /// <c>Tuple`8</c> anywhere: as itself, as an element of a single-dimensional array, or as an element of a tuple.
    /// </summary>
    public static bool RequiresConversion(Type type)
    {
        if (IsSingleDimensionalArray(type))
            return RequiresConversion(type.GetElementType());

        if (type == typeof(ValueTuple))
            return true;

        if (!type.IsGenericType)
            return false;

        var definition = type.GetGenericTypeDefinition();
        if (IsValueTupleDefinition(definition) || definition == typeof(Tuple<,,,,,,,>))
            return true;

        if (!IsSystemTupleDefinition(definition))
            return false;

        foreach (var argument in type.GetGenericArguments())
        {
            if (RequiresConversion(argument))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Returns the cached converter to <typeparamref name="T"/>. Call only when <see cref="RequiresConversion"/>
    /// holds for <typeparamref name="T"/>. A shape mismatch throws <see cref="InvalidCastException"/>.
    /// </summary>
    public static T Convert<T>(object value) => Cache<T>.Converter(value);

    private static class Cache<T>
    {
        public static readonly Func<object, T> Converter = BuildConverter<T>();
    }

    private static Func<object, T> BuildConverter<T>()
    {
        // Checked once for the whole target, before any data: a null nested value would otherwise skip the invalid
        // part of the target, and the outcome would depend on the data.
        var invalid = FindUnconstructibleTuple(typeof(T));
        if (invalid != null)
        {
            var message = $"'{invalid}' is not a valid tuple type: its last type argument must be a tuple of the same kind.";
            return _ => throw new InvalidCastException(message);
        }

        return (Func<object, T>)Build(typeof(T), path: string.Empty);
    }

    /// <param name="target">The type to convert to.</param>
    /// <param name="path">The position of the value in the column, such as <c>Item2[].Item9</c>, for error messages.</param>
    private static Delegate Build(Type target, string path)
    {
        var source = Expression.Parameter(typeof(object), "value");
        var body = BuildValue(target, source, path);
        return Expression.Lambda(Expression.GetFuncType(typeof(object), target), body, source).Compile();
    }

    private static Expression BuildValue(Type target, Expression source, string path)
    {
        if (!RequiresConversion(target))
            return Expression.Call(CastElementMethod.MakeGenericMethod(target), source, Expression.Constant(path));

        if (IsSingleDimensionalArray(target))
        {
            var elementType = target.GetElementType();
            return Expression.Call(
                ConvertArrayMethod.MakeGenericMethod(elementType),
                source,
                Expression.Constant(Build(elementType, path + "[]")),
                Expression.Constant(path));
        }

        // The source is read once: a nested source is an ITuple indexer call, which boxes a value-type element.
        var value = Expression.Variable(typeof(object), "value");
        var tuple = Expression.Variable(typeof(ITuple), "tuple");
        Expression converted = Expression.Block(
            target,
            new[] { tuple },
            Expression.Assign(
                tuple,
                Expression.Call(AsTupleMethod, value, Expression.Constant(GetFlatArity(target)), Expression.Constant(target), Expression.Constant(path))),
            BuildTuple(target, tuple, offset: 0, path));

        converted = TryBuildSystemTupleFastPath(target, value, converted) ?? converted;

        // A System.Tuple target is a reference type, so a null nested value converts to null as it does for a plain
        // reference element. AsTuple still rejects DBNull, the null of a whole column.
        if (!target.IsValueType)
        {
            converted = Expression.Condition(
                Expression.Equal(value, Expression.Constant(null)),
                Expression.Constant(null, target),
                converted);
        }

        return Expression.Block(target, new[] { value }, Expression.Assign(value, source), converted);
    }

    /// <summary>
    /// For a 1–7 element <see cref="ValueTuple"/> whose elements need no conversion, the source is usually exactly
    /// <c>System.Tuple</c> of the same element types. Copying its typed <c>ItemN</c> properties avoids the box per
    /// value-type element that reading through <see cref="ITuple"/> costs. Any other source takes
    /// <paramref name="fallback"/>.
    /// </summary>
    private static BlockExpression TryBuildSystemTupleFastPath(Type target, Expression source, Expression fallback)
    {
        var arguments = target.GetGenericArguments();
        if (arguments.Length == 0 || arguments.Length >= 8 || !IsValueTupleDefinition(target.GetGenericTypeDefinition()))
            return null;

        foreach (var argument in arguments)
        {
            if (RequiresConversion(argument))
                return null;
        }

        var sourceType = SystemTupleDefinitions[arguments.Length].MakeGenericType(arguments);
        var typed = Expression.Variable(sourceType, "typed");
        var items = new Expression[arguments.Length];
        for (var i = 0; i < arguments.Length; i++)
            items[i] = Expression.Property(typed, "Item" + (i + 1));

        return Expression.Block(
            target,
            new[] { typed },
            Expression.Assign(typed, Expression.TypeAs(source, sourceType)),
            Expression.Condition(
                Expression.NotEqual(typed, Expression.Constant(null, sourceType)),
                Expression.New(target.GetConstructor(arguments), items),
                fallback));
    }

    private static Expression BuildTuple(Type target, ParameterExpression tuple, int offset, string path)
    {
        var arguments = target.GetGenericArguments();

        // The empty ValueTuple has no constructor to call.
        if (arguments.Length == 0)
            return Expression.Default(target);

        var values = new Expression[arguments.Length];
        for (var i = 0; i < arguments.Length; i++)
        {
            // Positions are named as C# names a flattened tuple's elements: Item1, Item2, ... Item9 and on.
            values[i] = i == 7
                ? BuildTuple(arguments[i], tuple, offset + 7, path)
                : BuildValue(
                    arguments[i],
                    Expression.Property(tuple, TupleIndexer, Expression.Constant(offset + i)),
                    (path.Length == 0 ? "Item" : path + ".Item") + (offset + i + 1));
        }

        return Expression.New(target.GetConstructor(arguments), values);
    }

    /// <summary>
    /// Returns the number of elements <paramref name="tupleType"/> holds once its <c>TRest</c> chain is flattened,
    /// or -1 when a <c>TRest</c> is not a tuple of the same family.
    /// </summary>
    private static int GetFlatArity(Type tupleType)
    {
        var arguments = tupleType.GetGenericArguments();
        if (arguments.Length < 8)
            return arguments.Length;

        var rest = arguments[7];
        var sameKind = IsValueTupleDefinition(tupleType.GetGenericTypeDefinition())
            ? IsValueTupleType(rest)
            : rest.IsGenericType && IsSystemTupleDefinition(rest.GetGenericTypeDefinition());
        if (!sameKind)
            return -1;

        var restArity = GetFlatArity(rest);
        return restArity < 0 ? -1 : 7 + restArity;
    }

    /// <summary>
    /// Returns the first tuple type in <paramref name="target"/> (itself, array elements, tuple elements, the type a
    /// <see cref="Nullable{T}"/> wraps) whose <c>TRest</c> is not a tuple of its own kind, which the <c>Tuple`8</c>
    /// and <c>ValueTuple`8</c> constructors require; null when there is none.
    /// </summary>
    private static Type FindUnconstructibleTuple(Type target)
    {
        // A Nullable<ValueTuple> is not converted, only cast, but a NULL would still let an invalid one through.
        target = Nullable.GetUnderlyingType(target) ?? target;

        if (IsSingleDimensionalArray(target))
            return FindUnconstructibleTuple(target.GetElementType());

        if (!IsValueTupleType(target) && !(target.IsGenericType && IsSystemTupleDefinition(target.GetGenericTypeDefinition())))
            return null;

        var elements = new List<Type>();
        if (!FlattenTupleElements(target, elements))
            return target;

        foreach (var element in elements)
        {
            var invalid = FindUnconstructibleTuple(element);
            if (invalid != null)
                return invalid;
        }
        return null;
    }

    private static string At(string path) => path.Length == 0 ? string.Empty : path + ": ";

    private static ITuple AsTuple(object value, int arity, Type target, string path)
    {
        if (value is ITuple tuple && tuple.Length == arity)
            return tuple;

        throw new InvalidCastException(At(path) + value switch
        {
            null or DBNull => $"A null value cannot be converted to '{target}'.",
            ITuple other => $"A tuple of {other.Length} elements cannot be converted to '{target}', which holds {arity}.",
            _ => $"A value of type '{value.GetType()}' is not a tuple and cannot be converted to '{target}'.",
        });
    }

    private static T CastElement<T>(object value, string path)
    {
        if (value is T typed)
            return typed;

        if (value is null)
        {
            if (default(T) is null)
                return default;

            throw new InvalidCastException($"{At(path)}A null value cannot be converted to the non-nullable '{typeof(T)}'.");
        }

        // The same cast GetFieldValue<T> applies to a whole column, so its rules carry over (unboxing also accepts an
        // enum's underlying type, which the type test above does not). Only the position is added to its message.
        try
        {
            return (T)value;
        }
        catch (InvalidCastException ex)
        {
            throw new InvalidCastException(At(path) + ex.Message, ex);
        }
    }

    private static T[] ConvertArray<T>(object value, Func<object, T> convertElement, string path)
    {
        // A null nested value converts to null as it does for a plain reference element; DBNull, the null of a whole
        // column, is rejected below.
        if (value is null)
            return null;

        if (value is not IList list)
        {
            throw new InvalidCastException(At(path) + (value is DBNull
                ? $"A null value cannot be converted to '{typeof(T[])}'."
                : $"A value of type '{value.GetType()}' is not an array and cannot be converted to '{typeof(T[])}'."));
        }

        if (list.Count == 0)
        {
            var sourceElementType = GetListElementType(list.GetType());
            if (!EmptyArrayCompatibility.GetOrAdd((sourceElementType, typeof(T)), static key => IsCompatible(key.Source, key.Target)))
            {
                throw new InvalidCastException(
                    $"{At(path)}An empty array of '{sourceElementType}' cannot be converted to '{typeof(T[])}'.");
            }
            return Array.Empty<T>();
        }

        var result = new T[list.Count];
        for (var i = 0; i < result.Length; i++)
            result[i] = convertElement(list[i]);
        return result;
    }

    private static Type GetListElementType(Type type)
    {
        if (type.IsArray)
            return type.GetElementType();

        foreach (var candidate in type.GetInterfaces())
        {
            if (candidate.IsGenericType && candidate.GetGenericTypeDefinition() == typeof(IList<>))
                return candidate.GetGenericArguments()[0];
        }
        return typeof(object);
    }

    /// <summary>
    /// Whether some value of static type <paramref name="source"/> converts to <paramref name="target"/>. Answers
    /// "no" only when no value of that type can, so an unknown shape (<see cref="object"/>, <c>LargeTuple</c>) passes.
    /// The target is known to be constructible: <see cref="BuildConverter{T}"/> rejects it otherwise.
    /// </summary>
    private static bool IsCompatible(Type source, Type target)
    {
        // NULL converts to null for a target that holds null. Nullable(Nothing) reads as DBNull and only ever holds
        // NULL; a Nullable(T) element may hold it as well as a T.
        if (source == typeof(DBNull))
            return CanHoldNull(target);

        if (Nullable.GetUnderlyingType(source) != null && CanHoldNull(target))
            return true;

        if (!RequiresConversion(target))
            return IsCastCompatible(source, target);

        if (source == typeof(object))
            return true;

        if (IsSingleDimensionalArray(target))
        {
            if (!typeof(IList).IsAssignableFrom(source))
                return false;

            return IsCompatible(GetListElementType(source), target.GetElementType());
        }

        if (!typeof(ITuple).IsAssignableFrom(source))
            return false;

        // Only a System.Tuple records its element types; the reader never produces a Tuple`8.
        if (!source.IsGenericType || !IsSystemTupleDefinition(source.GetGenericTypeDefinition()))
            return true;

        var sourceElements = source.GetGenericArguments();
        var targetElements = new List<Type>();
        FlattenTupleElements(target, targetElements);
        if (sourceElements.Length != targetElements.Count)
            return false;

        for (var i = 0; i < sourceElements.Length; i++)
        {
            if (!IsCompatible(sourceElements[i], targetElements[i]))
                return false;
        }
        return true;
    }

    private static bool CanHoldNull(Type type) => !type.IsValueType || Nullable.GetUnderlyingType(type) != null;

    /// <summary>
    /// Collects the element types of <paramref name="tupleType"/> with its <c>TRest</c> chain flattened; false when a
    /// <c>TRest</c> is not a tuple of the same family.
    /// </summary>
    private static bool FlattenTupleElements(Type tupleType, List<Type> elements)
    {
        if (GetFlatArity(tupleType) < 0)
            return false;

        while (true)
        {
            var arguments = tupleType.GetGenericArguments();
            for (var i = 0; i < Math.Min(arguments.Length, 7); i++)
                elements.Add(arguments[i]);

            if (arguments.Length < 8)
                return true;

            tupleType = arguments[7];
        }
    }

    /// <summary>
    /// Whether the cast <see cref="CastElement{T}"/> applies can succeed for some non-null value of static type
    /// <paramref name="source"/>.
    /// </summary>
    private static bool IsCastCompatible(Type source, Type target)
    {
        // A boxed value is never a Nullable<T>: a Nullable<T> source boxes as its T, which is what the cast sees.
        source = Nullable.GetUnderlyingType(source) ?? source;
        if (target.IsAssignableFrom(source) || source.IsAssignableFrom(target))
            return true;

        // Unboxing into Nullable<T> needs T exactly; unboxing into a plain value type also lets an enum and its
        // underlying type stand in for each other.
        var targetUnderlying = Nullable.GetUnderlyingType(target);
        if (targetUnderlying != null)
            return source == targetUnderlying;

        return EnumUnderlyingOrSelf(source) == EnumUnderlyingOrSelf(target);
    }

    private static Type EnumUnderlyingOrSelf(Type type) => type.IsEnum ? Enum.GetUnderlyingType(type) : type;

    private static bool IsValueTupleType(Type type) =>
        type == typeof(ValueTuple) || (type.IsGenericType && IsValueTupleDefinition(type.GetGenericTypeDefinition()));

    private static bool IsSingleDimensionalArray(Type type) =>
        type.IsArray && type == type.GetElementType().MakeArrayType();

    private static bool IsValueTupleDefinition(Type definition) =>
        definition == typeof(ValueTuple<>) ||
        definition == typeof(ValueTuple<,>) ||
        definition == typeof(ValueTuple<,,>) ||
        definition == typeof(ValueTuple<,,,>) ||
        definition == typeof(ValueTuple<,,,,>) ||
        definition == typeof(ValueTuple<,,,,,>) ||
        definition == typeof(ValueTuple<,,,,,,>) ||
        definition == typeof(ValueTuple<,,,,,,,>);

    private static bool IsSystemTupleDefinition(Type definition) =>
        definition == typeof(Tuple<>) ||
        definition == typeof(Tuple<,>) ||
        definition == typeof(Tuple<,,>) ||
        definition == typeof(Tuple<,,,>) ||
        definition == typeof(Tuple<,,,,>) ||
        definition == typeof(Tuple<,,,,,>) ||
        definition == typeof(Tuple<,,,,,,>) ||
        definition == typeof(Tuple<,,,,,,,>);
}
