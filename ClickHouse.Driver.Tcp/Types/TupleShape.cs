using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The CLR shape of a <c>Tuple(...)</c> of N elements: a <see cref="ValueTuple"/> type as C# writes it. A tuple of 1 to 7
/// elements is <c>ValueTuple&lt;T1, ..., Tn&gt;</c>. A tuple of more elements is
/// <c>ValueTuple&lt;T1, ..., T7, TRest&gt;</c>, where <c>TRest</c> is the <see cref="ValueTuple"/> of the elements after
/// the seventh, by the same rule. So <c>(1, 2, 3, 4, 5, 6, 7, 8, 9)</c> in C# is the value of a <c>Tuple</c> of 9
/// elements, and <c>Item9</c> in C# is <c>Rest.Item2</c>.
/// </summary>
internal static class TupleShape
{
    /// <summary>
    /// The most elements that this client reads or writes in one tuple. The cost to build the CLR type of a tuple and the
    /// converters over it grows quickly with the depth of the <c>TRest</c> nesting, and a very deep nesting makes the
    /// runtime overflow the stack.
    /// </summary>
    public const int MaxElements = 256;

    /// <summary>The position of <c>TRest</c> in the type arguments of an eight-argument <see cref="ValueTuple"/>.</summary>
    public const int RestIndex = 7;

    // The open generic ValueTuple definitions indexed by the number of type arguments (index 0 unused).
    private static readonly Type[] Definitions =
    {
        null,
        typeof(ValueTuple<>),
        typeof(ValueTuple<,>),
        typeof(ValueTuple<,,>),
        typeof(ValueTuple<,,,>),
        typeof(ValueTuple<,,,,>),
        typeof(ValueTuple<,,,,,>),
        typeof(ValueTuple<,,,,,,>),
        typeof(ValueTuple<,,,,,,,>),
    };

    /// <summary>The <see cref="ValueTuple"/> type of the elements, nested through <c>TRest</c> after the seventh.</summary>
    /// <param name="elements">The CLR types of the elements, at least one.</param>
    /// <returns>The tuple type.</returns>
    /// <exception cref="ArgumentException"><paramref name="elements"/> is empty.</exception>
    public static Type Of(ReadOnlySpan<Type> elements)
    {
        if (elements.IsEmpty)
        {
            throw new ArgumentException("A ValueTuple type needs at least one element.", nameof(elements));
        }

        if (elements.Length <= RestIndex)
        {
            return Definitions[elements.Length].MakeGenericType(elements.ToArray());
        }

        var arguments = new Type[RestIndex + 1];
        elements[..RestIndex].CopyTo(arguments);
        arguments[RestIndex] = Of(elements[RestIndex..]);
        return Definitions[RestIndex + 1].MakeGenericType(arguments);
    }

    /// <summary>
    /// The CLR types of the elements of a <see cref="ValueTuple"/> type, in order, with the elements of <c>TRest</c>
    /// after the seventh.
    /// </summary>
    /// <param name="type">The type.</param>
    /// <param name="elements">The element types, or null.</param>
    /// <returns>
    /// Whether <paramref name="type"/> is a <see cref="ValueTuple"/> of one or more elements. An eight-argument
    /// <see cref="ValueTuple"/> whose <c>TRest</c> is not a <see cref="ValueTuple"/> of one or more elements is not: its
    /// constructor refuses every value.
    /// </returns>
    public static bool TryGetElements(Type type, out Type[] elements)
    {
        var found = new List<Type>();
        elements = Collect(type, found) ? found.ToArray() : null;
        return elements is not null;
    }

    private static bool Collect(Type type, List<Type> found)
    {
        if (!type.IsGenericType || type.IsGenericTypeDefinition)
        {
            return false;
        }

        Type[] arguments = type.GetGenericArguments();
        if (arguments.Length >= Definitions.Length || type.GetGenericTypeDefinition() != Definitions[arguments.Length])
        {
            return false;
        }

        if (arguments.Length <= RestIndex)
        {
            found.AddRange(arguments);
            return true;
        }

        found.AddRange(arguments[..RestIndex]);
        return Collect(arguments[RestIndex], found);
    }
}
