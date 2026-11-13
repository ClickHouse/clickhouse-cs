using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>Builds a <see cref="ValueTuple"/> value of a type known only at run time.</summary>
internal static class ValueTuples
{
    /// <summary>
    /// A value of <paramref name="type"/> from the values of its elements in order. After the seventh element the values
    /// go into <c>TRest</c>, as C# puts them.
    /// </summary>
    /// <param name="type">The <see cref="ValueTuple"/> type.</param>
    /// <param name="values">The values of all its elements.</param>
    /// <returns>The tuple, boxed.</returns>
    public static object Create(Type type, IReadOnlyList<object> values) => Create(type, values, 0);

    private static object Create(Type type, IReadOnlyList<object> values, int start)
    {
        Type[] arguments = type.GetGenericArguments();
        var constructorArguments = new object[arguments.Length];
        for (int i = 0; i < arguments.Length; i++)
        {
            constructorArguments[i] = i == 7 ? Create(arguments[i], values, start + 7) : values[start + i];
        }

        return Activator.CreateInstance(type, constructorArguments);
    }
}
