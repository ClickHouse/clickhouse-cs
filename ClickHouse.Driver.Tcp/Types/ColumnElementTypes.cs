using System;
using System.Collections.Concurrent;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>Resolves and caches the <c>T</c> in <see cref="IColumn{T}"/>.</summary>
internal static class ColumnElementTypes
{
    private static readonly ConcurrentDictionary<Type, Type> Cache = new();

    /// <summary>Returns the element type <paramref name="columnType"/> surfaces.</summary>
    /// <exception cref="InvalidOperationException">The type implements zero or multiple <see cref="IColumn{T}"/> interfaces.</exception>
    public static Type Of(Type columnType) => Cache.GetOrAdd(columnType, static type =>
    {
        Type found = null;
        foreach (Type candidate in type.GetInterfaces())
        {
            if (!candidate.IsGenericType || candidate.GetGenericTypeDefinition() != typeof(IColumn<>))
            {
                continue;
            }

            if (found is not null)
            {
                throw new InvalidOperationException($"Column type '{type}' implements IColumn<> more than once, so it has no single element type.");
            }

            found = candidate.GenericTypeArguments[0];
        }

        return found ?? throw new InvalidOperationException($"Column type '{type}' does not implement IColumn<>.");
    });
}
