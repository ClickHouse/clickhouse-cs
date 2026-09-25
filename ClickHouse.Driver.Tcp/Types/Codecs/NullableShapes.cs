using System;
using System.Collections.Concurrent;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>Resolves and caches the <see cref="INullableShape"/> for a given inner element type.</summary>
internal static class NullableShapes
{
    private static readonly ConcurrentDictionary<Type, INullableShape> Cache = new();

    /// <summary>Returns the shape for <paramref name="elementType"/>, building it once and caching it.</summary>
    /// <param name="elementType">The inner codec's CLR element type.</param>
    /// <returns>The shape.</returns>
    public static INullableShape For(Type elementType) => Cache.GetOrAdd(elementType, Build);

    private static INullableShape Build(Type elementType)
    {
        Type shapeType = elementType.IsValueType
            ? typeof(ValueNullableShape<>).MakeGenericType(elementType)
            : typeof(ReferenceNullableShape<>).MakeGenericType(elementType);

        // nonPublic: true so the shape's (implicit, but internal-assembly) constructor is always reachable here.
        return (INullableShape)Activator.CreateInstance(shapeType, nonPublic: true);
    }
}
