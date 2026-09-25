using System;
using System.Collections.Concurrent;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>Resolves and caches the <see cref="ILowCardinalityShape"/> for a given inner element type.</summary>
internal static class LowCardinalityShapes
{
    private static readonly ConcurrentDictionary<(Type ElementType, bool Nullable), ILowCardinalityShape> Cache = new();

    /// <summary>Returns the shape for <paramref name="elementType"/>, building it once and caching it.</summary>
    /// <param name="elementType">The inner (bare) codec's CLR element type.</param>
    /// <param name="nullable">Whether the inner is <c>Nullable</c> — the surfaced element type is then made nullable and the dictionary reserves a NULL slot.</param>
    /// <returns>The shape.</returns>
    public static ILowCardinalityShape For(Type elementType, bool nullable) => Cache.GetOrAdd((elementType, nullable), Build);

    // nonPublic: true so the shape's (implicit, but internal-assembly) constructor is always reachable here.
    private static ILowCardinalityShape Build((Type ElementType, bool Nullable) key)
    {
        Type shapeType = key.Nullable
            ? (key.ElementType.IsValueType ? typeof(ValueLowCardinalityShape<>) : typeof(ReferenceLowCardinalityShape<>))
            : typeof(LowCardinalityShape<>);

        return (ILowCardinalityShape)Activator.CreateInstance(shapeType.MakeGenericType(key.ElementType), nonPublic: true);
    }
}
