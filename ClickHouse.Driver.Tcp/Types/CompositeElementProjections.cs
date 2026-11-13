using System;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>The element type of a CLR array type, for the write shapes of the composite codecs.</summary>
internal static class CompositeElementProjections
{
    /// <summary>Gets the element type when <paramref name="candidate"/> is <c>T[]</c>.</summary>
    public static bool TryGetArrayElement(Type candidate, out Type elementType)
    {
        if (candidate.IsSZArray)
        {
            elementType = candidate.GetElementType();
            return true;
        }

        elementType = null;
        return false;
    }
}
