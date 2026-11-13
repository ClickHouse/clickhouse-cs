using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// The facts about a target column that the row inserts ask: the CLR types that its messages suggest, and whether it
/// holds NULL.
/// </summary>
internal static class PocoWriteConversion
{
    /// <summary>
    /// Returns the codec's preferred writable CLR types for row columns. The messages of the row inserts list them.
    /// </summary>
    /// <param name="codec">The target column's codec.</param>
    /// <returns>The write types, possibly none.</returns>
    public static IReadOnlyList<Type> AcceptedWriteTypes(IColumnCodec codec)
    {
        IReadOnlyList<Type> writable = codec.WritableElementTypes;
        var accepted = new List<Type>(writable.Count);
        for (int i = 0; i < writable.Count; i++)
        {
            if (codec.CanWriteElementType(writable[i]))
            {
                accepted.Add(writable[i]);
            }
        }

        return accepted;
    }

    /// <summary>
    /// Returns whether the codec has a true NULL representation rather than a non-null placeholder.
    /// </summary>
    /// <param name="codec">The target column's codec.</param>
    /// <returns>Whether a null can be written to it.</returns>
    public static bool TakesNull(IColumnCodec codec) => codec.NullPlaceholder is null;
}
