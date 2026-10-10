using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>The fact about a target column that the row inserts ask: whether it holds NULL.</summary>
internal static class PocoWriteConversion
{
    /// <summary>
    /// Returns whether the codec has a true NULL representation rather than a non-null placeholder.
    /// </summary>
    /// <param name="codec">The target column's codec.</param>
    /// <returns>Whether a null can be written to it.</returns>
    public static bool TakesNull(IColumnCodec codec) => codec.NullPlaceholder is null;
}
