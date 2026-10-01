using System.Text;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Builds cache keys for the block shapes used by POCO plans.
/// </summary>
internal static class PocoBlockSignature
{
    /// <summary>
    /// Returns a collision-safe key for the context, column names, and types.
    /// </summary>
    public static string Of(Block block, in ResolveContext context)
    {
        var key = new StringBuilder();
        Append(key, ContextKey(in context));
        for (int i = 0; i < block.ColumnCount; i++)
        {
            IColumn column = block[i];
            Append(key, column.Name);
            Append(key, column.TypeName);
        }

        return key.ToString();
    }

    /// <summary>Returns the context that affects codec resolution but is absent from the block header.</summary>
    public static string ContextKey(in ResolveContext context) => context.ServerTimezone ?? string.Empty;

    private static void Append(StringBuilder key, string part) => key.Append(part.Length).Append(':').Append(part);
}
