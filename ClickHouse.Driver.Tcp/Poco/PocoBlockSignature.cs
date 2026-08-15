using System.Text;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// The cache key for the block shape a POCO plan is compiled against — a result's header for a read plan, an
/// INSERT's sample block for a write plan. Only the wire shape lives here, so it is shared by every POCO type
/// rather than computed once per type.
/// </summary>
internal static class PocoBlockSignature
{
    /// <summary>
    /// The key for a block's shape: the resolution context, then each column's name and ClickHouse type. The type
    /// string is the server's own header text, so two types that print alike cannot collide.
    ///
    /// <para>
    /// Every part is length-prefixed rather than merely separated, because a column name is arbitrary text — a
    /// backtick- or double-quoted alias may itself contain any separator character. Joined on a delimiter alone, the
    /// header <c>[a Int32, b Int32]</c> and a single column named with that delimiter sequence produce the same key,
    /// and the cache then hands one shape's plan to the other: columns silently unread, or an index past the block's
    /// columns. Length prefixes make the key injective, so the shape a plan was compiled for is the only shape that
    /// can find it.
    /// </para>
    /// </summary>
    /// <param name="block">The block whose header to key on.</param>
    /// <param name="context">The context the plan resolves its codecs with.</param>
    /// <returns>The key.</returns>
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

    /// <summary>
    /// The part of the key that is not in the header. A <c>DateTime</c> column whose type string names no timezone
    /// resolves against the session timezone, so a plan built for one session timezone must not be reused for
    /// another — the header alone cannot tell the two apart.
    /// </summary>
    /// <param name="context">The context the plan's codecs were resolved with.</param>
    /// <returns>The key part.</returns>
    public static string ContextKey(in ResolveContext context) => context.ServerTimezone ?? string.Empty;

    private static void Append(StringBuilder key, string part) => key.Append(part.Length).Append(':').Append(part);
}
