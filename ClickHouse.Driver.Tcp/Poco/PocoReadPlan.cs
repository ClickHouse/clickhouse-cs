using System;
using System.Collections.Generic;
using System.Text;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// The header identity a <see cref="PocoReadPlan{T}"/> is built against. Only the wire shape lives here, so it is
/// shared by every POCO type rather than compiled once per type.
/// </summary>
internal static class PocoReadPlan
{
    /// <summary>
    /// The cache key for a block's shape: the resolution context, then each column's name and ClickHouse type. The
    /// type string is the server's own header text, so two types that print alike cannot collide.
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
    /// <returns>The key.</returns>
    public static string SignatureOf(Block block)
    {
        var key = new StringBuilder();
        Append(key, ContextKey(block.Context));
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
    /// <param name="context">The context the block's codecs were resolved with.</param>
    /// <returns>The key part.</returns>
    public static string ContextKey(in ResolveContext context) => context.ServerTimezone ?? string.Empty;

    private static void Append(StringBuilder key, string part) => key.Append(part.Length).Append(':').Append(part);
}

/// <summary>
/// The compiled read plan for one POCO type over one block shape: a scatter per mapped column, plus the
/// constructor the rows come from. Built once per (type, block shape) and cached, because the column types come
/// from the server and so cannot be known from the type alone.
///
/// <para>
/// A column that maps to no property is left with no scatter and never read — nothing has to be consumed to stay
/// aligned, the block being already decoded. A property no column maps to keeps its default.
/// </para>
/// </summary>
/// <typeparam name="T">The POCO type.</typeparam>
internal sealed class PocoReadPlan<T>
    where T : class
{
    private readonly Func<T> activator;
    private readonly string[] columnNames;
    private readonly string[] columnTypes;
    private readonly string contextKey;
    private readonly PocoColumnScatter<T>[] scatters;

    private PocoReadPlan(Func<T> activator, string[] columnNames, string[] columnTypes, string contextKey, PocoColumnScatter<T>[] scatters)
    {
        this.activator = activator;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.contextKey = contextKey;
        this.scatters = scatters;
    }

    /// <summary>
    /// Compiles the plan for <paramref name="block"/>'s shape. Everything that can fail — an unsettable or
    /// unreadable property, a type that cannot be constructed — fails here rather than part-way through a result.
    /// </summary>
    /// <param name="descriptor">The POCO type's mapping.</param>
    /// <param name="block">A block of the shape to plan for; only its header and its columns' runtime shapes are read.</param>
    /// <param name="forcedTier">A scatter tier to compile regardless of the runtime, or null to choose one.</param>
    /// <returns>The plan.</returns>
    /// <exception cref="InvalidOperationException"><typeparamref name="T"/> cannot be materialized, no column maps
    /// to a property, two columns map to one property, a mapped property cannot be set, or a column cannot be read
    /// as its property's type.</exception>
    public static PocoReadPlan<T> Build(PocoTypeDescriptor<T> descriptor, Block block, PocoScatterTier? forcedTier)
    {
        // Read first: an insert-only POCO (no accessible parameterless constructor) fails here, naming the reason.
        Func<T> activator = descriptor.Activator;

        int columnCount = block.ColumnCount;
        var names = new string[columnCount];
        var types = new string[columnCount];
        var scatters = new PocoColumnScatter<T>[columnCount];
        var claimedBy = new Dictionary<string, string>(columnCount, StringComparer.Ordinal);

        for (int i = 0; i < columnCount; i++)
        {
            IColumn column = block[i];
            names[i] = column.Name;
            types[i] = column.TypeName;

            if (!descriptor.TryMatchColumn(column.Name, out PocoMember member))
            {
                continue;
            }

            if (!member.CanSet)
            {
                throw new InvalidOperationException(
                    $"Column '{column.Name}' maps to property '{typeof(T).Name}.{member.MemberName}', which a query cannot fill: {member.DescribeWhyNotSettable()}. " +
                    $"Give it a public setter, or exclude it with [ClickHouseTcpNotMapped].");
            }

            if (claimedBy.TryGetValue(member.MemberName, out string claimed))
            {
                // Two columns reaching one property through the matcher's looser tiers, e.g. 'user_id' and
                // 'userId'. Whichever scattered last would win silently, so the caller has to disambiguate.
                throw new InvalidOperationException(
                    $"Columns '{claimed}' and '{column.Name}' both map to property '{typeof(T).Name}.{member.MemberName}'. " +
                    $"Point one of them at a property of its own with [ClickHouseTcpColumn(Name = \"...\")], or select only one of them.");
            }

            claimedBy[member.MemberName] = column.Name;
            IColumnCodec codec = block.Codecs.Resolve(column.TypeName, block.Context);
            scatters[i] = PocoColumnScatterFactory.Create<T>(column, codec, member, forcedTier);
        }

        if (claimedBy.Count == 0)
        {
            throw new InvalidOperationException(
                $"No column of the result maps to a property of '{typeof(T).Name}': the result has {Describe(names)}, and the type has {Describe(descriptor)}. " +
                $"Every row would be left at its defaults, so this is reported rather than returned.");
        }

        return new PocoReadPlan<T>(activator, names, types, PocoReadPlan.ContextKey(block.Context), scatters);
    }

    /// <summary>
    /// Whether <paramref name="block"/> has the shape this plan was compiled for, so an enumeration can reuse the
    /// plan across the blocks of one result without going back to the cache. Compared column by column rather than
    /// by the cache key, since every block builds its own header strings and no key has to be materialized to say
    /// no.
    /// </summary>
    /// <param name="block">The block to check.</param>
    /// <returns>Whether the plan applies to it.</returns>
    public bool MatchesHeader(Block block)
    {
        if (block.ColumnCount != columnNames.Length
            || !string.Equals(contextKey, PocoReadPlan.ContextKey(block.Context), StringComparison.Ordinal))
        {
            return false;
        }

        for (int i = 0; i < columnNames.Length; i++)
        {
            IColumn column = block[i];
            if (!string.Equals(column.Name, columnNames[i], StringComparison.Ordinal)
                || !string.Equals(column.TypeName, columnTypes[i], StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// A whole block in one pass. Convenient where the row count is known to be small, or where all of a block's
    /// rows are wanted at once; a reader streaming a result of any size wants the windowed overload instead, so
    /// that the rows it has handed on can be collected while it reads the rest.
    /// </summary>
    /// <param name="block">The block to materialize; must have the shape this plan was built for.</param>
    /// <param name="rows">The destination, at least <see cref="Block.RowCount"/> long.</param>
    /// <param name="rowOffset">How many rows of the result precede this block, so a failure names the row the caller
    /// counts. Only ever read on a failure path.</param>
    public void Materialize(Block block, T[] rows, long rowOffset)
        => Materialize(block, rows, 0, block.RowCount, rowOffset);

    /// <summary>
    /// Materializes rows <c>[start, start + count)</c> of a block into <c>rows[0, count)</c>: one instance per row,
    /// then one scatter per mapped column. Synchronous by necessity — the span tier holds a
    /// <see cref="ReadOnlySpan{T}"/>, which no async or iterator method may.
    ///
    /// <para>
    /// Taking a block in windows rather than whole bounds how many rows are reachable at once, which is otherwise
    /// however many rows the server chose to put in a block. That matters because a generational collector charges
    /// for what survives a collection rather than for what it frees, so rows held for a whole block are copied
    /// forward instead of being dropped.
    /// </para>
    /// </summary>
    /// <param name="block">The block to materialize; must have the shape this plan was built for.</param>
    /// <param name="rows">The destination, at least <paramref name="count"/> long.</param>
    /// <param name="start">The first row of the block to take.</param>
    /// <param name="count">How many rows to take.</param>
    /// <param name="rowOffset">How many rows of the result precede <c>rows[0]</c>, so a failure names the row the
    /// caller counts. Only ever read on a failure path.</param>
    public void Materialize(Block block, T[] rows, int start, int count, long rowOffset)
    {
        for (int i = 0; i < count; i++)
        {
            rows[i] = activator();
        }

        for (int i = 0; i < scatters.Length; i++)
        {
            scatters[i]?.Invoke(block[i], rows, start, count, rowOffset);
        }
    }

    private static string Describe(string[] columnNames)
        => columnNames.Length == 0 ? "no columns" : $"columns {string.Join(", ", columnNames)}";

    private static string Describe(PocoTypeDescriptor descriptor)
    {
        var names = new string[descriptor.Members.Count];
        for (int i = 0; i < names.Length; i++)
        {
            names[i] = descriptor.Members[i].ColumnName;
        }

        return $"properties mapping to {string.Join(", ", names)}";
    }
}
