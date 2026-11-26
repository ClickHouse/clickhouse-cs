using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// Builds cache keys for result shapes used by <see cref="PocoReadPlan{T}"/>.
/// </summary>
internal static class PocoReadPlan
{
    /// <summary>The cache key for the shape of a result block. See <see cref="PocoBlockSignature.Of"/>.</summary>
    /// <param name="block">The block whose header to key on.</param>
    /// <returns>The key.</returns>
    public static string SignatureOf(Block block) => PocoBlockSignature.Of(block, block.Context);
}

/// <summary>
/// A compiled constructor and per-column scatter set for one POCO type and result shape. Unmapped columns are
/// skipped; properties without columns retain their defaults.
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
    /// Compiles the plan for <paramref name="block"/>'s shape, validating construction, mapping and conversions.
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
                $"No column of the result maps to a property of '{typeof(T).Name}': the result has {Describe(names)}, and the type has {descriptor.DescribeMappedColumns()}. " +
                $"Every row would be left at its defaults, so this is reported rather than returned.");
        }

        return new PocoReadPlan<T>(activator, names, types, PocoBlockSignature.ContextKey(block.Context), scatters);
    }

    /// <summary>
    /// Whether <paramref name="block"/> matches this plan's column names, types and resolution context.
    /// </summary>
    /// <param name="block">The block to check.</param>
    /// <returns>Whether the plan applies to it.</returns>
    public bool MatchesHeader(Block block)
    {
        if (block.ColumnCount != columnNames.Length
            || !string.Equals(contextKey, PocoBlockSignature.ContextKey(block.Context), StringComparison.Ordinal))
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
    /// Materializes a whole block into <paramref name="rows"/>.
    /// </summary>
    /// <param name="block">The block to materialize; must have the shape this plan was built for.</param>
    /// <param name="rows">The destination, at least <see cref="Block.RowCount"/> long.</param>
    /// <param name="rowOffset">How many rows of the result precede this block, so a failure names the row the caller
    /// counts. Only ever read on a failure path.</param>
    public void Materialize(Block block, T[] rows, long rowOffset)
        => Materialize(block, rows, 0, block.RowCount, rowOffset);

    /// <summary>
    /// Materializes block rows <c>[start, start + count)</c> into <c>rows[0, count)</c>. Keeping this synchronous
    /// allows span-based scatters; callers may invoke it in windows to bound live rows.
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

}
