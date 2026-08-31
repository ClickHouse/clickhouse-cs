using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Builds cache keys for POCO write plans.
/// </summary>
internal static class PocoWritePlan
{
    /// <summary>Returns the sample block's shape and codec-resolution context.</summary>
    /// <param name="schema">The sample block whose target columns to key on.</param>
    /// <returns>The key.</returns>
    public static string SignatureOf(Block schema) => PocoBlockSignature.Of(schema, schema.Context);
}

/// <summary>
/// A cached set of property gathers, one for each target column in an INSERT sample block.
/// </summary>
/// <typeparam name="T">The row type.</typeparam>
internal sealed class PocoWritePlan<T>
    where T : class
{
    private readonly PocoColumnBuilder<T>[] builders;

    private PocoWritePlan(PocoColumnBuilder<T>[] builders) => this.builders = builders;

    /// <summary>
    /// Compiles and validates the property mapping for <paramref name="schema"/>.
    /// </summary>
    /// <param name="descriptor">The POCO type's mapping.</param>
    /// <param name="schema">The server's sample block, naming and typing the target columns.</param>
    /// <returns>The plan.</returns>
    /// <exception cref="InvalidOperationException">A target column maps to no property or to one that cannot be
    /// read, two target columns map to one property, or a property cannot be written as its target's type.</exception>
    public static PocoWritePlan<T> Build(PocoTypeDescriptor<T> descriptor, Block schema)
    {
        var builders = new PocoColumnBuilder<T>[schema.ColumnCount];
        var claimedBy = new Dictionary<string, string>(schema.ColumnCount, StringComparer.Ordinal);

        for (int i = 0; i < schema.ColumnCount; i++)
        {
            IColumn column = schema[i];

            if (!descriptor.TryMatchColumn(column.Name, out PocoMember member))
            {
                throw new InvalidOperationException(
                    $"The target column '{column.Name}' ({column.TypeName}) maps to no property of '{typeof(T).Name}', which has {descriptor.DescribeMappedColumns()}. " +
                    $"Add a property for it, point one at it with [ClickHouseTcpColumn(Name = \"{column.Name}\")], or leave the column out by naming the ones to insert in the statement (INSERT INTO t (a, b) VALUES).");
            }

            if (!member.CanGet)
            {
                throw new InvalidOperationException(
                    $"The target column '{column.Name}' maps to property '{typeof(T).Name}.{member.MemberName}', which an insert cannot read: it has no public getter. " +
                    $"Give it one, or exclude it with [ClickHouseTcpNotMapped] and leave the column out of the statement.");
            }

            if (claimedBy.TryGetValue(member.MemberName, out string claimed))
            {
                // Loose name matching must not map one property to two target columns.
                throw new InvalidOperationException(
                    $"Target columns '{claimed}' and '{column.Name}' both map to property '{typeof(T).Name}.{member.MemberName}'. " +
                    $"Point one of them at a property of its own with [ClickHouseTcpColumn(Name = \"...\")], or insert only one of them.");
            }

            claimedBy[member.MemberName] = column.Name;

            // Resolve through the sample context so timezone-less values use this operation's session zone.
            IColumnCodec codec = schema.Codecs.Resolve(column.TypeName, schema.Context);
            builders[i] = PocoColumnBuilderFactory.Create<T>(column, codec, member);
        }

        return new PocoWritePlan<T>(builders);
    }

    /// <summary>
    /// Opens one insert over this plan: a column per target in sample-block order, gathered a block at a time.
    /// </summary>
    /// <remarks>
    /// The plan is cached and shared between inserts, so the buffers belong to the returned source rather than
    /// to the plan.
    /// </remarks>
    /// <param name="rows">The insert's rows; not owned by the source.</param>
    /// <param name="blockRows">The most rows one wire block will hold.</param>
    /// <returns>The source, owning its gather buffers until it is disposed.</returns>
    public PocoInsertSource<T> CreateSource(PocoRowBuffer<T> rows, int blockRows)
        => new(builders, rows, blockRows);
}
