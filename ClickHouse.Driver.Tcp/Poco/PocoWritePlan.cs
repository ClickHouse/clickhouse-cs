using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// The schema identity a <see cref="PocoWritePlan{T}"/> is built against: the INSERT's sample block, keyed by
/// <see cref="PocoBlockSignature"/>.
/// </summary>
internal static class PocoWritePlan
{
    /// <summary>
    /// The cache key for the shape of an INSERT's sample block. See <see cref="PocoBlockSignature.Of"/>.
    /// </summary>
    /// <param name="schema">The sample block whose target columns to key on.</param>
    /// <returns>The key.</returns>
    /// <remarks>
    /// Includes the context the sample block was decoded with because a timezone-less <c>DateTime</c> or
    /// <c>DateTime64</c> target resolves against the session timezone. Reusing a plan from another timezone would
    /// give an <see cref="DateTimeKind.Unspecified"/> property a different instant from the one that session names.
    /// </remarks>
    public static string SignatureOf(Block schema) => PocoBlockSignature.Of(schema, schema.Context);
}

/// <summary>
/// The compiled write plan for one POCO type over one INSERT target: a builder per target column, each gathering
/// one property of every row into the buffer that column is written from. Built once per (type, target shape) and
/// cached, because the target types come from the server's sample block and so cannot be known from the type alone.
///
/// <para>
/// Every target column must be filled — the server expects a value for each column of the INSERT's column list — so
/// a target column that maps to no property fails the build. A property no target column maps to is simply not
/// inserted, which is what lets one POCO insert into a narrower column list.
/// </para>
/// </summary>
/// <typeparam name="T">The row type.</typeparam>
internal sealed class PocoWritePlan<T>
    where T : class
{
    private readonly PocoColumnBuilder<T>[] builders;

    private PocoWritePlan(PocoColumnBuilder<T>[] builders) => this.builders = builders;

    /// <summary>
    /// Compiles the plan for <paramref name="schema"/>'s target columns. Everything that can fail — an unreadable
    /// property, a target no property fills, a property type the target cannot be written from — fails here, before
    /// any row is gathered.
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
                // Two target columns reaching one property through the matcher's looser tiers, e.g. 'user_id' and
                // 'userId'. Writing one property into both is almost certainly not what was meant, and the read side
                // refuses the mirror of it, so the caller has to say which is which.
                throw new InvalidOperationException(
                    $"Target columns '{claimed}' and '{column.Name}' both map to property '{typeof(T).Name}.{member.MemberName}'. " +
                    $"Point one of them at a property of its own with [ClickHouseTcpColumn(Name = \"...\")], or insert only one of them.");
            }

            claimedBy[member.MemberName] = column.Name;

            // Use the registry and context that decoded the sample block. The insert's final alignment does the
            // same, so the plan chooses a write type for the exact codec that will serialize it. In particular, a
            // timezone-less DateTime target must interpret an Unspecified value in this operation's session zone.
            IColumnCodec codec = schema.Codecs.Resolve(column.TypeName, schema.Context);
            builders[i] = PocoColumnBuilderFactory.Create<T>(column, codec, member);
        }

        return new PocoWritePlan<T>(builders);
    }

    /// <summary>
    /// Gathers the rows into one column per target, in the sample block's order.
    /// </summary>
    /// <param name="rows">The rows, each non-null; at least <paramref name="rowCount"/> long.</param>
    /// <param name="rowCount">The number of rows to insert.</param>
    /// <returns>The columns, each owning a pooled buffer it returns when disposed.</returns>
    /// <exception cref="InvalidOperationException">A row has no value for a column that cannot hold null.</exception>
    public IReadOnlyList<IColumn> BuildColumns(T[] rows, int rowCount)
    {
        var columns = new IColumn[builders.Length];
        int built = 0;
        try
        {
            for (; built < builders.Length; built++)
            {
                columns[built] = builders[built].Build(rows, rowCount);
            }
        }
        catch
        {
            // A later column throwing (a null row for a non-nullable target) must not strand the buffers the
            // earlier ones already rented.
            for (int i = 0; i < built; i++)
            {
                columns[i].Dispose();
            }

            throw;
        }

        return columns;
    }
}
