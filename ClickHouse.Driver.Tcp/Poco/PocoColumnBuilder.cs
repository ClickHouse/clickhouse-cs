using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Copies one property of every row into the buffer one column is written from — the compiled unit of a POCO
/// insert, and the mirror of the read path's scatter. Column-major for the same reason: the loop is taken over one
/// property, which keeps the conversion and the property access out of any per-row dispatch.
/// </summary>
/// <typeparam name="T">The row type.</typeparam>
/// <typeparam name="TWrite">The CLR type the target column is written in.</typeparam>
/// <param name="rows">The rows to gather from; at least <paramref name="rowCount"/> long, each non-null.</param>
/// <param name="rowCount">The number of rows to gather.</param>
/// <param name="destination">The buffer to fill; at least <paramref name="rowCount"/> long.</param>
internal delegate void PocoColumnGather<in T, in TWrite>(T[] rows, int rowCount, TWrite[] destination);

/// <summary>
/// Builds one target column of an insert out of the caller's rows. One builder per column of the server's sample
/// block, held by the plan and reused across inserts, so it keeps no per-insert state: the buffer is rented inside
/// <see cref="Build"/> and owned by the column it returns.
/// </summary>
/// <typeparam name="T">The row type.</typeparam>
internal abstract class PocoColumnBuilder<T>
    where T : class
{
    /// <summary>Initializes the target column's identity.</summary>
    /// <param name="name">The target column's name.</param>
    /// <param name="typeName">The target column's ClickHouse type.</param>
    protected PocoColumnBuilder(string name, string typeName)
    {
        Name = name;
        TypeName = typeName;
    }

    /// <summary>The target column's name.</summary>
    protected string Name { get; }

    /// <summary>The target column's ClickHouse type.</summary>
    protected string TypeName { get; }

    /// <summary>
    /// Gathers <paramref name="rowCount"/> values into a fresh column.
    /// </summary>
    /// <param name="rows">The rows, each non-null.</param>
    /// <param name="rowCount">The number of rows to gather.</param>
    /// <returns>The column, owning a pooled buffer it returns when disposed.</returns>
    /// <exception cref="InvalidOperationException">A row has no value for a column that cannot hold null.</exception>
    public abstract IColumn Build(T[] rows, int rowCount);
}

/// <summary>
/// The typed builder: a rented <typeparamref name="TWrite"/> buffer, filled by the gather and handed to a column
/// that owns it. <see cref="ArrayColumn{T}"/> is what the column is, because it surfaces both
/// <see cref="IColumn{T}"/> — which every codec's <c>CanWrite</c> tests for — and a contiguous span, so a
/// fixed-width column reaches the wire as one blit.
/// </summary>
/// <typeparam name="T">The row type.</typeparam>
/// <typeparam name="TWrite">The CLR type the target column is written in.</typeparam>
internal sealed class PocoColumnBuilder<T, TWrite> : PocoColumnBuilder<T>
    where T : class
{
    private readonly PocoColumnGather<T, TWrite> gather;

    /// <summary>Initializes the builder over a compiled gather.</summary>
    /// <param name="name">The target column's name.</param>
    /// <param name="typeName">The target column's ClickHouse type.</param>
    /// <param name="gather">The gather filling the write buffer from the rows.</param>
    public PocoColumnBuilder(string name, string typeName, PocoColumnGather<T, TWrite> gather)
        : base(name, typeName) => this.gather = gather;

    /// <inheritdoc/>
    public override IColumn Build(T[] rows, int rowCount)
    {
        TWrite[] buffer = ArrayPool<TWrite>.Shared.Rent(rowCount);
        try
        {
            gather(rows, rowCount, buffer);
        }
        catch
        {
            // The column never took ownership of the rent, so return it rather than leak it on a failed gather.
            // Cleared unconditionally, unlike ArrayColumn's reference-type test: the gather stopped part-way, so
            // this is the one path where what the buffer holds is unknown, and it costs nothing on a failure.
            ArrayPool<TWrite>.Shared.Return(buffer, clearArray: true);
            throw;
        }

        return ArrayColumn<TWrite>.OverPooledBuffer(Name, TypeName, buffer, rowCount);
    }
}

/// <summary>
/// Compiles the per-column builders a <see cref="PocoWritePlan{T}"/> is made of. One builder handles one (property,
/// target column) pair for a whole insert, with the value conversion (<see cref="PocoWriteConversion"/>) inlined
/// into the loop rather than called through a delegate per row.
/// </summary>
internal static class PocoColumnBuilderFactory
{
    private static readonly MethodInfo CreateTypedMethod =
        typeof(PocoColumnBuilderFactory).GetMethod(nameof(CreateTyped), BindingFlags.NonPublic | BindingFlags.Static);

    /// <summary>
    /// Compiles the builder for one property into one target column.
    /// </summary>
    /// <typeparam name="T">The row type.</typeparam>
    /// <param name="column">The target column from the server's sample block, for its name and type.</param>
    /// <param name="codec">The target type's codec, resolved as the write path resolves it.</param>
    /// <param name="member">The property the column is filled from; must be gettable.</param>
    /// <returns>The compiled builder.</returns>
    /// <exception cref="InvalidOperationException">The property's type cannot be written as the column's type.</exception>
    public static PocoColumnBuilder<T> Create<T>(IColumn column, IColumnCodec codec, PocoMember member)
        where T : class
    {
        if (!PocoWriteConversion.TryChooseWriteType(codec, member.MemberType, out Type writeType))
        {
            throw NotWritableAs(column, codec, member, typeof(T));
        }

        return (PocoColumnBuilder<T>)CreateTypedMethod
            .MakeGenericMethod(typeof(T), writeType)
            .Invoke(null, new object[] { column.Name, column.TypeName, member, PocoWriteConversion.TakesNull(codec) });
    }

    /// <summary>
    /// Compiles the gather now that the write type is a type argument: <c>for (row) destination[row] =
    /// convert(rows[row].P);</c>.
    /// </summary>
    /// <typeparam name="T">The row type.</typeparam>
    /// <typeparam name="TWrite">The CLR type the target column is written in.</typeparam>
    /// <param name="name">The target column's name.</param>
    /// <param name="typeName">The target column's ClickHouse type.</param>
    /// <param name="member">The property to gather.</param>
    /// <param name="targetTakesNull">Whether the target column can carry a row with no value.</param>
    /// <returns>The builder.</returns>
    private static PocoColumnBuilder<T> CreateTyped<T, TWrite>(string name, string typeName, PocoMember member, bool targetTakesNull)
        where T : class
    {
        ParameterExpression rows = Expression.Parameter(typeof(T[]), "rows");
        ParameterExpression rowCount = Expression.Parameter(typeof(int), "rowCount");
        ParameterExpression destination = Expression.Parameter(typeof(TWrite[]), "destination");
        ParameterExpression row = Expression.Variable(typeof(int), "row");

        var site = new PocoGatherSite
        {
            ColumnName = name,
            ColumnType = typeName,
            PocoTypeName = typeof(T).Name,
            MemberName = member.MemberName,
            Row = Expression.Convert(row, typeof(long)),
        };

        Expression value = Expression.Property(Expression.ArrayIndex(rows, row), member.Property);
        Expression converted = PocoWriteConversion.Convert(value, typeof(TWrite), targetTakesNull, site);

        // row = 0; while (row < rowCount) { destination[row] = <converted>; row++; }
        LabelTarget done = Expression.Label("done");
        Expression body = Expression.Block(
            new[] { row },
            Expression.Assign(row, Expression.Constant(0)),
            Expression.Loop(
                Expression.IfThenElse(
                    Expression.LessThan(row, rowCount),
                    Expression.Block(
                        Expression.Assign(Expression.ArrayAccess(destination, row), converted),
                        Expression.PostIncrementAssign(row)),
                    Expression.Break(done)),
                done));

        var gather = Expression.Lambda<PocoColumnGather<T, TWrite>>(body, rows, rowCount, destination).Compile();
        return new PocoColumnBuilder<T, TWrite>(name, typeName, gather);
    }

    /// <summary>
    /// The failure for a property the column cannot be written from: raised at plan build, so it names the shape
    /// rather than surfacing as a rejected column once the INSERT is already open.
    /// </summary>
    /// <param name="column">The target column.</param>
    /// <param name="codec">The target type's codec, for the types it accepts.</param>
    /// <param name="member">The property that cannot fill it.</param>
    /// <param name="pocoType">The row type.</param>
    /// <returns>The exception to throw.</returns>
    private static Exception NotWritableAs(IColumn column, IColumnCodec codec, PocoMember member, Type pocoType)
    {
        IReadOnlyList<Type> accepted = PocoWriteConversion.AcceptedWriteTypes(codec);
        var offered = new string[accepted.Count];
        for (int i = 0; i < accepted.Count; i++)
        {
            offered[i] = accepted[i].ToString();
        }

        // No accepted type at all means the target's writer needs a column shape rows cannot be gathered into
        // (Nested), so naming property types would send the caller round a loop they cannot win.
        string remedy = accepted.Count == 0
            ? $"No property type can fill a '{column.TypeName}' column: insert it through the columnar API, which can build the column shape it needs."
            : $"It accepts {string.Join(" or ", offered)}. Give the property one of those types, or insert that column through the columnar API.";

        return new InvalidOperationException(
            $"Column '{column.Name}' ({column.TypeName}) is filled from property '{pocoType.Name}.{member.MemberName}' of type {member.MemberType}, which it cannot be written from. " + remedy);
    }
}
