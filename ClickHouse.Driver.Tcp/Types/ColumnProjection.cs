using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// Builds a view over <paramref name="source"/> whose values read as one type that source's ClickHouse type
/// offers. The view borrows the source, so it is only as valid as the block the source came from.
/// </summary>
/// <param name="source">The decoded column to read from.</param>
/// <returns>An <see cref="IColumn{T}"/> of the projected type.</returns>
internal delegate IColumn ColumnReadProjection(IColumn source);

/// <summary>
/// Resolves the column projection used by <see cref="Block.ReadAs{T}(string)"/>, POCO reads, and
/// <see cref="ClickHouseTcpTypes.CanRead"/>. It prefers a codec's column-level projection and otherwise builds an
/// elementwise view.
/// </summary>
internal static class ColumnProjection
{
    private static readonly MethodInfo ElementwiseViewMethod =
        typeof(ColumnProjection).GetMethod(nameof(ElementwiseView), BindingFlags.NonPublic | BindingFlags.Static);

    // The source already reads as the requested type, so the projection is the column itself.
    private static readonly ColumnReadProjection Identity = static source => source;

    /// <summary>
    /// The projection a codec offers to <paramref name="targetType"/>: the column itself when that is what it
    /// decodes to, then its own column-level reading, then an elementwise view over its values.
    /// </summary>
    /// <param name="codec">The column's codec.</param>
    /// <param name="targetType">The CLR type to read the values as.</param>
    /// <returns>The projection, or null when the type offers no such reading.</returns>
    public static ColumnReadProjection For(IColumnCodec codec, Type targetType)
    {
        if (targetType == codec.ElementType)
        {
            return Identity;
        }

        return codec.TryProjectColumnRead(targetType, out ColumnReadProjection projection)
            ? projection
            : Elementwise(codec, targetType);
    }

    /// <summary>
    /// Whether a codec offers a reading as <paramref name="targetType"/>, without compiling an elementwise view.
    /// </summary>
    /// <param name="codec">The column's codec.</param>
    /// <param name="targetType">The CLR type to read the values as.</param>
    /// <returns>Whether that type offers a reading as that CLR type.</returns>
    public static bool Offers(IColumnCodec codec, Type targetType)
        => targetType == codec.ElementType
            || codec.TryProjectColumnRead(targetType, out _)
            || codec.TryProjectRead(Expression.Parameter(codec.ElementType, "value"), targetType, out _);

    /// <summary>
    /// Closes a generic projection builder and binds its state once per resolution.
    /// </summary>
    /// <typeparam name="TState">The builder's second parameter: whatever the codec captured while resolving.</typeparam>
    /// <param name="builder">A static <c>IColumn Build&lt;...&gt;(IColumn source, TState state)</c> method.</param>
    /// <param name="state">The state to bind.</param>
    /// <param name="projectedTypes">The type arguments to close <paramref name="builder"/> over.</param>
    /// <returns>The projection.</returns>
    public static ColumnReadProjection Close<TState>(MethodInfo builder, TState state, params Type[] projectedTypes)
    {
        var bound = (Func<IColumn, TState, IColumn>)Delegate.CreateDelegate(
            typeof(Func<IColumn, TState, IColumn>),
            builder.MakeGenericMethod(projectedTypes));

        return source => bound(source, state);
    }

    /// <summary>
    /// The column's columnar surface, which a projection needs to reach its storage or its children.
    /// </summary>
    /// <typeparam name="TSurface">The surface interface the reading is taken through.</typeparam>
    /// <param name="column">The decoded column.</param>
    /// <returns>The column, as that surface.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="column"/> does not expose that surface.</exception>
    // Caller-built columns can carry a type name without exposing that type's decoded shape.
    public static TSurface Surface<TSurface>(IColumn column)
        where TSurface : class, IColumn
        => column as TSurface
            ?? throw new InvalidOperationException(
                $"Column '{column.Name}' ({column.TypeName}) was read as {column.GetType()}, which does not expose " +
                $"{typeof(TSurface).Name}, so a projected reading cannot reach its values.");

    /// <summary>
    /// Builds a view that converts one value at a time, for a codec whose reading is elementwise.
    /// </summary>
    /// <param name="codec">The column's codec.</param>
    /// <param name="targetType">The CLR type to read the values as.</param>
    /// <returns>The projection, or null when the codec offers no elementwise reading as that type.</returns>
    private static ColumnReadProjection Elementwise(IColumnCodec codec, Type targetType)
    {
        ParameterExpression column = Expression.Parameter(typeof(IColumn), "column");
        ParameterExpression row = Expression.Parameter(typeof(int), "row");
        Type typedColumn = typeof(IColumn<>).MakeGenericType(codec.ElementType);
        PropertyInfo indexer = typedColumn.GetProperty("Item")
            ?? throw new InvalidOperationException($"{typedColumn} has no indexer; an elementwise projection cannot be built.");

        Expression value = Expression.MakeIndex(Expression.Convert(column, typedColumn), indexer, new Expression[] { row });
        if (!codec.TryProjectRead(value, targetType, out Expression projected))
        {
            return null;
        }

        Delegate read = Expression
            .Lambda(typeof(Func<,,>).MakeGenericType(typeof(IColumn), typeof(int), targetType), projected, column, row)
            .Compile();

        return (ColumnReadProjection)ElementwiseViewMethod.MakeGenericMethod(targetType).Invoke(null, new object[] { read });
    }

    private static ColumnReadProjection ElementwiseView<T>(Func<IColumn, int, T> read)
        => source => new ProjectedReadColumn<T>(source, read);
}

/// <summary>
/// Caches one projected view by source-column identity so repeated POCO materialization windows reuse it. The
/// last source and view remain referenced after block disposal until another source replaces them.
/// </summary>
internal sealed class ProjectedViewCache
{
    private readonly ColumnReadProjection projection;

    private Entry entry;

    /// <summary>Initializes a memo over one projection.</summary>
    /// <param name="projection">The projection to apply, and to remember the result of.</param>
    public ProjectedViewCache(ColumnReadProjection projection) => this.projection = projection;

    /// <summary>The projected view of <paramref name="column"/>, reusing the last one when it is the same column.</summary>
    /// <param name="column">The decoded column to project.</param>
    /// <returns>The view.</returns>
    // The immutable entry is published atomically. A race may project twice or evict another block's view, but
    // cannot return a view for the wrong source.
    public IColumn For(IColumn column)
    {
        Entry current = Volatile.Read(ref entry);
        if (current is not null && ReferenceEquals(current.Source, column))
        {
            return current.View;
        }

        var fresh = new Entry(column, projection(column));
        Volatile.Write(ref entry, fresh);
        return fresh.View;
    }

    private sealed class Entry
    {
        public Entry(IColumn source, IColumn view)
        {
            Source = source;
            View = view;
        }

        public IColumn Source { get; }

        public IColumn View { get; }
    }
}
