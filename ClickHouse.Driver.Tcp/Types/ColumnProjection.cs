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
/// Resolves the reading a codec offers as a projection from column to column, which is the primitive every read
/// tier asks for: <see cref="Block.ReadAs{T}(string)"/>, the POCO scatter, and
/// <see cref="ClickHouseTcpTypes.CanRead"/>.
///
/// <para>
/// Column-level rather than value-level because several readings are not a function of one value. A
/// <c>String</c>'s bytes are in the column and gone from its decoded text. A <c>LowCardinality</c> row is a
/// dictionary slot, so the conversion belongs to the dictionary and its result is shared by every row holding
/// that key. A composite's is its child column's, projected once and then addressed per row. A codec that
/// converts one value at a time says so with <see cref="IColumnCodec.TryProjectRead"/> instead and gets
/// <see cref="Elementwise"/> built for it.
/// </para>
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
    /// Whether a codec offers any reading as <paramref name="targetType"/>. The same three questions
    /// <see cref="For"/> asks, in the same order, but stopping at the answer — so a leaf's elementwise reading is
    /// recognized without compiling it.
    /// </summary>
    /// <param name="codec">The column's codec.</param>
    /// <param name="targetType">The CLR type to read the values as.</param>
    /// <returns>Whether that type offers a reading as that CLR type.</returns>
    public static bool Offers(IColumnCodec codec, Type targetType)
        => targetType == codec.ElementType
            || codec.TryProjectColumnRead(targetType, out _)
            || codec.TryProjectRead(Expression.Parameter(codec.ElementType, "value"), targetType, out _);

    /// <summary>
    /// Closes a codec's generic projection builder over the projected type(s) and binds the state it needs, so the
    /// codec pays one reflective instantiation per resolution and none per block.
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
    // The reading is resolved from a type string, so a column a caller built and labelled with that type need not
    // have the shape the type implies. Named here rather than left to a bare cast failure, which would identify
    // neither the column nor the reading.
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
/// A one-entry memo of a projected view, keyed on the source column by reference, for a consumer that reads one
/// column through several calls. The POCO scatter is one: it runs once per materialization window, and a view
/// built per window would convert a dictionary — or a child column — again for every window of the block.
///
/// <para>
/// The entry outlives the block whose column it holds, until another column replaces it. That retains one view's
/// worth of converted values, which is the same order as the caches the column itself builds while it is alive.
/// </para>
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
    // The entry is read and published as one reference to an immutable object, so a reader either does not see a
    // concurrent write at all or sees a fully built entry, never a half-written one. A lost race projects twice and
    // drops one view; both are views over the column their own caller passed, so neither can be handed the wrong
    // one. Plans are cached and shared, so two enumerations of different blocks can take turns evicting each
    // other's entry — that costs the reuse, not correctness.
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
