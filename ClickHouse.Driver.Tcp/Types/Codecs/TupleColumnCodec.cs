using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// A codec for the ClickHouse <c>Tuple(...)</c> column. A tuple is serialized as its N element columns side by
/// side: every child's serialization-state prefix in order, then every child's full column body in order, each
/// body holding exactly <c>num_rows</c> values (no offsets, no null map). This codec owns one child codec per
/// element and drives each phase by looping the children — the layout, and therefore the codec, is independent
/// of how many elements the tuple has.
///
/// <para>
/// The decoded column is the typed <c>TupleColumn</c> for the element count (1 through 7), surfacing each row as
/// a <c>ValueTuple</c> of the element values. Wider tuples are rejected rather than silently mishandled. Element
/// names (a named tuple such as <c>Tuple(a Int32, b String)</c>) do not affect the wire layout or the CLR value;
/// they are preserved in the type string and carried on the column as metadata.
/// </para>
///
/// <para>
/// On the write path a dense <c>TupleColumn</c> (whose child columns already exist) is serialized straight from
/// those children with no copy. A flat column of <c>ValueTuple</c> values — the buffer
/// an <c>Array(Tuple(...))</c> flattens into, or one a caller builds directly — is un-transposed into the
/// per-child columns before the write when every child codec accepts that projection. A shape-only child such as
/// <c>Nested</c> requires the dense tuple form so its named field columns remain available.
/// </para>
/// </summary>
internal sealed class TupleColumnCodec : IColumnCodec
{
    private const int MaxArity = 7;

    private static readonly MethodInfo ProjectTupleMethod =
        typeof(TupleColumnCodec).GetMethod(nameof(ProjectTuple), BindingFlags.NonPublic | BindingFlags.Static);

    // The open generic ValueTuple / TupleColumn definitions indexed by arity (index 0 unused). MakeGenericType
    // closes them over the child element types once, at resolution time.
    private static readonly Type[] ValueTupleDefinitions =
    {
        null,
        typeof(ValueTuple<>),
        typeof(ValueTuple<,>),
        typeof(ValueTuple<,,>),
        typeof(ValueTuple<,,,>),
        typeof(ValueTuple<,,,,>),
        typeof(ValueTuple<,,,,,>),
        typeof(ValueTuple<,,,,,,>),
    };

    private static readonly Type[] ColumnDefinitions =
    {
        null,
        typeof(TupleColumn<>),
        typeof(TupleColumn<,>),
        typeof(TupleColumn<,,>),
        typeof(TupleColumn<,,,>),
        typeof(TupleColumn<,,,,>),
        typeof(TupleColumn<,,,,,>),
        typeof(TupleColumn<,,,,,,>),
    };

    // Cache projection builders only for tuple shapes that are used.
    private static readonly ConcurrentDictionary<Type, Func<string, IColumn, int, IColumn>[]> LiftedProjectionBuilders = new();

    private readonly IColumnCodec[] children;
    private readonly string[] fieldNames;
    private readonly ConstructorInfo columnConstructor;
    private readonly Type icolumnOfTupleType;
    private readonly Func<string, IColumn, int, IColumn>[] childProjectionBuilders;
    private object nullPlaceholder;

    private TupleColumnCodec(string typeName, IColumnCodec[] children, string[] fieldNames)
    {
        TypeName = typeName;
        this.children = children;
        this.fieldNames = fieldNames;

        int arity = children.Length;
        var elementTypes = new Type[arity];
        for (int i = 0; i < arity; i++)
        {
            elementTypes[i] = children[i].ElementType;
        }

        ElementType = ValueTupleDefinitions[arity].MakeGenericType(elementTypes);
        icolumnOfTupleType = typeof(IColumn<>).MakeGenericType(ElementType);

        // Cache the arity-specific column's constructor once. The parameter-type array is the exact signature of
        // the children-based constructor, disambiguating it from the ValueTuple[] convenience one; NonPublic is
        // what reaches it, since that constructor is internal.
        Type columnType = ColumnDefinitions[arity].MakeGenericType(elementTypes);
        columnConstructor = columnType.GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public,
            binder: null,
            new[] { typeof(string), typeof(string), typeof(IColumn[]), typeof(IReadOnlyList<string>), typeof(bool) },
            modifiers: null)
            ?? throw new InvalidOperationException($"The tuple column type '{columnType}' is missing its expected constructor.");

        // One cached delegate per element for the ergonomic write path: a lazy projection view over the flat
        // ValueTuple column that surfaces one element position, so the child codec writes strided through the tuples
        // with no per-child buffer materialized. BuildProjection<T> closed over the child's element type once.
        childProjectionBuilders = new Func<string, IColumn, int, IColumn>[arity];
        MethodInfo projectionTemplate = typeof(TupleColumnCodec).GetMethod(nameof(BuildProjection), BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Method '{nameof(BuildProjection)}' was not found.");

        for (int i = 0; i < arity; i++)
        {
            childProjectionBuilders[i] = (Func<string, IColumn, int, IColumn>)projectionTemplate
                .MakeGenericMethod(elementTypes[i])
                .CreateDelegate(typeof(Func<string, IColumn, int, IColumn>));
        }
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType { get; }

    /// <inheritdoc/>
    public object NullPlaceholder => nullPlaceholder ??= BuildNullPlaceholder(ElementType);

    /// <inheritdoc/>
    public object NullPlaceholderAs(Type writeType)
        => writeType == ElementType ? NullPlaceholder : BuildNullPlaceholder(writeType);

    /// <summary>Builds a <c>Tuple(...)</c> codec, resolving each element's codec through the registry.</summary>
    /// <param name="node">The parsed <c>Tuple</c> node; its arguments are the element types (each optionally name-prefixed).</param>
    /// <param name="context">The resolution context, forwarded to each element codec's factory.</param>
    /// <param name="registry">The registry used to resolve the element codecs.</param>
    /// <param name="typeName">The name to report as the codec's <see cref="IColumnCodec.TypeName"/>, or null to use
    /// <paramref name="node"/>'s own. An alias whose structure is a tuple (<c>Point</c>) passes its own name so
    /// diagnostics name the type the server sent rather than the structure it stands for.</param>
    /// <returns>The codec: <see cref="EmptyTupleColumnCodec"/> for <c>Tuple()</c>, otherwise this per-element one.</returns>
    /// <exception cref="FormatException">The type names no elements and has no argument list either.</exception>
    /// <exception cref="NotSupportedException">The tuple has more elements than this client supports.</exception>
    public static IColumnCodec Create(TypeNode node, in ResolveContext context, ColumnCodecRegistry registry, string typeName = null)
    {
        if (node.Arguments.Count == 0)
        {
            // Tuple() is the legal zero-element tuple. It has no element streams, so it is not this codec's
            // layout at all — one placeholder byte per row, like Nothing. A bare Tuple names no elements and
            // carries no argument list, and stays malformed.
            if (node.HasArgumentList)
            {
                return EmptyTupleColumnCodec.Instance;
            }

            throw new FormatException($"Tuple type '{node}' must have at least one element type argument.");
        }

        if (node.Arguments.Count > MaxArity)
        {
            throw new NotSupportedException(
                $"Tuple type '{node}' has {node.Arguments.Count} elements; this client supports at most {MaxArity} (wider tuples are not yet implemented).");
        }

        (string Name, TypeNode Type)[] elements = NamedElementParser.Split(node);
        var childCodecs = new IColumnCodec[elements.Length];
        var names = new string[elements.Length];
        bool anyNamed = false;
        for (int i = 0; i < elements.Length; i++)
        {
            childCodecs[i] = registry.ResolveNode(elements[i].Type, in context);
            names[i] = elements[i].Name;
            anyNamed |= elements[i].Name is not null;
        }

        return new TupleColumnCodec(typeName ?? node.ToString(), childCodecs, anyNamed ? names : null);
    }

    /// <inheritdoc/>
    public async ValueTask ReadStatePrefixAsync(ClickHouseBinaryReader reader, CancellationToken cancellationToken)
    {
        foreach (IColumnCodec child in children)
        {
            await child.ReadStatePrefixAsync(reader, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public async ValueTask<IColumn> ReadColumnAsync(ClickHouseBinaryReader reader, string columnName, string columnType, int rowCount, CancellationToken cancellationToken)
    {
        var childColumns = new IColumn[children.Length];
        int read = 0;
        try
        {
            for (int i = 0; i < children.Length; i++)
            {
                childColumns[i] = await children[i].ReadColumnAsync(reader, columnName, children[i].TypeName, rowCount, cancellationToken).ConfigureAwait(false);
                read = i + 1;
            }

            // Construct inside the try, because ownership transfers only once the column exists: a child whose
            // element type does not match this arity's IColumn<Ti> surfaces as a cast failure out of the reflected
            // constructor, and the catch below then disposes the children rather than leaking them. The column's row
            // count comes from the children, every one of which was just read at this block's rowCount.
            return (IColumn)columnConstructor.Invoke(new object[] { columnName, columnType, childColumns, fieldNames, true });
        }
        catch
        {
            // Dispose whatever children were read before the failure; the tuple column that would have owned
            // them was never constructed.
            for (int i = 0; i < read; i++)
            {
                childColumns[i].Dispose();
            }

            throw;
        }
    }

    /// <inheritdoc/>
    public bool TryProjectRead(Expression value, Type targetType, out Expression projected)
    {
        ColumnValueProjections.RequireSourceType(value, ElementType, TypeName);

        if (targetType == ElementType)
        {
            projected = value;
            return true;
        }

        projected = null;

        // Tuple arity must match before fields can be projected.
        int arity = children.Length;
        if (!targetType.IsGenericType || targetType.GetGenericTypeDefinition() != ValueTupleDefinitions[arity])
        {
            return false;
        }

        // Evaluate the source once for all field projections.
        ParameterExpression source = Expression.Variable(ElementType, "tuple");
        Type[] targetArguments = targetType.GetGenericArguments();
        var fieldProjections = new Expression[arity];
        for (int i = 0; i < arity; i++)
        {
            // Each child projects its field independently.
            if (!children[i].TryProjectRead(Expression.Field(source, "Item" + (i + 1).ToString(CultureInfo.InvariantCulture)), targetArguments[i], out fieldProjections[i]))
            {
                return false;
            }
        }

        projected = Expression.Block(
            new[] { source },
            Expression.Assign(source, value),
            Expression.New(
                targetType.GetConstructor(targetArguments) ?? throw new InvalidOperationException($"The tuple type '{targetType}' is missing its all-element constructor."),
                fieldProjections));
        return true;
    }

    /// <summary>
    /// Forwards a column-level reading to the child codecs over the per-element child columns, then rebuilds the
    /// tuple per row. Offered only where a child has such a reading; when every child converts its values one at a
    /// time, <see cref="TryProjectRead"/> builds the tuple more cheaply.
    /// </summary>
    public bool TryProjectColumnRead(Type targetType, out ColumnReadProjection projection)
    {
        projection = null;

        int arity = children.Length;
        if (targetType == ElementType || !targetType.IsGenericType || targetType.GetGenericTypeDefinition() != ValueTupleDefinitions[arity])
        {
            return false;
        }

        Type[] targetArguments = targetType.GetGenericArguments();
        var fieldProjections = new ColumnReadProjection[arity];
        bool anyChildNeedsColumn = false;
        for (int i = 0; i < arity; i++)
        {
            if (children[i].TryProjectColumnRead(targetArguments[i], out fieldProjections[i]))
            {
                anyChildNeedsColumn = true;
                continue;
            }

            // The other children may still read as their own type or convert elementwise.
            fieldProjections[i] = ColumnProjection.For(children[i], targetArguments[i]);
            if (fieldProjections[i] is null)
            {
                return false;
            }
        }

        if (!anyChildNeedsColumn)
        {
            return false;
        }

        projection = ColumnProjection.Close(ProjectTupleMethod, (fieldProjections, CompileRowReader(targetType, targetArguments)), targetType);
        return true;
    }

    /// <summary>
    /// Compiles <c>(columns, row) =&gt; new ValueTuple&lt;...&gt;(columns[0][row], ...)</c> over the projected child
    /// columns, once per resolution. An expression rather than a loop because each field has its own static type.
    /// </summary>
    /// <param name="targetType">The <c>ValueTuple</c> type to build.</param>
    /// <param name="targetArguments">Its type arguments, one per child.</param>
    /// <returns>A <c>Func&lt;IColumn[], int, targetType&gt;</c>.</returns>
    private static Delegate CompileRowReader(Type targetType, Type[] targetArguments)
    {
        ParameterExpression columns = Expression.Parameter(typeof(IColumn[]), "columns");
        ParameterExpression row = Expression.Parameter(typeof(int), "row");

        var fields = new Expression[targetArguments.Length];
        for (int i = 0; i < fields.Length; i++)
        {
            Type typedColumn = typeof(IColumn<>).MakeGenericType(targetArguments[i]);
            fields[i] = Expression.MakeIndex(
                Expression.Convert(Expression.ArrayIndex(columns, Expression.Constant(i)), typedColumn),
                typedColumn.GetProperty("Item"),
                new Expression[] { row });
        }

        Expression build = Expression.New(
            targetType.GetConstructor(targetArguments) ?? throw new InvalidOperationException($"The tuple type '{targetType}' is missing its all-element constructor."),
            fields);

        return Expression
            .Lambda(typeof(Func<,,>).MakeGenericType(typeof(IColumn[]), typeof(int), targetType), build, columns, row)
            .Compile();
    }

    /// <summary>
    /// Builds the view over one decoded column: each child column projected once, then read as one tuple per row.
    /// </summary>
    /// <typeparam name="T">The <c>ValueTuple</c> type the view surfaces.</typeparam>
    /// <param name="source">The decoded <c>Tuple(...)</c> column.</param>
    /// <param name="state">The children's projections and the compiled row reader over them.</param>
    /// <returns>The view.</returns>
    private static IColumn ProjectTuple<T>(IColumn source, (ColumnReadProjection[] Fields, Delegate Reader) state)
    {
        ITupleColumn tuple = ColumnProjection.Surface<ITupleColumn>(source);
        if (tuple.Children.Count != state.Fields.Length)
        {
            throw new InvalidOperationException(
                $"Column '{source.Name}' ({source.TypeName}) was read as a tuple of {tuple.Children.Count} children, " +
                $"but its type resolved to {state.Fields.Length}, so a projected reading cannot pair them.");
        }

        var read = (Func<IColumn[], int, T>)state.Reader;
        var projected = new IColumn[state.Fields.Length];
        for (int i = 0; i < projected.Length; i++)
        {
            projected[i] = state.Fields[i](tuple.Children[i]);
        }

        return new ProjectedReadColumn<T>(source, (column, row) => read(projected, row));
    }

    /// <inheritdoc/>
    public bool CanWriteElementType(Type elementType)
    {
        int arity = children.Length;
        if (!elementType.IsGenericType || elementType.GetGenericTypeDefinition() != ValueTupleDefinitions[arity])
        {
            return false;
        }

        Type[] arguments = elementType.GetGenericArguments();
        for (int i = 0; i < arity; i++)
        {
            if (!children[i].CanWriteElementType(arguments[i]))
            {
                return false;
            }
        }

        return true;
    }

    private object BuildNullPlaceholder(Type writeType)
    {
        if (!CanWriteElementType(writeType))
        {
            throw new NotSupportedException($"The '{TypeName}' codec has no null placeholder for {writeType}.");
        }

        Type[] arguments = writeType.GetGenericArguments();
        var values = new object[arguments.Length];
        for (int i = 0; i < arguments.Length; i++)
        {
            values[i] = children[i].NullPlaceholderAs(arguments[i]);
        }

        ConstructorInfo constructor = writeType.GetConstructor(arguments)
            ?? throw new InvalidOperationException($"The tuple type '{writeType}' is missing its all-element constructor.");
        return constructor.Invoke(values);
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column)
    {
        // Dense tuples must be writable through their actual child columns.
        if (column is ITupleColumn dense)
        {
            if (!icolumnOfTupleType.IsInstanceOfType(column) || dense.Children.Count != children.Length)
            {
                return false;
            }

            for (int i = 0; i < children.Length; i++)
            {
                if (!children[i].CanWrite(dense.Children[i]))
                {
                    return false;
                }
            }

            return true;
        }

        return CanWriteElementType(column.ElementType);
    }

    // Resolve builders for the canonical or child-lifted tuple shape.
    private Func<string, IColumn, int, IColumn>[] ProjectionBuildersFor(Type tupleType)
    {
        if (tupleType == ElementType)
        {
            return childProjectionBuilders;
        }

        if (!CanWriteElementType(tupleType))
        {
            throw new ArgumentException(
                $"A {TypeName} column must hold rows of a CLR tuple type its field codecs accept, not {tupleType}.",
                nameof(tupleType));
        }

        return LiftedProjectionBuilders.GetOrAdd(tupleType, BuildProjectionBuilders);
    }

    // Close one projection builder over each field type.
    private static Func<string, IColumn, int, IColumn>[] BuildProjectionBuilders(Type tupleType)
    {
        MethodInfo template = typeof(TupleColumnCodec).GetMethod(nameof(BuildProjection), BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Method '{nameof(BuildProjection)}' was not found.");

        Type[] arguments = tupleType.GetGenericArguments();
        var builders = new Func<string, IColumn, int, IColumn>[arguments.Length];
        for (int i = 0; i < arguments.Length; i++)
        {
            builders[i] = (Func<string, IColumn, int, IColumn>)template
                .MakeGenericMethod(arguments[i])
                .CreateDelegate(typeof(Func<string, IColumn, int, IColumn>));
        }

        return builders;
    }

    /// <inheritdoc/>
    // Prepare one column and write state per tuple field.
    public IColumnWriteState BeginWrite(IColumn column, int start, int length) => BuildState(column, start, length);

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        using TupleWriteState state = BuildState(column, start, length);
        WriteStatePrefixCore(writer, state);
    }

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        WriteStatePrefixCore(writer, state.Expect<TupleWriteState>(TypeName));
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        using TupleWriteState state = BuildState(column, start, length);
        WriteBodyCore(writer, state);
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        WriteBodyCore(writer, state.Expect<TupleWriteState>(TypeName));
    }

    private void WriteStatePrefixCore(ClickHouseBinaryWriter writer, TupleWriteState state)
    {
        for (int i = 0; i < children.Length; i++)
        {
            children[i].WriteStatePrefix(writer, state.ChildColumns[i], state.ChildStart, state.Length, state.ChildStates[i]);
        }
    }

    private void WriteBodyCore(ClickHouseBinaryWriter writer, TupleWriteState state)
    {
        for (int i = 0; i < children.Length; i++)
        {
            children[i].WriteColumn(writer, state.ChildColumns[i], state.ChildStart, state.Length, state.ChildStates[i]);
        }
    }

    // Dense tuples reuse child columns; flat tuples use lazy field projections.
    private TupleWriteState BuildState(IColumn column, int start, int length)
    {
        int arity = children.Length;
        var childColumns = new IColumn[arity];
        var childStates = new IColumnWriteState[arity];
        ITupleColumn dense = column is ITupleColumn tuple && tuple.Children.Count == arity ? tuple : null;
        Func<string, IColumn, int, IColumn>[] builders = dense is not null ? null : ProjectionBuildersFor(column.ElementType);

        int built = 0;
        try
        {
            for (int i = 0; i < arity; i++)
            {
                childColumns[i] = dense is not null
                    ? dense.Children[i]
                    : builders[i](children[i].TypeName, column, i);
                childStates[i] = children[i].BeginWrite(childColumns[i], start, length);
                built = i + 1;
            }
        }
        catch
        {
            // A later child's BeginWrite throwing must not leak the states already built (each may hold rented buffers).
            DisposeStates(childStates, built);
            throw;
        }

        return new TupleWriteState { ChildColumns = childColumns, ChildStart = start, Length = length, ChildStates = childStates };
    }

    // Dispose states created before a later child failed.
    private static void DisposeStates(IColumnWriteState[] states, int count)
    {
        for (int i = 0; i < count; i++)
        {
            states[i]?.Dispose();
        }
    }

    private static IColumn BuildProjection<T>(string typeName, IColumn source, int fieldIndex)
        => new TupleFieldColumn<T>(typeName, source, fieldIndex);

    // Per-field columns and states shared by the prefix and body.
    private sealed class TupleWriteState : IColumnWriteState
    {
        public IColumn[] ChildColumns;
        public int ChildStart;
        public int Length;
        public IColumnWriteState[] ChildStates;

        public void Dispose()
        {
            if (ChildStates is not null)
            {
                foreach (IColumnWriteState state in ChildStates)
                {
                    state?.Dispose();
                }
            }
        }
    }
}
