using System;
using System.Collections.Generic;
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
/// On the write path the column is always the dense <c>TupleColumn</c> (whose child columns already exist),
/// serialized straight from those children with no copy. A flat column of <c>ValueTuple</c> values — the buffer
/// an <c>Array(Tuple(...))</c> flattens into, or one a caller builds directly — is un-transposed into the
/// per-child columns before the write.
/// </para>
/// </summary>
internal sealed class TupleColumnCodec : IColumnCodec
{
    private const int MaxArity = 7;

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

    private readonly IColumnCodec[] children;
    private readonly string[] fieldNames;
    private readonly ConstructorInfo columnConstructor;
    private readonly Type icolumnOfTupleType;
    private readonly Func<string, IColumn, int, IColumn>[] childProjectionBuilders;
    private readonly bool allChildrenWritable;

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

        // A tuple is never nested inside Nullable (the server rejects Nullable(Tuple(...))), so this placeholder
        // is a formality the interface requires: the default ValueTuple of the element types.
        NullPlaceholder = Activator.CreateInstance(ElementType);

        // Cache the arity-specific column's constructor once. The parameter-type array is the exact signature of
        // the children-based constructor, disambiguating it from the ValueTuple[] convenience one; NonPublic is
        // what reaches it, since that constructor is internal.
        Type columnType = ColumnDefinitions[arity].MakeGenericType(elementTypes);
        columnConstructor = columnType.GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public,
            binder: null,
            new[] { typeof(string), typeof(string), typeof(IColumn[]), typeof(IReadOnlyList<string>), typeof(int), typeof(bool) },
            modifiers: null)
            ?? throw new InvalidOperationException($"The tuple column type '{columnType}' is missing its expected constructor.");

        // One cached delegate per element for the ergonomic write path: a lazy projection view over the flat
        // ValueTuple column that surfaces one element position, so the child codec writes strided through the tuples
        // with no per-child buffer materialized. BuildProjection<T> closed over the child's element type once.
        childProjectionBuilders = new Func<string, IColumn, int, IColumn>[arity];
        MethodInfo projectionTemplate = typeof(TupleColumnCodec).GetMethod(nameof(BuildProjection), BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Method '{nameof(BuildProjection)}' was not found.");

        // Closed once per element type to build an empty child column for the up-front writability probe below.
        MethodInfo emptyTemplate = typeof(TupleColumnCodec).GetMethod(nameof(BuildEmptyColumn), BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Method '{nameof(BuildEmptyColumn)}' was not found.");

        bool writable = true;
        for (int i = 0; i < arity; i++)
        {
            childProjectionBuilders[i] = (Func<string, IColumn, int, IColumn>)projectionTemplate
                .MakeGenericMethod(elementTypes[i])
                .CreateDelegate(typeof(Func<string, IColumn, int, IColumn>));

            // Probe writability with an empty child column so a Tuple over a non-writable element (e.g. Nothing)
            // is rejected up front rather than mid-write.
            var emptyBuilder = (Func<string, IColumn>)emptyTemplate
                .MakeGenericMethod(elementTypes[i])
                .CreateDelegate(typeof(Func<string, IColumn>));
            writable &= children[i].CanWrite(emptyBuilder(children[i].TypeName));
        }

        allChildrenWritable = writable;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType { get; }

    /// <inheritdoc/>
    public object NullPlaceholder { get; }

    /// <summary>Builds a <c>Tuple(...)</c> codec, resolving each element's codec through the registry.</summary>
    /// <param name="node">The parsed <c>Tuple</c> node; its arguments are the element types (each optionally name-prefixed).</param>
    /// <param name="context">The resolution context, forwarded to each element codec's factory.</param>
    /// <param name="registry">The registry used to resolve the element codecs.</param>
    /// <returns>The codec.</returns>
    /// <exception cref="FormatException">The tuple has no elements.</exception>
    /// <exception cref="NotSupportedException">The tuple has more elements than this client supports.</exception>
    public static TupleColumnCodec Create(TypeNode node, in ResolveContext context, ColumnCodecRegistry registry)
    {
        if (node.Arguments.Count == 0)
        {
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

        return new TupleColumnCodec(node.ToString(), childCodecs, anyNamed ? names : null);
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

        // The constructed column takes ownership of the child columns and disposes them with the block.
        return (IColumn)columnConstructor.Invoke(new object[] { columnName, columnType, childColumns, fieldNames, rowCount, true });
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => allChildrenWritable && icolumnOfTupleType.IsInstanceOfType(column);

    /// <inheritdoc/>
    // Project each element position into its own child column once (dense children as-is, a flat tuple column
    // distributed into per-child buffers), and create each child's own write state over it, so a data-dependent
    // child (Dynamic) sees its real values at prefix time and the projection is not repeated between phases.
    public IColumnWriteState BeginWrite(IColumn column, int start, int length) => BuildState(column, start, length);

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        if (IsTupleColumn(column))
        {
            using TupleWriteState state = BuildState(column, start, length);
            WriteStatePrefixCore(writer, state);
            return;
        }

        // An outer composite forwarded its own column (its children's prefixes are written from its own slice);
        // the children ignore it, preserving the prior contract for a Tuple nested in Variant/Map/Nested.
        foreach (IColumnCodec child in children)
        {
            child.WriteStatePrefix(writer, column, start, length);
        }
    }

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        if (state is TupleWriteState tupleState)
        {
            WriteStatePrefixCore(writer, tupleState);
            return;
        }

        WriteStatePrefix(writer, column, start, length);
    }

    /// <inheritdoc/>
    // Every column is densified before the write, so it is always the dense TupleColumn; each child is written
    // through its own codec and pre-built state, no copy. The flat ValueTuple form was un-transposed into the
    // per-child columns before the write.
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        using TupleWriteState state = BuildState(column, start, length);
        WriteBodyCore(writer, state);
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        if (state is TupleWriteState tupleState)
        {
            WriteBodyCore(writer, tupleState);
            return;
        }

        WriteColumn(writer, column, start, length);
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

    private bool IsTupleColumn(IColumn column)
        => (column is ITupleColumn dense && dense.Children.Count == children.Length) || icolumnOfTupleType.IsInstanceOfType(column);

    // Builds the per-element write state for a slice. A dense tuple column exposes each child column directly; an
    // ergonomic flat ValueTuple column is projected into one lazy per-element view per child (strided through the
    // tuples, no per-child buffer materialized). Each child's own BeginWrite runs over its column so a data-
    // dependent child (Dynamic) sees its real values at prefix time.
    private TupleWriteState BuildState(IColumn column, int start, int length)
    {
        int arity = children.Length;
        var childColumns = new IColumn[arity];
        var childStates = new IColumnWriteState[arity];
        ITupleColumn dense = column is ITupleColumn tuple && tuple.Children.Count == arity ? tuple : null;

        int built = 0;
        try
        {
            for (int i = 0; i < arity; i++)
            {
                childColumns[i] = dense is not null
                    ? dense.Children[i]
                    : childProjectionBuilders[i](children[i].TypeName, column, i);
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

    // Disposes the first count child write states after a mid-construction failure, so their pooled buffers are
    // returned rather than leaked.
    private static void DisposeStates(IColumnWriteState[] states, int count)
    {
        for (int i = 0; i < count; i++)
        {
            states[i]?.Dispose();
        }
    }

    // Builds an empty typed column for the constructor's up-front child writability probe.
    private static IColumn BuildEmptyColumn<T>(string typeName)
        => new ArrayColumn<T>(string.Empty, typeName, Array.Empty<T>());

    // Builds the lazy per-element projection view the ergonomic write path hands each child codec (reached through
    // the cached childProjectionBuilders delegates).
    private static IColumn BuildProjection<T>(string typeName, IColumn source, int fieldIndex)
        => new TupleFieldColumn<T>(typeName, source, fieldIndex);

    // The per-element write state of one slice, shared across the prefix and body phases: each element's dense
    // child column plus the child codec's own state.
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
