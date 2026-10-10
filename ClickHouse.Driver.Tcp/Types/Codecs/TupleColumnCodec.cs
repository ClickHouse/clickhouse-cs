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
/// element and drives each phase by looping the children. The layout, and therefore the codec, is independent
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
/// The codec writes a dense <c>TupleColumn</c> only (<see cref="CanWrite"/>): each child column through its child
/// codec, with no copy. The converter layer writes every other column.
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
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType { get; }

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
    // A tuple of this element type whose child columns the child codecs write from their storage.
    public bool CanWrite(IColumn column)
    {
        if (column is not ITupleColumn dense || !icolumnOfTupleType.IsInstanceOfType(column) || dense.Children.Count != children.Length)
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

    /// <inheritdoc/>
    // One write state for each child column.
    public IColumnWriteState BeginWrite(IColumn column, int start, int length)
    {
        IReadOnlyList<IColumn> childColumns = ((ITupleColumn)column).Children;
        var childStates = new IColumnWriteState[children.Length];
        int built = 0;
        try
        {
            for (int i = 0; i < children.Length; i++)
            {
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

        return new TupleWriteState(childStates);
    }

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        IColumnWriteState[] childStates = state.Expect<TupleWriteState>(TypeName).ChildStates;
        IReadOnlyList<IColumn> childColumns = ((ITupleColumn)column).Children;
        for (int i = 0; i < children.Length; i++)
        {
            children[i].WriteStatePrefix(writer, childColumns[i], start, length, childStates[i]);
        }
    }

    /// <inheritdoc/>
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length, IColumnWriteState state)
    {
        IColumnWriteState[] childStates = state.Expect<TupleWriteState>(TypeName).ChildStates;
        IReadOnlyList<IColumn> childColumns = ((ITupleColumn)column).Children;
        for (int i = 0; i < children.Length; i++)
        {
            children[i].WriteColumn(writer, childColumns[i], start, length, childStates[i]);
        }
    }

    // Dispose states created before a later child failed.
    private static void DisposeStates(IColumnWriteState[] states, int count)
    {
        for (int i = 0; i < count; i++)
        {
            states[i]?.Dispose();
        }
    }

    // The write states of the children, shared by the prefix and body.
    private sealed class TupleWriteState : IColumnWriteState
    {
        public TupleWriteState(IColumnWriteState[] childStates) => ChildStates = childStates;

        public IColumnWriteState[] ChildStates { get; }

        public void Dispose() => DisposeStates(ChildStates, ChildStates.Length);
    }
}
