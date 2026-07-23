using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
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
/// per-child columns by <see cref="TryDensify"/> before the write.
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
    private readonly Func<string, string, object[], int, IColumn>[] childFlatBuilders;
    private readonly bool allChildrenWritable;
    private readonly int? fixedRowByteSize;

    // The per-row byte size splits into a constant part — the fixed-width children, summed once — and the
    // variable-width children, which are the only ones that must be measured per row. Used when the tuple as a
    // whole is variable-width (at least one variable child); an all-fixed tuple takes the FixedRowByteSize path.
    private readonly long fixedChildrenByteTotal;
    private readonly int[] variableChildIndices;

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

        // One cached delegate per element for the "flat" write path (where the source is a single column of
        // whole ValueTuple rows, not N per-element columns as in the dense path.)
        // Writing it means un-transposing: gather each element position's values (pulled out of the
        // tuples boxed) into a temporary per-element column the child codec can serialize. Each delegate is
        // BuildFlatColumn<T> closed over the child's element type, signature
        // (columnName, typeName, boxed values, count) -> IColumn. Closing the generics once here keeps that write
        // path reflection-free.
        childFlatBuilders = new Func<string, string, object[], int, IColumn>[arity];
        MethodInfo builderTemplate = typeof(TupleColumnCodec).GetMethod(nameof(BuildFlatColumn), BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"Method '{nameof(BuildFlatColumn)}' was not found.");

        bool writable = true;
        int fixedTotal = 0;
        var variableIndices = new List<int>();
        for (int i = 0; i < arity; i++)
        {
            childFlatBuilders[i] = (Func<string, string, object[], int, IColumn>)builderTemplate
                .MakeGenericMethod(elementTypes[i])
                .CreateDelegate(typeof(Func<string, string, object[], int, IColumn>));

            // Probe writability with an empty child column so a Tuple over a non-writable element (e.g. Nothing)
            // is rejected up front rather than mid-write.
            IColumn probe = childFlatBuilders[i](string.Empty, children[i].TypeName, Array.Empty<object>(), 0);
            writable &= children[i].CanWrite(probe);

            if (children[i].FixedRowByteSize is int width)
            {
                fixedTotal += width;
            }
            else
            {
                variableIndices.Add(i);
            }
        }

        allChildrenWritable = writable;
        fixedChildrenByteTotal = fixedTotal;
        variableChildIndices = variableIndices.ToArray();
        fixedRowByteSize = variableIndices.Count == 0 ? fixedTotal : null;
    }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public Type ElementType { get; }

    /// <inheritdoc/>
    public object NullPlaceholder { get; }

    /// <inheritdoc/>
    public int? FixedRowByteSize => fixedRowByteSize;

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
    public long MeasureRowBytes(IColumn column, int row)
    {
        if (fixedRowByteSize is int width)
        {
            return width;
        }

        // The column is always the dense TupleColumn (the pipeline densifies first). The fixed-width children
        // contribute the same bytes every row, so only the variable-width ones are measured per row.
        var dense = (ITupleColumn)column;
        long total = fixedChildrenByteTotal;
        foreach (int i in variableChildIndices)
        {
            total += children[i].MeasureRowBytes(dense.Children[i], row);
        }

        return total;
    }

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => allChildrenWritable && icolumnOfTupleType.IsInstanceOfType(column);

    /// <inheritdoc/>
    // Produces the dense TupleColumn (one child column per element) so a later measure/write drives the children
    // directly with no per-row projection. A flat column of ValueTuple values is un-transposed once: each element
    // position is gathered into a per-child column (boxing each value, as the ergonomic write path does), and each
    // child is itself densified so a nested composite element (e.g. Tuple(Array(T)) / Tuple(Nullable(T))) becomes
    // dense too. An already-dense tuple recurses into its children and is returned unchanged (built = false) when
    // they are all already dense.
    public IColumn TryDensify(IColumn column, out bool built)
    {
        int rowCount = column.RowCount;

        if (column is ITupleColumn tuple && tuple.Children.Count == children.Length)
        {
            IColumn[] densified = null;
            bool[] owned = null;
            for (int i = 0; i < children.Length; i++)
            {
                IColumn original = tuple.Children[i];
                IColumn child = children[i].TryDensify(original, out bool childBuilt);
                if (densified is null && childBuilt)
                {
                    densified = new IColumn[children.Length];
                    owned = new bool[children.Length];
                    for (int j = 0; j < i; j++)
                    {
                        densified[j] = tuple.Children[j];
                    }
                }

                if (densified is not null)
                {
                    densified[i] = child;
                    owned[i] = childBuilt;
                }
            }

            if (densified is null)
            {
                built = false;
                return column;
            }

            // The rebuilt wrapper mixes children: the freshly densified ones are new columns this wrapper must
            // dispose, while the unchanged ones are borrowed by reference and stay owned by the source column. Build
            // it borrowing everything (ownsChildren: false), then restrict disposal to exactly the columns we built
            // — the per-child `owned` flags come straight from each child's `built` signal, so disposing the wrapper
            // frees the new children without double-disposing the borrowed ones.
            var rebuilt = (TupleColumnBase)columnConstructor.Invoke(new object[] { column.Name, column.TypeName, densified, fieldNames, rowCount, false });
            rebuilt.RestrictOwnership(owned);
            built = true;
            return rebuilt;
        }

        var childBoxed = new object[children.Length][];
        for (int i = 0; i < children.Length; i++)
        {
            childBoxed[i] = new object[rowCount];
        }

        for (int r = 0; r < rowCount; r++)
        {
            var tupleValue = (ITuple)column.GetValue(r);
            for (int i = 0; i < children.Length; i++)
            {
                childBoxed[i][r] = tupleValue[i];
            }
        }

        var childColumns = new IColumn[children.Length];
        for (int i = 0; i < children.Length; i++)
        {
            IColumn flat = childFlatBuilders[i](column.Name, children[i].TypeName, childBoxed[i], rowCount);
            childColumns[i] = children[i].TryDensify(flat, out _);
        }

        built = true;
        return (IColumn)columnConstructor.Invoke(new object[] { column.Name, column.TypeName, childColumns, fieldNames, rowCount, true });
    }

    /// <inheritdoc/>
    public void WriteStatePrefix(ClickHouseBinaryWriter writer)
    {
        foreach (IColumnCodec child in children)
        {
            child.WriteStatePrefix(writer);
        }
    }

    /// <inheritdoc/>
    // Every column is densified before the write, so it is always the dense TupleColumn: each child column already
    // exists, so serialize each with its own codec, no copy. The flat ValueTuple form was un-transposed into the
    // per-child columns by TryDensify.
    public void WriteColumn(ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var dense = (ITupleColumn)column;
        for (int i = 0; i < children.Length; i++)
        {
            children[i].WriteColumn(writer, dense.Children[i], start, length);
        }
    }

    // Builds a flat typed column from boxed element values — the per-child projection TryDensify uses to un-transpose
    // a flat ValueTuple column (reached through the cached childFlatBuilders delegates).
    private static IColumn BuildFlatColumn<T>(string name, string typeName, object[] boxed, int count)
    {
        var values = new T[count];
        for (int i = 0; i < count; i++)
        {
            values[i] = (T)boxed[i];
        }

        return new ArrayColumn<T>(name, typeName, values);
    }
}
