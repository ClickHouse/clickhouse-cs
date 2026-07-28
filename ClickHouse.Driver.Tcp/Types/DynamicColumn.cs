using System;
using System.Buffers;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The dense shape of a ClickHouse <c>Dynamic</c> column: the runtime type-name list discovered on the wire, a
/// per-row discriminator stream, and one child column per runtime type, each holding only the values of the rows
/// that selected it (in row order). This is exactly the flattened wire layout — a type list, then discriminators,
/// then a contiguous run per type — so a <see cref="DynamicColumn"/> is both what a read produces and the
/// zero-copy source for a write.
///
/// <para>
/// A row's discriminator is an index into the runtime types (<c>0</c> = the first type, and so on), or
/// <see cref="TypeCount"/> for a NULL row (one past the last type — unlike <c>Variant</c>, whose NULL is the
/// fixed value <c>255</c>). A NULL row consumes no value from any child column. Random access maps a row to its
/// value through a per-row index into the selected type's child column, precomputed once by a single walk of the
/// discriminators.
/// </para>
///
/// <para>
/// The child columns and the discriminator buffer are borrowed for this column's lifetime: the child columns are
/// disposed (when owned) and the discriminator buffer returned (when pooled) on <see cref="Dispose"/>. Read the
/// column only while the owning block is alive; copy values out to retain them.
/// </para>
/// </summary>
internal sealed class DynamicColumn : IColumn<object>, IDynamicColumn
{
    private readonly string[] typeNames;
    private readonly IColumn[] typeColumns;
    private readonly int rowCount;
    private readonly bool ownsColumns;
    private readonly bool pooledDiscriminators;
    private readonly int[] localIndex;
    private int[] discriminators;
    private object[] cache;

    /// <summary>Initializes a dynamic column over its runtime type list, discriminator stream, and per-type child columns.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The <c>Dynamic</c> type string.</param>
    /// <param name="typeNames">The runtime type names, in wire (discriminator) order.</param>
    /// <param name="discriminators">One discriminator per row; <c>typeNames.Length</c> marks a NULL row.</param>
    /// <param name="typeColumns">One child column per runtime type, in wire order; each holds the values of the rows that selected it, in row order.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="pooledDiscriminators">Whether <paramref name="discriminators"/> was rented and should be returned on dispose.</param>
    /// <param name="ownsColumns">Whether this column owns and disposes <paramref name="typeColumns"/> (false when a caller retains them).</param>
    /// <exception cref="ArgumentException"><paramref name="typeNames"/> and <paramref name="typeColumns"/> differ in length, or <paramref name="discriminators"/> holds fewer than <paramref name="rowCount"/> entries.</exception>
    public DynamicColumn(
        string name,
        string typeName,
        string[] typeNames,
        int[] discriminators,
        IColumn[] typeColumns,
        int rowCount,
        bool pooledDiscriminators,
        bool ownsColumns)
    {
        this.typeNames = typeNames ?? throw new ArgumentNullException(nameof(typeNames));
        this.typeColumns = typeColumns ?? throw new ArgumentNullException(nameof(typeColumns));
        if (typeNames.Length != typeColumns.Length)
        {
            throw new ArgumentException(
                $"Dynamic column '{name}' has {typeNames.Length} type name(s) but {typeColumns.Length} type column(s).", nameof(typeColumns));
        }

        Name = name;
        TypeName = typeName;
        this.discriminators = discriminators ?? throw new ArgumentNullException(nameof(discriminators));
        this.rowCount = rowCount;
        this.pooledDiscriminators = pooledDiscriminators;
        this.ownsColumns = ownsColumns;

        // The row count cannot be derived here: each child holds only the rows that selected it and a NULL row takes
        // a slot in none of them, so the children say nothing about the height, and the discriminators are typically
        // a pooled buffer longer than the column. So the one input that can disagree is checked instead — otherwise
        // the walk below would fault on a short buffer with nothing naming the cause.
        if (discriminators.Length < rowCount)
        {
            throw new ArgumentException(
                $"The discriminators for column '{name}' ({typeName}) hold {discriminators.Length} entries, fewer than the {rowCount} rows.",
                nameof(discriminators));
        }

        // Precompute each row's index into its selected type's child column: one walk of the discriminators,
        // keeping a per-type running counter. A NULL row gets -1 (it addresses no child value). The counters are
        // heap-allocated (the type count is unbounded, unlike Variant's byte-capped 255) and start zeroed.
        localIndex = rowCount == 0 ? Array.Empty<int>() : new int[rowCount];
        int nullDiscriminator = typeColumns.Length;
        var counters = new int[typeColumns.Length];
        for (int row = 0; row < rowCount; row++)
        {
            int d = discriminators[row];
            localIndex[row] = d == nullDiscriminator ? -1 : counters[d]++;
        }
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => rowCount;

    /// <inheritdoc/>
    public int TypeCount => typeColumns.Length;

    /// <inheritdoc/>
    public IReadOnlyList<string> TypeNames => typeNames;

    /// <inheritdoc/>
    public ReadOnlySpan<int> Discriminators => discriminators.AsSpan(0, rowCount);

    /// <inheritdoc/>
    public ReadOnlySpan<int> LocalIndices => localIndex.AsSpan(0, rowCount);

    /// <summary>
    /// The rows as boxed values, materialized once and cached — each row is the selected type's value, or
    /// <see langword="null"/> for a NULL row. Prefer <see cref="Discriminators"/> plus
    /// <see cref="GetTypeColumn(int)"/> for the allocation-free columnar path.
    /// </summary>
    public ReadOnlySpan<object> Values
    {
        get
        {
            if (cache is null)
            {
                var decoded = new object[rowCount];
                for (int row = 0; row < rowCount; row++)
                {
                    decoded[row] = this[row];
                }

                cache = decoded;
            }

            return cache.AsSpan(0, rowCount);
        }
    }

    /// <inheritdoc/>
    // Index through the RowCount-sliced discriminators, not the raw buffer: the buffer is usually a pooled array
    // longer than the column, so a row past RowCount read a stale value from its tail. That made the failure depend on
    // the leftover value — a stale NULL discriminator reported the row as an existing NULL, anything else fell through
    // to the exactly-sized local index and threw. Slicing makes it a bounds failure either way.
    public object this[int row]
    {
        get
        {
            int d = Discriminators[row];
            return d == typeColumns.Length ? null : typeColumns[d].GetValue(localIndex[row]);
        }
    }

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <inheritdoc/>
    public IColumn GetTypeColumn(int discriminator) => typeColumns[discriminator];

    /// <inheritdoc/>
    public void Dispose()
    {
        if (ownsColumns)
        {
            foreach (IColumn column in typeColumns)
            {
                column.Dispose();
            }
        }

        if (pooledDiscriminators && discriminators.Length != 0)
        {
            ArrayPool<int>.Shared.Return(discriminators);
        }

        discriminators = Array.Empty<int>();
        cache = null;
    }
}

/// <summary>
/// The zero-copy read surface of a decoded <c>Dynamic</c> column, and the shape its codec writes from without
/// copying: the runtime type-name list discovered on the wire, a per-row discriminator stream, and one child column
/// per runtime type, each holding only the values of the rows that selected it, in row order.
///
/// <para>
/// Like <see cref="IVariantColumn"/>, a <c>Dynamic</c> has no useful materialized element type — its
/// <see cref="IColumn{T}"/> surface is <c>IColumn&lt;object&gt;</c>, so every row read through it is boxed — and the
/// columnar view is the typed way in. Row <c>i</c>'s value lives at <c>LocalIndices[i]</c> within
/// <c>GetTypeColumn(Discriminators[i])</c>. Unlike <c>Variant</c>, whose NULL discriminator is the fixed value 255,
/// a <c>Dynamic</c> marks NULL with <see cref="TypeCount"/> — one past the last type — because the type list is
/// discovered per block rather than declared. A NULL row occupies no slot in any child.
/// </para>
///
/// <para>
/// The type names are the wire's own spelling of each runtime type, in discriminator order, so a caller can decide
/// how to read a child without inspecting its values. Child columns and both spans are borrowed views over the
/// owning block's storage: read them in place, and copy out only what must outlive the block. Obtain this view by
/// pattern-matching a column, e.g. <c>if (column is IDynamicColumn dynamicColumn)</c>.
/// </para>
/// </summary>
public interface IDynamicColumn : IColumn
{
    /// <summary>
    /// The number of runtime types; also the NULL discriminator value, since NULL is encoded as one past the last
    /// type rather than a fixed sentinel.
    /// </summary>
    int TypeCount { get; }

    /// <summary>The runtime type names, in wire (discriminator) order.</summary>
    IReadOnlyList<string> TypeNames { get; }

    /// <summary>One discriminator per row; <see cref="TypeCount"/> marks a NULL row.</summary>
    ReadOnlySpan<int> Discriminators { get; }

    /// <summary>
    /// Each row's index into its selected type's child column (the count of that discriminator in the rows before
    /// it), precomputed once; a NULL row's entry is <c>-1</c>. Lets a caller price or address a row in O(1)
    /// rather than rescanning the discriminators.
    /// </summary>
    ReadOnlySpan<int> LocalIndices { get; }

    /// <summary>The child column for the given discriminator (holding the values of the rows that selected it).</summary>
    /// <param name="discriminator">The runtime-type index.</param>
    /// <returns>That type's child column.</returns>
    IColumn GetTypeColumn(int discriminator);
}
