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
    /// <exception cref="ArgumentException"><paramref name="typeNames"/> and <paramref name="typeColumns"/> differ in length.</exception>
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
    public object this[int row]
    {
        get
        {
            int d = discriminators[row];
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
/// The dense-column contract a <c>Dynamic</c> codec writes from without copying: the runtime type-name list, the
/// per-row discriminator stream, and the per-type child columns. Implemented by <see cref="DynamicColumn"/>.
/// </summary>
internal interface IDynamicColumn : IColumn
{
    /// <summary>The number of runtime types; also the NULL discriminator value.</summary>
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
