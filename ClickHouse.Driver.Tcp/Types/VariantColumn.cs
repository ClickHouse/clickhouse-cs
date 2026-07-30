using System;
using System.Buffers;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The dense shape of a ClickHouse <c>Variant(T1, ..., Tn)</c> column: a per-row discriminator stream plus one
/// child column per alternative type, each holding only the values of the rows that selected it (in row order).
/// This is exactly how the type is laid out on the wire — a discriminator array followed by a contiguous run per
/// type — so a <see cref="VariantColumn"/> is both what a read produces and the zero-copy source for a write.
///
/// <para>
/// A row's discriminator is an index into the alternative types (<c>0</c> = the first type, and so on), or
/// <see cref="NullDiscriminator"/> (<c>255</c>) for a NULL row, which consumes no value from any child column.
/// Random access maps a row to its value through a per-row index into the selected type's child column,
/// precomputed once by a single walk of the discriminators.
/// </para>
///
/// <para>
/// The child columns and the discriminator buffer are borrowed for this column's lifetime: the child columns are
/// disposed (when owned) and the discriminator buffer returned (when pooled) on <see cref="Dispose"/>. Read the
/// column only while the owning block is alive; copy values out to retain them.
/// </para>
/// </summary>
internal sealed class VariantColumn : IColumn<object>, IVariantColumn
{
    /// <summary>The discriminator value marking a NULL row; it selects no alternative type.</summary>
    internal const byte NullDiscriminator = 255;

    private readonly IColumn[] typeColumns;
    private readonly int rowCount;
    private readonly bool ownsColumns;
    private readonly bool pooledDiscriminators;
    private readonly int[] localIndex;
    private byte[] discriminators;
    private object[] cache;

    // When non-null, overrides ownsColumns per type column: Dispose disposes type column i only when
    // columnOwnership[i] is true. Set once by RestrictOwnership immediately after construction so a densified
    // wrapper that mixes freshly built type columns with columns borrowed from another column disposes only the
    // ones it created.
    private bool[] columnOwnership;

    /// <summary>Initializes a variant column over its discriminator stream and per-type child columns.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The full <c>Variant(...)</c> type string.</param>
    /// <param name="discriminators">One discriminator per row; <see cref="NullDiscriminator"/> marks a NULL row.</param>
    /// <param name="typeColumns">One child column per alternative type, in declared (discriminator) order; each holds the values of the rows that selected it, in row order.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="pooledDiscriminators">Whether <paramref name="discriminators"/> was rented and should be returned on dispose.</param>
    /// <param name="ownsColumns">Whether this column owns and disposes <paramref name="typeColumns"/> (false when a caller retains them).</param>
    public VariantColumn(string name, string typeName, byte[] discriminators, IColumn[] typeColumns, int rowCount, bool pooledDiscriminators, bool ownsColumns)
    {
        Name = name;
        TypeName = typeName;
        this.discriminators = discriminators ?? throw new ArgumentNullException(nameof(discriminators));
        this.typeColumns = typeColumns ?? throw new ArgumentNullException(nameof(typeColumns));
        this.rowCount = rowCount;
        this.pooledDiscriminators = pooledDiscriminators;
        this.ownsColumns = ownsColumns;

        // Precompute each row's index into its selected type's child column: walk the discriminators once,
        // keeping a per-type running counter. A NULL row gets -1 (it addresses no child value).
        localIndex = rowCount == 0 ? Array.Empty<int>() : new int[rowCount];

        // Zero the stack counters explicitly rather than trusting the compiler's `.locals init` — a future
        // `[SkipLocalsInit]` would drop that guarantee and leave garbage counts, silently corrupting the local
        // indices. The Clear is a cheap memset.
        Span<int> counters = stackalloc int[typeColumns.Length];
        counters.Clear();
        for (int row = 0; row < rowCount; row++)
        {
            byte d = discriminators[row];
            localIndex[row] = d == NullDiscriminator ? -1 : counters[d]++;
        }
    }

    /// <summary>
    /// Restricts disposal to the type columns flagged in <paramref name="owned"/> (one entry per alternative),
    /// overriding the all-or-nothing <c>ownsColumns</c> passed at construction. Used when rebuilding a densified
    /// variant that keeps some type columns by reference (owned by the source column) and replaces others with
    /// freshly built ones, so disposing this wrapper frees only the columns it created. Must be called before the
    /// column is observed.
    /// </summary>
    internal void RestrictOwnership(bool[] owned)
    {
        if (owned is null || owned.Length != typeColumns.Length)
        {
            throw new ArgumentException("Ownership mask must have one entry per type column.", nameof(owned));
        }

        columnOwnership = owned;
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
    public ReadOnlySpan<byte> Discriminators => discriminators.AsSpan(0, rowCount);

    /// <inheritdoc/>
    public ReadOnlySpan<int> LocalIndices => localIndex.AsSpan(0, rowCount);

    /// <summary>
    /// The rows as boxed values, materialized once and cached — each row is the selected alternative's value, or
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
            byte d = discriminators[row];
            return d == NullDiscriminator ? null : typeColumns[d].GetValue(localIndex[row]);
        }
    }

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <inheritdoc/>
    public IColumn GetTypeColumn(int discriminator) => typeColumns[discriminator];

    /// <inheritdoc/>
    public void Dispose()
    {
        if (columnOwnership is not null)
        {
            for (int i = 0; i < typeColumns.Length; i++)
            {
                if (columnOwnership[i])
                {
                    typeColumns[i].Dispose();
                }
            }
        }
        else if (ownsColumns)
        {
            foreach (IColumn column in typeColumns)
            {
                column.Dispose();
            }
        }

        if (pooledDiscriminators && discriminators.Length != 0)
        {
            ArrayPool<byte>.Shared.Return(discriminators);
        }

        discriminators = Array.Empty<byte>();
        cache = null;
    }
}

/// <summary>
/// The dense-column contract a <c>Variant(...)</c> codec writes from without copying: the per-row discriminator
/// stream and the per-type child columns. Implemented by <see cref="VariantColumn"/>.
/// </summary>
internal interface IVariantColumn : IColumn
{
    /// <summary>The number of alternative types.</summary>
    int TypeCount { get; }

    /// <summary>One discriminator per row; <see cref="VariantColumn.NullDiscriminator"/> marks a NULL row.</summary>
    ReadOnlySpan<byte> Discriminators { get; }

    /// <summary>
    /// Each row's index into its selected type's child column (the count of that discriminator in the rows before
    /// it), precomputed once; a NULL row's entry is <c>-1</c>. Lets a caller price or address a row in O(1)
    /// rather than rescanning the discriminators.
    /// </summary>
    ReadOnlySpan<int> LocalIndices { get; }

    /// <summary>The child column for the given discriminator (holding the values of the rows that selected it).</summary>
    /// <param name="discriminator">The alternative-type index.</param>
    /// <returns>That type's child column.</returns>
    IColumn GetTypeColumn(int discriminator);
}
