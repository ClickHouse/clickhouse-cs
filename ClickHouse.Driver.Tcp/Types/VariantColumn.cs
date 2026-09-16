using System;
using System.Buffers;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The dense shape of a ClickHouse <c>Variant(T1, ..., Tn)</c> column: a per-row discriminator stream plus one
/// child column per alternative type, each holding only the values of the rows that selected it (in row order).
/// This is exactly how the type is laid out on the wire — a discriminator array followed by a contiguous run per
/// type — so a <see cref="VariantColumn"/> is both what a read produces and the zero-copy source for a write.
///
/// <para>
/// A row's discriminator is an index into the alternative types (<c>0</c> = the first type, and so on), or
/// <see cref="IVariantColumn.NullDiscriminator"/> (<c>255</c>) for a NULL row, which consumes no value from any
/// child column.
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
    private readonly IColumn[] typeColumns;
    private readonly int rowCount;
    private readonly bool ownsColumns;
    private readonly bool pooledDiscriminators;
    private readonly int[] localIndex;
    private byte[] discriminators;
    private object[] cache;
    private IReadOnlyList<string> typeNames;

    // When non-null, overrides ownsColumns per type column: Dispose disposes type column i only when
    // columnOwnership[i] is true. Set once by RestrictOwnership immediately after construction so a densified
    // wrapper that mixes freshly built type columns with columns borrowed from another column disposes only the
    // ones it created.
    private bool[] columnOwnership;

    /// <summary>Initializes a variant column over its discriminator stream and per-type child columns.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The full <c>Variant(...)</c> type string.</param>
    /// <param name="discriminators">One discriminator per row; <see cref="IVariantColumn.NullDiscriminator"/> marks a NULL row.</param>
    /// <param name="typeColumns">One child column per alternative type, in declared (discriminator) order; each holds the values of the rows that selected it, in row order.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="pooledDiscriminators">Whether <paramref name="discriminators"/> was rented and should be returned on dispose.</param>
    /// <param name="ownsColumns">Whether this column owns and disposes <paramref name="typeColumns"/> (false when a caller retains them).</param>
    /// <exception cref="ArgumentNullException"><paramref name="discriminators"/> or <paramref name="typeColumns"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="discriminators"/> holds fewer than <paramref name="rowCount"/> entries.</exception>
    public VariantColumn(string name, string typeName, byte[] discriminators, IColumn[] typeColumns, int rowCount, bool pooledDiscriminators, bool ownsColumns)
    {
        Name = name;
        TypeName = typeName;
        this.discriminators = discriminators ?? throw new ArgumentNullException(nameof(discriminators));
        this.typeColumns = typeColumns ?? throw new ArgumentNullException(nameof(typeColumns));
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
            localIndex[row] = d == IVariantColumn.NullDiscriminator ? -1 : counters[d]++;
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
    // Read off the children rather than carried separately: each was stamped with its own codec's type name when it
    // was decoded, so this cannot disagree with GetTypeColumn(i).TypeName. Built on first use, like Values, and
    // wrapped so a caller cannot cast the list back to its array and rewrite an entry.
    public IReadOnlyList<string> TypeNames
        => typeNames ??= Array.AsReadOnly(Array.ConvertAll(typeColumns, column => column.TypeName));

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
    // Index through the RowCount-sliced discriminators, not the raw buffer: the buffer is usually a pooled array
    // longer than the column, so a row past RowCount read a stale byte from its tail. That made the failure depend on
    // the leftover value — a stale 255 returned null as though the row existed, anything else fell through to the
    // exactly-sized local index and threw. Slicing makes it a bounds failure either way.
    public object this[int row]
    {
        get
        {
            byte d = Discriminators[row];
            return d == IVariantColumn.NullDiscriminator ? null : typeColumns[d].GetValue(localIndex[row]);
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
