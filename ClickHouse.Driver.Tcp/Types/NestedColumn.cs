using System;
using System.Buffers;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// A ClickHouse <c>Nested(name1 T1, ..., namen Tn)</c> column carried as a single wire column (the
/// <c>flatten_nested = 0</c> form). It is columnar and arity-agnostic: it holds one flat field column per named
/// field — every row's elements for that field concatenated end-to-end — plus a per-row offsets array shared by
/// all fields (within a row every field has the same element count, so one set of row boundaries delimits them
/// all). This is the dense shape the wire uses (offsets + one stream per field, byte-identical to
/// <c>Array(Tuple(T1, ..., Tn))</c>), so it is also the zero-copy source for writing — handed back to the
/// <c>Nested(...)</c> codec it serializes straight from the field columns and offsets without rebuilding anything.
///
/// <para>
/// The primary, allocation-free access is columnar: <see cref="GetField(int)"/> / <see cref="GetField(string)"/>
/// return a field's flat column (each holding <em>total elements</em> values, aligned across fields), and
/// <see cref="Offsets"/> maps a row to its element range in those columns. Because a <c>Nested</c> can carry any
/// number of fields, there is deliberately no single generic per-row value type; the <see cref="IColumn{T}"/>
/// surface materializes each row as an array of records (<c>object[][]</c> — one <c>object[]</c> of field values
/// per element), a boxed convenience for generic consumers rather than the fast path.
/// </para>
///
/// <para>
/// The field columns' storage and the offsets are borrowed for this column's lifetime; the field columns are
/// disposed (when owned) and the offsets returned (when pooled) on <see cref="Dispose"/>. Each row access copies
/// that row's slice into fresh arrays, so retain those freely, but read the column itself only while the owning
/// block is alive.
/// </para>
/// </summary>
internal sealed class NestedColumn : IColumn<object[][]>
{
    private readonly IColumn[] fields;
    private readonly string[] fieldNames;
    private readonly int rowCount;
    private readonly bool pooledOffsets;
    private readonly bool ownsFields;
    private int[] offsets;
    private object[][][] cache;

    // When non-null, overrides ownsFields per field: Dispose disposes field i only when fieldOwnership[i] is true.
    // Set once by RestrictOwnership immediately after construction so a densified wrapper that mixes freshly built
    // field columns with fields borrowed from another column disposes only the ones it created.
    private bool[] fieldOwnership;

    /// <summary>Initializes a nested column over its flat field columns and their shared per-row offsets.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The full <c>Nested(...)</c> type string.</param>
    /// <param name="fieldNames">The field names, in declaration order; must align with <paramref name="fields"/>.</param>
    /// <param name="fields">One flat column per field, each holding every row's elements for that field concatenated end-to-end.</param>
    /// <param name="offsets">The per-row offsets: <c>offsets[0]</c> is 0 and <c>offsets[i + 1]</c> is the exclusive end of row <c>i</c>'s elements; must have at least <paramref name="rowCount"/> + 1 entries.</param>
    /// <param name="rowCount">The number of rows.</param>
    /// <param name="pooledOffsets">Whether <paramref name="offsets"/> was rented and should be returned on dispose.</param>
    /// <param name="ownsFields">Whether this column owns and disposes <paramref name="fields"/> (false when a caller retains them).</param>
    /// <exception cref="ArgumentException"><paramref name="fieldNames"/> and <paramref name="fields"/> differ in length, or <paramref name="fields"/> is empty.</exception>
    public NestedColumn(string name, string typeName, string[] fieldNames, IColumn[] fields, int[] offsets, int rowCount, bool pooledOffsets, bool ownsFields)
    {
        this.fieldNames = fieldNames ?? throw new ArgumentNullException(nameof(fieldNames));
        this.fields = fields ?? throw new ArgumentNullException(nameof(fields));
        if (fields.Length == 0)
        {
            throw new ArgumentException("A Nested column must have at least one field.", nameof(fields));
        }

        if (fieldNames.Length != fields.Length)
        {
            throw new ArgumentException($"Field name count ({fieldNames.Length}) does not match field column count ({fields.Length}).", nameof(fieldNames));
        }

        Name = name;
        TypeName = typeName;
        this.offsets = offsets ?? throw new ArgumentNullException(nameof(offsets));
        this.rowCount = rowCount;
        this.pooledOffsets = pooledOffsets;
        this.ownsFields = ownsFields;
    }

    /// <summary>
    /// Restricts disposal to the fields flagged in <paramref name="owned"/> (one entry per field), overriding the
    /// all-or-nothing <c>ownsFields</c> passed at construction. Used when rebuilding a densified nested column that
    /// keeps some field columns by reference (owned by the source column) and replaces others with freshly built
    /// ones, so disposing this wrapper frees only the columns it created. Must be called before the column is observed.
    /// </summary>
    internal void RestrictOwnership(bool[] owned)
    {
        if (owned is null || owned.Length != fields.Length)
        {
            throw new ArgumentException("Ownership mask must have one entry per field column.", nameof(owned));
        }

        fieldOwnership = owned;
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount => rowCount;

    /// <summary>The number of fields.</summary>
    public int FieldCount => fields.Length;

    /// <summary>The field names, in declaration order.</summary>
    public IReadOnlyList<string> FieldNames => fieldNames;

    /// <summary>
    /// The per-row offsets, sliced to <see cref="RowCount"/> + 1 entries: <c>[0]</c> is 0 and <c>[i + 1]</c> is
    /// the exclusive end of row <c>i</c>'s slice of every field column — a zero-copy write source and the way to
    /// address a single row's elements within the flat field columns from <see cref="GetField(int)"/>.
    /// </summary>
    internal ReadOnlySpan<int> Offsets => offsets.AsSpan(0, rowCount + 1);

    /// <summary>The flat column for field <paramref name="index"/> (every row's elements concatenated) — a zero-copy access.</summary>
    /// <param name="index">The zero-based field index.</param>
    /// <returns>The field's flat column, holding the same total-element count as every other field.</returns>
    public IColumn GetField(int index) => fields[index];

    /// <summary>The flat column for the field named <paramref name="name"/> (ordinal match) — a zero-copy access.</summary>
    /// <param name="name">The field name.</param>
    /// <returns>The field's flat column.</returns>
    /// <exception cref="KeyNotFoundException">No field has that name.</exception>
    public IColumn GetField(string name)
    {
        for (int i = 0; i < fieldNames.Length; i++)
        {
            if (string.Equals(fieldNames[i], name, StringComparison.Ordinal))
            {
                return fields[i];
            }
        }

        throw new KeyNotFoundException($"Nested column '{Name}' ({TypeName}) has no field named '{name}'.");
    }

    /// <summary>
    /// The rows as arrays of records, materialized once and cached — each row is an <c>object[][]</c> of its
    /// elements, and each element an <c>object[]</c> of the fields' values in declaration order (boxed). Prefer
    /// <see cref="GetField(int)"/> plus <see cref="Offsets"/> for the allocation-free columnar path.
    /// </summary>
    public ReadOnlySpan<object[][]> Values
    {
        get
        {
            if (cache is null)
            {
                var decoded = new object[rowCount][][];
                for (int i = 0; i < rowCount; i++)
                {
                    decoded[i] = Materialize(i);
                }

                cache = decoded;
            }

            return cache.AsSpan(0, rowCount);
        }
    }

    /// <inheritdoc/>
    public object[][] this[int row] => cache is not null ? cache[row] : Materialize(row);

    /// <inheritdoc/>
    public object GetValue(int row) => this[row];

    /// <inheritdoc/>
    public void Dispose()
    {
        if (fieldOwnership is not null)
        {
            for (int i = 0; i < fields.Length; i++)
            {
                if (fieldOwnership[i])
                {
                    fields[i].Dispose();
                }
            }
        }
        else if (ownsFields)
        {
            foreach (IColumn field in fields)
            {
                field.Dispose();
            }
        }

        if (pooledOffsets && offsets.Length != 0)
        {
            ArrayPool<int>.Shared.Return(offsets);
        }

        offsets = Array.Empty<int>();
        cache = null;
    }

    /// <summary>Copies row <paramref name="row"/>'s elements into an array of records, boxing each field value.</summary>
    private object[][] Materialize(int row)
    {
        int start = offsets[row];
        int length = offsets[row + 1] - start;
        if (length == 0)
        {
            return Array.Empty<object[]>();
        }

        var records = new object[length][];
        for (int e = 0; e < length; e++)
        {
            var record = new object[fields.Length];
            for (int f = 0; f < fields.Length; f++)
            {
                record[f] = fields[f].GetValue(start + e);
            }

            records[e] = record;
        }

        return records;
    }
}
