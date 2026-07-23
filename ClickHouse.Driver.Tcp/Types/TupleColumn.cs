using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// The arity-independent state of a tuple column: the child columns, the optional element names, the row count,
/// and lifetime. The generic <c>TupleColumn</c> subclasses add the typed <c>ValueTuple</c> materialization for a
/// specific number of elements (1 through 7).
/// </summary>
internal abstract class TupleColumnBase : ITupleColumn
{
    private readonly IColumn[] children;
    private readonly bool ownsChildren;

    /// <summary>Initializes the shared tuple-column state.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="typeName">The full <c>Tuple(...)</c> type string (element names included when named).</param>
    /// <param name="children">The child columns, one per element; each must be an <see cref="IColumn{T}"/> of the corresponding element type.</param>
    /// <param name="fieldNames">The element names (one per element, null entry for an unnamed element), or null when the tuple is unnamed.</param>
    /// <param name="rowCount">The number of rows; every child must have this many.</param>
    /// <param name="ownsChildren">Whether disposing this column disposes the child columns.</param>
    protected TupleColumnBase(string name, string typeName, IColumn[] children, IReadOnlyList<string> fieldNames, int rowCount, bool ownsChildren)
    {
        this.children = children ?? throw new ArgumentNullException(nameof(children));
        Name = name;
        TypeName = typeName;
        FieldNames = fieldNames;
        RowCount = rowCount;
        this.ownsChildren = ownsChildren;
    }

    /// <inheritdoc/>
    public string Name { get; }

    /// <inheritdoc/>
    public string TypeName { get; }

    /// <inheritdoc/>
    public int RowCount { get; }

    /// <inheritdoc/>
    public IReadOnlyList<IColumn> Children => children;

    /// <inheritdoc/>
    public IReadOnlyList<string> FieldNames { get; }

    /// <inheritdoc/>
    public abstract object GetValue(int row);

    /// <inheritdoc/>
    public void Dispose()
    {
        if (!ownsChildren)
        {
            return;
        }

        foreach (IColumn child in children)
        {
            child.Dispose();
        }
    }

    /// <summary>The child column at <paramref name="index"/>, for a subclass to cast to its typed element column.</summary>
    protected IColumn Child(int index) => children[index];
}

/// <summary>A single-element tuple column, surfacing each row as <see cref="ValueTuple{T1}"/>.</summary>
/// <typeparam name="T1">The element type.</typeparam>
internal sealed class TupleColumn<T1> : TupleColumnBase, IColumn<ValueTuple<T1>>
{
    private readonly IColumn<T1> item1;
    private ValueTuple<T1>[] cache;

    internal TupleColumn(string name, string typeName, IColumn[] children, IReadOnlyList<string> fieldNames, int rowCount, bool ownsChildren)
        : base(name, typeName, children, fieldNames, rowCount, ownsChildren)
    {
        item1 = (IColumn<T1>)Child(0);
    }

    internal TupleColumn(string name, string typeName, ValueTuple<T1>[] rows)
        : this(name, typeName, BuildChildren(name, typeName, rows), null, rows.Length, ownsChildren: true)
    {
    }

    /// <inheritdoc/>
    public ReadOnlySpan<ValueTuple<T1>> Values
    {
        get
        {
            if (cache is null)
            {
                var decoded = new ValueTuple<T1>[RowCount];
                for (int i = 0; i < RowCount; i++)
                {
                    decoded[i] = new ValueTuple<T1>(item1[i]);
                }

                cache = decoded;
            }

            return cache.AsSpan(0, RowCount);
        }
    }

    /// <inheritdoc/>
    public ValueTuple<T1> this[int row] => cache is not null ? cache[row] : new ValueTuple<T1>(item1[row]);

    /// <inheritdoc/>
    public override object GetValue(int row) => this[row];

    private static IColumn[] BuildChildren(string name, string typeName, ValueTuple<T1>[] rows)
    {
        string[] types = NamedElementParser.ElementTypeStrings(typeName, 1);
        var values1 = new T1[rows.Length];
        for (int i = 0; i < rows.Length; i++)
        {
            values1[i] = rows[i].Item1;
        }

        return new IColumn[] { new ArrayColumn<T1>(name, types[0], values1) };
    }
}

/// <summary>A two-element tuple column, surfacing each row as <c>(T1, T2)</c>.</summary>
internal sealed class TupleColumn<T1, T2> : TupleColumnBase, IColumn<(T1, T2)>
{
    private readonly IColumn<T1> item1;
    private readonly IColumn<T2> item2;
    private (T1, T2)[] cache;

    internal TupleColumn(string name, string typeName, IColumn[] children, IReadOnlyList<string> fieldNames, int rowCount, bool ownsChildren)
        : base(name, typeName, children, fieldNames, rowCount, ownsChildren)
    {
        item1 = (IColumn<T1>)Child(0);
        item2 = (IColumn<T2>)Child(1);
    }

    internal TupleColumn(string name, string typeName, (T1, T2)[] rows)
        : this(name, typeName, BuildChildren(name, typeName, rows), null, rows.Length, ownsChildren: true)
    {
    }

    /// <inheritdoc/>
    public ReadOnlySpan<(T1, T2)> Values
    {
        get
        {
            if (cache is null)
            {
                var decoded = new (T1, T2)[RowCount];
                for (int i = 0; i < RowCount; i++)
                {
                    decoded[i] = (item1[i], item2[i]);
                }

                cache = decoded;
            }

            return cache.AsSpan(0, RowCount);
        }
    }

    /// <inheritdoc/>
    public (T1, T2) this[int row] => cache is not null ? cache[row] : (item1[row], item2[row]);

    /// <inheritdoc/>
    public override object GetValue(int row) => this[row];

    private static IColumn[] BuildChildren(string name, string typeName, (T1, T2)[] rows)
    {
        string[] types = NamedElementParser.ElementTypeStrings(typeName, 2);
        var values1 = new T1[rows.Length];
        var values2 = new T2[rows.Length];
        for (int i = 0; i < rows.Length; i++)
        {
            values1[i] = rows[i].Item1;
            values2[i] = rows[i].Item2;
        }

        return new IColumn[]
        {
            new ArrayColumn<T1>(name, types[0], values1),
            new ArrayColumn<T2>(name, types[1], values2),
        };
    }
}

/// <summary>A three-element tuple column, surfacing each row as <c>(T1, T2, T3)</c>.</summary>
internal sealed class TupleColumn<T1, T2, T3> : TupleColumnBase, IColumn<(T1, T2, T3)>
{
    private readonly IColumn<T1> item1;
    private readonly IColumn<T2> item2;
    private readonly IColumn<T3> item3;
    private (T1, T2, T3)[] cache;

    internal TupleColumn(string name, string typeName, IColumn[] children, IReadOnlyList<string> fieldNames, int rowCount, bool ownsChildren)
        : base(name, typeName, children, fieldNames, rowCount, ownsChildren)
    {
        item1 = (IColumn<T1>)Child(0);
        item2 = (IColumn<T2>)Child(1);
        item3 = (IColumn<T3>)Child(2);
    }

    internal TupleColumn(string name, string typeName, (T1, T2, T3)[] rows)
        : this(name, typeName, BuildChildren(name, typeName, rows), null, rows.Length, ownsChildren: true)
    {
    }

    /// <inheritdoc/>
    public ReadOnlySpan<(T1, T2, T3)> Values
    {
        get
        {
            if (cache is null)
            {
                var decoded = new (T1, T2, T3)[RowCount];
                for (int i = 0; i < RowCount; i++)
                {
                    decoded[i] = (item1[i], item2[i], item3[i]);
                }

                cache = decoded;
            }

            return cache.AsSpan(0, RowCount);
        }
    }

    /// <inheritdoc/>
    public (T1, T2, T3) this[int row] => cache is not null ? cache[row] : (item1[row], item2[row], item3[row]);

    /// <inheritdoc/>
    public override object GetValue(int row) => this[row];

    private static IColumn[] BuildChildren(string name, string typeName, (T1, T2, T3)[] rows)
    {
        string[] types = NamedElementParser.ElementTypeStrings(typeName, 3);
        var values1 = new T1[rows.Length];
        var values2 = new T2[rows.Length];
        var values3 = new T3[rows.Length];
        for (int i = 0; i < rows.Length; i++)
        {
            values1[i] = rows[i].Item1;
            values2[i] = rows[i].Item2;
            values3[i] = rows[i].Item3;
        }

        return new IColumn[]
        {
            new ArrayColumn<T1>(name, types[0], values1),
            new ArrayColumn<T2>(name, types[1], values2),
            new ArrayColumn<T3>(name, types[2], values3),
        };
    }
}

/// <summary>A four-element tuple column, surfacing each row as <c>(T1, T2, T3, T4)</c>.</summary>
internal sealed class TupleColumn<T1, T2, T3, T4> : TupleColumnBase, IColumn<(T1, T2, T3, T4)>
{
    private readonly IColumn<T1> item1;
    private readonly IColumn<T2> item2;
    private readonly IColumn<T3> item3;
    private readonly IColumn<T4> item4;
    private (T1, T2, T3, T4)[] cache;

    internal TupleColumn(string name, string typeName, IColumn[] children, IReadOnlyList<string> fieldNames, int rowCount, bool ownsChildren)
        : base(name, typeName, children, fieldNames, rowCount, ownsChildren)
    {
        item1 = (IColumn<T1>)Child(0);
        item2 = (IColumn<T2>)Child(1);
        item3 = (IColumn<T3>)Child(2);
        item4 = (IColumn<T4>)Child(3);
    }

    internal TupleColumn(string name, string typeName, (T1, T2, T3, T4)[] rows)
        : this(name, typeName, BuildChildren(name, typeName, rows), null, rows.Length, ownsChildren: true)
    {
    }

    /// <inheritdoc/>
    public ReadOnlySpan<(T1, T2, T3, T4)> Values
    {
        get
        {
            if (cache is null)
            {
                var decoded = new (T1, T2, T3, T4)[RowCount];
                for (int i = 0; i < RowCount; i++)
                {
                    decoded[i] = (item1[i], item2[i], item3[i], item4[i]);
                }

                cache = decoded;
            }

            return cache.AsSpan(0, RowCount);
        }
    }

    /// <inheritdoc/>
    public (T1, T2, T3, T4) this[int row] => cache is not null ? cache[row] : (item1[row], item2[row], item3[row], item4[row]);

    /// <inheritdoc/>
    public override object GetValue(int row) => this[row];

    private static IColumn[] BuildChildren(string name, string typeName, (T1, T2, T3, T4)[] rows)
    {
        string[] types = NamedElementParser.ElementTypeStrings(typeName, 4);
        var values1 = new T1[rows.Length];
        var values2 = new T2[rows.Length];
        var values3 = new T3[rows.Length];
        var values4 = new T4[rows.Length];
        for (int i = 0; i < rows.Length; i++)
        {
            values1[i] = rows[i].Item1;
            values2[i] = rows[i].Item2;
            values3[i] = rows[i].Item3;
            values4[i] = rows[i].Item4;
        }

        return new IColumn[]
        {
            new ArrayColumn<T1>(name, types[0], values1),
            new ArrayColumn<T2>(name, types[1], values2),
            new ArrayColumn<T3>(name, types[2], values3),
            new ArrayColumn<T4>(name, types[3], values4),
        };
    }
}

/// <summary>A five-element tuple column, surfacing each row as <c>(T1, T2, T3, T4, T5)</c>.</summary>
internal sealed class TupleColumn<T1, T2, T3, T4, T5> : TupleColumnBase, IColumn<(T1, T2, T3, T4, T5)>
{
    private readonly IColumn<T1> item1;
    private readonly IColumn<T2> item2;
    private readonly IColumn<T3> item3;
    private readonly IColumn<T4> item4;
    private readonly IColumn<T5> item5;
    private (T1, T2, T3, T4, T5)[] cache;

    internal TupleColumn(string name, string typeName, IColumn[] children, IReadOnlyList<string> fieldNames, int rowCount, bool ownsChildren)
        : base(name, typeName, children, fieldNames, rowCount, ownsChildren)
    {
        item1 = (IColumn<T1>)Child(0);
        item2 = (IColumn<T2>)Child(1);
        item3 = (IColumn<T3>)Child(2);
        item4 = (IColumn<T4>)Child(3);
        item5 = (IColumn<T5>)Child(4);
    }

    internal TupleColumn(string name, string typeName, (T1, T2, T3, T4, T5)[] rows)
        : this(name, typeName, BuildChildren(name, typeName, rows), null, rows.Length, ownsChildren: true)
    {
    }

    /// <inheritdoc/>
    public ReadOnlySpan<(T1, T2, T3, T4, T5)> Values
    {
        get
        {
            if (cache is null)
            {
                var decoded = new (T1, T2, T3, T4, T5)[RowCount];
                for (int i = 0; i < RowCount; i++)
                {
                    decoded[i] = (item1[i], item2[i], item3[i], item4[i], item5[i]);
                }

                cache = decoded;
            }

            return cache.AsSpan(0, RowCount);
        }
    }

    /// <inheritdoc/>
    public (T1, T2, T3, T4, T5) this[int row] => cache is not null ? cache[row] : (item1[row], item2[row], item3[row], item4[row], item5[row]);

    /// <inheritdoc/>
    public override object GetValue(int row) => this[row];

    private static IColumn[] BuildChildren(string name, string typeName, (T1, T2, T3, T4, T5)[] rows)
    {
        string[] types = NamedElementParser.ElementTypeStrings(typeName, 5);
        var values1 = new T1[rows.Length];
        var values2 = new T2[rows.Length];
        var values3 = new T3[rows.Length];
        var values4 = new T4[rows.Length];
        var values5 = new T5[rows.Length];
        for (int i = 0; i < rows.Length; i++)
        {
            values1[i] = rows[i].Item1;
            values2[i] = rows[i].Item2;
            values3[i] = rows[i].Item3;
            values4[i] = rows[i].Item4;
            values5[i] = rows[i].Item5;
        }

        return new IColumn[]
        {
            new ArrayColumn<T1>(name, types[0], values1),
            new ArrayColumn<T2>(name, types[1], values2),
            new ArrayColumn<T3>(name, types[2], values3),
            new ArrayColumn<T4>(name, types[3], values4),
            new ArrayColumn<T5>(name, types[4], values5),
        };
    }
}

/// <summary>A six-element tuple column, surfacing each row as <c>(T1, T2, T3, T4, T5, T6)</c>.</summary>
internal sealed class TupleColumn<T1, T2, T3, T4, T5, T6> : TupleColumnBase, IColumn<(T1, T2, T3, T4, T5, T6)>
{
    private readonly IColumn<T1> item1;
    private readonly IColumn<T2> item2;
    private readonly IColumn<T3> item3;
    private readonly IColumn<T4> item4;
    private readonly IColumn<T5> item5;
    private readonly IColumn<T6> item6;
    private (T1, T2, T3, T4, T5, T6)[] cache;

    internal TupleColumn(string name, string typeName, IColumn[] children, IReadOnlyList<string> fieldNames, int rowCount, bool ownsChildren)
        : base(name, typeName, children, fieldNames, rowCount, ownsChildren)
    {
        item1 = (IColumn<T1>)Child(0);
        item2 = (IColumn<T2>)Child(1);
        item3 = (IColumn<T3>)Child(2);
        item4 = (IColumn<T4>)Child(3);
        item5 = (IColumn<T5>)Child(4);
        item6 = (IColumn<T6>)Child(5);
    }

    internal TupleColumn(string name, string typeName, (T1, T2, T3, T4, T5, T6)[] rows)
        : this(name, typeName, BuildChildren(name, typeName, rows), null, rows.Length, ownsChildren: true)
    {
    }

    /// <inheritdoc/>
    public ReadOnlySpan<(T1, T2, T3, T4, T5, T6)> Values
    {
        get
        {
            if (cache is null)
            {
                var decoded = new (T1, T2, T3, T4, T5, T6)[RowCount];
                for (int i = 0; i < RowCount; i++)
                {
                    decoded[i] = (item1[i], item2[i], item3[i], item4[i], item5[i], item6[i]);
                }

                cache = decoded;
            }

            return cache.AsSpan(0, RowCount);
        }
    }

    /// <inheritdoc/>
    public (T1, T2, T3, T4, T5, T6) this[int row] => cache is not null ? cache[row] : (item1[row], item2[row], item3[row], item4[row], item5[row], item6[row]);

    /// <inheritdoc/>
    public override object GetValue(int row) => this[row];

    private static IColumn[] BuildChildren(string name, string typeName, (T1, T2, T3, T4, T5, T6)[] rows)
    {
        string[] types = NamedElementParser.ElementTypeStrings(typeName, 6);
        var values1 = new T1[rows.Length];
        var values2 = new T2[rows.Length];
        var values3 = new T3[rows.Length];
        var values4 = new T4[rows.Length];
        var values5 = new T5[rows.Length];
        var values6 = new T6[rows.Length];
        for (int i = 0; i < rows.Length; i++)
        {
            values1[i] = rows[i].Item1;
            values2[i] = rows[i].Item2;
            values3[i] = rows[i].Item3;
            values4[i] = rows[i].Item4;
            values5[i] = rows[i].Item5;
            values6[i] = rows[i].Item6;
        }

        return new IColumn[]
        {
            new ArrayColumn<T1>(name, types[0], values1),
            new ArrayColumn<T2>(name, types[1], values2),
            new ArrayColumn<T3>(name, types[2], values3),
            new ArrayColumn<T4>(name, types[3], values4),
            new ArrayColumn<T5>(name, types[4], values5),
            new ArrayColumn<T6>(name, types[5], values6),
        };
    }
}

/// <summary>A seven-element tuple column, surfacing each row as <c>(T1, T2, T3, T4, T5, T6, T7)</c>.</summary>
internal sealed class TupleColumn<T1, T2, T3, T4, T5, T6, T7> : TupleColumnBase, IColumn<(T1, T2, T3, T4, T5, T6, T7)>
{
    private readonly IColumn<T1> item1;
    private readonly IColumn<T2> item2;
    private readonly IColumn<T3> item3;
    private readonly IColumn<T4> item4;
    private readonly IColumn<T5> item5;
    private readonly IColumn<T6> item6;
    private readonly IColumn<T7> item7;
    private (T1, T2, T3, T4, T5, T6, T7)[] cache;

    internal TupleColumn(string name, string typeName, IColumn[] children, IReadOnlyList<string> fieldNames, int rowCount, bool ownsChildren)
        : base(name, typeName, children, fieldNames, rowCount, ownsChildren)
    {
        item1 = (IColumn<T1>)Child(0);
        item2 = (IColumn<T2>)Child(1);
        item3 = (IColumn<T3>)Child(2);
        item4 = (IColumn<T4>)Child(3);
        item5 = (IColumn<T5>)Child(4);
        item6 = (IColumn<T6>)Child(5);
        item7 = (IColumn<T7>)Child(6);
    }

    internal TupleColumn(string name, string typeName, (T1, T2, T3, T4, T5, T6, T7)[] rows)
        : this(name, typeName, BuildChildren(name, typeName, rows), null, rows.Length, ownsChildren: true)
    {
    }

    /// <inheritdoc/>
    public ReadOnlySpan<(T1, T2, T3, T4, T5, T6, T7)> Values
    {
        get
        {
            if (cache is null)
            {
                var decoded = new (T1, T2, T3, T4, T5, T6, T7)[RowCount];
                for (int i = 0; i < RowCount; i++)
                {
                    decoded[i] = (item1[i], item2[i], item3[i], item4[i], item5[i], item6[i], item7[i]);
                }

                cache = decoded;
            }

            return cache.AsSpan(0, RowCount);
        }
    }

    /// <inheritdoc/>
    public (T1, T2, T3, T4, T5, T6, T7) this[int row] => cache is not null ? cache[row] : (item1[row], item2[row], item3[row], item4[row], item5[row], item6[row], item7[row]);

    /// <inheritdoc/>
    public override object GetValue(int row) => this[row];

    private static IColumn[] BuildChildren(string name, string typeName, (T1, T2, T3, T4, T5, T6, T7)[] rows)
    {
        string[] types = NamedElementParser.ElementTypeStrings(typeName, 7);
        var values1 = new T1[rows.Length];
        var values2 = new T2[rows.Length];
        var values3 = new T3[rows.Length];
        var values4 = new T4[rows.Length];
        var values5 = new T5[rows.Length];
        var values6 = new T6[rows.Length];
        var values7 = new T7[rows.Length];
        for (int i = 0; i < rows.Length; i++)
        {
            values1[i] = rows[i].Item1;
            values2[i] = rows[i].Item2;
            values3[i] = rows[i].Item3;
            values4[i] = rows[i].Item4;
            values5[i] = rows[i].Item5;
            values6[i] = rows[i].Item6;
            values7[i] = rows[i].Item7;
        }

        return new IColumn[]
        {
            new ArrayColumn<T1>(name, types[0], values1),
            new ArrayColumn<T2>(name, types[1], values2),
            new ArrayColumn<T3>(name, types[2], values3),
            new ArrayColumn<T4>(name, types[3], values4),
            new ArrayColumn<T5>(name, types[4], values5),
            new ArrayColumn<T6>(name, types[5], values6),
            new ArrayColumn<T7>(name, types[6], values7),
        };
    }
}
