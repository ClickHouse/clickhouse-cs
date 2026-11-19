using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// <c>Tuple(...)</c> of 1 to 7 elements read as a <c>ValueTuple</c> of the same arity. Each field child reads its child
/// column (<see cref="ITupleColumn.Children"/>, by position), and the reader builds one tuple for each row. The
/// derivation closes one subclass for each arity.
/// </summary>
/// <typeparam name="T">The <c>ValueTuple</c> type.</typeparam>
internal abstract class TupleReaderBase<T> : ColumnReader<T>
    where T : struct
{
    private static readonly MethodInfo SurfaceMethod =
        typeof(TupleReaderBase<T>).GetMethod(nameof(TupleSurface), BindingFlags.NonPublic | BindingFlags.Static);

    private static readonly MethodInfo ChildMethod =
        typeof(TupleReaderBase<T>).GetMethod(nameof(Child), BindingFlags.NonPublic | BindingFlags.Static);

    private readonly ColumnReader[] fields;

    /// <summary>Initializes the reader over one reader for each field.</summary>
    /// <param name="fields">The field readers, in tuple order. Their value types are the type arguments of <typeparamref name="T"/>.</param>
    protected TupleReaderBase(params ColumnReader[] fields) => this.fields = fields;

    /// <inheritdoc/>
    [RequiresDynamicCode("Builds an expression tree, which the caller compiles.")]
    public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
    {
        ParameterExpression tuple = scope.Local(
            typeof(ITupleColumn),
            "tuple",
            Expression.Call(SurfaceMethod, Expression.Convert(column, typeof(IColumn)), Expression.Constant(fields.Length)));
        var values = new Expression[fields.Length];
        var types = new Type[fields.Length];
        for (int i = 0; i < fields.Length; i++)
        {
            ParameterExpression child = scope.Local(typeof(IColumn), "field", Expression.Call(ChildMethod, tuple, Expression.Constant(i)));
            values[i] = fields[i].Emit(child, row, scope);
            types[i] = fields[i].ValueType;
        }

        return Expression.New(typeof(T).GetConstructor(types), values);
    }

    /// <summary>The column as a tuple column with one child for each field.</summary>
    /// <param name="column">The decoded column.</param>
    /// <param name="arity">The number of fields of the reader.</param>
    /// <returns>The tuple column.</returns>
    /// <exception cref="InvalidOperationException">The column is not a tuple column, or it has another number of children.</exception>
    protected static ITupleColumn TupleSurface(IColumn column, int arity)
    {
        ITupleColumn tuple = ColumnSurface.Of<ITupleColumn>(column);
        if (tuple.Children.Count != arity)
        {
            throw new InvalidOperationException(
                $"Column '{column.Name}' ({column.TypeName}) was read as a tuple of {tuple.Children.Count} children, " +
                $"but its type resolved to {arity}, so a converter cannot pair them.");
        }

        return tuple;
    }

    /// <summary>Reads one field of each row into a pooled array, for the bulk read of a tuple.</summary>
    /// <typeparam name="TField">The CLR type of the field.</typeparam>
    /// <param name="field">The bound field reader.</param>
    /// <param name="start">The first row.</param>
    /// <param name="length">The number of rows.</param>
    /// <returns>The pooled array, at least <paramref name="length"/> long. Give it back with <see cref="Release{TField}"/>.</returns>
    protected static TField[] Read<TField>(BoundReader<TField> field, int start, int length)
    {
        TField[] values = ArrayPool<TField>.Shared.Rent(length);
        try
        {
            field.Fill(start, values.AsSpan(0, length));
            return values;
        }
        catch
        {
            Release(values);
            throw;
        }
    }

    /// <summary>Gives back an array of <see cref="Read{TField}"/>. A null array is ignored.</summary>
    /// <typeparam name="TField">The CLR type of the field.</typeparam>
    /// <param name="values">The array, or null.</param>
    protected static void Release<TField>(TField[] values)
    {
        if (values is not null)
        {
            ArrayPool<TField>.Shared.Return(values, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<TField>());
        }
    }

    private static IColumn Child(ITupleColumn tuple, int index) => tuple.Children[index];
}

/// <summary>A tuple of one element (see <see cref="TupleReaderBase{T}"/>).</summary>
/// <typeparam name="T1">The CLR type of field 1.</typeparam>
internal sealed class TupleReader<T1> : TupleReaderBase<ValueTuple<T1>>
{
    private readonly ColumnReader<T1> f1;

    /// <summary>Initializes the reader over the field readers.</summary>
    /// <param name="f1">Reads field 1.</param>
    public TupleReader(ColumnReader<T1> f1)
        : base(f1) => this.f1 = f1;

    /// <inheritdoc/>
    public override BoundReader<ValueTuple<T1>> Bind(IColumn column)
    {
        ITupleColumn tuple = TupleSurface(column, 1);
        return new Bound(f1.Bind(tuple.Children[0]));
    }

    private sealed class Bound : BoundReader<ValueTuple<T1>>
    {
        private readonly BoundReader<T1> b1;

        public Bound(BoundReader<T1> b1) => this.b1 = b1;

        public override void Fill(int start, Span<ValueTuple<T1>> destination)
        {
            T1[] v1 = null;
            try
            {
                v1 = Read(b1, start, destination.Length);
                for (int i = 0; i < destination.Length; i++)
                {
                    destination[i] = new ValueTuple<T1>(v1[i]);
                }
            }
            finally
            {
                Release(v1);
            }
        }
    }
}

/// <summary>A tuple of two elements (see <see cref="TupleReaderBase{T}"/>).</summary>
/// <typeparam name="T1">The CLR type of field 1.</typeparam>
/// <typeparam name="T2">The CLR type of field 2.</typeparam>
internal sealed class TupleReader<T1, T2> : TupleReaderBase<(T1, T2)>
{
    private readonly ColumnReader<T1> f1;
    private readonly ColumnReader<T2> f2;

    /// <summary>Initializes the reader over the field readers.</summary>
    /// <param name="f1">Reads field 1.</param>
    /// <param name="f2">Reads field 2.</param>
    public TupleReader(ColumnReader<T1> f1, ColumnReader<T2> f2)
        : base(f1, f2)
    {
        this.f1 = f1;
        this.f2 = f2;
    }

    /// <inheritdoc/>
    public override BoundReader<(T1, T2)> Bind(IColumn column)
    {
        ITupleColumn tuple = TupleSurface(column, 2);
        return new Bound(f1.Bind(tuple.Children[0]), f2.Bind(tuple.Children[1]));
    }

    private sealed class Bound : BoundReader<(T1, T2)>
    {
        private readonly BoundReader<T1> b1;
        private readonly BoundReader<T2> b2;

        public Bound(BoundReader<T1> b1, BoundReader<T2> b2)
        {
            this.b1 = b1;
            this.b2 = b2;
        }

        public override void Fill(int start, Span<(T1, T2)> destination)
        {
            T1[] v1 = null;
            T2[] v2 = null;
            try
            {
                v1 = Read(b1, start, destination.Length);
                v2 = Read(b2, start, destination.Length);
                for (int i = 0; i < destination.Length; i++)
                {
                    destination[i] = (v1[i], v2[i]);
                }
            }
            finally
            {
                Release(v1);
                Release(v2);
            }
        }
    }
}

/// <summary>A tuple of three elements (see <see cref="TupleReaderBase{T}"/>).</summary>
/// <typeparam name="T1">The CLR type of field 1.</typeparam>
/// <typeparam name="T2">The CLR type of field 2.</typeparam>
/// <typeparam name="T3">The CLR type of field 3.</typeparam>
internal sealed class TupleReader<T1, T2, T3> : TupleReaderBase<(T1, T2, T3)>
{
    private readonly ColumnReader<T1> f1;
    private readonly ColumnReader<T2> f2;
    private readonly ColumnReader<T3> f3;

    /// <summary>Initializes the reader over the field readers.</summary>
    /// <param name="f1">Reads field 1.</param>
    /// <param name="f2">Reads field 2.</param>
    /// <param name="f3">Reads field 3.</param>
    public TupleReader(ColumnReader<T1> f1, ColumnReader<T2> f2, ColumnReader<T3> f3)
        : base(f1, f2, f3)
    {
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
    }

    /// <inheritdoc/>
    public override BoundReader<(T1, T2, T3)> Bind(IColumn column)
    {
        ITupleColumn tuple = TupleSurface(column, 3);
        return new Bound(f1.Bind(tuple.Children[0]), f2.Bind(tuple.Children[1]), f3.Bind(tuple.Children[2]));
    }

    private sealed class Bound : BoundReader<(T1, T2, T3)>
    {
        private readonly BoundReader<T1> b1;
        private readonly BoundReader<T2> b2;
        private readonly BoundReader<T3> b3;

        public Bound(BoundReader<T1> b1, BoundReader<T2> b2, BoundReader<T3> b3)
        {
            this.b1 = b1;
            this.b2 = b2;
            this.b3 = b3;
        }

        public override void Fill(int start, Span<(T1, T2, T3)> destination)
        {
            T1[] v1 = null;
            T2[] v2 = null;
            T3[] v3 = null;
            try
            {
                v1 = Read(b1, start, destination.Length);
                v2 = Read(b2, start, destination.Length);
                v3 = Read(b3, start, destination.Length);
                for (int i = 0; i < destination.Length; i++)
                {
                    destination[i] = (v1[i], v2[i], v3[i]);
                }
            }
            finally
            {
                Release(v1);
                Release(v2);
                Release(v3);
            }
        }
    }
}

/// <summary>A tuple of four elements (see <see cref="TupleReaderBase{T}"/>).</summary>
/// <typeparam name="T1">The CLR type of field 1.</typeparam>
/// <typeparam name="T2">The CLR type of field 2.</typeparam>
/// <typeparam name="T3">The CLR type of field 3.</typeparam>
/// <typeparam name="T4">The CLR type of field 4.</typeparam>
internal sealed class TupleReader<T1, T2, T3, T4> : TupleReaderBase<(T1, T2, T3, T4)>
{
    private readonly ColumnReader<T1> f1;
    private readonly ColumnReader<T2> f2;
    private readonly ColumnReader<T3> f3;
    private readonly ColumnReader<T4> f4;

    /// <summary>Initializes the reader over the field readers.</summary>
    /// <param name="f1">Reads field 1.</param>
    /// <param name="f2">Reads field 2.</param>
    /// <param name="f3">Reads field 3.</param>
    /// <param name="f4">Reads field 4.</param>
    public TupleReader(ColumnReader<T1> f1, ColumnReader<T2> f2, ColumnReader<T3> f3, ColumnReader<T4> f4)
        : base(f1, f2, f3, f4)
    {
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
        this.f4 = f4;
    }

    /// <inheritdoc/>
    public override BoundReader<(T1, T2, T3, T4)> Bind(IColumn column)
    {
        ITupleColumn tuple = TupleSurface(column, 4);
        return new Bound(f1.Bind(tuple.Children[0]), f2.Bind(tuple.Children[1]), f3.Bind(tuple.Children[2]), f4.Bind(tuple.Children[3]));
    }

    private sealed class Bound : BoundReader<(T1, T2, T3, T4)>
    {
        private readonly BoundReader<T1> b1;
        private readonly BoundReader<T2> b2;
        private readonly BoundReader<T3> b3;
        private readonly BoundReader<T4> b4;

        public Bound(BoundReader<T1> b1, BoundReader<T2> b2, BoundReader<T3> b3, BoundReader<T4> b4)
        {
            this.b1 = b1;
            this.b2 = b2;
            this.b3 = b3;
            this.b4 = b4;
        }

        public override void Fill(int start, Span<(T1, T2, T3, T4)> destination)
        {
            T1[] v1 = null;
            T2[] v2 = null;
            T3[] v3 = null;
            T4[] v4 = null;
            try
            {
                v1 = Read(b1, start, destination.Length);
                v2 = Read(b2, start, destination.Length);
                v3 = Read(b3, start, destination.Length);
                v4 = Read(b4, start, destination.Length);
                for (int i = 0; i < destination.Length; i++)
                {
                    destination[i] = (v1[i], v2[i], v3[i], v4[i]);
                }
            }
            finally
            {
                Release(v1);
                Release(v2);
                Release(v3);
                Release(v4);
            }
        }
    }
}

/// <summary>A tuple of five elements (see <see cref="TupleReaderBase{T}"/>).</summary>
/// <typeparam name="T1">The CLR type of field 1.</typeparam>
/// <typeparam name="T2">The CLR type of field 2.</typeparam>
/// <typeparam name="T3">The CLR type of field 3.</typeparam>
/// <typeparam name="T4">The CLR type of field 4.</typeparam>
/// <typeparam name="T5">The CLR type of field 5.</typeparam>
internal sealed class TupleReader<T1, T2, T3, T4, T5> : TupleReaderBase<(T1, T2, T3, T4, T5)>
{
    private readonly ColumnReader<T1> f1;
    private readonly ColumnReader<T2> f2;
    private readonly ColumnReader<T3> f3;
    private readonly ColumnReader<T4> f4;
    private readonly ColumnReader<T5> f5;

    /// <summary>Initializes the reader over the field readers.</summary>
    /// <param name="f1">Reads field 1.</param>
    /// <param name="f2">Reads field 2.</param>
    /// <param name="f3">Reads field 3.</param>
    /// <param name="f4">Reads field 4.</param>
    /// <param name="f5">Reads field 5.</param>
    public TupleReader(ColumnReader<T1> f1, ColumnReader<T2> f2, ColumnReader<T3> f3, ColumnReader<T4> f4, ColumnReader<T5> f5)
        : base(f1, f2, f3, f4, f5)
    {
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
        this.f4 = f4;
        this.f5 = f5;
    }

    /// <inheritdoc/>
    public override BoundReader<(T1, T2, T3, T4, T5)> Bind(IColumn column)
    {
        ITupleColumn tuple = TupleSurface(column, 5);
        return new Bound(
            f1.Bind(tuple.Children[0]),
            f2.Bind(tuple.Children[1]),
            f3.Bind(tuple.Children[2]),
            f4.Bind(tuple.Children[3]),
            f5.Bind(tuple.Children[4]));
    }

    private sealed class Bound : BoundReader<(T1, T2, T3, T4, T5)>
    {
        private readonly BoundReader<T1> b1;
        private readonly BoundReader<T2> b2;
        private readonly BoundReader<T3> b3;
        private readonly BoundReader<T4> b4;
        private readonly BoundReader<T5> b5;

        public Bound(BoundReader<T1> b1, BoundReader<T2> b2, BoundReader<T3> b3, BoundReader<T4> b4, BoundReader<T5> b5)
        {
            this.b1 = b1;
            this.b2 = b2;
            this.b3 = b3;
            this.b4 = b4;
            this.b5 = b5;
        }

        public override void Fill(int start, Span<(T1, T2, T3, T4, T5)> destination)
        {
            T1[] v1 = null;
            T2[] v2 = null;
            T3[] v3 = null;
            T4[] v4 = null;
            T5[] v5 = null;
            try
            {
                v1 = Read(b1, start, destination.Length);
                v2 = Read(b2, start, destination.Length);
                v3 = Read(b3, start, destination.Length);
                v4 = Read(b4, start, destination.Length);
                v5 = Read(b5, start, destination.Length);
                for (int i = 0; i < destination.Length; i++)
                {
                    destination[i] = (v1[i], v2[i], v3[i], v4[i], v5[i]);
                }
            }
            finally
            {
                Release(v1);
                Release(v2);
                Release(v3);
                Release(v4);
                Release(v5);
            }
        }
    }
}

/// <summary>A tuple of six elements (see <see cref="TupleReaderBase{T}"/>).</summary>
/// <typeparam name="T1">The CLR type of field 1.</typeparam>
/// <typeparam name="T2">The CLR type of field 2.</typeparam>
/// <typeparam name="T3">The CLR type of field 3.</typeparam>
/// <typeparam name="T4">The CLR type of field 4.</typeparam>
/// <typeparam name="T5">The CLR type of field 5.</typeparam>
/// <typeparam name="T6">The CLR type of field 6.</typeparam>
internal sealed class TupleReader<T1, T2, T3, T4, T5, T6> : TupleReaderBase<(T1, T2, T3, T4, T5, T6)>
{
    private readonly ColumnReader<T1> f1;
    private readonly ColumnReader<T2> f2;
    private readonly ColumnReader<T3> f3;
    private readonly ColumnReader<T4> f4;
    private readonly ColumnReader<T5> f5;
    private readonly ColumnReader<T6> f6;

    /// <summary>Initializes the reader over the field readers.</summary>
    /// <param name="f1">Reads field 1.</param>
    /// <param name="f2">Reads field 2.</param>
    /// <param name="f3">Reads field 3.</param>
    /// <param name="f4">Reads field 4.</param>
    /// <param name="f5">Reads field 5.</param>
    /// <param name="f6">Reads field 6.</param>
    public TupleReader(ColumnReader<T1> f1, ColumnReader<T2> f2, ColumnReader<T3> f3, ColumnReader<T4> f4, ColumnReader<T5> f5, ColumnReader<T6> f6)
        : base(f1, f2, f3, f4, f5, f6)
    {
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
        this.f4 = f4;
        this.f5 = f5;
        this.f6 = f6;
    }

    /// <inheritdoc/>
    public override BoundReader<(T1, T2, T3, T4, T5, T6)> Bind(IColumn column)
    {
        ITupleColumn tuple = TupleSurface(column, 6);
        return new Bound(
            f1.Bind(tuple.Children[0]),
            f2.Bind(tuple.Children[1]),
            f3.Bind(tuple.Children[2]),
            f4.Bind(tuple.Children[3]),
            f5.Bind(tuple.Children[4]),
            f6.Bind(tuple.Children[5]));
    }

    private sealed class Bound : BoundReader<(T1, T2, T3, T4, T5, T6)>
    {
        private readonly BoundReader<T1> b1;
        private readonly BoundReader<T2> b2;
        private readonly BoundReader<T3> b3;
        private readonly BoundReader<T4> b4;
        private readonly BoundReader<T5> b5;
        private readonly BoundReader<T6> b6;

        public Bound(BoundReader<T1> b1, BoundReader<T2> b2, BoundReader<T3> b3, BoundReader<T4> b4, BoundReader<T5> b5, BoundReader<T6> b6)
        {
            this.b1 = b1;
            this.b2 = b2;
            this.b3 = b3;
            this.b4 = b4;
            this.b5 = b5;
            this.b6 = b6;
        }

        public override void Fill(int start, Span<(T1, T2, T3, T4, T5, T6)> destination)
        {
            T1[] v1 = null;
            T2[] v2 = null;
            T3[] v3 = null;
            T4[] v4 = null;
            T5[] v5 = null;
            T6[] v6 = null;
            try
            {
                v1 = Read(b1, start, destination.Length);
                v2 = Read(b2, start, destination.Length);
                v3 = Read(b3, start, destination.Length);
                v4 = Read(b4, start, destination.Length);
                v5 = Read(b5, start, destination.Length);
                v6 = Read(b6, start, destination.Length);
                for (int i = 0; i < destination.Length; i++)
                {
                    destination[i] = (v1[i], v2[i], v3[i], v4[i], v5[i], v6[i]);
                }
            }
            finally
            {
                Release(v1);
                Release(v2);
                Release(v3);
                Release(v4);
                Release(v5);
                Release(v6);
            }
        }
    }
}

/// <summary>A tuple of seven elements (see <see cref="TupleReaderBase{T}"/>).</summary>
/// <typeparam name="T1">The CLR type of field 1.</typeparam>
/// <typeparam name="T2">The CLR type of field 2.</typeparam>
/// <typeparam name="T3">The CLR type of field 3.</typeparam>
/// <typeparam name="T4">The CLR type of field 4.</typeparam>
/// <typeparam name="T5">The CLR type of field 5.</typeparam>
/// <typeparam name="T6">The CLR type of field 6.</typeparam>
/// <typeparam name="T7">The CLR type of field 7.</typeparam>
internal sealed class TupleReader<T1, T2, T3, T4, T5, T6, T7> : TupleReaderBase<(T1, T2, T3, T4, T5, T6, T7)>
{
    private readonly ColumnReader<T1> f1;
    private readonly ColumnReader<T2> f2;
    private readonly ColumnReader<T3> f3;
    private readonly ColumnReader<T4> f4;
    private readonly ColumnReader<T5> f5;
    private readonly ColumnReader<T6> f6;
    private readonly ColumnReader<T7> f7;

    /// <summary>Initializes the reader over the field readers.</summary>
    /// <param name="f1">Reads field 1.</param>
    /// <param name="f2">Reads field 2.</param>
    /// <param name="f3">Reads field 3.</param>
    /// <param name="f4">Reads field 4.</param>
    /// <param name="f5">Reads field 5.</param>
    /// <param name="f6">Reads field 6.</param>
    /// <param name="f7">Reads field 7.</param>
    public TupleReader(ColumnReader<T1> f1, ColumnReader<T2> f2, ColumnReader<T3> f3, ColumnReader<T4> f4, ColumnReader<T5> f5, ColumnReader<T6> f6, ColumnReader<T7> f7)
        : base(f1, f2, f3, f4, f5, f6, f7)
    {
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
        this.f4 = f4;
        this.f5 = f5;
        this.f6 = f6;
        this.f7 = f7;
    }

    /// <inheritdoc/>
    public override BoundReader<(T1, T2, T3, T4, T5, T6, T7)> Bind(IColumn column)
    {
        ITupleColumn tuple = TupleSurface(column, 7);
        return new Bound(
            f1.Bind(tuple.Children[0]),
            f2.Bind(tuple.Children[1]),
            f3.Bind(tuple.Children[2]),
            f4.Bind(tuple.Children[3]),
            f5.Bind(tuple.Children[4]),
            f6.Bind(tuple.Children[5]),
            f7.Bind(tuple.Children[6]));
    }

    private sealed class Bound : BoundReader<(T1, T2, T3, T4, T5, T6, T7)>
    {
        private readonly BoundReader<T1> b1;
        private readonly BoundReader<T2> b2;
        private readonly BoundReader<T3> b3;
        private readonly BoundReader<T4> b4;
        private readonly BoundReader<T5> b5;
        private readonly BoundReader<T6> b6;
        private readonly BoundReader<T7> b7;

        public Bound(BoundReader<T1> b1, BoundReader<T2> b2, BoundReader<T3> b3, BoundReader<T4> b4, BoundReader<T5> b5, BoundReader<T6> b6, BoundReader<T7> b7)
        {
            this.b1 = b1;
            this.b2 = b2;
            this.b3 = b3;
            this.b4 = b4;
            this.b5 = b5;
            this.b6 = b6;
            this.b7 = b7;
        }

        public override void Fill(int start, Span<(T1, T2, T3, T4, T5, T6, T7)> destination)
        {
            T1[] v1 = null;
            T2[] v2 = null;
            T3[] v3 = null;
            T4[] v4 = null;
            T5[] v5 = null;
            T6[] v6 = null;
            T7[] v7 = null;
            try
            {
                v1 = Read(b1, start, destination.Length);
                v2 = Read(b2, start, destination.Length);
                v3 = Read(b3, start, destination.Length);
                v4 = Read(b4, start, destination.Length);
                v5 = Read(b5, start, destination.Length);
                v6 = Read(b6, start, destination.Length);
                v7 = Read(b7, start, destination.Length);
                for (int i = 0; i < destination.Length; i++)
                {
                    destination[i] = (v1[i], v2[i], v3[i], v4[i], v5[i], v6[i], v7[i]);
                }
            }
            finally
            {
                Release(v1);
                Release(v2);
                Release(v3);
                Release(v4);
                Release(v5);
                Release(v6);
                Release(v7);
            }
        }
    }
}
