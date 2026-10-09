using System;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// <c>Tuple(...)</c> from a <see cref="ValueTuple"/>: one writer for each field. The prefixes of the fields, in field
/// order, then the bodies of the fields, in field order. <see cref="Begin"/> copies each field of all the values into a
/// pooled buffer, so the prefix and the body of a field read the same values.
/// </summary>
/// <remarks>
/// Each field keeps the rows and the marks of the source: a position that the source marks (a NULL of
/// <c>Nullable(Tuple(...))</c>) is marked in every field, so each field writes its canonical placeholder there.
/// </remarks>
/// <typeparam name="T">The <see cref="ValueTuple"/> type.</typeparam>
internal abstract class TupleWriterBase<T> : ColumnWriter<T>
    where T : struct
{
    private readonly TupleField[] fields;

    /// <summary>Initializes the writer.</summary>
    /// <param name="fields">One field for each element of the tuple, in order.</param>
    protected TupleWriterBase(params TupleField[] fields) => this.fields = fields;

    /// <inheritdoc/>
    public sealed override bool HasPrefix => Array.Exists(fields, static f => f.HasPrefix);

    /// <inheritdoc/>
    public sealed override IColumnWriteState Begin(ValueSource<T> values)
    {
        var state = new State(fields);
        try
        {
            for (int i = 0; i < fields.Length; i++)
            {
                state.Buffers[i] = fields[i].Rent(values.Count);
            }

            int position = 0;
            for (int r = 0; r < values.RunCount; r++)
            {
                ReadOnlySpan<T> run = values.Run(r);
                Gather(run, state.Buffers, position);
                position += run.Length;
            }

            for (int i = 0; i < fields.Length; i++)
            {
                state.States[i] = fields[i].Begin(state.Buffers[i], Shape(values));
            }

            return state;
        }
        catch
        {
            state.Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public sealed override void WritePrefix(ClickHouseBinaryWriter writer, ValueSource<T> values, IColumnWriteState state)
    {
        var own = (State)state;
        for (int i = 0; i < fields.Length; i++)
        {
            fields[i].WritePrefix(writer, own.Buffers[i], Shape(values), own.States[i]);
        }
    }

    /// <inheritdoc/>
    public sealed override void Write(ClickHouseBinaryWriter writer, ValueSource<T> values, IColumnWriteState state)
    {
        var own = (State)state;
        for (int i = 0; i < fields.Length; i++)
        {
            fields[i].Write(writer, own.Buffers[i], Shape(values), own.States[i]);
        }
    }

    /// <summary>Copies each field of <paramref name="run"/> into its buffer, from position <paramref name="at"/>.</summary>
    /// <param name="run">The tuples.</param>
    /// <param name="buffers">One buffer for each field (<see cref="TupleField.Rent"/>).</param>
    /// <param name="at">The position of <c>run[0]</c> in the write.</param>
    protected abstract void Gather(ReadOnlySpan<T> run, Array[] buffers, int at);

    private static TupleFieldShape Shape(ValueSource<T> values)
        => new(values.Count, values.FirstRow, values.Column, values.Absent, values.HasAbsent);

    private sealed class State : IColumnWriteState
    {
        private readonly TupleField[] fields;

        public State(TupleField[] fields)
        {
            this.fields = fields;
            Buffers = new Array[fields.Length];
            States = new IColumnWriteState[fields.Length];
        }

        public Array[] Buffers { get; }

        public IColumnWriteState[] States { get; }

        public void Dispose()
        {
            for (int i = 0; i < fields.Length; i++)
            {
                States[i]?.Dispose();
                States[i] = null;
                if (Buffers[i] is not null)
                {
                    fields[i].Return(Buffers[i]);
                    Buffers[i] = null;
                }
            }
        }
    }
}

/// <summary>The count, rows, column name and marks that every field of one tuple write shares.</summary>
internal readonly ref struct TupleFieldShape
{
    /// <summary>Initializes the shape.</summary>
    /// <param name="count">The number of values.</param>
    /// <param name="firstRow">The row of the first value (<see cref="ValueSource{T}.FirstRow"/>).</param>
    /// <param name="column">The column name (<see cref="ValueSource{T}.Column"/>).</param>
    /// <param name="absent">The marks (<see cref="ValueSource{T}.Absent"/>).</param>
    /// <param name="marked">Whether the source has marks.</param>
    public TupleFieldShape(int count, int firstRow, string column, ReadOnlySpan<byte> absent, bool marked)
    {
        Count = count;
        FirstRow = firstRow;
        Column = column;
        Absent = absent;
        Marked = marked;
    }

    /// <summary>The number of values.</summary>
    public int Count { get; }

    /// <summary>The row of the first value.</summary>
    public int FirstRow { get; }

    /// <summary>The column name.</summary>
    public string Column { get; }

    /// <summary>The marks, when <see cref="Marked"/> is true.</summary>
    public ReadOnlySpan<byte> Absent { get; }

    /// <summary>Whether the source has marks.</summary>
    public bool Marked { get; }
}

/// <summary>One field of a tuple writer: its writer and the operations on its buffer, which hold its CLR type.</summary>
internal abstract class TupleField
{
    /// <summary>Whether the writer of the field has a prefix.</summary>
    public abstract bool HasPrefix { get; }

    /// <summary>Rents a buffer for <paramref name="count"/> values of the field.</summary>
    /// <param name="count">The number of values.</param>
    /// <returns>The buffer.</returns>
    public abstract Array Rent(int count);

    /// <summary>Returns a buffer from <see cref="Rent"/>.</summary>
    /// <param name="buffer">The buffer.</param>
    public abstract void Return(Array buffer);

    /// <summary>Begins the write of the field.</summary>
    /// <param name="buffer">The values of the field.</param>
    /// <param name="shape">The shape of the write.</param>
    /// <returns>The state of the field's writer.</returns>
    public abstract IColumnWriteState Begin(Array buffer, TupleFieldShape shape);

    /// <summary>Writes the prefix of the field.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="buffer">The values of the field.</param>
    /// <param name="shape">The shape of the write.</param>
    /// <param name="state">The state from <see cref="Begin"/>.</param>
    public abstract void WritePrefix(ClickHouseBinaryWriter writer, Array buffer, TupleFieldShape shape, IColumnWriteState state);

    /// <summary>Writes the body of the field.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="buffer">The values of the field.</param>
    /// <param name="shape">The shape of the write.</param>
    /// <param name="state">The state from <see cref="Begin"/>.</param>
    public abstract void Write(ClickHouseBinaryWriter writer, Array buffer, TupleFieldShape shape, IColumnWriteState state);
}

/// <summary>A field of <typeparamref name="TField"/>.</summary>
/// <typeparam name="TField">The CLR type of the field.</typeparam>
internal sealed class TupleField<TField> : TupleField
{
    private readonly ColumnWriter<TField> writer;

    /// <summary>Initializes the field.</summary>
    /// <param name="writer">The writer of the field.</param>
    public TupleField(ColumnWriter<TField> writer) => this.writer = writer;

    /// <inheritdoc/>
    public override bool HasPrefix => writer.HasPrefix;

    /// <inheritdoc/>
    public override Array Rent(int count) => WriteBuffers.Rent<TField>(count);

    /// <inheritdoc/>
    public override void Return(Array buffer) => WriteBuffers.Return((TField[])buffer);

    /// <inheritdoc/>
    public override IColumnWriteState Begin(Array buffer, TupleFieldShape shape) => writer.Begin(Source(buffer, shape));

    /// <inheritdoc/>
    public override void WritePrefix(ClickHouseBinaryWriter output, Array buffer, TupleFieldShape shape, IColumnWriteState state)
        => writer.WritePrefix(output, Source(buffer, shape), state);

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter output, Array buffer, TupleFieldShape shape, IColumnWriteState state)
        => writer.Write(output, Source(buffer, shape), state);

    private static ValueSource<TField> Source(Array buffer, TupleFieldShape shape)
    {
        var source = ValueSource<TField>.Of(((TField[])buffer).AsSpan(0, shape.Count), shape.FirstRow, shape.Column);
        return shape.Marked ? source.WithAbsent(shape.Absent) : source;
    }
}

/// <summary>A tuple writer of 1 field(s).</summary>
/// <typeparam name="T1">The CLR type of field 1.</typeparam>
internal sealed class TupleWriter<T1> : TupleWriterBase<ValueTuple<T1>>
{
    /// <summary>Initializes the writer.</summary>
    /// <param name="field1">The writer of field 1.</param>
    public TupleWriter(ColumnWriter<T1> field1)
        : base(new TupleField<T1>(field1))
    {
    }

    /// <inheritdoc/>
    protected override void Gather(ReadOnlySpan<ValueTuple<T1>> run, Array[] buffers, int at)
    {
        Span<T1> b1 = ((T1[])buffers[0]).AsSpan(at, run.Length);
        for (int i = 0; i < run.Length; i++)
        {
            b1[i] = run[i].Item1;
        }
    }
}

/// <summary>A tuple writer of 2 field(s).</summary>
/// <typeparam name="T1">The CLR type of field 1.</typeparam>
/// <typeparam name="T2">The CLR type of field 2.</typeparam>
internal sealed class TupleWriter<T1, T2> : TupleWriterBase<ValueTuple<T1, T2>>
{
    /// <summary>Initializes the writer.</summary>
    /// <param name="field1">The writer of field 1.</param>
    /// <param name="field2">The writer of field 2.</param>
    public TupleWriter(ColumnWriter<T1> field1, ColumnWriter<T2> field2)
        : base(new TupleField<T1>(field1), new TupleField<T2>(field2))
    {
    }

    /// <inheritdoc/>
    protected override void Gather(ReadOnlySpan<ValueTuple<T1, T2>> run, Array[] buffers, int at)
    {
        Span<T1> b1 = ((T1[])buffers[0]).AsSpan(at, run.Length);
        Span<T2> b2 = ((T2[])buffers[1]).AsSpan(at, run.Length);
        for (int i = 0; i < run.Length; i++)
        {
            b1[i] = run[i].Item1;
            b2[i] = run[i].Item2;
        }
    }
}

/// <summary>A tuple writer of 3 field(s).</summary>
/// <typeparam name="T1">The CLR type of field 1.</typeparam>
/// <typeparam name="T2">The CLR type of field 2.</typeparam>
/// <typeparam name="T3">The CLR type of field 3.</typeparam>
internal sealed class TupleWriter<T1, T2, T3> : TupleWriterBase<ValueTuple<T1, T2, T3>>
{
    /// <summary>Initializes the writer.</summary>
    /// <param name="field1">The writer of field 1.</param>
    /// <param name="field2">The writer of field 2.</param>
    /// <param name="field3">The writer of field 3.</param>
    public TupleWriter(ColumnWriter<T1> field1, ColumnWriter<T2> field2, ColumnWriter<T3> field3)
        : base(new TupleField<T1>(field1), new TupleField<T2>(field2), new TupleField<T3>(field3))
    {
    }

    /// <inheritdoc/>
    protected override void Gather(ReadOnlySpan<ValueTuple<T1, T2, T3>> run, Array[] buffers, int at)
    {
        Span<T1> b1 = ((T1[])buffers[0]).AsSpan(at, run.Length);
        Span<T2> b2 = ((T2[])buffers[1]).AsSpan(at, run.Length);
        Span<T3> b3 = ((T3[])buffers[2]).AsSpan(at, run.Length);
        for (int i = 0; i < run.Length; i++)
        {
            b1[i] = run[i].Item1;
            b2[i] = run[i].Item2;
            b3[i] = run[i].Item3;
        }
    }
}

/// <summary>A tuple writer of 4 field(s).</summary>
/// <typeparam name="T1">The CLR type of field 1.</typeparam>
/// <typeparam name="T2">The CLR type of field 2.</typeparam>
/// <typeparam name="T3">The CLR type of field 3.</typeparam>
/// <typeparam name="T4">The CLR type of field 4.</typeparam>
internal sealed class TupleWriter<T1, T2, T3, T4> : TupleWriterBase<ValueTuple<T1, T2, T3, T4>>
{
    /// <summary>Initializes the writer.</summary>
    /// <param name="field1">The writer of field 1.</param>
    /// <param name="field2">The writer of field 2.</param>
    /// <param name="field3">The writer of field 3.</param>
    /// <param name="field4">The writer of field 4.</param>
    public TupleWriter(ColumnWriter<T1> field1, ColumnWriter<T2> field2, ColumnWriter<T3> field3, ColumnWriter<T4> field4)
        : base(new TupleField<T1>(field1), new TupleField<T2>(field2), new TupleField<T3>(field3), new TupleField<T4>(field4))
    {
    }

    /// <inheritdoc/>
    protected override void Gather(ReadOnlySpan<ValueTuple<T1, T2, T3, T4>> run, Array[] buffers, int at)
    {
        Span<T1> b1 = ((T1[])buffers[0]).AsSpan(at, run.Length);
        Span<T2> b2 = ((T2[])buffers[1]).AsSpan(at, run.Length);
        Span<T3> b3 = ((T3[])buffers[2]).AsSpan(at, run.Length);
        Span<T4> b4 = ((T4[])buffers[3]).AsSpan(at, run.Length);
        for (int i = 0; i < run.Length; i++)
        {
            b1[i] = run[i].Item1;
            b2[i] = run[i].Item2;
            b3[i] = run[i].Item3;
            b4[i] = run[i].Item4;
        }
    }
}

/// <summary>A tuple writer of 5 field(s).</summary>
/// <typeparam name="T1">The CLR type of field 1.</typeparam>
/// <typeparam name="T2">The CLR type of field 2.</typeparam>
/// <typeparam name="T3">The CLR type of field 3.</typeparam>
/// <typeparam name="T4">The CLR type of field 4.</typeparam>
/// <typeparam name="T5">The CLR type of field 5.</typeparam>
internal sealed class TupleWriter<T1, T2, T3, T4, T5> : TupleWriterBase<ValueTuple<T1, T2, T3, T4, T5>>
{
    /// <summary>Initializes the writer.</summary>
    /// <param name="field1">The writer of field 1.</param>
    /// <param name="field2">The writer of field 2.</param>
    /// <param name="field3">The writer of field 3.</param>
    /// <param name="field4">The writer of field 4.</param>
    /// <param name="field5">The writer of field 5.</param>
    public TupleWriter(ColumnWriter<T1> field1, ColumnWriter<T2> field2, ColumnWriter<T3> field3, ColumnWriter<T4> field4, ColumnWriter<T5> field5)
        : base(new TupleField<T1>(field1), new TupleField<T2>(field2), new TupleField<T3>(field3), new TupleField<T4>(field4), new TupleField<T5>(field5))
    {
    }

    /// <inheritdoc/>
    protected override void Gather(ReadOnlySpan<ValueTuple<T1, T2, T3, T4, T5>> run, Array[] buffers, int at)
    {
        Span<T1> b1 = ((T1[])buffers[0]).AsSpan(at, run.Length);
        Span<T2> b2 = ((T2[])buffers[1]).AsSpan(at, run.Length);
        Span<T3> b3 = ((T3[])buffers[2]).AsSpan(at, run.Length);
        Span<T4> b4 = ((T4[])buffers[3]).AsSpan(at, run.Length);
        Span<T5> b5 = ((T5[])buffers[4]).AsSpan(at, run.Length);
        for (int i = 0; i < run.Length; i++)
        {
            b1[i] = run[i].Item1;
            b2[i] = run[i].Item2;
            b3[i] = run[i].Item3;
            b4[i] = run[i].Item4;
            b5[i] = run[i].Item5;
        }
    }
}

/// <summary>A tuple writer of 6 field(s).</summary>
/// <typeparam name="T1">The CLR type of field 1.</typeparam>
/// <typeparam name="T2">The CLR type of field 2.</typeparam>
/// <typeparam name="T3">The CLR type of field 3.</typeparam>
/// <typeparam name="T4">The CLR type of field 4.</typeparam>
/// <typeparam name="T5">The CLR type of field 5.</typeparam>
/// <typeparam name="T6">The CLR type of field 6.</typeparam>
internal sealed class TupleWriter<T1, T2, T3, T4, T5, T6> : TupleWriterBase<ValueTuple<T1, T2, T3, T4, T5, T6>>
{
    /// <summary>Initializes the writer.</summary>
    /// <param name="field1">The writer of field 1.</param>
    /// <param name="field2">The writer of field 2.</param>
    /// <param name="field3">The writer of field 3.</param>
    /// <param name="field4">The writer of field 4.</param>
    /// <param name="field5">The writer of field 5.</param>
    /// <param name="field6">The writer of field 6.</param>
    public TupleWriter(ColumnWriter<T1> field1, ColumnWriter<T2> field2, ColumnWriter<T3> field3, ColumnWriter<T4> field4, ColumnWriter<T5> field5, ColumnWriter<T6> field6)
        : base(new TupleField<T1>(field1), new TupleField<T2>(field2), new TupleField<T3>(field3), new TupleField<T4>(field4), new TupleField<T5>(field5), new TupleField<T6>(field6))
    {
    }

    /// <inheritdoc/>
    protected override void Gather(ReadOnlySpan<ValueTuple<T1, T2, T3, T4, T5, T6>> run, Array[] buffers, int at)
    {
        Span<T1> b1 = ((T1[])buffers[0]).AsSpan(at, run.Length);
        Span<T2> b2 = ((T2[])buffers[1]).AsSpan(at, run.Length);
        Span<T3> b3 = ((T3[])buffers[2]).AsSpan(at, run.Length);
        Span<T4> b4 = ((T4[])buffers[3]).AsSpan(at, run.Length);
        Span<T5> b5 = ((T5[])buffers[4]).AsSpan(at, run.Length);
        Span<T6> b6 = ((T6[])buffers[5]).AsSpan(at, run.Length);
        for (int i = 0; i < run.Length; i++)
        {
            b1[i] = run[i].Item1;
            b2[i] = run[i].Item2;
            b3[i] = run[i].Item3;
            b4[i] = run[i].Item4;
            b5[i] = run[i].Item5;
            b6[i] = run[i].Item6;
        }
    }
}

/// <summary>A tuple writer of 7 field(s).</summary>
/// <typeparam name="T1">The CLR type of field 1.</typeparam>
/// <typeparam name="T2">The CLR type of field 2.</typeparam>
/// <typeparam name="T3">The CLR type of field 3.</typeparam>
/// <typeparam name="T4">The CLR type of field 4.</typeparam>
/// <typeparam name="T5">The CLR type of field 5.</typeparam>
/// <typeparam name="T6">The CLR type of field 6.</typeparam>
/// <typeparam name="T7">The CLR type of field 7.</typeparam>
internal sealed class TupleWriter<T1, T2, T3, T4, T5, T6, T7> : TupleWriterBase<ValueTuple<T1, T2, T3, T4, T5, T6, T7>>
{
    /// <summary>Initializes the writer.</summary>
    /// <param name="field1">The writer of field 1.</param>
    /// <param name="field2">The writer of field 2.</param>
    /// <param name="field3">The writer of field 3.</param>
    /// <param name="field4">The writer of field 4.</param>
    /// <param name="field5">The writer of field 5.</param>
    /// <param name="field6">The writer of field 6.</param>
    /// <param name="field7">The writer of field 7.</param>
    public TupleWriter(ColumnWriter<T1> field1, ColumnWriter<T2> field2, ColumnWriter<T3> field3, ColumnWriter<T4> field4, ColumnWriter<T5> field5, ColumnWriter<T6> field6, ColumnWriter<T7> field7)
        : base(new TupleField<T1>(field1), new TupleField<T2>(field2), new TupleField<T3>(field3), new TupleField<T4>(field4), new TupleField<T5>(field5), new TupleField<T6>(field6), new TupleField<T7>(field7))
    {
    }

    /// <inheritdoc/>
    protected override void Gather(ReadOnlySpan<ValueTuple<T1, T2, T3, T4, T5, T6, T7>> run, Array[] buffers, int at)
    {
        Span<T1> b1 = ((T1[])buffers[0]).AsSpan(at, run.Length);
        Span<T2> b2 = ((T2[])buffers[1]).AsSpan(at, run.Length);
        Span<T3> b3 = ((T3[])buffers[2]).AsSpan(at, run.Length);
        Span<T4> b4 = ((T4[])buffers[3]).AsSpan(at, run.Length);
        Span<T5> b5 = ((T5[])buffers[4]).AsSpan(at, run.Length);
        Span<T6> b6 = ((T6[])buffers[5]).AsSpan(at, run.Length);
        Span<T7> b7 = ((T7[])buffers[6]).AsSpan(at, run.Length);
        for (int i = 0; i < run.Length; i++)
        {
            b1[i] = run[i].Item1;
            b2[i] = run[i].Item2;
            b3[i] = run[i].Item3;
            b4[i] = run[i].Item4;
            b5[i] = run[i].Item5;
            b6[i] = run[i].Item6;
            b7[i] = run[i].Item7;
        }
    }
}
