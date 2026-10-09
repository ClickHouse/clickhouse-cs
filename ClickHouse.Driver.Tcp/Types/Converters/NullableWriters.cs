using System;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// <c>Nullable(X)</c> from <c>T?</c>: the null map, then the values of X for all positions. The child writes its
/// canonical placeholder at a NULL, because the null map marks that position (<see cref="ValueSource{T}.WithAbsent"/>).
/// </summary>
/// <remarks>
/// A <see cref="Span{T}"/> of <c>T?</c> is not a span of <typeparamref name="T"/>, so <see cref="Begin"/> copies the
/// values into a pooled buffer of <typeparamref name="T"/> for the child. A position that the source marks (a NULL of an
/// enclosing <c>Nullable(Tuple(...))</c>) is NULL here too.
/// </remarks>
/// <typeparam name="T">The value type of X.</typeparam>
internal sealed class NullableValueWriter<T> : ColumnWriter<T?>
    where T : struct
{
    private readonly ColumnWriter<T> inner;

    /// <summary>Initializes the writer.</summary>
    /// <param name="inner">The writer of X.</param>
    public NullableValueWriter(ColumnWriter<T> inner) => this.inner = inner;

    /// <inheritdoc/>
    public override bool HasPrefix => inner.HasPrefix;

    /// <inheritdoc/>
    public override IColumnWriteState Begin(ValueSource<T?> values)
    {
        var state = new State(values.Count);
        try
        {
            Span<T> present = state.Values.AsSpan(0, values.Count);
            Span<byte> nulls = state.Nulls.AsSpan(0, values.Count);
            ReadOnlySpan<byte> absent = values.Absent;
            bool marked = values.HasAbsent;
            int position = 0;
            for (int r = 0; r < values.RunCount; r++)
            {
                foreach (T? value in values.Run(r))
                {
                    bool isNull = !value.HasValue || (marked && absent[position] != 0);
                    nulls[position] = isNull ? (byte)1 : (byte)0;
                    present[position] = isNull ? default : value.GetValueOrDefault();
                    position++;
                }
            }

            state.Inner = inner.Begin(state.Child(values));
            return state;
        }
        catch
        {
            state.Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public override void WritePrefix(ClickHouseBinaryWriter writer, ValueSource<T?> values, IColumnWriteState state)
    {
        var own = (State)state;
        inner.WritePrefix(writer, own.Child(values), own.Inner);
    }

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<T?> values, IColumnWriteState state)
    {
        var own = (State)state;
        writer.WriteBytes(own.Nulls.AsSpan(0, values.Count));
        inner.Write(writer, own.Child(values), own.Inner);
    }

    private sealed class State : IColumnWriteState
    {
        public State(int count)
        {
            Values = WriteBuffers.Rent<T>(count);
            Nulls = WriteBuffers.Rent<byte>(count);
        }

        public T[] Values { get; private set; }

        public byte[] Nulls { get; private set; }

        public IColumnWriteState Inner { get; set; }

        // The child's values keep the rows of the source, so a refused value is named by the same row.
        public ValueSource<T> Child(ValueSource<T?> values)
            => ValueSource<T>.Of(Values.AsSpan(0, values.Count), values.FirstRow, values.Column).WithAbsent(Nulls.AsSpan(0, values.Count));

        public void Dispose()
        {
            Inner?.Dispose();
            Inner = null;
            WriteBuffers.Return(Values);
            WriteBuffers.Return(Nulls);
            Values = null;
            Nulls = null;
        }
    }
}

/// <summary>
/// <c>Nullable(X)</c> from a reference type that holds the NULL itself: the null map, then the child over the same
/// values, with each NULL marked, so the child writes its canonical placeholder there and does not read the value.
/// </summary>
/// <typeparam name="T">The reference type.</typeparam>
internal sealed class NullableReferenceWriter<T> : ColumnWriter<T>
    where T : class
{
    private readonly ColumnWriter<T> inner;

    /// <summary>Initializes the writer.</summary>
    /// <param name="inner">The writer of X.</param>
    public NullableReferenceWriter(ColumnWriter<T> inner) => this.inner = inner;

    /// <inheritdoc/>
    public override bool HasPrefix => inner.HasPrefix;

    /// <inheritdoc/>
    public override IColumnWriteState Begin(ValueSource<T> values)
    {
        var state = new State(values.Count);
        try
        {
            Span<byte> nulls = state.Nulls.AsSpan(0, values.Count);
            ReadOnlySpan<byte> absent = values.Absent;
            bool marked = values.HasAbsent;
            int position = 0;
            for (int r = 0; r < values.RunCount; r++)
            {
                foreach (T value in values.Run(r))
                {
                    nulls[position] = value is null || (marked && absent[position] != 0) ? (byte)1 : (byte)0;
                    position++;
                }
            }

            state.Inner = inner.Begin(state.Child(values));
            return state;
        }
        catch
        {
            state.Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public override void WritePrefix(ClickHouseBinaryWriter writer, ValueSource<T> values, IColumnWriteState state)
    {
        var own = (State)state;
        inner.WritePrefix(writer, own.Child(values), own.Inner);
    }

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<T> values, IColumnWriteState state)
    {
        var own = (State)state;
        writer.WriteBytes(own.Nulls.AsSpan(0, values.Count));
        inner.Write(writer, own.Child(values), own.Inner);
    }

    private sealed class State : IColumnWriteState
    {
        public State(int count) => Nulls = WriteBuffers.Rent<byte>(count);

        public byte[] Nulls { get; private set; }

        public IColumnWriteState Inner { get; set; }

        public ValueSource<T> Child(ValueSource<T> values) => values.WithAbsent(Nulls.AsSpan(0, values.Count));

        public void Dispose()
        {
            Inner?.Dispose();
            Inner = null;
            WriteBuffers.Return(Nulls);
            Nulls = null;
        }
    }
}
