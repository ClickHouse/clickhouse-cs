using System;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// <c>Array(X)</c> from <c>T[]</c>: the offsets, then the child over the elements of all the rows. The child takes the
/// rows' arrays as its segments, so the elements are not copied into one flat buffer.
/// </summary>
/// <remarks>
/// The arrays of a source over one span already are the list of segments. A segmented source (the rows of an
/// <c>Array(Array(X))</c>) and a source with marked positions are copied into a pooled list of segments, in which a
/// marked row is empty.
/// </remarks>
/// <typeparam name="T">The CLR type of one element.</typeparam>
internal sealed class ArrayWriter<T> : ColumnWriter<T[]>, IArrayWriter
{
    private readonly ColumnWriter<T> inner;

    /// <summary>Initializes the writer.</summary>
    /// <param name="inner">The writer of the elements.</param>
    public ArrayWriter(ColumnWriter<T> inner) => this.inner = inner;

    /// <inheritdoc/>
    ColumnWriter IArrayWriter.Elements => inner;

    /// <inheritdoc/>
    public override bool HasPrefix => inner.HasPrefix;

    /// <inheritdoc/>
    /// <exception cref="ArgumentException">A row that the source does not mark is null.</exception>
    /// <exception cref="NotSupportedException">The rows hold more elements than one buffer can address.</exception>
    public override IColumnWriteState Begin(ValueSource<T[]> values)
    {
        ReadOnlySpan<byte> absent = values.Absent;
        bool marked = values.HasAbsent;
        long total = 0;
        int position = 0;
        for (int r = 0; r < values.RunCount; r++)
        {
            foreach (T[] row in values.Run(r))
            {
                if (!(marked && absent[position] != 0))
                {
                    if (row is null)
                    {
                        throw NullRow(values, position);
                    }

                    total += row.Length;
                    if (total > Array.MaxLength)
                    {
                        throw new NotSupportedException(
                            $"Array column '{values.Column}' holds more than {Array.MaxLength} elements in one block, exceeding the maximum this client can buffer.");
                    }
                }

                position++;
            }
        }

        var state = new State();
        try
        {
            if (values.IsSegmented || marked)
            {
                state.Rows = WriteBuffers.Rent<T[]>(values.Count);
                position = 0;
                for (int r = 0; r < values.RunCount; r++)
                {
                    foreach (T[] row in values.Run(r))
                    {
                        state.Rows[position] = marked && absent[position] != 0 ? null : row;
                        position++;
                    }
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
    public override void WritePrefix(ClickHouseBinaryWriter writer, ValueSource<T[]> values, IColumnWriteState state)
    {
        var own = (State)state;
        inner.WritePrefix(writer, own.Child(values), own.Inner);
    }

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<T[]> values, IColumnWriteState state)
    {
        var own = (State)state;
        ReadOnlySpan<T[]> rows = own.Rows is null ? values.Span : own.Rows.AsSpan(0, values.Count);
        ulong offset = 0;
        foreach (T[] row in rows)
        {
            offset += (ulong)(row?.Length ?? 0);
            writer.WriteUInt64(offset);
        }

        inner.Write(writer, own.Child(values), own.Inner);
    }

    // The parameter name is the one that the Array codec reports for the same row.
#pragma warning disable CA2208 // Instantiate argument exceptions correctly
    private static ArgumentException NullRow(ValueSource<T[]> values, int position)
        => new(
            $"Array column '{values.Column}' has a null value at row {values.FirstRow + position}; Array(T) rows are non-nullable. Use an empty array for an empty row, or declare the column Array(Nullable(T)) to carry null elements.",
            "column");
#pragma warning restore CA2208

    private sealed class State : IColumnWriteState
    {
        // The rows as segments, when the source's own span cannot serve: a segmented or a marked source. Else null.
        public T[][] Rows { get; set; }

        public IColumnWriteState Inner { get; set; }

        public ValueSource<T> Child(ValueSource<T[]> values)
            => ValueSource<T>.OfSegments(Rows is null ? values.Span : Rows.AsSpan(0, values.Count), values.Column);

        public void Dispose()
        {
            Inner?.Dispose();
            Inner = null;
            WriteBuffers.Return(Rows);
            Rows = null;
        }
    }
}

/// <summary>An <c>Array(X)</c> writer.</summary>
internal interface IArrayWriter
{
    /// <summary>The writer of the elements.</summary>
    ColumnWriter Elements { get; }
}
