using System;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// The values that one <see cref="ColumnWriter{T}"/> call writes, in wire order. The values are one
/// span (a top-level column, a flat child) or a list of segments (the arrays of a jagged column, one for each row).
/// The writer reads them as <see cref="RunCount"/> runs and treats the runs as one sequence.
/// </summary>
/// <remarks>
/// <para>
/// A source can also mark positions that have no value (<see cref="Absent"/>). A writer writes its canonical
/// placeholder at such a position and does not read the value there. So a <c>Nullable</c> writer can put the
/// placeholder under each NULL, and a composite can give the same marks to each of its streams.
/// </para>
/// <para>
/// The source borrows its spans, so it is valid only while the caller keeps them alive.
/// </para>
/// </remarks>
/// <typeparam name="T">The CLR type of one value.</typeparam>
internal readonly ref struct ValueSource<T>
{
    private readonly ReadOnlySpan<T> values;
    private readonly ReadOnlySpan<T[]> segments;
    private readonly ReadOnlySpan<byte> absent;

    private ValueSource(ReadOnlySpan<T> values, ReadOnlySpan<T[]> segments, bool isSegmented, int count, int firstRow, ReadOnlySpan<byte> absent, bool hasAbsent, string column)
    {
        this.values = values;
        this.segments = segments;
        this.absent = absent;
        IsSegmented = isSegmented;
        Count = count;
        FirstRow = firstRow;
        HasAbsent = hasAbsent;
        Column = column;
    }

    /// <summary>The number of values in all runs together.</summary>
    public int Count { get; }

    /// <summary>Whether the values are a list of segments rather than one span.</summary>
    public bool IsSegmented { get; }

    /// <summary>
    /// The row of the column that the first value comes from, for error messages. A segmented source has no rows
    /// of its own, so this is 0 for it.
    /// </summary>
    public int FirstRow { get; }

    /// <summary>Whether <see cref="Absent"/> marks positions. When false, every position has a value.</summary>
    public bool HasAbsent { get; }

    /// <summary>
    /// The name of the column that the values come from, for error messages. A composite gives it to the sources of its
    /// children. Null when the caller did not give one.
    /// </summary>
    public string Column { get; }

    /// <summary>
    /// One byte for each position in all runs together, when <see cref="HasAbsent"/> is true. A byte that is not
    /// zero marks a position that has no value. Otherwise empty.
    /// </summary>
    public ReadOnlySpan<byte> Absent => absent;

    /// <summary>The number of runs: 1 for a span, else the number of segments.</summary>
    public int RunCount => IsSegmented ? segments.Length : 1;

    /// <summary>The values of the span. Only for a source that is not segmented.</summary>
    /// <exception cref="InvalidOperationException">The source is segmented.</exception>
    public ReadOnlySpan<T> Span => IsSegmented
        ? throw new InvalidOperationException("A segmented value source has no single span; read it run by run.")
        : values;

    /// <summary>The segments. Only for a segmented source.</summary>
    /// <exception cref="InvalidOperationException">The source is not segmented.</exception>
    public ReadOnlySpan<T[]> Segments => IsSegmented
        ? segments
        : throw new InvalidOperationException("A value source over one span has no segments; read its Span.");

    /// <summary>Makes a source over one span.</summary>
    /// <param name="values">The values.</param>
    /// <param name="firstRow">The row of the column that <c>values[0]</c> comes from, for error messages.</param>
    /// <param name="column">The name of the column, for error messages (<see cref="Column"/>).</param>
    /// <returns>The source.</returns>
    public static ValueSource<T> Of(ReadOnlySpan<T> values, int firstRow = 0, string column = null)
        => new(values, default, isSegmented: false, values.Length, firstRow, default, hasAbsent: false, column);

    /// <summary>Makes a source over a list of segments. A null segment counts as an empty one.</summary>
    /// <param name="segments">The segments, in wire order.</param>
    /// <param name="column">The name of the column, for error messages (<see cref="Column"/>).</param>
    /// <returns>The source.</returns>
    public static ValueSource<T> OfSegments(ReadOnlySpan<T[]> segments, string column = null)
    {
        long count = 0;
        foreach (T[] segment in segments)
        {
            count += segment?.Length ?? 0;
        }

        if (count > int.MaxValue)
        {
            throw new ArgumentException($"The segments hold {count} values; a value source holds at most {int.MaxValue}.", nameof(segments));
        }

        return new(default, segments, isSegmented: true, (int)count, firstRow: 0, default, hasAbsent: false, column);
    }

    /// <summary>Gives a copy of this source whose positions <paramref name="absent"/> marks.</summary>
    /// <param name="absent">One byte for each position. A byte that is not zero marks a position that has no value.</param>
    /// <returns>The marked source.</returns>
    /// <exception cref="ArgumentException"><paramref name="absent"/> does not have one byte for each position.</exception>
    public ValueSource<T> WithAbsent(ReadOnlySpan<byte> absent)
    {
        if (absent.Length != Count)
        {
            throw new ArgumentException($"The marks cover {absent.Length} positions, but the source has {Count} values.", nameof(absent));
        }

        return new(values, segments, IsSegmented, Count, FirstRow, absent, hasAbsent: true, Column);
    }

    /// <summary>One run of values. A source over one span has one run: the span.</summary>
    /// <param name="index">The zero-based run index, less than <see cref="RunCount"/>.</param>
    /// <returns>The values of the run.</returns>
    public ReadOnlySpan<T> Run(int index) => IsSegmented ? segments[index] : index == 0 ? values : throw new ArgumentOutOfRangeException(nameof(index));
}
