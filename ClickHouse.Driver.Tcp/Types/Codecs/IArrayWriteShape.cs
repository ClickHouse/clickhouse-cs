using System;
using System.Collections.Concurrent;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>
/// The generic half of an <c>Array(T)</c> column's ergonomic write path, closed over the CLR type one row's elements
/// arrive in. That type is not the codec's own: an <c>Array(DateTime)</c> column decodes as <c>uint[]</c>, but it can be
/// written from a <c>DateTime[]</c> row, because the inner codec accepts a <see cref="System.DateTime"/> column. So the
/// per-row work — summing offsets, flattening, blitting a run — is parameterized on the row's element type rather than
/// the codec's.
///
/// <para>
/// The dense wire-shaped path needs no shape. A dense column is by definition already in the codec's canonical element
/// type, so it stays on the codec's own type argument.
/// </para>
/// </summary>
internal interface IArrayWriteShape
{
    /// <summary>
    /// Whether the inner codec writes this element type as a flat run of values with no sections of its own, so each
    /// row can go straight to the writer without a flattening view.
    /// </summary>
    /// <param name="inner">The inner codec.</param>
    /// <returns>Whether the inner is span-writable at this element type.</returns>
    bool InnerWritesSpans(IColumnCodec inner);

    /// <summary>
    /// Sums each row's element count into a slice-relative cumulative offsets array (<c>offsets[0] = 0</c>), rejecting
    /// null rows and guarding that the run fits one array.
    /// </summary>
    /// <param name="column">The ergonomic jagged column.</param>
    /// <param name="start">The first row of the slice.</param>
    /// <param name="length">The number of rows in the slice.</param>
    /// <returns>The cumulative element ends, of length <paramref name="length"/> + 1.</returns>
    int[] ComputeOffsets(IColumn column, int start, int length);

    /// <summary>Creates the lazy flattening view a sectioned inner codec is handed instead of a copied buffer.</summary>
    /// <param name="typeName">The inner codec's type name, which the view reports.</param>
    /// <param name="column">The ergonomic jagged column.</param>
    /// <param name="start">The first row the view flattens.</param>
    /// <param name="sliceOffsets">The slice-relative cumulative element ends.</param>
    /// <param name="total">The total element count in the slice.</param>
    /// <returns>The view, as a column of the row's element type.</returns>
    IColumn CreateFlatteningView(string typeName, IColumn column, int start, int[] sliceOffsets, int total);

    /// <summary>Writes each row of the slice as its own contiguous run, for a span-writable inner.</summary>
    /// <param name="inner">The inner codec, span-writable at this element type.</param>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="column">The ergonomic jagged column.</param>
    /// <param name="start">The first row to write.</param>
    /// <param name="length">The number of rows to write.</param>
    void WriteRuns(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn column, int start, int length);
}

/// <summary>Resolves and caches the <see cref="IArrayWriteShape"/> for a row element type.</summary>
internal static class ArrayWriteShapes
{
    private static readonly ConcurrentDictionary<Type, IArrayWriteShape> Cache = new();

    /// <summary>
    /// Returns the shape for <paramref name="elementType"/>, building it once and caching it. Built on demand rather
    /// than one per accepted type up front: a container's accepted set is its children's product, so materializing
    /// them all would be the blowup the interrogative contract exists to avoid. Only the types actually written get a
    /// shape.
    /// </summary>
    /// <param name="elementType">The CLR type one row's elements arrive in.</param>
    /// <returns>The shape.</returns>
    public static IArrayWriteShape For(Type elementType) => Cache.GetOrAdd(elementType, Build);

    // nonPublic: true so the shape's implicit internal constructor is always reachable here.
    private static IArrayWriteShape Build(Type elementType)
        => (IArrayWriteShape)Activator.CreateInstance(typeof(ArrayWriteShape<>).MakeGenericType(elementType), nonPublic: true);
}

/// <summary>The shape for rows whose elements arrive as <typeparamref name="TWrite"/>, so a row is <c>TWrite[]</c>.</summary>
/// <typeparam name="TWrite">The CLR type one row's elements arrive in.</typeparam>
internal sealed class ArrayWriteShape<TWrite> : IArrayWriteShape
{
    /// <inheritdoc/>
    public bool InnerWritesSpans(IColumnCodec inner) => inner is ISpanWritableCodec<TWrite>;

    /// <inheritdoc/>
    public int[] ComputeOffsets(IColumn column, int start, int length)
    {
        var source = (IColumn<TWrite[]>)column;
        var offsets = new int[length + 1];
        ulong total64 = 0;
        for (int i = 0; i < length; i++)
        {
            TWrite[] row = source[start + i];
            if (row is null)
            {
                throw new ArgumentException(
                    $"Array column '{source.Name}' has a null value at row {start + i}; Array(T) rows are non-nullable. Use an empty array for an empty row, or declare the column Array(Nullable(T)) to carry null elements.",
                    "source");
            }

            total64 += (ulong)row.Length;
            if (total64 > (ulong)Array.MaxLength)
            {
                throw new NotSupportedException(
                    $"Array column '{source.Name}' holds more than {Array.MaxLength} elements in one block, exceeding the maximum this client can buffer.");
            }

            offsets[i + 1] = (int)total64;
        }

        return offsets;
    }

    /// <inheritdoc/>
    public IColumn CreateFlatteningView(string typeName, IColumn column, int start, int[] sliceOffsets, int total)
        => new ConcatColumn<TWrite>(typeName, (IColumn<TWrite[]>)column, start, sliceOffsets, total);

    /// <inheritdoc/>
    public void WriteRuns(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        var spanCodec = (ISpanWritableCodec<TWrite>)inner;
        var source = (IColumn<TWrite[]>)column;
        for (int i = 0; i < length; i++)
        {
            spanCodec.WriteValues(writer, source[start + i]);
        }
    }
}
