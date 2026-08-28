using System;
using System.Collections.Concurrent;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>Writes <c>Array(T)</c> rows for one CLR element type.</summary>
internal interface IArrayWriteShape
{
    /// <summary>Whether the inner codec can write each row as one span.</summary>
    bool InnerWritesSpans(IColumnCodec inner);

    /// <summary>Builds slice-relative offsets and validates each row.</summary>
    int[] ComputeOffsets(IColumn column, int start, int length);

    /// <summary>Creates a lazy flattened view over the selected rows.</summary>
    IColumn CreateFlatteningView(string typeName, IColumn column, int start, int[] sliceOffsets, int total);

    /// <summary>Writes each selected row as one span.</summary>
    void WriteRuns(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn column, int start, int length);
}

/// <summary>Caches array write shapes by CLR element type.</summary>
internal static class ArrayWriteShapes
{
    private static readonly ConcurrentDictionary<Type, IArrayWriteShape> Cache = new();

    /// <summary>Returns the cached shape for <paramref name="elementType"/>.</summary>
    public static IArrayWriteShape For(Type elementType) => Cache.GetOrAdd(elementType, Build);

    private static IArrayWriteShape Build(Type elementType)
        => (IArrayWriteShape)Activator.CreateInstance(typeof(ArrayWriteShape<>).MakeGenericType(elementType), nonPublic: true);
}

/// <summary>Writes rows whose CLR type is <typeparamref name="TWrite"/>[].</summary>
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
