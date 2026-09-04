using System;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>The bridge for an inner element type <typeparamref name="T"/>: the low-cardinality column surfaces <c>T</c>.</summary>
/// <typeparam name="T">The inner codec's element type.</typeparam>
internal sealed class LowCardinalityShape<T> : ILowCardinalityShape
{
    /// <inheritdoc/>
    public Type SurfaceElementType => typeof(T);

    /// <inheritdoc/>
    public IColumn Wrap(string name, string typeName, IColumn dictionary, int[] keys, int rowCount, bool pooledKeys)
        => new LowCardinalityColumn<T>(name, typeName, (IColumn<T>)dictionary, keys, rowCount, pooledKeys);

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<T>;

    /// <inheritdoc/>
    public bool CanInnerWrite(IColumnCodec inner) => inner.CanWriteElementType(typeof(T));

    /// <inheritdoc/>
    public void WriteBody(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        // A zero-length slice writes no body at all: only the state prefix (emitted by the block layer, or by a
        // composite's prefix phase) precedes it. Metadata, dictionary, and keys are all absent.
        if (length == 0)
        {
            return;
        }

        if (column is LowCardinalityColumn<T> dense)
        {
            // Dense form (the wire's own layout): the dictionary and keys already exist, so re-emit both directly.
            // The whole dictionary is written even for a slice — unused entries are harmless, and the key width is
            // fixed by the dictionary size, so a slice's keys keep the same encoding.
            IColumn<T> dictionary = dense.Dictionary;
            int dictSize = dictionary.RowCount;
            int code = LowCardinalityWire.SelectKeyWidthCode(dictSize);

            writer.WriteUInt64(LowCardinalityWire.NativeFlags | (ulong)code);
            writer.WriteUInt64((ulong)dictSize);
            inner.WriteColumn(writer, dictionary, 0, dictSize);
            writer.WriteUInt64((ulong)length);

            ReadOnlySpan<int> keys = dense.Keys;
            for (int i = 0; i < length; i++)
            {
                LowCardinalityWire.WriteKey(writer, code, keys[start + i]);
            }

            return;
        }

        WriteErgonomic(inner, writer, (IColumn<T>)column, start, length);
    }

    // Convert through the inner codec first, then deduplicate the values its canonical writer consumes.
    private static void WriteErgonomic(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn<T> source, int start, int length)
    {
        IColumn canonical = inner.ToCanonicalWriteColumn(source);
        if (canonical.ElementType != inner.CanonicalWriteElementType)
        {
            throw new InvalidOperationException(
                $"The '{inner.TypeName}' codec projected {canonical.ElementType}, expected {inner.CanonicalWriteElementType}.");
        }

        CanonicalLowCardinalityWriters.For(canonical.ElementType)
            .Write(inner, writer, canonical, source, nullMap: null, start, length);
    }
}
