using System;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Codecs;

/// <summary>The shape for a reference-type inner: the nullable column surfaces the nullable reference.</summary>
/// <typeparam name="T">The inner reference type.</typeparam>
internal sealed class ReferenceNullableShape<T> : INullableShape
    where T : class
{
    /// <inheritdoc/>
    public Type NullableElementType => typeof(T);

    /// <inheritdoc/>
    public IColumn Wrap(string name, string typeName, IColumn inner, byte[] nullMap, int rowCount, bool pooledMap)
        => new NullableReferenceColumn<T>(name, typeName, (IColumn<T>)inner, nullMap, rowCount, pooledMap);

    /// <inheritdoc/>
    public bool CanWrite(IColumn column) => column is IColumn<T>;

    /// <inheritdoc/>
    public bool CanInnerWrite(IColumnCodec inner) => inner.CanWrite(new ArrayColumn<T>(string.Empty, inner.TypeName, Array.Empty<T>()));

    /// <inheritdoc/>
    // A dense column (inner column + null-map) writes both directly with no copy. The ergonomic nullable-reference
    // column writes the null-map from each row's nullness, then hands the inner codec a substitute view that reads
    // the present reference or the inner placeholder per row — no flat placeholder buffer is built; the null
    // positions the wire ignores are still written from the placeholder so the inner codec never sees a null.
    public void WriteBody(IColumnCodec inner, ClickHouseBinaryWriter writer, IColumn column, int start, int length)
    {
        if (column is NullableReferenceColumn<T> dense)
        {
            writer.WriteBytes(dense.NullMap.Slice(start, length));
            inner.WriteColumn(writer, dense.Inner, start, length);
            return;
        }

        var source = (IColumn<T>)column;
        for (int i = 0; i < length; i++)
        {
            writer.WriteBool(source[start + i] is null);
        }

        var placeholder = (T)inner.NullPlaceholderAs(typeof(T));
        inner.WriteColumn(writer, new SubstituteReferenceColumn<T>(inner.TypeName, source, placeholder), start, length);
    }
}
