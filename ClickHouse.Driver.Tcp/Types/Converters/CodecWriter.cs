using System;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// A type that is written only from its canonical CLR type, through its codec: <c>Dynamic</c>, <c>QBit</c>, the geo
/// types and <c>Tuple()</c>. <see cref="Begin"/> copies the values into a pooled buffer, gives it to the codec as a
/// column, and keeps the state of the codec, so the codec writes its prefix and its body from one state.
/// </summary>
/// <remarks>
/// <c>Dynamic</c> infers a ClickHouse type for each value (<c>DynamicTypeInference</c>) when the codec begins the write,
/// and writes the list of the inferred types in its prefix. A position that the source marks gets the null
/// placeholder of the codec for <typeparamref name="T"/>.
/// </remarks>
/// <typeparam name="T">The canonical CLR type of the codec.</typeparam>
internal sealed class CodecWriter<T> : ColumnWriter<T>
{
    private readonly IColumnCodec codec;

    /// <summary>Initializes the writer.</summary>
    /// <param name="codec">The codec of the type. It keeps no state of a write between calls.</param>
    public CodecWriter(IColumnCodec codec) => this.codec = codec;

    /// <inheritdoc/>
    // The codec does not say whether it writes a prefix.
    public override bool HasPrefix => true;

    /// <inheritdoc/>
    public override IColumnWriteState Begin(ValueSource<T> values)
    {
        int count = values.Count;
        T[] buffer = WriteBuffers.Rent<T>(count);
        var state = new State(buffer);
        try
        {
            WriteBuffers.CopyTo(values, buffer);
            if (values.HasAbsent)
            {
                var placeholder = (T)codec.NullPlaceholderAs(typeof(T));
                ReadOnlySpan<byte> absent = values.Absent;
                for (int i = 0; i < count; i++)
                {
                    if (absent[i] != 0)
                    {
                        buffer[i] = placeholder;
                    }
                }
            }

            state.Column = ArrayColumn<T>.OverBuffer(values.Column, codec.TypeName, buffer, count);
            state.Codec = codec.BeginWrite(state.Column, 0, count);
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
        codec.WriteStatePrefix(writer, own.Column, 0, values.Count, own.Codec);
    }

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<T> values, IColumnWriteState state)
    {
        var own = (State)state;
        codec.WriteColumn(writer, own.Column, 0, values.Count, own.Codec);
    }

    private sealed class State : IColumnWriteState
    {
        private T[] buffer;

        public State(T[] buffer) => this.buffer = buffer;

        public ArrayColumn<T> Column { get; set; }

        public IColumnWriteState Codec { get; set; }

        public void Dispose()
        {
            Codec?.Dispose();
            Codec = null;
            WriteBuffers.Return(buffer);
            buffer = null;
        }
    }
}
