using System;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// A converter tree that writes values of one CLR type as one ClickHouse type. It converts each value to the
/// canonical value of its leaf just before it encodes it. A tree holds no per-write state, so one tree serves every
/// write and every thread. <see cref="ConverterDerivation"/> builds and caches the trees.
/// </summary>
/// <remarks>
/// A write has three phases, as for a codec: <see cref="ColumnWriter{T}.Begin"/>, then
/// <see cref="ColumnWriter{T}.WritePrefix"/>, then <see cref="ColumnWriter{T}.Write"/>, each given the same values.
/// For each column, the block layer writes the prefix before the body, and a composite writes the prefixes of its
/// children before its own body.
/// </remarks>
internal abstract class ColumnWriter
{
    /// <summary>The CLR type of one value that the tree writes.</summary>
    public abstract Type ValueType { get; }

    /// <summary>
    /// Whether <see cref="ColumnWriter{T}.WritePrefix"/> writes bytes. A composite can ask its children, and skip
    /// the prefix phase when no child has a prefix.
    /// </summary>
    public virtual bool HasPrefix => false;

    /// <summary>
    /// Whether the body is the encoding of each value, one after the other, with no part that covers the whole write.
    /// Then two writes, one after the other, give the body of one write of all their values, and they share the
    /// prefix. A leaf is flat. A composite is not: it writes each of its streams across all the values.
    /// </summary>
    public virtual bool IsFlat => false;
}

/// <summary>A converter tree that writes values of <typeparamref name="T"/> as one ClickHouse type.</summary>
/// <typeparam name="T">The CLR type of one value.</typeparam>
internal abstract class ColumnWriter<T> : ColumnWriter
{
    /// <inheritdoc/>
    public sealed override Type ValueType => typeof(T);

    /// <summary>
    /// Computes the state that the prefix and the body share, for a writer whose prefix depends on the values. The
    /// caller disposes the state after <see cref="Write"/>.
    /// </summary>
    /// <param name="values">The values that the prefix and the body will write.</param>
    /// <returns>The state, or <see langword="null"/> when the writer needs none.</returns>
    public virtual IColumnWriteState Begin(ValueSource<T> values) => null;

    /// <summary>Writes the serialization prefix. The default writes nothing.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="values">The values that the body will write.</param>
    /// <param name="state">The state from <see cref="Begin"/>.</param>
    public virtual void WritePrefix(ClickHouseBinaryWriter writer, ValueSource<T> values, IColumnWriteState state)
    {
    }

    /// <summary>Writes the body: every value of <paramref name="values"/>, in order.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="values">The values to write.</param>
    /// <param name="state">The state from <see cref="Begin"/>.</param>
    /// <exception cref="ArgumentException">A value cannot be stored in the ClickHouse type.</exception>
    public abstract void Write(ClickHouseBinaryWriter writer, ValueSource<T> values, IColumnWriteState state);
}
