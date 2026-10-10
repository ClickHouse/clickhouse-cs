using System;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>How a LowCardinality writer finds the value of a leaf in one source value, or a NULL.</summary>
/// <typeparam name="TSource">The CLR type of a source value.</typeparam>
/// <typeparam name="T">The CLR type that the leaf writes.</typeparam>
internal interface IDictionaryValue<TSource, T>
{
    /// <summary>Whether the value is a NULL of <c>LowCardinality(Nullable(X))</c>.</summary>
    /// <param name="value">The source value.</param>
    /// <returns>Whether it is a NULL.</returns>
    bool IsNull(TSource value);

    /// <summary>The value for the leaf. Asked only when <see cref="IsNull"/> is false.</summary>
    /// <param name="value">The source value.</param>
    /// <returns>The value for the leaf.</returns>
    T Value(TSource value);
}

/// <summary>The values of <c>LowCardinality(X)</c>: no value is a NULL.</summary>
/// <typeparam name="T">The CLR type that the leaf writes.</typeparam>
internal readonly struct PlainDictionaryValue<T> : IDictionaryValue<T, T>
{
    /// <inheritdoc/>
    public bool IsNull(T value) => false;

    /// <inheritdoc/>
    public T Value(T value) => value;
}

/// <summary>The values of <c>LowCardinality(Nullable(X))</c> from <c>T?</c>.</summary>
/// <typeparam name="T">The value type that the leaf writes.</typeparam>
internal readonly struct LiftedDictionaryValue<T> : IDictionaryValue<T?, T>
    where T : struct
{
    /// <inheritdoc/>
    public bool IsNull(T? value) => !value.HasValue;

    /// <inheritdoc/>
    public T Value(T? value) => value.GetValueOrDefault();
}

/// <summary>The values of <c>LowCardinality(Nullable(X))</c> from a reference type that holds the NULL itself.</summary>
/// <typeparam name="T">The reference type that the leaf writes.</typeparam>
internal readonly struct ReferenceDictionaryValue<T> : IDictionaryValue<T, T>
    where T : class
{
    /// <inheritdoc/>
    public bool IsNull(T value) => value is null;

    /// <inheritdoc/>
    public T Value(T value) => value;
}

/// <summary>
/// <c>LowCardinality(X)</c> and <c>LowCardinality(Nullable(X))</c>: the prefix (the serialization version), then a body
/// with a dictionary of the distinct canonical values of the leaf X and one key for each value.
/// </summary>
/// <remarks>
/// <para>
/// Two values share a dictionary entry exactly when their canonical values are equal, so when they encode to the same
/// bytes. The reserved entries are those of the interners: the placeholder, and for <c>LowCardinality(Nullable(X))</c>
/// the NULL slot before it. A NULL, and a position that the source marks, get key 0: the NULL slot, or the placeholder
/// of a dictionary without NULL.
/// </para>
/// <para>
/// A body of zero values is empty: no dictionary and no keys. The interner is made only for a body with values. The
/// position that a refused value gives to the leaf is the dictionary slot that the value would take, so a refusal names
/// that slot.
/// </para>
/// </remarks>
/// <typeparam name="TSource">The CLR type of a source value.</typeparam>
internal abstract class DictionaryWriter<TSource> : ColumnWriter<TSource>
{
    /// <summary>Initializes the writer.</summary>
    /// <param name="nullable">Whether the dictionary has the NULL slot (<c>LowCardinality(Nullable(X))</c>).</param>
    protected DictionaryWriter(bool nullable) => Nullable = nullable;

    /// <inheritdoc/>
    public sealed override bool HasPrefix => true;

    /// <summary>Whether the dictionary has the NULL slot.</summary>
    protected bool Nullable { get; }

    /// <inheritdoc/>
    public sealed override void WritePrefix(ClickHouseBinaryWriter writer, ValueSource<TSource> values, IColumnWriteState state)
        => writer.WriteInt64(LowCardinalityWire.StatePrefixVersion);

    /// <inheritdoc/>
    public sealed override void Write(ClickHouseBinaryWriter writer, ValueSource<TSource> values, IColumnWriteState state)
    {
        int count = values.Count;
        if (count == 0)
        {
            return;
        }

        int[] keys = WriteBuffers.Rent<int>(count);
        try
        {
            WriteBody(writer, values, keys.AsSpan(0, count));
        }
        finally
        {
            WriteBuffers.Return(keys);
        }
    }

    /// <summary>Writes the header, the dictionary, the value count and the keys.</summary>
    /// <typeparam name="TState">The writer of the entries; a struct, so the call is direct.</typeparam>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="entries">The number of dictionary entries.</param>
    /// <param name="writeEntries">Writes the entries.</param>
    /// <param name="keys">The keys, one for each value.</param>
    protected static void WriteDictionary<TState>(ClickHouseBinaryWriter writer, int entries, TState writeEntries, ReadOnlySpan<int> keys)
        where TState : IDictionaryEntries
    {
        int code = LowCardinalityWire.SelectKeyWidthCode(entries);
        writer.WriteUInt64(LowCardinalityWire.NativeFlags | (ulong)code);
        writer.WriteUInt64((ulong)entries);
        writeEntries.Write(writer);
        writer.WriteUInt64((ulong)keys.Length);
        WriteBuffers.WriteKeys(writer, code, keys);
    }

    /// <summary>Interns the values into <paramref name="keys"/>, then writes the dictionary and the keys.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="values">The values; at least one.</param>
    /// <param name="keys">Receives one key for each value.</param>
    protected abstract void WriteBody(ClickHouseBinaryWriter writer, ValueSource<TSource> values, Span<int> keys);

    /// <summary>Writes the entries of an interner.</summary>
    protected interface IDictionaryEntries
    {
        /// <summary>Writes the entries in key order.</summary>
        /// <param name="writer">The writer to encode into.</param>
        void Write(ClickHouseBinaryWriter writer);
    }
}

/// <summary>
/// A LowCardinality writer over a fixed-width leaf, with a <see cref="ClrKeyedFixedInterner{T, TCanon}"/> over a
/// <see cref="FixedInterner{TCanon}"/>.
/// </summary>
/// <typeparam name="TSource">The CLR type of a source value.</typeparam>
/// <typeparam name="T">The CLR type that the leaf writes.</typeparam>
/// <typeparam name="TCanon">The canonical value of the leaf.</typeparam>
/// <typeparam name="TValue">How a source value gives the leaf value, or a NULL.</typeparam>
internal sealed class FixedDictionaryWriter<TSource, T, TCanon, TValue> : DictionaryWriter<TSource>
    where TCanon : unmanaged, IEquatable<TCanon>
    where TValue : struct, IDictionaryValue<TSource, T>
{
    private readonly FixedLeafWriter<T, TCanon> leaf;

    /// <summary>Initializes the writer.</summary>
    /// <param name="leaf">The leaf of X.</param>
    /// <param name="nullable">Whether the dictionary has the NULL slot.</param>
    public FixedDictionaryWriter(FixedLeafWriter<T, TCanon> leaf, bool nullable)
        : base(nullable)
        => this.leaf = leaf;

    /// <inheritdoc/>
    protected override void WriteBody(ClickHouseBinaryWriter writer, ValueSource<TSource> values, Span<int> keys)
    {
        using var interner = new ClrKeyedFixedInterner<T, TCanon>(leaf, Nullable);
        FixedInterner<TCanon> entries = interner.Entries;

        TValue unwrap = default;
        ReadOnlySpan<byte> absent = values.Absent;
        bool marked = values.HasAbsent;
        int position = 0;
        for (int r = 0; r < values.RunCount; r++)
        {
            foreach (TSource value in values.Run(r))
            {
                if ((marked && absent[position] != 0) || unwrap.IsNull(value))
                {
                    keys[position] = 0;
                }
                else
                {
                    // With no CLR lookup (the leaf has none, or the probe stopped it), the canonical value is interned
                    // here, with no call through the CLR-keyed interner.
                    T leafValue = unwrap.Value(value);
                    keys[position] = interner.UsesClrKeys ? interner.Intern(leafValue) : entries.Intern(leaf.ToCanonical(leafValue, entries.Count));
                }

                position++;
            }
        }

        WriteDictionary(writer, entries.Count, new Entries(entries), keys);
    }

    private readonly struct Entries : IDictionaryEntries
    {
        private readonly FixedInterner<TCanon> interner;

        public Entries(FixedInterner<TCanon> interner) => this.interner = interner;

        public void Write(ClickHouseBinaryWriter writer) => FixedLeafWriter<T, TCanon>.Encode(writer, interner.Entries);
    }
}

/// <summary>
/// A LowCardinality writer over a byte-run leaf (<c>String</c>, <c>FixedString</c>), with a
/// <see cref="ClrKeyedByteInterner{T}"/>: a value is looked up by its CLR value first when the leaf allows it.
/// </summary>
/// <typeparam name="TSource">The CLR type of a source value.</typeparam>
/// <typeparam name="T">The CLR type that the leaf writes.</typeparam>
/// <typeparam name="TValue">How a source value gives the leaf value, or a NULL.</typeparam>
internal sealed class BytesDictionaryWriter<TSource, T, TValue> : DictionaryWriter<TSource>
    where TValue : struct, IDictionaryValue<TSource, T>
{
    private readonly BytesLeafWriter<T> leaf;

    /// <summary>Initializes the writer.</summary>
    /// <param name="leaf">The leaf of X.</param>
    /// <param name="nullable">Whether the dictionary has the NULL slot.</param>
    public BytesDictionaryWriter(BytesLeafWriter<T> leaf, bool nullable)
        : base(nullable)
        => this.leaf = leaf;

    /// <inheritdoc/>
    protected override void WriteBody(ClickHouseBinaryWriter writer, ValueSource<TSource> values, Span<int> keys)
    {
        using var interner = new ClrKeyedByteInterner<T>(leaf, Nullable);
        ByteInterner entries = interner.Entries;
        TValue unwrap = default;
        ReadOnlySpan<byte> absent = values.Absent;
        bool marked = values.HasAbsent;
        int position = 0;
        for (int r = 0; r < values.RunCount; r++)
        {
            foreach (TSource value in values.Run(r))
            {
                keys[position] = (marked && absent[position] != 0) || unwrap.IsNull(value)
                    ? 0
                    : interner.Intern(unwrap.Value(value));
                position++;
            }
        }

        WriteDictionary(writer, entries.Count, new Entries(leaf, entries), keys);
    }

    private readonly struct Entries : IDictionaryEntries
    {
        private readonly BytesLeafWriter<T> leaf;
        private readonly ByteInterner interner;

        public Entries(BytesLeafWriter<T> leaf, ByteInterner interner)
        {
            this.leaf = leaf;
            this.interner = interner;
        }

        public void Write(ClickHouseBinaryWriter writer)
        {
            for (int i = 0; i < interner.Count; i++)
            {
                leaf.Encode(writer, interner.Entry(i));
            }
        }
    }
}
