using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// <c>Map(K, V)</c> from <c>KeyValuePair&lt;TKey, TValue&gt;[]</c>: the prefixes of the key and of the value, then the
/// offsets, the keys of all the rows and the values of all the rows. <see cref="Begin"/> copies the keys and the values
/// into pooled buffers, so the prefixes and the bodies of both children read the same values.
/// </summary>
/// <remarks>A row that the source marks is empty.</remarks>
/// <typeparam name="TKey">The CLR type of a key.</typeparam>
/// <typeparam name="TValue">The CLR type of a value.</typeparam>
internal sealed class MapWriter<TKey, TValue> : ColumnWriter<KeyValuePair<TKey, TValue>[]>
{
    private readonly ColumnWriter<TKey> keys;
    private readonly ColumnWriter<TValue> values;

    /// <summary>Initializes the writer.</summary>
    /// <param name="keys">The writer of the keys.</param>
    /// <param name="values">The writer of the values.</param>
    public MapWriter(ColumnWriter<TKey> keys, ColumnWriter<TValue> values)
    {
        this.keys = keys;
        this.values = values;
    }

    /// <inheritdoc/>
    public override bool HasPrefix => keys.HasPrefix || values.HasPrefix;

    /// <inheritdoc/>
    /// <exception cref="ArgumentException">A row that the source does not mark is null.</exception>
    /// <exception cref="NotSupportedException">The rows hold more pairs than one buffer can address.</exception>
    public override IColumnWriteState Begin(ValueSource<KeyValuePair<TKey, TValue>[]> rows)
    {
        ReadOnlySpan<byte> absent = rows.Absent;
        bool marked = rows.HasAbsent;
        ulong running = 0;
        int position = 0;
        for (int r = 0; r < rows.RunCount; r++)
        {
            foreach (KeyValuePair<TKey, TValue>[] row in rows.Run(r))
            {
                if (!(marked && absent[position] != 0))
                {
                    if (row is null)
                    {
                        throw NullRow(rows, position);
                    }

                    running += (ulong)row.Length;
                }

                position++;
            }
        }

        if (running > (ulong)Array.MaxLength)
        {
            throw new NotSupportedException(
                $"Map column '{rows.Column}' holds {running} pairs in one block, exceeding the maximum ({Array.MaxLength}) this client can buffer.");
        }

        var state = new State((int)running);
        try
        {
            int pair = 0;
            position = 0;
            for (int r = 0; r < rows.RunCount; r++)
            {
                foreach (KeyValuePair<TKey, TValue>[] row in rows.Run(r))
                {
                    if (!(marked && absent[position] != 0))
                    {
                        foreach (KeyValuePair<TKey, TValue> entry in row)
                        {
                            state.Keys[pair] = entry.Key;
                            state.Values[pair] = entry.Value;
                            pair++;
                        }
                    }

                    position++;
                }
            }

            state.KeyState = keys.Begin(state.KeysOf(rows));
            state.ValueState = values.Begin(state.ValuesOf(rows));
            return state;
        }
        catch
        {
            state.Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public override void WritePrefix(ClickHouseBinaryWriter writer, ValueSource<KeyValuePair<TKey, TValue>[]> rows, IColumnWriteState state)
    {
        var own = (State)state;
        keys.WritePrefix(writer, own.KeysOf(rows), own.KeyState);
        values.WritePrefix(writer, own.ValuesOf(rows), own.ValueState);
    }

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<KeyValuePair<TKey, TValue>[]> rows, IColumnWriteState state)
    {
        var own = (State)state;
        ReadOnlySpan<byte> absent = rows.Absent;
        bool marked = rows.HasAbsent;
        ulong offset = 0;
        int position = 0;
        for (int r = 0; r < rows.RunCount; r++)
        {
            foreach (KeyValuePair<TKey, TValue>[] row in rows.Run(r))
            {
                if (!(marked && absent[position] != 0))
                {
                    offset += (ulong)row.Length;
                }

                writer.WriteUInt64(offset);
                position++;
            }
        }

        keys.Write(writer, own.KeysOf(rows), own.KeyState);
        values.Write(writer, own.ValuesOf(rows), own.ValueState);
    }

    // The parameter name is the one that the Map codec reports for the same row.
#pragma warning disable CA2208 // Instantiate argument exceptions correctly
    private static ArgumentException NullRow(ValueSource<KeyValuePair<TKey, TValue>[]> rows, int position)
        => new(
            $"Map column '{rows.Column}' has a null value at row {rows.FirstRow + position}; Map(K, V) rows are non-nullable. Use Array.Empty<KeyValuePair<K, V>>() for an empty row, or Map(K, Nullable(V)) to carry null values.",
            "column");
#pragma warning restore CA2208

    private sealed class State : IColumnWriteState
    {
        private readonly int count;

        public State(int count)
        {
            this.count = count;
            Keys = WriteBuffers.Rent<TKey>(count);
            Values = WriteBuffers.Rent<TValue>(count);
        }

        public TKey[] Keys { get; private set; }

        public TValue[] Values { get; private set; }

        public IColumnWriteState KeyState { get; set; }

        public IColumnWriteState ValueState { get; set; }

        // The pairs of all the rows are one flat stream, so a refused key or value is named by its position in it.
        public ValueSource<TKey> KeysOf(ValueSource<KeyValuePair<TKey, TValue>[]> rows) => ValueSource<TKey>.Of(Keys.AsSpan(0, count), 0, rows.Column);

        public ValueSource<TValue> ValuesOf(ValueSource<KeyValuePair<TKey, TValue>[]> rows) => ValueSource<TValue>.Of(Values.AsSpan(0, count), 0, rows.Column);

        public void Dispose()
        {
            KeyState?.Dispose();
            ValueState?.Dispose();
            KeyState = null;
            ValueState = null;
            WriteBuffers.Return(Keys);
            WriteBuffers.Return(Values);
            Keys = null;
            Values = null;
        }
    }
}
