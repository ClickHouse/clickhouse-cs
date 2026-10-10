using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// <c>Dynamic</c> from <see cref="object"/>, in the flattened serialization of <see cref="DynamicColumnCodec"/>. The
/// ClickHouse type of each value comes from the value (<see cref="DynamicTypeInference"/>), which also gives the value
/// as the canonical CLR type of that type.
/// </summary>
/// <remarks>
/// <para>
/// The prefix is the serialization version, the number of types, the names of the types in ordinal order, and the
/// prefix of each type. The body is one discriminator for each value (the index of its type, or the number of types for a
/// NULL), then the values of each type in row order, the types one after the other.
/// </para>
/// <para>
/// The values of one type are written by the converter tree of the type from its canonical CLR type. A null value, and
/// a position that the source marks, is a NULL.
/// </para>
/// </remarks>
internal sealed class DynamicWriter : ColumnWriter<object>
{
    // The writers of the types that the writes meet. The writer is cached and shared, so the cache has a limit.
    private const int MaxCachedTypes = 1024;

    private readonly Func<string, VariantChild> derive;
    private readonly ConcurrentDictionary<string, VariantChild> types = new(StringComparer.Ordinal);

    /// <summary>Initializes the writer.</summary>
    /// <param name="derive">Derives the writer of a ClickHouse type from its canonical CLR type.</param>
    public DynamicWriter(Func<string, VariantChild> derive) => this.derive = derive;

    /// <inheritdoc/>
    public override bool HasPrefix => true;

    /// <inheritdoc/>
    /// <exception cref="NotSupportedException">No ClickHouse type is inferred for a value.</exception>
    public override IColumnWriteState Begin(ValueSource<object> values)
    {
        int count = values.Count;
        string[] rowTypes = WriteBuffers.Rent<string>(count);
        object[] rowValues = WriteBuffers.Rent<object>(count);
        object[] grouped = null;
        var state = new State();
        try
        {
            // The type and the canonical value of each value.
            var distinct = new SortedSet<string>(StringComparer.Ordinal);
            ReadOnlySpan<byte> absent = values.Absent;
            bool marked = values.HasAbsent;
            int position = 0;
            for (int r = 0; r < values.RunCount; r++)
            {
                foreach (object value in values.Run(r))
                {
                    if (value is null || (marked && absent[position] != 0))
                    {
                        rowTypes[position] = null;
                    }
                    else
                    {
                        (string type, object canonical) = DynamicTypeInference.Infer(value);
                        rowTypes[position] = type;
                        rowValues[position] = canonical;
                        distinct.Add(type);
                    }

                    position++;
                }
            }

            int typeCount = distinct.Count;
            state.Names = new string[typeCount];
            distinct.CopyTo(state.Names);
            var index = new Dictionary<string, int>(typeCount, StringComparer.Ordinal);
            var counts = new int[typeCount];
            for (int i = 0; i < typeCount; i++)
            {
                index[state.Names[i]] = i;
            }

            state.Discriminators = WriteBuffers.Rent<int>(count);
            for (int row = 0; row < count; row++)
            {
                if (rowTypes[row] is string type)
                {
                    int discriminator = index[type];
                    state.Discriminators[row] = discriminator;
                    counts[discriminator]++;
                }
                else
                {
                    state.Discriminators[row] = typeCount;
                }
            }

            // The values of each type, in row order, the types one after the other.
            var starts = new int[typeCount];
            var next = new int[typeCount];
            int total = 0;
            for (int i = 0; i < typeCount; i++)
            {
                starts[i] = total;
                next[i] = total;
                total += counts[i];
            }

            grouped = WriteBuffers.Rent<object>(total);
            for (int row = 0; row < count; row++)
            {
                int discriminator = state.Discriminators[row];
                if (discriminator < typeCount)
                {
                    grouped[next[discriminator]++] = rowValues[row];
                }
            }

            state.Runs = new VariantRun[typeCount];
            for (int i = 0; i < typeCount; i++)
            {
                state.Runs[i] = Writer(state.Names[i]).Begin(grouped, starts[i], counts[i], values.Column);
            }

            return state;
        }
        catch
        {
            state.Dispose();
            throw;
        }
        finally
        {
            WriteBuffers.Return(rowTypes);
            WriteBuffers.Return(rowValues);
            WriteBuffers.Return(grouped);
        }
    }

    /// <inheritdoc/>
    public override void WritePrefix(ClickHouseBinaryWriter writer, ValueSource<object> values, IColumnWriteState state)
    {
        var own = (State)state;
        writer.WriteUInt64(DynamicColumnCodec.FlattenedVersion);
        writer.WriteVarUInt((ulong)own.Names.Length);
        foreach (string name in own.Names)
        {
            writer.WriteString(name);
        }

        foreach (VariantRun run in own.Runs)
        {
            run.Child.WritePrefix(writer, run, values.Column);
        }
    }

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<object> values, IColumnWriteState state)
    {
        var own = (State)state;
        DynamicColumnCodec.WriteDiscriminators(writer, own.Discriminators.AsSpan(0, values.Count), DynamicColumnCodec.DiscriminatorWidth(own.Names.Length));
        foreach (VariantRun run in own.Runs)
        {
            run.Child.Write(writer, run, values.Column);
        }
    }

    private VariantChild Writer(string type)
    {
        if (types.TryGetValue(type, out VariantChild child))
        {
            return child;
        }

        child = derive(type);
        return types.Count < MaxCachedTypes ? types.GetOrAdd(type, child) : child;
    }

    // The types, the discriminators and one run of values for each type.
    private sealed class State : IColumnWriteState
    {
        public string[] Names { get; set; }

        public int[] Discriminators { get; set; }

        public VariantRun[] Runs { get; set; }

        public void Dispose()
        {
            if (Runs is not null)
            {
                foreach (VariantRun run in Runs)
                {
                    run?.Dispose();
                }

                Runs = null;
            }

            WriteBuffers.Return(Discriminators);
            Discriminators = null;
        }
    }
}
