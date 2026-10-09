using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// <c>Variant(...)</c> from <see cref="object"/>: the discriminator of each value, then the values of each alternative,
/// in alternative order. The prefix is the discriminators mode, then the prefix of each alternative.
/// </summary>
/// <remarks>
/// <para>
/// The alternative of a value comes from the CLR type of the value. An alternative whose canonical CLR type is the type
/// of the value takes it. When several alternatives have that canonical type, the value decides
/// (<see cref="IColumnCodec.ClaimsValue"/>): exactly one alternative must claim it. A value whose type is the canonical
/// type of no alternative goes to the alternatives that are written from its type (the derivation of each alternative),
/// with the same rule for a tie. So <c>Variant(String, UInt64)</c> takes a <see cref="T:byte[]"/> value as a String.
/// </para>
/// <para>
/// Each alternative writes its values as one run, in row order. A leaf writes the values of two CLR types, one after the
/// other (<see cref="ColumnWriter.IsFlat"/>). A composite alternative takes the values of one CLR type in one write.
/// Every alternative writes its prefix, also when no value selects it.
/// </para>
/// </remarks>
internal sealed class VariantWriter : ColumnWriter<object>
{
    // The placements of the CLR types that the writes meet. The writer is cached and shared, so the cache has a limit.
    private const int MaxCachedPlacements = 1024;

    private readonly string typeName;
    private readonly IColumnCodec[] alternatives;
    private readonly VariantChild[] canonical;
    private readonly Func<int, Type, VariantChild> derive;
    private readonly Dictionary<Type, int> canonicalOwner = new();
    private readonly Dictionary<Type, int[]> canonicalCollisions = new();
    private readonly ConcurrentDictionary<Type, Placement> placements = new();

    /// <summary>Initializes the writer.</summary>
    /// <param name="typeName">The Variant type, for the messages.</param>
    /// <param name="alternatives">The codec of each alternative, in discriminator order.</param>
    /// <param name="canonical">The writer of each alternative from its canonical CLR type.</param>
    /// <param name="derive">Derives the writer of alternative i from a CLR type, or gives null when there is none.</param>
    public VariantWriter(string typeName, IColumnCodec[] alternatives, VariantChild[] canonical, Func<int, Type, VariantChild> derive)
    {
        this.typeName = typeName;
        this.alternatives = alternatives;
        this.canonical = canonical;
        this.derive = derive;

        var owners = new Dictionary<Type, List<int>>();
        for (int i = 0; i < alternatives.Length; i++)
        {
            Type elementType = alternatives[i].ElementType;
            if (!owners.TryGetValue(elementType, out List<int> sharing))
            {
                sharing = new List<int>(1);
                owners[elementType] = sharing;
            }

            sharing.Add(i);
        }

        foreach (KeyValuePair<Type, List<int>> entry in owners)
        {
            if (entry.Value.Count == 1)
            {
                canonicalOwner[entry.Key] = entry.Value[0];
            }
            else
            {
                canonicalCollisions[entry.Key] = entry.Value.ToArray();
            }
        }
    }

    /// <inheritdoc/>
    public override bool HasPrefix => true;

    /// <inheritdoc/>
    /// <exception cref="ArgumentException">No alternative, or more than one, takes a value.</exception>
    public override IColumnWriteState Begin(ValueSource<object> values)
    {
        int count = values.Count;
        var state = new State(alternatives.Length, count);
        try
        {
            // The discriminator and the writer of each value, and the number of values of each alternative.
            ReadOnlySpan<byte> absent = values.Absent;
            bool marked = values.HasAbsent;
            int position = 0;
            for (int r = 0; r < values.RunCount; r++)
            {
                foreach (object value in values.Run(r))
                {
                    if (value is null || (marked && absent[position] != 0))
                    {
                        state.Discriminators[position] = IVariantColumn.NullDiscriminator;
                    }
                    else
                    {
                        int discriminator = Place(value, out VariantChild child);
                        state.Discriminators[position] = (byte)discriminator;
                        state.Selected(position, discriminator, child);
                    }

                    position++;
                }
            }

            state.Group(values);
            for (int i = 0; i < alternatives.Length; i++)
            {
                state.BeginRuns(i, canonical[i], values.Column, alternatives[i].TypeName, typeName);
            }

            return state;
        }
        catch
        {
            state.Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public override void WritePrefix(ClickHouseBinaryWriter writer, ValueSource<object> values, IColumnWriteState state)
    {
        var own = (State)state;
        writer.WriteUInt64(VariantWire.BasicDiscriminatorsMode);
        for (int i = 0; i < alternatives.Length; i++)
        {
            VariantRun first = own.FirstRun(i);
            first.Child.WritePrefix(writer, first, values.Column);
        }
    }

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter writer, ValueSource<object> values, IColumnWriteState state)
    {
        var own = (State)state;
        writer.WriteBytes(own.Discriminators.AsSpan(0, values.Count));
        for (int i = 0; i < alternatives.Length; i++)
        {
            for (int k = own.FirstRunIndex(i); k < own.FirstRunIndex(i + 1); k++)
            {
                VariantRun run = own.Run(k);
                run.Child.Write(writer, run, values.Column);
            }
        }
    }

    // The alternative of one value, and the writer of that alternative for the value's CLR type.
    private int Place(object value, out VariantChild child)
    {
        Type clrType = value.GetType();
        if (!placements.TryGetValue(clrType, out Placement placement))
        {
            placement = PlacementOf(clrType);
            if (placements.Count < MaxCachedPlacements)
            {
                placement = placements.GetOrAdd(clrType, placement);
            }
        }

        int discriminator = placement.Discriminator >= 0 ? placement.Discriminator : Settle(value, clrType, placement);
        child = placement.Children[discriminator];
        return discriminator;
    }

    private Placement PlacementOf(Type clrType)
    {
        var children = new VariantChild[alternatives.Length];
        if (canonicalOwner.TryGetValue(clrType, out int owner))
        {
            children[owner] = canonical[owner];
            return new Placement(owner, null, byDerivation: false, children, refusal: null);
        }

        if (canonicalCollisions.TryGetValue(clrType, out int[] colliding))
        {
            foreach (int candidate in colliding)
            {
                children[candidate] = canonical[candidate];
            }

            return new Placement(-1, colliding, byDerivation: false, children, refusal: null);
        }

        var derived = new List<int>();
        for (int i = 0; i < alternatives.Length; i++)
        {
            children[i] = derive(i, clrType);
            if (children[i] is not null)
            {
                derived.Add(i);
            }
        }

        return derived.Count switch
        {
            0 => new Placement(-1, null, byDerivation: true, children, $"Variant '{typeName}' has no alternative for a value of CLR type '{clrType}'. Supported CLR types: {SupportedClrTypes()}."),
            1 => new Placement(derived[0], null, byDerivation: true, children, refusal: null),
            _ => new Placement(-1, derived.ToArray(), byDerivation: true, children, refusal: null),
        };
    }

    // More than one alternative takes the CLR type of the value, so the value is asked. Exactly one must claim it.
    private int Settle(object value, Type clrType, Placement placement)
    {
        if (placement.Candidates is null)
        {
            throw new ArgumentException(placement.Refusal);
        }

        int claimed = -1;
        foreach (int candidate in placement.Candidates)
        {
            if (!alternatives[candidate].ClaimsValue(value))
            {
                continue;
            }

            if (claimed >= 0)
            {
                throw new ArgumentException(placement.ByDerivation
                    ? $"Variant '{typeName}' cannot place a value of CLR type '{clrType}': the alternatives {Names(placement.Candidates)} are all written from that type, and the value does not say which of them is meant."
                    : $"Variant '{typeName}' cannot place a value of CLR type '{clrType}': the alternatives {Names(placement.Candidates)} all " +
                      "surface that type, and the value does not say which of them is meant.");
            }

            claimed = candidate;
        }

        if (claimed < 0)
        {
            throw new ArgumentException(placement.ByDerivation
                ? $"Variant '{typeName}' cannot place a value of CLR type '{clrType}': the alternatives {Names(placement.Candidates)} are written from that type, but none of them claims the value."
                : $"Variant '{typeName}' cannot place a value of CLR type '{clrType}': it surfaces the type of the alternatives " +
                  $"{Names(placement.Candidates)}, but matches none of them.");
        }

        return claimed;
    }

    private string Names(int[] candidates)
        => string.Join(", ", Array.ConvertAll(candidates, candidate => $"'{alternatives[candidate].TypeName}'"));

    // The canonical CLR type of each alternative, in discriminator order and without repeats.
    private string SupportedClrTypes()
    {
        var seen = new HashSet<Type>();
        var names = new List<string>(alternatives.Length);
        foreach (IColumnCodec alternative in alternatives)
        {
            if (seen.Add(alternative.ElementType))
            {
                names.Add(alternative.ElementType.ToString());
            }
        }

        return string.Join(", ", names);
    }

    // Where the values of one CLR type go: one alternative, or candidates that the value decides between, or a refusal.
    private sealed class Placement
    {
        public Placement(int discriminator, int[] candidates, bool byDerivation, VariantChild[] children, string refusal)
        {
            Discriminator = discriminator;
            Candidates = candidates;
            ByDerivation = byDerivation;
            Children = children;
            Refusal = refusal;
        }

        public int Discriminator { get; }

        public int[] Candidates { get; }

        public bool ByDerivation { get; }

        public VariantChild[] Children { get; }

        public string Refusal { get; }
    }

    // The discriminators, the values of each alternative in row order, and the runs that write them. The buffers hold one
    // entry for each value, whatever the number of alternatives: the values are grouped by alternative in one array.
    private sealed class State : IColumnWriteState
    {
        private readonly int[] counts;
        private readonly int[] starts;
        private readonly int[] firstRun;
        private readonly List<VariantRun> runs;
        private VariantChild[] rowChildren;
        private object[] grouped;
        private VariantChild[] groupedChildren;

        public State(int alternativeCount, int count)
        {
            Discriminators = WriteBuffers.Rent<byte>(count);
            rowChildren = WriteBuffers.Rent<VariantChild>(count);
            counts = new int[alternativeCount];
            starts = new int[alternativeCount];
            firstRun = new int[alternativeCount + 1];
            runs = new List<VariantRun>(alternativeCount);
        }

        public byte[] Discriminators { get; private set; }

        public VariantRun FirstRun(int alternative) => runs[firstRun[alternative]];

        // The index of the first run of an alternative; the runs of alternative i end where those of i + 1 begin.
        public int FirstRunIndex(int alternative) => firstRun[alternative];

        public VariantRun Run(int index) => runs[index];

        public void Selected(int position, int alternative, VariantChild child)
        {
            rowChildren[position] = child;
            counts[alternative]++;
        }

        // Puts the values of each alternative together, in row order, the alternatives one after the other.
        public void Group(ValueSource<object> values)
        {
            var next = new int[counts.Length];
            int total = 0;
            for (int i = 0; i < counts.Length; i++)
            {
                starts[i] = total;
                next[i] = total;
                total += counts[i];
            }

            grouped = WriteBuffers.Rent<object>(total);
            groupedChildren = WriteBuffers.Rent<VariantChild>(total);
            int position = 0;
            for (int r = 0; r < values.RunCount; r++)
            {
                foreach (object value in values.Run(r))
                {
                    byte discriminator = Discriminators[position];
                    if (discriminator != IVariantColumn.NullDiscriminator)
                    {
                        int at = next[discriminator]++;
                        grouped[at] = value;
                        groupedChildren[at] = rowChildren[position];
                    }

                    position++;
                }
            }

            WriteBuffers.Return(rowChildren);
            rowChildren = null;
        }

        // Splits the values of one alternative into runs of one writer. An alternative with no value writes one empty
        // run of its canonical writer, for its prefix. Called for each alternative in order.
        public void BeginRuns(int alternative, VariantChild canonicalChild, string column, string alternativeType, string variantType)
        {
            int first = starts[alternative];
            int count = counts[alternative];
            firstRun[alternative] = runs.Count;
            if (count == 0)
            {
                runs.Add(canonicalChild.Begin(grouped, first, 0, column));
                firstRun[alternative + 1] = runs.Count;
                return;
            }

            int start = first;
            int end = first + count;
            for (int i = first + 1; i <= end; i++)
            {
                if (i < end && ReferenceEquals(groupedChildren[i], groupedChildren[start]))
                {
                    continue;
                }

                if (start > first && !groupedChildren[start].IsFlat)
                {
                    throw new ArgumentException(
                        $"Variant '{variantType}' cannot write the values of its alternative '{alternativeType}' from more than one CLR type in one block " +
                        $"({groupedChildren[first].ValueType} and {groupedChildren[start].ValueType}). Give the values of that alternative as one CLR type.");
                }

                runs.Add(groupedChildren[start].Begin(grouped, start, i - start, column));
                start = i;
            }

            firstRun[alternative + 1] = runs.Count;
        }

        public void Dispose()
        {
            foreach (VariantRun run in runs)
            {
                run.Dispose();
            }

            runs.Clear();
            WriteBuffers.Return(rowChildren);
            WriteBuffers.Return(grouped);
            WriteBuffers.Return(groupedChildren);
            WriteBuffers.Return(Discriminators);
            rowChildren = null;
            grouped = null;
            groupedChildren = null;
            Discriminators = null;
        }
    }
}

/// <summary>The writer of one Variant alternative from one CLR type, over the values that a bucket holds as objects.</summary>
internal abstract class VariantChild
{
    /// <summary>The CLR type of the values.</summary>
    public abstract Type ValueType { get; }

    /// <summary>Whether two runs of the alternative can be written one after the other (<see cref="ColumnWriter.IsFlat"/>).</summary>
    public abstract bool IsFlat { get; }

    /// <summary>Returns the typed buffer of a run to the pool.</summary>
    /// <param name="buffer">The buffer.</param>
    public abstract void ReturnBuffer(Array buffer);

    /// <summary>Copies values of the bucket into a typed buffer and begins their write.</summary>
    /// <param name="bucket">The values of the alternative, in row order.</param>
    /// <param name="start">The first value of the run in the bucket.</param>
    /// <param name="count">The number of values of the run.</param>
    /// <param name="column">The column name, for the messages.</param>
    /// <returns>The run.</returns>
    public abstract VariantRun Begin(object[] bucket, int start, int count, string column);

    /// <summary>Writes the prefix of the alternative.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="run">A run from <see cref="Begin"/>.</param>
    /// <param name="column">The column name, for the messages.</param>
    public abstract void WritePrefix(ClickHouseBinaryWriter writer, VariantRun run, string column);

    /// <summary>Writes the values of one run.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="run">A run from <see cref="Begin"/>.</param>
    /// <param name="column">The column name, for the messages.</param>
    public abstract void Write(ClickHouseBinaryWriter writer, VariantRun run, string column);
}

/// <summary>The writer of one Variant alternative from <typeparamref name="T"/>.</summary>
/// <typeparam name="T">The CLR type of the values.</typeparam>
internal sealed class VariantChild<T> : VariantChild
{
    private readonly ColumnWriter<T> writer;

    /// <summary>Initializes the child.</summary>
    /// <param name="writer">The writer of the alternative from <typeparamref name="T"/>.</param>
    public VariantChild(ColumnWriter<T> writer) => this.writer = writer;

    /// <inheritdoc/>
    public override Type ValueType => typeof(T);

    /// <inheritdoc/>
    public override bool IsFlat => writer.IsFlat;

    /// <inheritdoc/>
    public override void ReturnBuffer(Array buffer) => WriteBuffers.Return((T[])buffer);

    /// <inheritdoc/>
    public override VariantRun Begin(object[] bucket, int start, int count, string column)
    {
        T[] values = WriteBuffers.Rent<T>(count);
        var run = new VariantRun(this, values, count);
        try
        {
            for (int i = 0; i < count; i++)
            {
                values[i] = (T)bucket[start + i];
            }

            run.State = writer.Begin(Source(run, column));
            return run;
        }
        catch
        {
            run.Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public override void WritePrefix(ClickHouseBinaryWriter output, VariantRun run, string column)
        => writer.WritePrefix(output, Source(run, column), run.State);

    /// <inheritdoc/>
    public override void Write(ClickHouseBinaryWriter output, VariantRun run, string column)
        => writer.Write(output, Source(run, column), run.State);

    // The values of one alternative are a column of their own, so a refused value is named by its position in it.
    private static ValueSource<T> Source(VariantRun run, string column) => ValueSource<T>.Of(((T[])run.Values).AsSpan(0, run.Count), 0, column);
}

/// <summary>One run of values of one Variant alternative: a typed buffer and the state of its write.</summary>
internal sealed class VariantRun : IDisposable
{
    /// <summary>Initializes the run.</summary>
    /// <param name="child">The writer of the run.</param>
    /// <param name="values">The typed buffer, pooled.</param>
    /// <param name="count">The number of values.</param>
    public VariantRun(VariantChild child, Array values, int count)
    {
        Child = child;
        Values = values;
        Count = count;
    }

    /// <summary>The writer of the run.</summary>
    public VariantChild Child { get; }

    /// <summary>The typed buffer.</summary>
    public Array Values { get; private set; }

    /// <summary>The number of values.</summary>
    public int Count { get; }

    /// <summary>The state of the write.</summary>
    public IColumnWriteState State { get; set; }

    /// <inheritdoc/>
    public void Dispose()
    {
        State?.Dispose();
        State = null;
        if (Values is not null)
        {
            Child.ReturnBuffer(Values);
            Values = null;
        }
    }
}
