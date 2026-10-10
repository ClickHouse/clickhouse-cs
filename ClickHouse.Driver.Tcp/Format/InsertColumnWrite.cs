using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Format;

/// <summary>
/// How an insert writes one column, in the three phases of a column write (begin, state prefix, body) for a range of
/// rows. A column that the codec writes from its own storage (a decoded column, or a dense column in the wire layout of
/// the type) goes to the codec, with no conversion (decision D3). Every other column goes to the converter tree of its
/// CLR type (<see cref="ConverterDerivation"/>), over a value source of its rows (decision D2).
/// </summary>
/// <remarks>
/// <para>
/// The converter write reads the values of the column from its span when the column has one
/// (<see cref="ISpanColumn{T}"/>), from its stored values (<see cref="IStoredValuesColumn"/>), or else through its indexer
/// into a pooled buffer: <see cref="IColumn{T}.Values"/> of a view can throw or compute every row of the column. The
/// buffer lives in the write state, so the prefix and the body read the same values.
/// </para>
/// <para>
/// A dense array (<see cref="ClickHouseTcpColumn.CreateArray{TElement}"/>) that the codec does not write from its storage
/// is written as its offsets, then its inner column through the element writer of the tree of the array. So the rows
/// are not copied into arrays of their own.
/// </para>
/// </remarks>
internal abstract class InsertColumnWrite
{
    // One converter write for each cached tree. The write holds no state, so every insert of the tree shares it.
    private static readonly ConditionalWeakTable<ColumnWriter, InsertColumnWrite> ConverterWrites = new();

    private static readonly ConcurrentDictionary<Type, Func<ColumnWriter, InsertColumnWrite>> Factories = new();

    private static readonly ConcurrentDictionary<Type, Func<ColumnReader, ColumnWriter, INullableColumn, InsertColumnWrite>> DecodedConversions = new();

    /// <summary>Begins the write of rows [<paramref name="start"/>, <paramref name="start"/> + <paramref name="length"/>).</summary>
    /// <param name="values">The column.</param>
    /// <param name="start">The first row.</param>
    /// <param name="length">The number of rows.</param>
    /// <returns>The state that the prefix and the body share, or <see langword="null"/>. The caller disposes it.</returns>
    public abstract IColumnWriteState Begin(IColumn values, int start, int length);

    /// <summary>Writes the state prefix of the rows.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="values">The column.</param>
    /// <param name="start">The first row.</param>
    /// <param name="length">The number of rows.</param>
    /// <param name="state">The state from <see cref="Begin"/>.</param>
    public abstract void WritePrefix(ClickHouseBinaryWriter writer, IColumn values, int start, int length, IColumnWriteState state);

    /// <summary>Writes the body of the rows.</summary>
    /// <param name="writer">The writer to encode into.</param>
    /// <param name="values">The column.</param>
    /// <param name="start">The first row.</param>
    /// <param name="length">The number of rows.</param>
    /// <param name="state">The state from <see cref="Begin"/>.</param>
    public abstract void Write(ClickHouseBinaryWriter writer, IColumn values, int start, int length, IColumnWriteState state);

    /// <summary>
    /// The write of <paramref name="values"/> as <paramref name="typeName"/>: through the codec when it writes the column
    /// from its storage, else through the converter tree of the column's CLR type. A column of no single CLR type (it
    /// implements <see cref="IColumn{T}"/> zero times or more than once) is written as the first of the suggested CLR
    /// types of the type (<see cref="ConverterDerivation.SuggestedTypes"/>) that it implements.
    /// </summary>
    /// <param name="codec">The codec of the target type, resolved with <paramref name="context"/>.</param>
    /// <param name="values">The column that the caller gives.</param>
    /// <param name="typeName">The target type.</param>
    /// <param name="context">The resolution context of the target (its session timezone).</param>
    /// <param name="derivation">The converter derivation of the codec registry.</param>
    /// <returns>The write, or <see langword="null"/> when the column cannot be written as the type.</returns>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    public static InsertColumnWrite For(IColumnCodec codec, IColumn values, string typeName, in ResolveContext context, ConverterDerivation derivation)
        => For(codec, values, typeName, in context, derivation, out _);

    /// <summary>
    /// The write of <paramref name="values"/> as <paramref name="typeName"/>, as the other overload gives it. A column that
    /// a query read as another type whose canonical values mean other values in the target (a <c>DateTime64</c> or a
    /// <c>Time64</c> of another scale, an <c>Enum</c> with other members) is written part by part
    /// (<see cref="ConverterDerivation.PlanDecodedPart"/>): the parts whose values mean other values are converted through
    /// the meaning of their values, and the codecs write the other parts from their storage. The reason is set when such a
    /// column cannot be written.
    /// </summary>
    /// <param name="codec">The codec of the target type, resolved with <paramref name="context"/>.</param>
    /// <param name="values">The column that the caller gives.</param>
    /// <param name="typeName">The target type.</param>
    /// <param name="context">The resolution context of the target (its session timezone).</param>
    /// <param name="derivation">The converter derivation of the codec registry.</param>
    /// <param name="refusal">Why a column that a query read as another type cannot be converted, or null.</param>
    /// <returns>The write, or <see langword="null"/> when the column cannot be written as the type.</returns>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    public static InsertColumnWrite For(IColumnCodec codec, IColumn values, string typeName, in ResolveContext context, ConverterDerivation derivation, out string refusal)
    {
        refusal = null;
        if (codec.CanWrite(values))
        {
            return new CodecWrite(codec);
        }

        string source = SourceType(values);
        if (source is not null
            && !string.Equals(source, typeName, StringComparison.Ordinal)
            && derivation.IsReadColumnOfAnotherMeaning(values, source, typeName, in context))
        {
            return ReadColumnWrite(codec, values, source, typeName, in context, derivation, nulls: null, out refusal);
        }

        return ConverterTreeWrite(values, typeName, in context, derivation, out _);
    }

    // The write of one part of a column that a query read as another type. nulls: the Nullable column whose null map marks
    // the rows of the part that hold no value, or null.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private static InsertColumnWrite ReadColumnWrite(
        IColumnCodec codec,
        IColumn values,
        string source,
        string target,
        in ResolveContext context,
        ConverterDerivation derivation,
        INullableColumn nulls,
        out string refusal)
    {
        refusal = null;
        DecodedPartPlan plan = derivation.PlanDecodedPart(source, target);
        switch (plan.Kind)
        {
            case DecodedPart.Kept:
                return codec.CanWrite(values) ? new CodecWrite(codec) : ConverterTreeWrite(values, target, in context, derivation, out refusal);

            case DecodedPart.Converted:
                return derivation.TryDeriveMeaningConversion(source, target, in context, out ColumnReader reader, out ColumnWriter writer, out refusal)
                    ? DecodedConversions.GetOrAdd(reader.ValueType, BuildDecodedConversionFactory)(reader, writer, nulls)
                    : null;

            case DecodedPart.Tuple when values is ITupleColumn tuple && tuple.Children.Count == plan.SourceParts.Length:
            {
                var elements = new InsertColumnWrite[plan.SourceParts.Length];
                for (int i = 0; i < elements.Length; i++)
                {
                    elements[i] = PartWrite(tuple.Children[i], plan.SourceParts[i], plan.TargetParts[i], in context, derivation, nulls, out refusal);
                    if (elements[i] is null)
                    {
                        return null;
                    }
                }

                return new TupleParts(elements);
            }

            // The elements of a row that holds no value are not in the column, so no mark reaches them.
            case DecodedPart.Array when values is IDenseArrayColumn dense:
            {
                InsertColumnWrite elements = PartWrite(dense.Inner, plan.SourceParts[0], plan.TargetParts[0], in context, derivation, nulls: null, out refusal);
                return elements is null ? null : new DenseArray(elements);
            }

            case DecodedPart.Map when values is IMapColumn map:
            {
                InsertColumnWrite keys = PartWrite(map.KeyColumn, plan.SourceParts[0], plan.TargetParts[0], in context, derivation, nulls: null, out refusal);
                InsertColumnWrite pairs = keys is null ? null : PartWrite(map.ValueColumn, plan.SourceParts[1], plan.TargetParts[1], in context, derivation, nulls: null, out refusal);
                return pairs is null ? null : new MapParts(keys, pairs);
            }

            case DecodedPart.Nullable when !plan.SourceHoldsNull || values is INullableColumn:
            {
                // A row that holds no value is written into a Nullable target with the placeholder of each converted part,
                // and is refused by a target that is not Nullable.
                IColumn inner = plan.SourceHoldsNull ? ((INullableColumn)values).Inner : values;
                INullableColumn innerNulls = plan.SourceHoldsNull ? (plan.TargetHoldsNull ? (INullableColumn)values : null) : nulls;
                InsertColumnWrite part = PartWrite(inner, plan.SourceParts[0], plan.TargetParts[0], in context, derivation, innerNulls, out refusal);
                return part is null ? null : new NullableParts(part, plan.SourceHoldsNull, plan.TargetHoldsNull, target);
            }

            case DecodedPart.Refused:
                refusal = plan.Refusal;
                return null;

            default:
                refusal = $"A column read as {source} is written into {target} only from the column that a query of {source} reads.";
                return null;
        }
    }

    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private static InsertColumnWrite PartWrite(IColumn values, string source, string target, in ResolveContext context, ConverterDerivation derivation, INullableColumn nulls, out string refusal)
        => ReadColumnWrite(derivation.ResolvePart(target, in context), values, source, target, in context, derivation, nulls, out refusal);

    // The converter tree of the column's CLR type, or of the first suggested CLR type of the target type that the column
    // implements when it has no single CLR type.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private static InsertColumnWrite ConverterTreeWrite(IColumn values, string typeName, in ResolveContext context, ConverterDerivation derivation, out string refusal)
    {
        refusal = null;
        Type elementType;
        try
        {
            elementType = values.ElementType;
        }
        catch (InvalidOperationException)
        {
            InsertColumnWrite suggested = ForSuggestedType(values, typeName, in context, derivation);
            refusal = suggested is null ? $"'{typeName}' cannot be written from a column of {values.GetType()}, which has no single CLR type." : null;
            return suggested;
        }

        Derivation derived = derivation.Derive(typeName, in context, elementType, ConversionDirection.Write);
        if (!derived.Succeeded)
        {
            refusal = derived.Refusal;
            return null;
        }

        var tree = (ColumnWriter)derived.Converter;
        if (values is IDenseArrayColumn dense && TryGetElements(tree, out ColumnWriter elements))
        {
            return DenseArrayWrite(elements, dense.Inner);
        }

        return TreeWrite(tree);
    }

    // The type that a column says it holds: its type name, or for an array that CreateArray builds (which has none) the
    // array of the type of its inner column. Null when the column names no type.
    private static string SourceType(IColumn values)
    {
        if (values.TypeName is { Length: > 0 } typeName)
        {
            return typeName;
        }

        return values is IDenseArrayColumn dense && SourceType(dense.Inner) is string inner ? $"Array({inner})" : null;
    }

    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private static Func<ColumnReader, ColumnWriter, INullableColumn, InsertColumnWrite> BuildDecodedConversionFactory(Type valueType)
    {
        MethodInfo make = typeof(InsertColumnWrite).GetMethod(nameof(MakeDecodedConversion), BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"{nameof(MakeDecodedConversion)} was not found.");
        return make.MakeGenericMethod(valueType).CreateDelegate<Func<ColumnReader, ColumnWriter, INullableColumn, InsertColumnWrite>>();
    }

    private static DecodedConversion<T> MakeDecodedConversion<T>(ColumnReader reader, ColumnWriter writer, INullableColumn nulls)
        => new((ColumnReader<T>)reader, (ColumnWriter<T>)writer, nulls);

    // A column that implements IColumn<> zero or several times has no single CLR type to derive a tree for.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private static InsertColumnWrite ForSuggestedType(IColumn values, string typeName, in ResolveContext context, ConverterDerivation derivation)
    {
        foreach (Type candidate in derivation.SuggestedTypes(typeName, in context, ConversionDirection.Write))
        {
            if (typeof(IColumn<>).MakeGenericType(candidate).IsInstanceOfType(values))
            {
                Derivation derived = derivation.Derive(typeName, in context, candidate, ConversionDirection.Write);
                if (derived.Succeeded)
                {
                    return TreeWrite((ColumnWriter)derived.Converter);
                }
            }
        }

        return null;
    }

    // The write of a column of the CLR type of the tree.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private static InsertColumnWrite TreeWrite(ColumnWriter tree)
        => ConverterWrites.GetValue(tree, static writer => Factories.GetOrAdd(writer.ValueType, BuildFactory)(writer));

    // The write of the inner column of a dense array: a dense array again when the elements are arrays, else the
    // converter write of the element writer.
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private static DenseArray DenseArrayWrite(ColumnWriter elements, IColumn inner)
    {
        InsertColumnWrite innerWrite = inner is IDenseArrayColumn dense && TryGetElements(elements, out ColumnWriter nested)
            ? DenseArrayWrite(nested, dense.Inner)
            : TreeWrite(elements);
        return new DenseArray(innerWrite);
    }

    // The element writer of an Array writer.
    private static bool TryGetElements(ColumnWriter tree, out ColumnWriter elements)
    {
        elements = (tree as IArrayWriter)?.Elements;
        return elements is not null;
    }

    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    private static Func<ColumnWriter, InsertColumnWrite> BuildFactory(Type valueType)
    {
        MethodInfo make = typeof(InsertColumnWrite).GetMethod(nameof(MakeConverterWrite), BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"{nameof(MakeConverterWrite)} was not found.");
        return make.MakeGenericMethod(valueType).CreateDelegate<Func<ColumnWriter, InsertColumnWrite>>();
    }

    private static ConverterWrite<T> MakeConverterWrite<T>(ColumnWriter writer) => new((ColumnWriter<T>)writer);

    private sealed class CodecWrite : InsertColumnWrite
    {
        private readonly IColumnCodec codec;

        public CodecWrite(IColumnCodec codec) => this.codec = codec;

        public override IColumnWriteState Begin(IColumn values, int start, int length) => codec.BeginWrite(values, start, length);

        public override void WritePrefix(ClickHouseBinaryWriter writer, IColumn values, int start, int length, IColumnWriteState state)
            => codec.WriteStatePrefix(writer, values, start, length, state);

        public override void Write(ClickHouseBinaryWriter writer, IColumn values, int start, int length, IColumnWriteState state)
            => codec.WriteColumn(writer, values, start, length, state);
    }

    // The offsets of the rows, then the elements of the rows through the write of the inner column. The prefix is the
    // prefix of the elements.
    private sealed class DenseArray : InsertColumnWrite
    {
        private readonly InsertColumnWrite elements;

        public DenseArray(InsertColumnWrite elements) => this.elements = elements;

        public override IColumnWriteState Begin(IColumn values, int start, int length)
        {
            var dense = (IDenseArrayColumn)values;
            ReadOnlySpan<int> offsets = dense.Offsets;
            int first = offsets[start];
            int count = offsets[start + length] - first;
            return new State(first, count, elements.Begin(dense.Inner, first, count));
        }

        public override void WritePrefix(ClickHouseBinaryWriter writer, IColumn values, int start, int length, IColumnWriteState state)
        {
            var own = (State)state;
            elements.WritePrefix(writer, ((IDenseArrayColumn)values).Inner, own.First, own.Count, own.Elements);
        }

        public override void Write(ClickHouseBinaryWriter writer, IColumn values, int start, int length, IColumnWriteState state)
        {
            var own = (State)state;
            var dense = (IDenseArrayColumn)values;
            ReadOnlySpan<int> offsets = dense.Offsets;
            for (int i = 0; i < length; i++)
            {
                writer.WriteUInt64((ulong)(offsets[start + i + 1] - own.First));
            }

            elements.Write(writer, dense.Inner, own.First, own.Count, own.Elements);
        }

        // The range of the elements of the rows, and the state of their write.
        private sealed class State : IColumnWriteState
        {
            public State(int first, int count, IColumnWriteState elements)
            {
                First = first;
                Count = count;
                Elements = elements;
            }

            public int First { get; }

            public int Count { get; }

            public IColumnWriteState Elements { get; private set; }

            public void Dispose()
            {
                Elements?.Dispose();
                Elements = null;
            }
        }
    }

    // The elements of a Tuple, each through its own write, as the Tuple codec writes them: the prefixes of the elements in
    // order, then their bodies in order.
    private sealed class TupleParts : InsertColumnWrite
    {
        private readonly InsertColumnWrite[] elements;

        public TupleParts(InsertColumnWrite[] elements) => this.elements = elements;

        public override IColumnWriteState Begin(IColumn values, int start, int length)
        {
            IReadOnlyList<IColumn> children = ((ITupleColumn)values).Children;
            var state = new PartStates(elements.Length);
            try
            {
                for (int i = 0; i < elements.Length; i++)
                {
                    state.States[i] = elements[i].Begin(children[i], start, length);
                }

                return state;
            }
            catch
            {
                state.Dispose();
                throw;
            }
        }

        public override void WritePrefix(ClickHouseBinaryWriter writer, IColumn values, int start, int length, IColumnWriteState state)
        {
            IReadOnlyList<IColumn> children = ((ITupleColumn)values).Children;
            IColumnWriteState[] states = ((PartStates)state).States;
            for (int i = 0; i < elements.Length; i++)
            {
                elements[i].WritePrefix(writer, children[i], start, length, states[i]);
            }
        }

        public override void Write(ClickHouseBinaryWriter writer, IColumn values, int start, int length, IColumnWriteState state)
        {
            IReadOnlyList<IColumn> children = ((ITupleColumn)values).Children;
            IColumnWriteState[] states = ((PartStates)state).States;
            for (int i = 0; i < elements.Length; i++)
            {
                elements[i].Write(writer, children[i], start, length, states[i]);
            }
        }
    }

    // The pairs of a Map, as the Map codec writes them: the stored offsets of the rows, rebased to the slice, then the keys
    // and the values of the pairs of the slice, each through its own write. The prefix is the prefix of the keys, then the
    // prefix of the values.
    private sealed class MapParts : InsertColumnWrite
    {
        private readonly InsertColumnWrite keys;
        private readonly InsertColumnWrite pairValues;

        public MapParts(InsertColumnWrite keys, InsertColumnWrite pairValues)
        {
            this.keys = keys;
            this.pairValues = pairValues;
        }

        public override IColumnWriteState Begin(IColumn values, int start, int length)
        {
            var map = (IMapColumn)values;
            ReadOnlySpan<int> offsets = map.Offsets;
            int first = offsets[start];
            int count = offsets[start + length] - first;
            var state = new PartStates(2) { First = first, Count = count };
            try
            {
                state.States[0] = keys.Begin(map.KeyColumn, first, count);
                state.States[1] = pairValues.Begin(map.ValueColumn, first, count);
                return state;
            }
            catch
            {
                state.Dispose();
                throw;
            }
        }

        public override void WritePrefix(ClickHouseBinaryWriter writer, IColumn values, int start, int length, IColumnWriteState state)
        {
            var map = (IMapColumn)values;
            var own = (PartStates)state;
            keys.WritePrefix(writer, map.KeyColumn, own.First, own.Count, own.States[0]);
            pairValues.WritePrefix(writer, map.ValueColumn, own.First, own.Count, own.States[1]);
        }

        public override void Write(ClickHouseBinaryWriter writer, IColumn values, int start, int length, IColumnWriteState state)
        {
            var map = (IMapColumn)values;
            var own = (PartStates)state;
            ReadOnlySpan<int> offsets = map.Offsets;
            for (int i = 0; i < length; i++)
            {
                writer.WriteUInt64((ulong)(offsets[start + i + 1] - own.First));
            }

            keys.Write(writer, map.KeyColumn, own.First, own.Count, own.States[0]);
            pairValues.Write(writer, map.ValueColumn, own.First, own.Count, own.States[1]);
        }
    }

    // A Nullable of a composite on either side: the null map of the source (or no NULL when the source is not Nullable),
    // then the composite through its own write. A target that is not Nullable refuses a row that holds no value.
    private sealed class NullableParts : InsertColumnWrite
    {
        private readonly InsertColumnWrite inner;
        private readonly bool sourceHoldsNull;
        private readonly bool targetHoldsNull;
        private readonly string targetType;

        public NullableParts(InsertColumnWrite inner, bool sourceHoldsNull, bool targetHoldsNull, string targetType)
        {
            this.inner = inner;
            this.sourceHoldsNull = sourceHoldsNull;
            this.targetHoldsNull = targetHoldsNull;
            this.targetType = targetType;
        }

        public override IColumnWriteState Begin(IColumn values, int start, int length)
        {
            if (sourceHoldsNull && !targetHoldsNull)
            {
                int position = ((INullableColumn)values).NullMap.Slice(start, length).IndexOfAnyExcept((byte)0);
                if (position >= 0)
                {
                    throw WriteRules.NullNotWritable(values.Name, targetType, (long)start + position);
                }
            }

            return inner.Begin(Inner(values), start, length);
        }

        public override void WritePrefix(ClickHouseBinaryWriter writer, IColumn values, int start, int length, IColumnWriteState state)
            => inner.WritePrefix(writer, Inner(values), start, length, state);

        public override void Write(ClickHouseBinaryWriter writer, IColumn values, int start, int length, IColumnWriteState state)
        {
            if (targetHoldsNull)
            {
                if (sourceHoldsNull)
                {
                    writer.WriteBytes(((INullableColumn)values).NullMap.Slice(start, length));
                }
                else
                {
                    for (int i = 0; i < length; i++)
                    {
                        writer.WriteByte(0);
                    }
                }
            }

            inner.Write(writer, Inner(values), start, length, state);
        }

        private IColumn Inner(IColumn values) => sourceHoldsNull ? ((INullableColumn)values).Inner : values;
    }

    // The write states of the parts of a composite, and for a Map the range of the pairs of the slice.
    private sealed class PartStates : IColumnWriteState
    {
        public PartStates(int count) => States = new IColumnWriteState[count];

        public IColumnWriteState[] States { get; }

        public int First { get; init; }

        public int Count { get; init; }

        public void Dispose()
        {
            for (int i = 0; i < States.Length; i++)
            {
                States[i]?.Dispose();
                States[i] = null;
            }
        }
    }

    // A part of a column that a query read as another type, whose values mean other values in the target: the reader of
    // the source type converts the rows of a slice into the CLR type that holds their meaning, and the writer of the target
    // type writes them. The rows keep their numbers in the column, so a refusal names the row of the column. The null map
    // of an enclosing Nullable marks the rows that hold no value, so the writer writes its placeholder there and does not
    // convert the value under the NULL.
    private sealed class DecodedConversion<T> : InsertColumnWrite
    {
        private readonly ColumnReader<T> reader;
        private readonly ColumnWriter<T> writer;
        private readonly INullableColumn nulls;

        public DecodedConversion(ColumnReader<T> reader, ColumnWriter<T> writer, INullableColumn nulls)
        {
            this.reader = reader;
            this.writer = writer;
            this.nulls = nulls;
        }

        public override IColumnWriteState Begin(IColumn values, int start, int length)
        {
            var state = new State { Buffer = WriteBuffers.Rent<T>(length) };
            try
            {
                reader.Bind(values).Fill(start, state.Buffer.AsSpan(0, length));
                state.Inner = writer.Begin(Source(values, start, length, state));
                return state;
            }
            catch
            {
                state.Dispose();
                throw;
            }
        }

        public override void WritePrefix(ClickHouseBinaryWriter output, IColumn values, int start, int length, IColumnWriteState state)
        {
            var own = (State)state;
            writer.WritePrefix(output, Source(values, start, length, own), own.Inner);
        }

        public override void Write(ClickHouseBinaryWriter output, IColumn values, int start, int length, IColumnWriteState state)
        {
            var own = (State)state;
            writer.Write(output, Source(values, start, length, own), own.Inner);
        }

        private ValueSource<T> Source(IColumn values, int start, int length, State state)
        {
            ValueSource<T> source = ValueSource<T>.Of(state.Buffer.AsSpan(0, length), start, values.Name);
            return nulls is null ? source : source.WithAbsent(nulls.NullMap.Slice(start, length));
        }

        private sealed class State : IColumnWriteState
        {
            public T[] Buffer { get; set; }

            public IColumnWriteState Inner { get; set; }

            public void Dispose()
            {
                Inner?.Dispose();
                Inner = null;
                WriteBuffers.Return(Buffer);
                Buffer = null;
            }
        }
    }

    private sealed class ConverterWrite<T> : InsertColumnWrite
    {
        private readonly ColumnWriter<T> writer;

        public ConverterWrite(ColumnWriter<T> writer) => this.writer = writer;

        public override IColumnWriteState Begin(IColumn values, int start, int length)
        {
            var column = (IColumn<T>)values;
            var state = new State();
            try
            {
                if (column is not ISpanColumn<T> and not IStoredValuesColumn)
                {
                    state.Buffer = WriteBuffers.Rent<T>(length);
                    for (int i = 0; i < length; i++)
                    {
                        state.Buffer[i] = column[start + i];
                    }
                }

                state.Inner = writer.Begin(Source(column, start, length, state));
                return state;
            }
            catch
            {
                state.Dispose();
                throw;
            }
        }

        public override void WritePrefix(ClickHouseBinaryWriter output, IColumn values, int start, int length, IColumnWriteState state)
        {
            var own = (State)state;
            writer.WritePrefix(output, Source((IColumn<T>)values, start, length, own), own.Inner);
        }

        public override void Write(ClickHouseBinaryWriter output, IColumn values, int start, int length, IColumnWriteState state)
        {
            var own = (State)state;
            writer.Write(output, Source((IColumn<T>)values, start, length, own), own.Inner);
        }

        // The rows of the column, from the span, the stored values or the gathered buffer.
        private static ValueSource<T> Source(IColumn<T> column, int start, int length, State state)
        {
            ReadOnlySpan<T> rows = state.Buffer is not null
                ? state.Buffer.AsSpan(0, length)
                : column is ISpanColumn<T> span ? span.Span.Slice(start, length) : column.Values.Slice(start, length);
            return ValueSource<T>.Of(rows, start, column.Name);
        }

        private sealed class State : IColumnWriteState
        {
            public T[] Buffer { get; set; }

            public IColumnWriteState Inner { get; set; }

            public void Dispose()
            {
                Inner?.Dispose();
                Inner = null;
                WriteBuffers.Return(Buffer);
                Buffer = null;
            }
        }
    }
}
