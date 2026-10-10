using System;
using System.Collections.Concurrent;
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
/// The converter write reads the values of the column from its span when the column has one
/// (<see cref="ISpanColumn{T}"/>), from its stored values (<see cref="IStoredValuesColumn"/>), or else through its indexer
/// into a pooled buffer: <see cref="IColumn{T}.Values"/> of a view can throw or compute every row of the column. The
/// buffer lives in the write state, so the prefix and the body read the same values.
/// </remarks>
internal abstract class InsertColumnWrite
{
    // One converter write for each cached tree. The write holds no state, so every insert of the tree shares it.
    private static readonly ConditionalWeakTable<ColumnWriter, InsertColumnWrite> ConverterWrites = new();

    private static readonly ConcurrentDictionary<Type, Func<ColumnWriter, InsertColumnWrite>> Factories = new();

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

    /// <summary>The write of a column through its codec, as the codec accepts it.</summary>
    /// <param name="codec">The codec of the target type.</param>
    /// <returns>The write.</returns>
    public static InsertColumnWrite ThroughCodec(IColumnCodec codec) => new CodecWrite(codec);

    /// <summary>
    /// The write of <paramref name="values"/> as <paramref name="typeName"/>: through the codec when it writes the column
    /// from its storage, else through the converter tree of the column's CLR type. A column of no single CLR type (it
    /// implements <see cref="IColumn{T}"/> more than once) goes to the codec when the codec accepts it.
    /// </summary>
    /// <param name="codec">The codec of the target type, resolved with <paramref name="context"/>.</param>
    /// <param name="values">The column that the caller gives.</param>
    /// <param name="typeName">The target type.</param>
    /// <param name="context">The resolution context of the target (its session timezone).</param>
    /// <param name="derivation">The converter derivation of the codec registry.</param>
    /// <returns>The write, or <see langword="null"/> when the column cannot be written as the type.</returns>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    public static InsertColumnWrite For(IColumnCodec codec, IColumn values, string typeName, in ResolveContext context, ConverterDerivation derivation)
    {
        if (codec.WritesFromStorage(values))
        {
            return ThroughCodec(codec);
        }

        Type elementType;
        try
        {
            elementType = values.ElementType;
        }
        catch (InvalidOperationException)
        {
            // A column that implements IColumn<> zero or several times has no single CLR type to derive a tree for, so
            // the codec decides whether it writes the column.
            return codec.CanWrite(values) ? ThroughCodec(codec) : null;
        }

        Derivation derived = derivation.Derive(typeName, in context, elementType, ConversionDirection.Write);
        if (!derived.Succeeded)
        {
            return null;
        }

        var tree = (ColumnWriter)derived.Converter;
        return ConverterWrites.GetValue(tree, static writer => Factories.GetOrAdd(writer.ValueType, BuildFactory)(writer));
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
