using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// Runs the leaf converters in the differential tests, for every case whose column type is a leaf: reads through
/// <see cref="BoundReader{T}.Fill"/> and through a compiled <see cref="ColumnReader.Emit"/>, writes from the built
/// and read-back columns, and the answers of the derivation. Each arm must give the outcome of the old path.
/// </summary>
/// <remarks>
/// <para>
/// A refusal of the derivation is an <see cref="ArmRefusal"/>: its message has another text than the old one, and the
/// refusal texts have their own tests (<see cref="ConverterDerivationTests"/>).
/// </para>
/// <para>
/// The write arm does not run the <c>decoded</c> input: a decoded column is written from its own storage, by the
/// codec, and not through the converters (decision D3).
/// </para>
/// </remarks>
internal sealed class LeafConverterRegistration : IDifferentialRegistration
{
    // The facets of the case list that each arm covers. A new case of a leaf type changes these counts.
    internal const int ReadFacets = 209;
    internal const int WriteFacets = 196;
    internal const int CanReadFacets = ReadFacets;
    internal const int CanWriteFacets = 258;

    // FixedString from text: no codec writes it, and the leaf table has the pair (UTF-8, zero-padded to N).
    internal const int FixedStringTextWriteFacets = 1;

    private static readonly ConcurrentDictionary<string, bool> LeafTypes = new(StringComparer.Ordinal);

    /// <inheritdoc/>
    public void Register(DifferentialRegistry registry)
    {
        registry.Add(new FillArm(), ReadFacets);
        registry.Add(new EmitArm(), ReadFacets);
        registry.Add(new WriteLeafArm(), WriteFacets);
        registry.Add(new AnswerLeafArm(Tier.CanRead), CanReadFacets);
        registry.Add(new AnswerLeafArm(Tier.CanWrite), CanWriteFacets);

        // The leaf writes the text of a FixedString read back as text, so the bytes are those of the source column.
        registry.DeclareChanges(
            "Leaf converters: FixedString from string",
            facet => facet.Tier == Tier.Write && IsFixedStringFromText(facet),
            facet => Expectation.SameAsWrite(facet.Case.WriteInputs[0].Label),
            "The leaf table has FixedString from string (UTF-8, zero-padded); the codec refuses it. A later part makes it visible (D7).",
            FixedStringTextWriteFacets);
        registry.DeclareChanges(
            "Leaf converters: CanWrite FixedString from string",
            facet => facet.Tier == Tier.CanWrite && IsFixedStringFromText(facet),
            _ => Expectation.Answer(true),
            "The leaf table has FixedString from string; the codec refuses it. A later part makes it visible (D7).",
            FixedStringTextWriteFacets);
    }

    /// <summary>Whether the derivation of <paramref name="columnType"/> ends at a leaf, so a leaf arm runs it.</summary>
    /// <param name="columnType">The ClickHouse type of the case.</param>
    /// <returns>Whether the type is a leaf, or a <c>SimpleAggregateFunction</c> of a leaf.</returns>
    internal static bool IsLeafType(string columnType) => LeafTypes.GetOrAdd(columnType, static type =>
    {
        TypeNode node = TypeParser.Parse(type);
        while (Canonical(node.Name) == "SimpleAggregateFunction" && node.Arguments.Count == 2)
        {
            node = node.Arguments[1];
        }

        return LeafTable.TryGet(Canonical(node.Name), out _);
    });

    private static string Canonical(string name) => ColumnCodecRegistry.Default.TryCanonicalName(name, out string canonical) ? canonical : name;

    private static bool IsFixedStringFromText(Facet facet)
        => facet.Input.ElementType == typeof(string)
            && IsLeafType(facet.Case.ColumnType)
            && Canonical(TypeParser.Parse(facet.Case.ColumnType).Name) == "FixedString";

    private static ColumnReader<T> Reader<T>(Block block)
    {
        Derivation derivation = ConverterDerivation.Default.Derive(block[0].TypeName, block.Context, typeof(T), ConversionDirection.Read);
        return derivation.Succeeded ? (ColumnReader<T>)derivation.Converter : throw new ArmRefusal(derivation.Refusal);
    }

    // Reads a range through Bind, then Fill.
    private sealed class FillArm : ReadArm
    {
        public FillArm()
            : base("Leaf converters: Fill", Tier.ReadAs)
        {
        }

        public override bool Covers(Facet facet) => IsLeafType(facet.Case.ColumnType);

        public override RowReader<T> Bind<T>(Block block)
        {
            BoundReader<T> bound = Reader<T>(block).Bind(block[0]);
            return (start, count) =>
            {
                var values = new T[count];
                bound.Fill(start, values);
                return values;
            };
        }
    }

    // Reads a range through Emit, compiled into a loop as a POCO scatter compiles it.
    private sealed class EmitArm : ReadArm
    {
        // One compiled loop for each derived tree, so a tree is compiled once for all the cases.
        private static readonly ConditionalWeakTable<ColumnReader, Delegate> Loops = new();

        public EmitArm()
            : base("Leaf converters: Emit", Tier.ReadAs)
        {
        }

        public override bool Covers(Facet facet) => IsLeafType(facet.Case.ColumnType);

        public override RowReader<T> Bind<T>(Block block)
        {
            ColumnReader<T> reader = Reader<T>(block);
            var loop = (Action<IColumn, int, T[]>)Loops.GetValue(reader, static tree => Compile((ColumnReader<T>)tree));
            IColumn column = block[0];
            return (start, count) =>
            {
                var values = new T[count];
                loop(column, start, values);
                return values;
            };
        }

        private static Action<IColumn, int, T[]> Compile<T>(ColumnReader<T> reader)
        {
            ParameterExpression column = Expression.Parameter(typeof(IColumn), "column");
            ParameterExpression start = Expression.Parameter(typeof(int), "start");
            ParameterExpression destination = Expression.Parameter(typeof(T[]), "destination");
            ParameterExpression i = Expression.Variable(typeof(int), "i");
            ParameterExpression row = Expression.Variable(typeof(int), "row");

            var scope = new EmitScope();
            Expression value = reader.Emit(column, row, scope);
            LabelTarget done = Expression.Label("done");
            var body = new List<Expression>(scope.Setup)
            {
                Expression.Assign(i, Expression.Constant(0)),
                Expression.Loop(
                    Expression.IfThenElse(
                        Expression.LessThan(i, Expression.ArrayLength(destination)),
                        Expression.Block(
                            Expression.Assign(row, Expression.Add(start, i)),
                            Expression.Assign(Expression.ArrayAccess(destination, i), value),
                            Expression.PostIncrementAssign(i)),
                        Expression.Break(done)),
                    done),
            };

            var locals = new List<ParameterExpression>(scope.Locals) { i, row };
            return Expression.Lambda<Action<IColumn, int, T[]>>(Expression.Block(locals, body), column, start, destination).Compile();
        }
    }

    // Writes a slice through Begin, WritePrefix and Write, from one span.
    private sealed class WriteLeafArm : WriteArm
    {
        public WriteLeafArm()
            : base("Leaf converters: Write")
        {
        }

        public override bool Covers(Facet facet) => facet.Input.Kind != WriteInputKind.Decoded && IsLeafType(facet.Case.ColumnType);

        public override SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context)
        {
            Derivation derivation = ConverterDerivation.Default.Derive(columnType, context, typeof(T), ConversionDirection.Write);
            if (!derivation.Succeeded)
            {
                throw new ArmRefusal(derivation.Refusal);
            }

            var writer = (ColumnWriter<T>)derivation.Converter;
            return (output, start, length) =>
            {
                // A caller's column gives its values through the indexer; the values span of a view can throw.
                T[] values = ArrayPool<T>.Shared.Rent(Math.Max(length, 1));
                try
                {
                    for (int i = 0; i < length; i++)
                    {
                        values[i] = column[start + i];
                    }

                    ConverterHarness.WriteAll(writer, output, ValueSource<T>.Of(values.AsSpan(0, length), start));
                }
                finally
                {
                    ArrayPool<T>.Shared.Return(values, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<T>());
                }
            };
        }
    }

    // CanRead and CanWrite: whether the derivation succeeds, with the context of ClickHouseTcpTypes.
    private sealed class AnswerLeafArm : AnswerArm
    {
        public AnswerLeafArm(Tier tier)
            : base($"Leaf converters: {tier}", tier)
        {
        }

        public override bool Covers(Facet facet) => IsLeafType(facet.Case.ColumnType);

        public override bool Answer(string columnType, Type elementType)
            => ConverterDerivation.Default.Derive(
                columnType,
                ResolveContext.ForWrite,
                elementType,
                Tier == Tier.CanRead ? ConversionDirection.Read : ConversionDirection.Write).Succeeded;
    }
}
