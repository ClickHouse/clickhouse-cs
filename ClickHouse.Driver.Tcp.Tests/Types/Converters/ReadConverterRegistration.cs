using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// Runs the read combinators and the read rules of D6 in the differential tests, for every case whose column type is
/// not a leaf (<see cref="LeafConverterRegistration"/> runs the leaf cases): reads through
/// <see cref="BoundReader{T}.Fill"/> and through a compiled <see cref="ColumnReader.Emit"/>, and the answer of the
/// derivation. It declares the outcome of the readings that fail at a NULL (<see cref="NullFailures"/>), whose tail the
/// arms read in two ways that are both right.
/// </summary>
/// <remarks>
/// A refusal of the derivation is an <see cref="ArmRefusal"/>, as in <see cref="LeafConverterRegistration"/>. A NULL
/// that a read rule meets (<see cref="NullValueException"/>) is reported with the text of <c>Block.ReadAs</c>
/// (<see cref="DerivedColumn.NullFailure"/>), for the row of the column that the reader reads.
/// </remarks>
internal sealed class ReadConverterRegistration : IDifferentialRegistration
{
    // The ReadAs and CanRead facets of the case list whose column type is not a leaf. A new case changes these counts.
    internal const int CompositeReadFacets = 358;

    /// <summary>
    /// The readings of a nullable column as a value type that cannot hold NULL (a read rule of D6): (case, target). The
    /// sample column has its first NULL at row 1, and the first NULL of its tail [2, 5) at row 4.
    /// </summary>
    internal static readonly (string CaseId, Type Target)[] NullFailures =
    {
        ("ColumnReadProjection: Nullable(Time64(3))", typeof(TimeSpan)),
        ("ColumnReadProjection: Nullable(DateTime('UTC'))", typeof(DateTime)),
        ("ColumnReadProjection: Nullable(DateTime('UTC'))", typeof(uint)),
        ("ColumnReadProjection: LowCardinality(Nullable(DateTime('UTC')))", typeof(DateTime)),
        ("ColumnReadProjection: LowCardinality(Nullable(DateTime('UTC')))", typeof(DateTimeOffset)),
    };

    /// <inheritdoc/>
    public void Register(DifferentialRegistry registry)
    {
        registry.Add(new FillArm(), CompositeReadFacets);
        registry.Add(new EmitArm(), CompositeReadFacets);
        registry.Add(new CanReadArm(), CompositeReadFacets);
        registry.DeclareOutcomes(
            "ReadAs of a nullable column as a type that cannot hold NULL",
            facet => facet.Tier == Tier.ReadAs && NullFailures.Contains((facet.Case.Id, facet.Target)),
            facet => FailsAtNull(facet.Case.ColumnType, facet.Target),
            "Block.ReadAs converts the whole column on the first access, so its read of the tail fails at the first NULL of the column; a reader of the tail alone fails at the first NULL of the tail.",
            NullFailures.Length);
    }

    /// <summary>
    /// The failure of a NULL in a column read as a type that cannot hold NULL. A read of all rows fails at row 1, the
    /// first NULL of the sample column. A read of the tail fails at the first NULL that the reader meets: row 4 for a
    /// reader of the tail alone, row 1 for <c>Block.ReadAs</c>, which converts the whole column on the first access.
    /// </summary>
    private static Expectation FailsAtNull(string columnType, Type target)
    {
        string column = $"Column 'value' ({columnType}) has NULL at row ";
        string rest = $", and the target type {target} cannot hold NULL. Read the column as a nullable type, or remove the NULL values in the query.";
        return Expectation.ForRows(
            Expectation.Fails<InvalidOperationException>(column + "1" + rest),
            Expectation.Fails<InvalidOperationException>(column, rest));
    }

    private static bool IsComposite(Facet facet) => !LeafConverterRegistration.IsLeafType(facet.Case.ColumnType);

    private static ColumnReader<T> Reader<T>(Block block)
    {
        Derivation derivation = ConverterDerivation.Default.Derive(block[0].TypeName, block.Context, typeof(T), ConversionDirection.Read);
        return derivation.Succeeded ? (ColumnReader<T>)derivation.Converter : throw new ArmRefusal(derivation.Refusal);
    }

    // Runs a read and gives a NULL failure the message of Block.ReadAs.
    private static T[] Read<T>(IColumn column, int count, Action<T[]> read)
    {
        var values = new T[count];
        try
        {
            read(values);
        }
        catch (NullValueException e)
        {
            throw DerivedColumn.NullFailure(column, typeof(T), e.Row, e);
        }

        return values;
    }

    private sealed class FillArm : ReadArm
    {
        public FillArm()
            : base("Composite read converters: Fill", Tier.ReadAs)
        {
        }

        public override bool Covers(Facet facet) => IsComposite(facet);

        public override RowReader<T> Bind<T>(Block block)
        {
            IColumn column = block[0];
            BoundReader<T> bound = Reader<T>(block).Bind(column);
            return (start, count) => Read<T>(column, count, values => bound.Fill(start, values));
        }
    }

    private sealed class EmitArm : ReadArm
    {
        // One compiled loop for each derived tree, so a tree is compiled once for all the cases.
        private static readonly ConditionalWeakTable<ColumnReader, Delegate> Loops = new();

        public EmitArm()
            : base("Composite read converters: Emit", Tier.ReadAs)
        {
        }

        public override bool Covers(Facet facet) => IsComposite(facet);

        public override RowReader<T> Bind<T>(Block block)
        {
            ColumnReader<T> reader = Reader<T>(block);
            var loop = (Action<IColumn, int, T[]>)Loops.GetValue(reader, static tree => Compile((ColumnReader<T>)tree));
            IColumn column = block[0];
            return (start, count) => Read<T>(column, count, values => loop(column, start, values));
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

    // CanRead: whether the derivation succeeds, with the context of ClickHouseTcpTypes.
    private sealed class CanReadArm : AnswerArm
    {
        public CanReadArm()
            : base("Composite read converters: CanRead", Tier.CanRead)
        {
        }

        public override bool Covers(Facet facet) => IsComposite(facet);

        public override bool Answer(string columnType, Type elementType)
            => ConverterDerivation.Default.Derive(columnType, ResolveContext.ForWrite, elementType, ConversionDirection.Read).Succeeded;
    }
}
