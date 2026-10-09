using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Copies one decoded column into one property across a range of POCO rows.
/// </summary>
/// <typeparam name="T">The POCO type.</typeparam>
/// <param name="column">The column to read.</param>
/// <param name="rows">The rows to scatter into; at least <paramref name="rowCount"/> long, each already constructed.</param>
/// <param name="start">The first column row to read; destination rows start at zero.</param>
/// <param name="rowCount">The number of rows to fill.</param>
/// <param name="rowOffset">The result-wide index of <c>rows[0]</c>, used in failures.</param>
internal delegate void PocoColumnScatter<in T>(IColumn column, T[] rows, int start, int rowCount, long rowOffset);

/// <summary>
/// Makes the scatter that fills one POCO property from one column. The converter derivation gives the tree that reads
/// the column type as the property type, with the read rules of POCO mapping (<see cref="ReadRules"/>). The scatter
/// runs that tree in one of two ways (<see cref="PocoScatterTier"/>): as one compiled loop for the column, or as a
/// bulk read followed by a setter delegate for each row.
/// </summary>
/// <remarks>
/// A tree reads the decoded shape of its column type (for example the null map and the inner column of a
/// <c>Nullable</c> column), which every column that a codec decodes has. A NULL that a read rule finds in a column read
/// as a value type stops the read with <see cref="NullValueException"/>, which holds the row of the column. The scatter
/// gives it the message of POCO mapping, which names the row of the result.
/// </remarks>
internal static class PocoColumnScatterFactory
{
    private static readonly MethodInfo CreateTypedMethod =
        typeof(PocoColumnScatterFactory).GetMethod(nameof(CreateTyped), BindingFlags.NonPublic | BindingFlags.Static);

    /// <summary>Makes the scatter for one column into one property.</summary>
    /// <typeparam name="T">The POCO type.</typeparam>
    /// <param name="column">A column of the shape the plan was built for, for its name and type.</param>
    /// <param name="codec">The column's codec, resolved the way the read resolved it.</param>
    /// <param name="member">The property the column maps to; must be settable.</param>
    /// <param name="derivation">The converter derivation of the codec registry that decoded the column.</param>
    /// <param name="context">The resolution context that decoded the column.</param>
    /// <param name="forcedTier">A tier to use regardless of the runtime, or null to choose one (<see cref="SelectTier"/>).</param>
    /// <returns>The scatter.</returns>
    /// <exception cref="InvalidOperationException">The column's values cannot be read as the property's type, or the
    /// column does not surface its codec's element type.</exception>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    public static PocoColumnScatter<T> Create<T>(
        IColumn column,
        IColumnCodec codec,
        PocoMember member,
        ConverterDerivation derivation,
        in ResolveContext context,
        PocoScatterTier? forcedTier)
        where T : class
    {
        if (!typeof(IColumn<>).MakeGenericType(codec.ElementType).IsInstanceOfType(column))
        {
            throw PocoReadErrors.NotSurfacingItsElementType(column, codec);
        }

        Derivation derived = derivation.Derive(column.TypeName, in context, member.MemberType, ConversionDirection.Read);
        if (!derived.Succeeded)
        {
            throw PocoReadErrors.NotReadableAs(column, codec, member, typeof(T));
        }

        return ForReader<T>((ColumnReader)derived.Converter, column, member, SelectTier(forcedTier));
    }

    /// <summary>Makes the scatter that runs one converter tree into one property, in one tier.</summary>
    /// <typeparam name="T">The POCO type.</typeparam>
    /// <param name="reader">The tree, which reads values of the property's type.</param>
    /// <param name="column">A column of the shape the plan was built for, for its name and type.</param>
    /// <param name="member">The property the column maps to; must be settable.</param>
    /// <param name="tier">The tier.</param>
    /// <returns>The scatter.</returns>
    [RequiresDynamicCode("Closes generic types at run time, and the Emit tier compiles an expression tree.")]
    internal static PocoColumnScatter<T> ForReader<T>(ColumnReader reader, IColumn column, PocoMember member, PocoScatterTier tier)
        where T : class
    {
        var site = new Site(column.Name, column.TypeName, typeof(T).Name, member.MemberName, member.MemberType.ToString());
        return (PocoColumnScatter<T>)CreateTypedMethod
            .MakeGenericMethod(typeof(T), member.MemberType)
            .Invoke(null, BindingFlags.DoNotWrapExceptions, binder: null, new object[] { reader, member.Property, site, tier }, culture: null);
    }

    /// <summary>
    /// Uses the forced tier, or <see cref="PocoScatterTier.Emit"/> when the runtime compiles expression trees, else
    /// <see cref="PocoScatterTier.Fill"/>.
    /// </summary>
    /// <param name="forcedTier">A tier to use in place of the choice, or null to choose.</param>
    /// <returns>The tier.</returns>
    internal static PocoScatterTier SelectTier(PocoScatterTier? forcedTier)
        => forcedTier ?? (RuntimeFeature.IsDynamicCodeCompiled ? PocoScatterTier.Emit : PocoScatterTier.Fill);

    [RequiresDynamicCode("Compiles an expression tree.")]
    private static PocoColumnScatter<T> CreateTyped<T, TProp>(ColumnReader<TProp> reader, PropertyInfo property, Site site, PocoScatterTier tier)
        where T : class
        => tier == PocoScatterTier.Fill
            ? new FillScatter<T, TProp>(reader, (Action<T, TProp>)Delegate.CreateDelegate(typeof(Action<T, TProp>), property.SetMethod), site).Run
            : new EmitScatter<T, TProp>(CompileLoop<T, TProp>(reader, property), site).Run;

    // for (i = 0; i < count; i++) { row = start + i; rows[i].Property = <the tree at row>; }, after the setup of the
    // tree, which runs once for each call.
    [RequiresDynamicCode("Compiles an expression tree.")]
    private static Action<IColumn, T[], int, int> CompileLoop<T, TProp>(ColumnReader<TProp> reader, PropertyInfo property)
    {
        ParameterExpression column = Expression.Parameter(typeof(IColumn), "column");
        ParameterExpression rows = Expression.Parameter(typeof(T[]), "rows");
        ParameterExpression start = Expression.Parameter(typeof(int), "start");
        ParameterExpression count = Expression.Parameter(typeof(int), "count");
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
                    Expression.LessThan(i, count),
                    Expression.Block(
                        Expression.Assign(row, Expression.Add(start, i)),
                        Expression.Assign(Expression.Property(Expression.ArrayIndex(rows, i), property), value),
                        Expression.PostIncrementAssign(i)),
                    Expression.Break(done)),
                done),
        };

        var locals = new List<ParameterExpression>(scope.Locals) { i, row };
        return Expression.Lambda<Action<IColumn, T[], int, int>>(Expression.Block(locals, body), column, rows, start, count).Compile();
    }

    /// <summary>The names that a NULL failure of one scatter gives.</summary>
    private sealed class Site
    {
        private readonly string columnName;
        private readonly string columnType;
        private readonly string pocoTypeName;
        private readonly string memberName;
        private readonly string memberType;

        public Site(string columnName, string columnType, string pocoTypeName, string memberName, string memberType)
        {
            this.columnName = columnName;
            this.columnType = columnType;
            this.pocoTypeName = pocoTypeName;
            this.memberName = memberName;
            this.memberType = memberType;
        }

        // The reader names the row of the column. rows[0] holds column row start, and is row rowOffset of the result.
        public Exception NullFailure(NullValueException failure, int start, long rowOffset)
            => PocoReadErrors.NullNotAssignable(columnName, columnType, pocoTypeName, memberName, memberType, rowOffset + (failure.Row - start));
    }

    /// <summary>The scatter of <see cref="PocoScatterTier.Emit"/>: one compiled loop.</summary>
    private sealed class EmitScatter<T, TProp>
    {
        private readonly Action<IColumn, T[], int, int> loop;
        private readonly Site site;

        public EmitScatter(Action<IColumn, T[], int, int> loop, Site site)
        {
            this.loop = loop;
            this.site = site;
        }

        public void Run(IColumn column, T[] rows, int start, int rowCount, long rowOffset)
        {
            try
            {
                loop(column, rows, start, rowCount);
            }
            catch (NullValueException failure)
            {
                throw site.NullFailure(failure, start, rowOffset);
            }
        }
    }

    /// <summary>
    /// The scatter of <see cref="PocoScatterTier.Fill"/>: a bulk read of the rows into a pooled buffer, then the setter
    /// of the property for each row. It compiles no code.
    /// </summary>
    private sealed class FillScatter<T, TProp>
    {
        private readonly ColumnReader<TProp> reader;
        private readonly Action<T, TProp> set;
        private readonly Site site;

        public FillScatter(ColumnReader<TProp> reader, Action<T, TProp> set, Site site)
        {
            this.reader = reader;
            this.set = set;
            this.site = site;
        }

        public void Run(IColumn column, T[] rows, int start, int rowCount, long rowOffset)
        {
            // Bind is cheap for every reader. A LowCardinality reader keeps the converted dictionary of each column, so
            // the windows of one block convert it once.
            BoundReader<TProp> bound = reader.Bind(column);
            TProp[] values = ArrayPool<TProp>.Shared.Rent(rowCount);
            try
            {
                try
                {
                    bound.Fill(start, values.AsSpan(0, rowCount));
                }
                catch (NullValueException failure)
                {
                    throw site.NullFailure(failure, start, rowOffset);
                }

                Action<T, TProp> setter = set;
                for (int i = 0; i < rowCount; i++)
                {
                    setter(rows[i], values[i]);
                }
            }
            finally
            {
                ArrayPool<TProp>.Shared.Return(values, clearArray: RuntimeHelpers.IsReferenceOrContainsReferences<TProp>());
            }
        }
    }
}
