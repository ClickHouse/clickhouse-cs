using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Fills one target-column buffer from a property of each row in a range.
/// </summary>
/// <typeparam name="T">The row type.</typeparam>
/// <typeparam name="TWrite">The CLR type the target column is written in.</typeparam>
/// <param name="rows">The rows to gather from; holds the range at <paramref name="start"/>, each non-null.</param>
/// <param name="start">The index in <paramref name="rows"/> the range begins at.</param>
/// <param name="rowNumber">The insert row number of that first row, for error messages.</param>
/// <param name="count">The number of rows to gather.</param>
/// <param name="destination">The buffer to fill from index zero; at least <paramref name="count"/> long.</param>
internal delegate void PocoColumnGather<in T, in TWrite>(T[] rows, int start, int rowNumber, int count, TWrite[] destination);

/// <summary>
/// Gathers one target column, one block at a time, into a buffer it rents for the whole insert.
/// </summary>
/// <typeparam name="T">The row type.</typeparam>
internal abstract class PocoColumnBuilder<T>
    where T : class
{
    /// <summary>Initializes the target column's identity.</summary>
    /// <param name="name">The target column's name.</param>
    /// <param name="typeName">The target column's ClickHouse type.</param>
    protected PocoColumnBuilder(string name, string typeName)
    {
        Name = name;
        TypeName = typeName;
    }

    /// <summary>The target column's name.</summary>
    protected string Name { get; }

    /// <summary>The target column's ClickHouse type.</summary>
    protected string TypeName { get; }

    /// <summary>Rents this column's gather destination, sized for one block.</summary>
    /// <param name="blockRows">The most rows one block will hold.</param>
    /// <returns>The column, owning a pooled buffer it returns when disposed.</returns>
    public abstract IColumn CreateColumn(int blockRows);

    /// <summary>Gathers one block's values into a column from <see cref="CreateColumn"/>.</summary>
    /// <param name="column">The column to fill, from this builder's <see cref="CreateColumn"/>.</param>
    /// <param name="rows">The rows, each non-null.</param>
    /// <param name="start">The index in <paramref name="rows"/> the block begins at.</param>
    /// <param name="rowNumber">The insert row number of that first row, for error messages.</param>
    /// <param name="count">The number of rows to gather.</param>
    /// <exception cref="InvalidOperationException">A row has no value for a column that cannot hold null.</exception>
    public abstract void Gather(IColumn column, T[] rows, int start, int rowNumber, int count);
}

/// <summary>
/// Gathers rows into a <see cref="PocoGatherColumn{T}"/>'s reused <typeparamref name="TWrite"/> buffer.
/// </summary>
/// <typeparam name="T">The row type.</typeparam>
/// <typeparam name="TWrite">The CLR type the target column is written in.</typeparam>
internal sealed class PocoColumnBuilder<T, TWrite> : PocoColumnBuilder<T>
    where T : class
{
    private readonly PocoColumnGather<T, TWrite> gather;

    /// <summary>Initializes the builder over a compiled gather.</summary>
    /// <param name="name">The target column's name.</param>
    /// <param name="typeName">The target column's ClickHouse type.</param>
    /// <param name="gather">The gather filling the write buffer from the rows.</param>
    public PocoColumnBuilder(string name, string typeName, PocoColumnGather<T, TWrite> gather)
        : base(name, typeName) => this.gather = gather;

    /// <inheritdoc/>
    public override IColumn CreateColumn(int blockRows) => new PocoGatherColumn<TWrite>(Name, TypeName, blockRows);

    /// <inheritdoc/>
    public override void Gather(IColumn column, T[] rows, int start, int rowNumber, int count)
    {
        var destination = (PocoGatherColumn<TWrite>)column;

        // Publish the rows only once they are all written, so a failed gather leaves no half-filled range
        // readable through the column.
        destination.Publish(0);
        gather(rows, start, rowNumber, count, destination.Buffer);
        destination.Publish(count);
    }
}

/// <summary>
/// Makes the builder for each property-to-column mapping of a <see cref="PocoWritePlan{T}"/>. The converter derivation
/// decides whether the column type is written from the property type, with the write rules of POCO mapping
/// (<see cref="WriteRules"/>), and the insert writes the gathered values through the same tree. So the gather only copies
/// each property value into the buffer of its column, in one of two ways (<see cref="PocoGatherTier"/>).
/// </summary>
/// <remarks>
/// The gather finds a null that the column cannot hold before the block is written, so the error names the property and
/// the row, and the connection stays at a block boundary. A reference type is null where the column type has no NULL
/// (<see cref="PocoWriteConversion.TakesNull"/>); a nullable value type is null where the tree writes its value type
/// (<see cref="NonNullWriter{T}"/>).
/// </remarks>
internal static class PocoColumnBuilderFactory
{
    private static readonly MethodInfo CreateTypedMethod =
        typeof(PocoColumnBuilderFactory).GetMethod(nameof(CreateTyped), BindingFlags.NonPublic | BindingFlags.Static);

    private static readonly MethodInfo NullNotWritableMethod =
        typeof(PocoWriteErrors).GetMethod(nameof(PocoWriteErrors.NullNotWritable), BindingFlags.Public | BindingFlags.Static);

    /// <summary>Makes the builder for one property and target column.</summary>
    /// <typeparam name="T">The row type.</typeparam>
    /// <param name="column">The target column from the server's sample block, for its name and type.</param>
    /// <param name="codec">The target type's codec, resolved as the write path resolves it.</param>
    /// <param name="member">The property the column is filled from; must be gettable.</param>
    /// <param name="derivation">The converter derivation of the codec registry of the sample block.</param>
    /// <param name="context">The resolution context of the sample block.</param>
    /// <param name="forcedTier">A tier to use regardless of the runtime, or null to choose one (<see cref="SelectTier"/>).</param>
    /// <returns>The builder.</returns>
    /// <exception cref="InvalidOperationException">The property's type cannot be written as the column's type.</exception>
    [RequiresDynamicCode("A converter over a CLR type that is known only at run time closes generic types at run time.")]
    public static PocoColumnBuilder<T> Create<T>(
        IColumn column,
        IColumnCodec codec,
        PocoMember member,
        ConverterDerivation derivation,
        in ResolveContext context,
        PocoGatherTier? forcedTier)
        where T : class
    {
        Derivation derived = derivation.Derive(column.TypeName, in context, member.MemberType, ConversionDirection.Write);
        if (!derived.Succeeded)
        {
            throw PocoWriteErrors.NotWritableAs(column, codec, member, typeof(T));
        }

        bool refusesNull = member.MemberType.IsValueType
            ? derived.Converter.GetType() is { IsGenericType: true } writer && writer.GetGenericTypeDefinition() == typeof(NonNullWriter<>)
            : !PocoWriteConversion.TakesNull(codec);

        return (PocoColumnBuilder<T>)CreateTypedMethod
            .MakeGenericMethod(typeof(T), member.MemberType)
            .Invoke(null, BindingFlags.DoNotWrapExceptions, binder: null, new object[] { column.Name, column.TypeName, member, refusesNull, SelectTier(forcedTier) }, culture: null);
    }

    /// <summary>
    /// Uses the forced tier, or <see cref="PocoGatherTier.Compiled"/> when the runtime compiles expression trees, else
    /// <see cref="PocoGatherTier.Delegate"/>.
    /// </summary>
    /// <param name="forcedTier">A tier to use in place of the choice, or null to choose.</param>
    /// <returns>The tier.</returns>
    internal static PocoGatherTier SelectTier(PocoGatherTier? forcedTier)
        => forcedTier ?? (RuntimeFeature.IsDynamicCodeCompiled ? PocoGatherTier.Compiled : PocoGatherTier.Delegate);

    [RequiresDynamicCode("The Compiled tier compiles an expression tree.")]
    private static PocoColumnBuilder<T> CreateTyped<T, TMember>(string name, string typeName, PocoMember member, bool refusesNull, PocoGatherTier tier)
        where T : class
    {
        PocoColumnGather<T, TMember> gather = tier == PocoGatherTier.Delegate
            ? DelegateGather<T, TMember>(name, typeName, member, refusesNull)
            : CompileGather<T, TMember>(name, typeName, member, refusesNull);
        return new PocoColumnBuilder<T, TMember>(name, typeName, gather);
    }

    // for (slot = 0; slot < count; slot++) { value = rows[start + slot].Property; <check null>; destination[slot] = value; }
    [RequiresDynamicCode("Compiles an expression tree.")]
    private static PocoColumnGather<T, TMember> CompileGather<T, TMember>(string name, string typeName, PocoMember member, bool refusesNull)
        where T : class
    {
        ParameterExpression rows = Expression.Parameter(typeof(T[]), "rows");
        ParameterExpression start = Expression.Parameter(typeof(int), "start");
        ParameterExpression rowNumber = Expression.Parameter(typeof(int), "rowNumber");
        ParameterExpression count = Expression.Parameter(typeof(int), "count");
        ParameterExpression destination = Expression.Parameter(typeof(TMember[]), "destination");
        ParameterExpression slot = Expression.Variable(typeof(int), "slot");
        ParameterExpression value = Expression.Variable(typeof(TMember), "value");

        var step = new List<Expression>
        {
            Expression.Assign(value, Expression.Property(Expression.ArrayIndex(rows, Expression.Add(start, slot)), member.Property)),
        };

        if (refusesNull)
        {
            // Name the row by its number in the insert, not by its position in the block.
            Expression isNull = typeof(TMember).IsValueType
                ? Expression.Not(Expression.Property(value, nameof(Nullable<int>.HasValue)))
                : Expression.ReferenceEqual(value, Expression.Constant(null, typeof(TMember)));
            step.Add(Expression.IfThen(
                isNull,
                Expression.Throw(Expression.Call(
                    NullNotWritableMethod,
                    Expression.Constant(name, typeof(string)),
                    Expression.Constant(typeName, typeof(string)),
                    Expression.Constant(typeof(T).Name, typeof(string)),
                    Expression.Constant(member.MemberName, typeof(string)),
                    Expression.Convert(Expression.Add(rowNumber, slot), typeof(long))))));
        }

        step.Add(Expression.Assign(Expression.ArrayAccess(destination, slot), value));
        step.Add(Expression.PostIncrementAssign(slot));

        LabelTarget done = Expression.Label("done");
        Expression body = Expression.Block(
            new[] { slot, value },
            Expression.Assign(slot, Expression.Constant(0)),
            Expression.Loop(
                Expression.IfThenElse(Expression.LessThan(slot, count), Expression.Block(step), Expression.Break(done)),
                done));

        return Expression.Lambda<PocoColumnGather<T, TMember>>(body, rows, start, rowNumber, count, destination).Compile();
    }

    // The same loop with a getter delegate, which compiles no code.
    private static PocoColumnGather<T, TMember> DelegateGather<T, TMember>(string name, string typeName, PocoMember member, bool refusesNull)
        where T : class
    {
        var get = (Func<T, TMember>)Delegate.CreateDelegate(typeof(Func<T, TMember>), member.Property.GetMethod);
        string pocoType = typeof(T).Name;
        string memberName = member.MemberName;
        return (rows, start, rowNumber, count, destination) =>
        {
            for (int slot = 0; slot < count; slot++)
            {
                TMember value = get(rows[start + slot]);
                if (refusesNull && value is null)
                {
                    throw PocoWriteErrors.NullNotWritable(name, typeName, pocoType, memberName, (long)rowNumber + slot);
                }

                destination[slot] = value;
            }
        };
    }
}
