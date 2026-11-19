using System;
using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// The reference path of the POCO write plan for the differential tests (<see cref="PocoWritePlan{T}.BuildLegacy"/>): a
/// compiled gather that converts each property value to a CLR write type of the codec's preferred write types, which the
/// codec then writes. Only the tests call it. The client builds its columns through <see cref="PocoColumnBuilderFactory"/>.
/// </summary>
internal static class LegacyPocoColumnBuilderFactory
{
    private static readonly MethodInfo CreateTypedMethod =
        typeof(LegacyPocoColumnBuilderFactory).GetMethod(nameof(CreateTyped), BindingFlags.NonPublic | BindingFlags.Static);

    /// <summary>Compiles the builder for one property and target column.</summary>
    /// <typeparam name="T">The row type.</typeparam>
    /// <param name="column">The target column from the server's sample block, for its name and type.</param>
    /// <param name="codec">The target type's codec, resolved as the write path resolves it.</param>
    /// <param name="member">The property the column is filled from; must be gettable.</param>
    /// <returns>The compiled builder.</returns>
    /// <exception cref="InvalidOperationException">The property's type cannot be written as the column's type.</exception>
    public static PocoColumnBuilder<T> Create<T>(IColumn column, IColumnCodec codec, PocoMember member)
        where T : class
    {
        if (!LegacyPocoWriteConversion.TryChooseWriteType(codec, member.MemberType, out Type writeType))
        {
            throw PocoWriteErrors.NotWritableAs(column, codec, member, typeof(T));
        }

        return (PocoColumnBuilder<T>)CreateTypedMethod
            .MakeGenericMethod(typeof(T), writeType)
            .Invoke(null, new object[] { column.Name, column.TypeName, member, PocoWriteConversion.TakesNull(codec) });
    }

    /// <summary>Compiles the typed property gather.</summary>
    /// <typeparam name="T">The row type.</typeparam>
    /// <typeparam name="TWrite">The CLR type the target column is written in.</typeparam>
    /// <param name="name">The target column's name.</param>
    /// <param name="typeName">The target column's ClickHouse type.</param>
    /// <param name="member">The property to gather.</param>
    /// <param name="targetTakesNull">Whether the target column can carry a row with no value.</param>
    /// <returns>The builder.</returns>
    private static PocoColumnBuilder<T> CreateTyped<T, TWrite>(string name, string typeName, PocoMember member, bool targetTakesNull)
        where T : class
    {
        ParameterExpression rows = Expression.Parameter(typeof(T[]), "rows");
        ParameterExpression start = Expression.Parameter(typeof(int), "start");
        ParameterExpression rowNumber = Expression.Parameter(typeof(int), "rowNumber");
        ParameterExpression count = Expression.Parameter(typeof(int), "count");
        ParameterExpression destination = Expression.Parameter(typeof(TWrite[]), "destination");
        ParameterExpression slot = Expression.Variable(typeof(int), "slot");

        var site = new PocoGatherSite
        {
            ColumnName = name,
            ColumnType = typeName,
            PocoTypeName = typeof(T).Name,
            MemberName = member.MemberName,

            // Name the row by its number in the insert, not by its position in the block. Evaluated only where
            // a conversion throws, so the addition costs nothing per row.
            Row = Expression.Convert(Expression.Add(rowNumber, slot), typeof(long)),
        };

        Expression value = Expression.Property(Expression.ArrayIndex(rows, Expression.Add(start, slot)), member.Property);
        Expression converted = LegacyPocoWriteConversion.Convert(value, typeof(TWrite), targetTakesNull, site);

        // slot = 0; while (slot < count) { destination[slot] = <converted from rows[start + slot]>; slot++; }
        LabelTarget done = Expression.Label("done");
        Expression body = Expression.Block(
            new[] { slot },
            Expression.Assign(slot, Expression.Constant(0)),
            Expression.Loop(
                Expression.IfThenElse(
                    Expression.LessThan(slot, count),
                    Expression.Block(
                        Expression.Assign(Expression.ArrayAccess(destination, slot), converted),
                        Expression.PostIncrementAssign(slot)),
                    Expression.Break(done)),
                done));

        var gather = Expression.Lambda<PocoColumnGather<T, TWrite>>(body, rows, start, rowNumber, count, destination).Compile();
        return new PocoColumnBuilder<T, TWrite>(name, typeName, gather);
    }
}
