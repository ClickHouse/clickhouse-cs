using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// Resolves how one POCO property value becomes one value of the CLR type a target column is written in, as an
/// <see cref="Expression"/> the gather builder inlines into its per-row loop. Resolution happens once per
/// (property, target column) pair at plan build, so a shape that cannot work fails before any row is gathered.
///
/// <para>
/// The read side has to ask the codec for its conversions, because a column decodes into the raw wire value (a
/// <c>DateTime</c> column reads as epoch seconds). The write side does not: a codec already accepts the
/// calendar types directly — <see cref="IColumnCodec.WritableElementTypes"/> lists them and
/// <see cref="IColumnCodec.WriteColumn"/> does the arithmetic — so the only conversions left here are the CLR-level
/// ones a C# cast would do anyway.
/// </para>
///
/// <para>
/// One wrapper is asymmetric today, which a caller sees as a property that reads but does not insert:
/// <c>LowCardinality</c> re-offers its inner codec's readable types but not its writable ones, so a
/// <c>LowCardinality(DateTime)</c> column reads into a <see cref="DateTime"/> property and is written only from the
/// raw epoch seconds. That is the wrapper's gap, not this class's — <c>Nullable</c> lifts both lists — and closing
/// it means giving the <c>LowCardinality</c> write path a shape per write type, as <c>Nullable</c> has.
/// </para>
///
/// <para>
/// The rules, first match winning, applied to each of the codec's writable types in its own preference order:
/// <list type="number">
/// <item>the property type is that write type — the identity;</item>
/// <item>a nullable property into a write type that cannot hold null — every row is required to have a value, and a
/// null throws naming the row, mirroring the read side's D6a;</item>
/// <item>a property into a nullable write type — null stays null, and a non-nullable property simply lifts;</item>
/// <item>a CLR <c>enum</c> property over the column's own ordinal type — a blind unchecked cast (D6b);</item>
/// <item>a property type assignable to the write type — a reference upcast, or the boxing a <c>Variant</c>/
/// <c>Dynamic</c> column's <see cref="object"/> surface asks for.</item>
/// </list>
/// </para>
///
/// <para>
/// Numeric conversion is deliberately absent, as it is on the read side (D6c): an <see cref="int"/> property does
/// not fill a <c>UInt64</c> column. Declining keeps a POCO that round-trips: every shape this accepts is a shape
/// <see cref="PocoValueProjection"/> can read back into the same property.
/// </para>
/// </summary>
internal static class PocoWriteConversion
{
    private static readonly MethodInfo NullNotWritableMethod =
        typeof(PocoWriteConversion).GetMethod(nameof(NullNotWritable), BindingFlags.Public | BindingFlags.Static);

    /// <summary>
    /// The CLR types a row-oriented insert can actually build a column of for this codec, in the codec's own
    /// preference order: its <see cref="IColumnCodec.WritableElementTypes"/>, minus those it turns down when offered
    /// as an array-backed column.
    ///
    /// <para>
    /// The list names the CLR element types the write path understands, but a codec whose writer needs a particular
    /// column implementation — <c>Nested</c> wants its own column — refuses a plain array-backed one whatever its
    /// element type. Probing with the empty column the plan would build runs the same test the insert itself runs,
    /// so such a column is reported at plan build rather than after the INSERT has gone out. An empty result means
    /// the type is reachable only through the columnar API.
    /// </para>
    /// </summary>
    /// <param name="codec">The target column's codec.</param>
    /// <returns>The write types, possibly none.</returns>
    public static IReadOnlyList<Type> AcceptedWriteTypes(IColumnCodec codec)
    {
        IReadOnlyList<Type> writable = codec.WritableElementTypes;
        var accepted = new List<Type>(writable.Count);
        for (int i = 0; i < writable.Count; i++)
        {
            if (Accepts(codec, writable[i]))
            {
                accepted.Add(writable[i]);
            }
        }

        return accepted;
    }

    /// <summary>
    /// Picks the CLR type a column is written in to carry <paramref name="memberType"/>: the codec's most preferred
    /// accepted write type the property converts to.
    /// </summary>
    /// <param name="codec">The target column's codec.</param>
    /// <param name="memberType">The property type the values come from.</param>
    /// <param name="writeType">The chosen write type, or null when none applies.</param>
    /// <returns>Whether a write type applies.</returns>
    public static bool TryChooseWriteType(IColumnCodec codec, Type memberType, out Type writeType)
    {
        IReadOnlyList<Type> accepted = AcceptedWriteTypes(codec);
        for (int i = 0; i < accepted.Count; i++)
        {
            if (CanConvert(memberType, accepted[i]))
            {
                writeType = accepted[i];
                return true;
            }
        }

        writeType = null;
        return false;
    }

    /// <summary>
    /// Builds the conversion from one property value to one value of <paramref name="writeType"/>. Only ever called
    /// for a pair <see cref="TryChooseWriteType"/> accepted.
    /// </summary>
    /// <param name="value">An expression yielding the property's value for one row.</param>
    /// <param name="writeType">The CLR type the column is written in.</param>
    /// <param name="targetTakesNull">Whether the target column has a NULL of its own — see
    /// <see cref="TakesNull"/>.</param>
    /// <param name="site">The column/property names a failed conversion reports at runtime.</param>
    /// <returns>An expression of type <paramref name="writeType"/>.</returns>
    public static Expression Convert(Expression value, Type writeType, bool targetTakesNull, PocoGatherSite site)
    {
        Type memberType = value.Type;

        // A reference-typed property holds null whatever its type, and a target with no NULL of its own has nowhere
        // to put one: the value would reach the codec, which faults part-way through a block and takes the
        // connection with it. Tested here instead, before any byte goes out, naming the row — the same contract a
        // nullable value-typed property gets below (D6a).
        if (!targetTakesNull && !memberType.IsValueType)
        {
            value = RequireNotNull(value, site);
        }

        if (memberType == writeType)
        {
            return value;
        }

        Type memberUnderlying = Nullable.GetUnderlyingType(memberType);
        Type writeUnderlying = Nullable.GetUnderlyingType(writeType);

        if (memberUnderlying is not null)
        {
            // Bound to a local first: HasValue and Value both read it, and it is a property access, which must not
            // run twice. A null row passes through as the write type's own null when the target has one to write
            // and the write type can carry it — a Nullable spelling, or the object a Variant or Dynamic column is
            // written from.
            bool nullSurvives = targetTakesNull && (writeUnderlying is not null || !writeType.IsValueType);
            ParameterExpression source = Expression.Variable(memberType, "nullable");
            Expression present = Convert(Expression.Property(source, "Value"), writeUnderlying ?? writeType, targetTakesNull, site);
            return Expression.Block(
                new[] { source },
                Expression.Assign(source, value),
                Expression.Condition(
                    Expression.Property(source, "HasValue"),
                    Lift(present, writeType),
                    nullSurvives ? Expression.Default(writeType) : ThrowNull(site, writeType)));
        }

        return writeUnderlying is not null
            ? Lift(Convert(value, writeUnderlying, targetTakesNull, site), writeType)
            : Expression.Convert(value, writeType);
    }

    /// <summary>
    /// Whether a column of this type can carry a row with no value. True exactly for the types that have a NULL of
    /// their own — <c>Nullable</c>, a nullable <c>LowCardinality</c>, <c>Variant</c> and <c>Dynamic</c> — which is
    /// what a null placeholder of <see langword="null"/> means: every other codec answers with the zero value it
    /// would encode instead, so a real null reaching it is an error rather than an absence.
    /// </summary>
    /// <param name="codec">The target column's codec.</param>
    /// <returns>Whether a null can be written to it.</returns>
    public static bool TakesNull(IColumnCodec codec) => codec.NullPlaceholder is null;

    /// <summary>
    /// The exception a gather throws when a null property value reaches a column that cannot hold one. Called from
    /// compiled code, so the constant parts arrive as arguments rather than being formatted at build time.
    /// </summary>
    /// <param name="columnName">The target column's name.</param>
    /// <param name="columnType">The target column's ClickHouse type.</param>
    /// <param name="pocoType">The POCO type's name.</param>
    /// <param name="memberName">The property name.</param>
    /// <param name="row">The zero-based row of the insert the null was found at.</param>
    /// <returns>The exception to throw.</returns>
    public static Exception NullNotWritable(string columnName, string columnType, string pocoType, string memberName, long row)
        => new InvalidOperationException(
            $"Property '{pocoType}.{memberName}' is null at row {row} of the insert, but it maps to column '{columnName}' ({columnType}), which cannot hold null. " +
            $"Make the column Nullable(...), or leave out the rows with no value.");

    /// <summary>
    /// Whether a plain cast converts one type to the other. The same rule the read side applies in reverse: a CLR
    /// <c>enum</c> over its own ordinal type, or a target the source is assignable to. Nullability is transparent —
    /// each side is unwrapped and the underlying types decide — because a null is carried, not converted.
    /// </summary>
    /// <param name="from">The property type.</param>
    /// <param name="to">The candidate write type.</param>
    /// <returns>Whether the conversion applies.</returns>
    private static bool CanConvert(Type from, Type to)
    {
        if (from == to)
        {
            return true;
        }

        Type fromUnderlying = Nullable.GetUnderlyingType(from);
        Type toUnderlying = Nullable.GetUnderlyingType(to);
        if (fromUnderlying is not null)
        {
            return CanConvert(fromUnderlying, toUnderlying ?? to);
        }

        if (toUnderlying is not null)
        {
            return CanConvert(from, toUnderlying);
        }

        // The ordinal cast is unchecked, matching (sbyte)value in C#: the column's declared labels are not consulted
        // (D6b), so an enum member the column does not name is written as its ordinal rather than being rejected.
        return (from.IsEnum && Enum.GetUnderlyingType(from) == to) || to.IsAssignableFrom(from);
    }

    /// <summary>Whether the codec accepts a column of <paramref name="writeType"/> built the way the plan builds one.</summary>
    /// <param name="codec">The target column's codec.</param>
    /// <param name="writeType">The candidate write type.</param>
    /// <returns>Whether the codec accepts such a column.</returns>
    private static bool Accepts(IColumnCodec codec, Type writeType) => codec.CanWriteElementType(writeType);

    private static Expression Lift(Expression value, Type target)
        => value.Type == target ? value : Expression.Convert(value, target);

    /// <summary>
    /// Wraps a reference-typed expression in a null test that throws. Bound to a local, because the test and the
    /// result both read it and it is a property access, which must not run twice.
    /// </summary>
    /// <param name="value">An expression of a reference type.</param>
    /// <param name="site">The column/property names the failure reports.</param>
    /// <returns>An expression of the same type, throwing where that would be null.</returns>
    private static Expression RequireNotNull(Expression value, PocoGatherSite site)
    {
        ParameterExpression source = Expression.Variable(value.Type, "reference");
        return Expression.Block(
            new[] { source },
            Expression.Assign(source, value),
            Expression.Condition(
                Expression.ReferenceEqual(source, Expression.Constant(null, value.Type)),
                ThrowNull(site, value.Type),
                source));
    }

    private static Expression ThrowNull(PocoGatherSite site, Type writeType) => Expression.Throw(
        Expression.Call(
            NullNotWritableMethod,
            Expression.Constant(site.ColumnName, typeof(string)),
            Expression.Constant(site.ColumnType, typeof(string)),
            Expression.Constant(site.PocoTypeName, typeof(string)),
            Expression.Constant(site.MemberName, typeof(string)),
            site.Row),
        writeType);
}

/// <summary>
/// What a gather needs to name in a runtime failure: which column, which property, and which row. The row is an
/// expression because it is the gather's loop counter, not a constant.
/// </summary>
internal readonly struct PocoGatherSite
{
    /// <summary>The target column's name.</summary>
    public string ColumnName { get; init; }

    /// <summary>The target column's ClickHouse type.</summary>
    public string ColumnType { get; init; }

    /// <summary>The POCO type's name.</summary>
    public string PocoTypeName { get; init; }

    /// <summary>The property's name.</summary>
    public string MemberName { get; init; }

    /// <summary>
    /// A <see cref="long"/> expression yielding the row being gathered, counted over the whole insert — the caller
    /// hands over all their rows at once, so this is the index into what they passed.
    /// </summary>
    public Expression Row { get; init; }
}
