using System;
using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Poco;

/// <summary>
/// Builds box-free read expressions for the POCO read (materialization) fast path (#509).
///
/// The default read path decodes each column via <see cref="ClickHouseType.Read(ExtendedBinaryReader)"/>
/// — which returns <see cref="object"/> and so boxes every value-type column, once per value, per row —
/// stores it into the reader's shared <c>object[]</c> row buffer, and then unboxes it inside a compiled
/// <c>Action&lt;T,object&gt;</c> setter (<c>MapTo&lt;T&gt;</c>). For columns a type can decode straight into the
/// target CLR type, we instead compile a fused delegate that reads the strongly-typed value from the stream
/// and assigns it to the property, eliminating both the box and the unbox and bypassing the row buffer.
///
/// Dispatch is driven by <see cref="ITypedReader{T}"/>: a column type produces the fast path for a given
/// property CLR type iff it implements <c>ITypedReader&lt;thatType&gt;</c>. This also unlocks multiple read
/// representations of one column (e.g. a DateTime column as <see cref="DateTime"/>/<see cref="DateTimeOffset"/>/
/// <see cref="DateOnly"/>, a String column as <see cref="string"/>/<c>byte[]</c>), each an exact-typed read.
/// <see cref="NullableType"/> and <see cref="LowCardinalityType"/> are handled transparently. Anything with
/// no typed reader (composites, Variant/Dynamic/JSON, or an unsupported target type) returns <c>null</c> and
/// the caller falls back to the boxed path for that column.
/// </summary>
internal static class PocoReadExpressionFactory
{
    private static readonly MethodInfo ReadByteMethod =
        typeof(System.IO.BinaryReader).GetMethod(nameof(System.IO.BinaryReader.ReadByte), Type.EmptyTypes);

    /// <summary>
    /// Returns an expression that reads the column value of exact CLR type <paramref name="targetClrType"/>
    /// from <paramref name="reader"/> without boxing, or <c>null</c> if there is no fast path and the caller
    /// should use the boxed <see cref="ClickHouseType.Read(ExtendedBinaryReader)"/> path for the column.
    /// </summary>
    /// <param name="type">The resolved ClickHouse column type.</param>
    /// <param name="reader">The <see cref="ExtendedBinaryReader"/> parameter expression.</param>
    /// <param name="targetClrType">The bound property's CLR type; the returned expression has this type.</param>
    public static Expression TryBuildReadBody(ClickHouseType type, Expression reader, Type targetClrType)
    {
        // LowCardinality is transparent on the RowBinary wire (LowCardinalityType.Read delegates to the
        // underlying), so read the underlying value directly.
        if (type is LowCardinalityType lowCardinality)
            return TryBuildReadBody(lowCardinality.UnderlyingType, reader, targetClrType);

        if (type is NullableType nullableType)
            return TryBuildNullableRead(nullableType, reader, targetClrType);

        // Non-nullable column with a Nullable<U> property: read U from the column and wrap it — the column
        // never yields null, so this is always a value.
        var propUnderlying = Nullable.GetUnderlyingType(targetClrType);
        if (propUnderlying != null)
        {
            var inner = TryBuildTypedRead(type, reader, propUnderlying);
            return inner == null ? null : Expression.Convert(inner, targetClrType);
        }

        return TryBuildTypedRead(type, reader, targetClrType);
    }

    // Nullable(T) column: read the 1-byte null marker (byte-identical to NullableType.Read, which treats
    // marker > 0 as null), then either the default (null) or the underlying value. Only fires when the target
    // can represent null (a reference type or Nullable<U>); a non-nullable value-type property on a nullable
    // column falls back to the boxed path, which correctly throws on an actual null.
    private static Expression TryBuildNullableRead(NullableType nullableType, Expression reader, Type targetClrType)
    {
        var targetUnderlying = Nullable.GetUnderlyingType(targetClrType);
        var targetCanBeNull = !targetClrType.IsValueType || targetUnderlying != null;
        if (!targetCanBeNull)
            return null;

        var innerTarget = targetUnderlying ?? targetClrType;
        var inner = TryBuildReadBody(nullableType.UnderlyingType, reader, innerTarget);
        if (inner == null)
            return null;

        var hasValue = inner.Type == targetClrType ? inner : Expression.Convert(inner, targetClrType);

        var marker = Expression.Variable(typeof(byte), "marker");
        // marker > 0 => null; else the value. The value branch reads the underlying; the null branch does not
        // (the underlying wrote nothing), matching NullableType.Read's short-circuit.
        return Expression.Block(
            new[] { marker },
            Expression.Assign(marker, Expression.Call(reader, ReadByteMethod)),
            Expression.Condition(
                Expression.GreaterThan(marker, Expression.Constant((byte)0)),
                Expression.Default(targetClrType),
                hasValue));
    }

    // ((ITypedReader<clrType>)type).ReadValue(reader) — result typed exactly clrType, no box. Invariance in T
    // enforces an exact-CLR match, mirroring the write side's ITypedWriter<T> dispatch.
    private static Expression TryBuildTypedRead(ClickHouseType type, Expression reader, Type clrType)
    {
        var readerInterface = typeof(ITypedReader<>).MakeGenericType(clrType);
        if (!readerInterface.IsInstanceOfType(type))
            return null;

        var method = readerInterface.GetMethod(nameof(ITypedReader<object>.ReadValue));
        return Expression.Call(Expression.Constant(type, readerInterface), method, reader);
    }
}
