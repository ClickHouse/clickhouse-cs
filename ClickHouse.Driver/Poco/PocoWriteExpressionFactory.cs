using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Poco;

/// <summary>
/// Builds box-free write expressions for the POCO binary-insert fast path (issue #505).
///
/// The default insert path reads each property through a compiled <c>Func&lt;T,object&gt;</c> getter —
/// which boxes every value-type property, once per value, per row — and then unboxes it inside
/// <see cref="ClickHouseType.Write(ExtendedBinaryWriter, object)"/>. For the common scalar columns we can
/// instead compile a fused delegate that reads the strongly-typed property and calls the matching
/// <see cref="System.IO.BinaryWriter"/> overload directly, eliminating both the box and the unbox.
///
/// Every fast path is byte-identical to the boxed <c>Write</c> by construction. It fires for:
/// <list type="bullet">
///   <item>fixed-width scalars whose CLR type exactly matches the column (e.g. <see cref="long"/> ↔ Int64);</item>
///   <item><see cref="string"/> on a String column (no box to remove, but skips the virtual dispatch);</item>
///   <item><see cref="NullableType"/> — emits the null marker and recurses into the underlying type;</item>
///   <item>value types with bespoke serialization exposing <see cref="ITypedWriter{T}"/> (Guid, the DateTime
///     family, decimal, BigInteger, TimeSpan, …), dispatched through a direct interface call;</item>
///   <item>box-free numeric coercion — any other numeric value-type property (or C# enum) on an integer,
///     float, <c>Decimal</c>, or BFloat16 column, mirroring the boxed path's <c>Convert.ToXxx</c>.</item>
/// </list>
/// Anything with no fast path (arrays, tuples, maps, reference types other than string, value types with
/// no matching conversion, …) returns <c>null</c> and the caller falls back to the existing boxed path.
/// </summary>
internal static class PocoWriteExpressionFactory
{
    // BinaryWriter.Write(<primitive>) overloads, resolved once. ExtendedBinaryWriter inherits these.
    private static readonly Dictionary<Type, MethodInfo> WriteOverloads = new()
    {
        [typeof(sbyte)] = GetWriteOverload(typeof(sbyte)),
        [typeof(byte)] = GetWriteOverload(typeof(byte)),
        [typeof(short)] = GetWriteOverload(typeof(short)),
        [typeof(ushort)] = GetWriteOverload(typeof(ushort)),
        [typeof(int)] = GetWriteOverload(typeof(int)),
        [typeof(uint)] = GetWriteOverload(typeof(uint)),
        [typeof(long)] = GetWriteOverload(typeof(long)),
        [typeof(ulong)] = GetWriteOverload(typeof(ulong)),
        [typeof(float)] = GetWriteOverload(typeof(float)),
        [typeof(double)] = GetWriteOverload(typeof(double)),
        [typeof(bool)] = GetWriteOverload(typeof(bool)),
    };

    private static readonly MethodInfo WriteStringMethod =
        typeof(PocoWriteExpressionFactory).GetMethod(nameof(WriteString), BindingFlags.Static | BindingFlags.NonPublic);

    // Integer/float column types whose boxed Write is `writer.Write(Convert.ToXxx(value, InvariantCulture))`.
    // A value-type property that isn't the exact framework type still hits the boxed path today (widening,
    // narrowing, differing signedness, enums). We can instead call the SAME Convert.ToXxx box-free: it has a
    // dedicated single-arg overload for every numeric primitive (plus bool/char/decimal), and numeric->numeric
    // conversion is culture-independent, so the emitted bytes and any overflow are identical to the boxed path.
    private static readonly Dictionary<Type, (Type Target, string ConvertMethod)> NumericCoercionTargets = new()
    {
        [typeof(Int8Type)] = (typeof(sbyte), nameof(Convert.ToSByte)),
        [typeof(Int16Type)] = (typeof(short), nameof(Convert.ToInt16)),
        [typeof(Int32Type)] = (typeof(int), nameof(Convert.ToInt32)),
        [typeof(Int64Type)] = (typeof(long), nameof(Convert.ToInt64)),
        [typeof(UInt8Type)] = (typeof(byte), nameof(Convert.ToByte)),
        [typeof(UInt16Type)] = (typeof(ushort), nameof(Convert.ToUInt16)),
        [typeof(UInt32Type)] = (typeof(uint), nameof(Convert.ToUInt32)),
        [typeof(UInt64Type)] = (typeof(ulong), nameof(Convert.ToUInt64)),
        [typeof(Float32Type)] = (typeof(float), nameof(Convert.ToSingle)),
        [typeof(Float64Type)] = (typeof(double), nameof(Convert.ToDouble)),
    };

    /// <summary>
    /// Returns a void-typed expression that writes <paramref name="value"/> (a typed property access) to
    /// <paramref name="writer"/> without boxing, or <c>null</c> if <paramref name="type"/>/<paramref name="value"/>
    /// has no fast path and the caller should use the boxed <see cref="ClickHouseType.Write(ExtendedBinaryWriter, object)"/>.
    /// </summary>
    /// <param name="type">The resolved ClickHouse column type.</param>
    /// <param name="value">Expression yielding the property value; its <see cref="Expression.Type"/> is the CLR property type.</param>
    /// <param name="writer">The <see cref="ExtendedBinaryWriter"/> parameter expression.</param>
    public static Expression TryBuildWriteBody(ClickHouseType type, Expression value, Expression writer)
    {
        var clrType = value.Type;

        switch (type)
        {
            // Fixed-width scalars map straight onto a BinaryWriter overload. Only fire on an exact CLR
            // match so we never silently skip the coercion (Convert.ToXxx) the boxed path would perform
            // for a mismatched property type.
            case Int8Type when clrType == typeof(sbyte):
            case Int16Type when clrType == typeof(short):
            case Int32Type when clrType == typeof(int):
            case Int64Type when clrType == typeof(long):
            case UInt8Type when clrType == typeof(byte):
            case UInt16Type when clrType == typeof(ushort):
            case UInt32Type when clrType == typeof(uint):
            case UInt64Type when clrType == typeof(ulong):
            case Float32Type when clrType == typeof(float):
            case Float64Type when clrType == typeof(double):
            case BooleanType when clrType == typeof(bool):
                return Expression.Call(writer, WriteOverloads[clrType], value);

            // String: reference type, so there is no boxing to eliminate, but the fused call still avoids
            // the virtual dispatch + type probing in StringType.Write. Routed through WriteString so null
            // handling stays byte/behaviour-identical to StringType.
            case StringType when clrType == typeof(string):
                return Expression.Call(WriteStringMethod, writer, value);

            // Nullable(T) with a Nullable<U> property: emit the 1-byte null marker (byte-identical to
            // NullableType.Write) and, for a non-null value, recurse into the underlying type on the
            // strongly-typed .Value — so e.g. int?/long?/DateTime?/Guid?/decimal? never box. Falls back
            // (returns null) when the underlying itself has no fast path.
            case NullableType nullableType when Nullable.GetUnderlyingType(clrType) != null:
                return TryBuildNullableWrite(nullableType, value, writer);

            default:
                // First: value types with bespoke serialization (Guid, DateTime family, decimal, BigInteger,
                // TimeSpan, ...) expose a strongly-typed ITypedWriter<T>; a direct interface call avoids the
                // box while reusing the type's own logic verbatim. Invariance in T enforces the same exact-CLR
                // rule as the scalar cases above. Falling back to numeric coercion catches the "close but not
                // exact" value types (e.g. an int property on a UInt64 column, or an enum on an Int8 column).
                return TryBuildTypedWriterCall(type, clrType, value, writer)
                    ?? TryBuildNumericCoercion(type, clrType, value, writer);
        }
    }

    // Box-free coercion of a non-exact numeric value-type property to an integer/float/Decimal column,
    // mirroring the boxed path's Convert.ToXxx. Returns null (→ boxed fallback) for reference types, for
    // value types with no matching Convert overload (DateTime, Guid, custom structs — the boxed path throws
    // for those, and we preserve that by falling back), and for column types not covered here.
    private static Expression TryBuildNumericCoercion(ClickHouseType type, Type clrType, Expression value, Expression writer)
    {
        // Only value types box on the getter, so only they can gain. Reference types (string) are skipped:
        // besides having no box to remove, single-arg Convert.ToXxx(string) uses the CURRENT culture — unlike
        // the boxed path's InvariantCulture — so strings must stay on the boxed path.
        if (!clrType.IsValueType)
            return null;

        // Enums: unwrap to the underlying integral type (box-free). The boxed path's IConvertible conversion
        // yields the same integral value, so this stays byte-identical.
        var source = value;
        var sourceType = clrType;
        if (clrType.IsEnum)
        {
            sourceType = Enum.GetUnderlyingType(clrType);
            source = Expression.Convert(value, sourceType);
        }

        if (NumericCoercionTargets.TryGetValue(type.GetType(), out var target))
        {
            var convert = GetExactConvert(target.ConvertMethod, sourceType);
            if (convert == null)
                return null;

            // writer.Write(Convert.ToXxx(source))
            return Expression.Call(writer, WriteOverloads[target.Target], Expression.Call(convert, source));
        }

        // Decimal columns coerce any non-ClickHouseDecimal input via Convert.ToDecimal; route through the
        // box-free WriteValue(decimal) so numeric properties (int/long/double/…) on a Decimal column don't box.
        if (type is DecimalType)
        {
            var toDecimal = GetExactConvert(nameof(Convert.ToDecimal), sourceType);
            if (toDecimal == null)
                return null;

            var writerInterface = typeof(ITypedWriter<decimal>);
            var method = writerInterface.GetMethod(nameof(ITypedWriter<decimal>.WriteValue));
            return Expression.Call(Expression.Constant(type, writerInterface), method, writer, Expression.Call(toDecimal, source));
        }

        // BFloat16 columns coerce any numeric input via Convert.ToSingle, then apply the BFloat16 bit
        // truncation inside WriteValue(float). Float32/Float64 write the raw value and are handled by
        // NumericCoercionTargets above; BFloat16 needs the custom truncation, so it routes through its
        // own ITypedWriter<float> call. Convert.ToSingle for a numeric source is culture-independent,
        // so the emitted bytes and any overflow match the boxed Write's Convert.ToSingle(value, Invariant).
        if (type is BFloat16Type)
        {
            var toSingle = GetExactConvert(nameof(Convert.ToSingle), sourceType);
            if (toSingle == null)
                return null;

            var writerInterface = typeof(ITypedWriter<float>);
            var method = writerInterface.GetMethod(nameof(ITypedWriter<float>.WriteValue));
            return Expression.Call(Expression.Constant(type, writerInterface), method, writer, Expression.Call(toSingle, source));
        }

        return null;
    }

    // Returns the Convert.ToXxx overload that takes EXACTLY sourceType, or null. GetMethod(name, Type[]) alone
    // would fall back to the Convert.ToXxx(object) overload for any type (everything is assignable to object),
    // which then fails Expression.Call because a value type would need boxing — exactly what we're avoiding.
    private static MethodInfo GetExactConvert(string convertMethod, Type sourceType)
    {
        var method = typeof(Convert).GetMethod(convertMethod, new[] { sourceType });
        return method != null && method.GetParameters()[0].ParameterType == sourceType ? method : null;
    }

    private static Expression TryBuildNullableWrite(NullableType nullableType, Expression value, Expression writer)
    {
        // value.Type is Nullable<U>; recurse on the typed .Value (type U).
        var innerBody = TryBuildWriteBody(nullableType.UnderlyingType, Expression.Property(value, "Value"), writer);
        if (innerBody == null)
            return null;

        var writeMarker = WriteOverloads[typeof(byte)];
        var writeNotNull = Expression.Block(
            Expression.Call(writer, writeMarker, Expression.Constant((byte)0)),
            innerBody);
        var writeNull = Expression.Call(writer, writeMarker, Expression.Constant((byte)1));

        return Expression.IfThenElse(Expression.Property(value, "HasValue"), writeNotNull, writeNull);
    }

    private static Expression TryBuildTypedWriterCall(ClickHouseType type, Type clrType, Expression value, Expression writer)
    {
        var writerInterface = typeof(ITypedWriter<>).MakeGenericType(clrType);
        if (!writerInterface.IsInstanceOfType(type))
            return null;

        var method = writerInterface.GetMethod(nameof(ITypedWriter<object>.WriteValue));

        // ((ITypedWriter<clrType>)type).WriteValue(writer, value) — value is already typed clrType, no box.
        return Expression.Call(Expression.Constant(type, writerInterface), method, writer, value);
    }

    private static MethodInfo GetWriteOverload(Type parameterType) =>
        typeof(System.IO.BinaryWriter).GetMethod(nameof(System.IO.BinaryWriter.Write), new[] { parameterType });

    /// <summary>
    /// Mirrors <see cref="StringType"/>'s handling of a <see cref="string"/> value, including its
    /// exception for null (a non-nullable String column rejects null), so the fast path is behaviourally
    /// identical to the boxed path.
    /// </summary>
    private static void WriteString(ExtendedBinaryWriter writer, string value)
    {
        if (value is null)
            throw new ArgumentException("String requires string, byte[], ReadOnlyMemory<byte>, or Stream, got null");
        writer.Write(value);
    }
}
