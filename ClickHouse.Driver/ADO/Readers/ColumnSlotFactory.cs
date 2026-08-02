using System;
using System.Collections.Generic;
using System.Net;
using System.Numerics;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.ADO.Readers;

/// <summary>
/// Builds the <see cref="ColumnSlot"/> for a resolved column type.
///
/// <para>A column gets a typed slot iff its type implements <c>ITypedReader&lt;FrameworkType&gt;</c> — that
/// is, iff it can decode straight into the very CLR type its boxed
/// <see cref="ClickHouseType.Read(Formats.ExtendedBinaryReader)"/> would have returned. Every current
/// implementor satisfies that by construction (its <c>Read</c> body <i>is</i> the typed read, or picks
/// between typed reads by the same setting that picks <c>FrameworkType</c>), so a typed slot and the boxed
/// path always produce the same value from the same bytes. Anything else gets a <see cref="BoxedSlot"/>.</para>
///
/// <para>Slots are built from the resolved type <i>instance</i>, never from a cached shape, so settings that
/// change <c>FrameworkType</c> — <c>ReadStringsAsByteArrays</c> (string vs byte[]), <c>UseCustomDecimals</c>
/// (decimal vs ClickHouseDecimal) — are handled without any of the cache-key hazards a per-query-shape cache
/// would have.</para>
/// </summary>
internal static class ColumnSlotFactory
{
    /// <summary>
    /// CLR type → the slot constructor for it. Every entry is a static generic instantiation the compiler
    /// emits, rather than a <c>MakeGenericMethod</c> built at runtime, so NativeAOT and trimming can see
    /// every <c>ValueSlot&lt;T&gt;</c>/<c>NullableSlot&lt;T&gt;</c> the reader will ever need. Runtime generic
    /// construction over value types is exactly what NativeAOT cannot satisfy, and this sits on the read path
    /// of every scalar column, so it is worth spelling out.
    /// </summary>
    /// <remarks>
    /// A few entries are unreachable today — <see cref="DateTimeOffset"/>, <see cref="DateOnly"/> and the
    /// native <c>Int128</c>/<c>UInt128</c> are alternative read representations offered <i>alongside</i> a
    /// type's <c>FrameworkType</c>, never as it. They are listed anyway so the table is exactly "every
    /// <c>ITypedReader&lt;T&gt;</c> target", which
    /// <c>ColumnSlotTests.Binders_CoverEveryTypedReadTarget</c> can then check mechanically. Over-inclusion
    /// is inert; a missing entry would silently demote a column to the boxed path.
    /// </remarks>
    private static readonly Dictionary<Type, Func<ClickHouseType, bool, ColumnSlot>> Binders = new()
    {
        [typeof(sbyte)] = Bind<sbyte>,
        [typeof(short)] = Bind<short>,
        [typeof(int)] = Bind<int>,
        [typeof(long)] = Bind<long>,
        [typeof(byte)] = Bind<byte>,
        [typeof(ushort)] = Bind<ushort>,
        [typeof(uint)] = Bind<uint>,
        [typeof(ulong)] = Bind<ulong>,
        [typeof(BigInteger)] = Bind<BigInteger>,
        [typeof(float)] = Bind<float>,
        [typeof(double)] = Bind<double>,
        [typeof(decimal)] = Bind<decimal>,
        [typeof(ClickHouseDecimal)] = Bind<ClickHouseDecimal>,
        [typeof(bool)] = Bind<bool>,
        [typeof(string)] = Bind<string>,
        [typeof(byte[])] = Bind<byte[]>,
        [typeof(Guid)] = Bind<Guid>,
        [typeof(IPAddress)] = Bind<IPAddress>,
        [typeof(DateTime)] = Bind<DateTime>,
        [typeof(DateTimeOffset)] = Bind<DateTimeOffset>,
        [typeof(DateOnly)] = Bind<DateOnly>,
        [typeof(TimeSpan)] = Bind<TimeSpan>,
#if NET8_0_OR_GREATER
        [typeof(Int128)] = Bind<Int128>,
        [typeof(UInt128)] = Bind<UInt128>,
#endif
    };

    /// <summary>
    /// Returns the slot for <paramref name="type"/>. Never null: falls back to a <see cref="BoxedSlot"/>
    /// over the original (still-wrapped) type, so the wire read is unchanged for anything unsupported.
    /// </summary>
    public static ColumnSlot Create(ClickHouseType type)
    {
        var unwrapped = TransparentWrapper.Unwrap(type);

        // Nullable is not wire-transparent — it prefixes a marker byte — so it selects the slot kind rather
        // than being unwrapped away. Its underlying may itself be wrapped, e.g. Nullable(LowCardinality(T)).
        if (unwrapped is NullableType nullableType)
            return TryCreateTyped(TransparentWrapper.Unwrap(nullableType.UnderlyingType), nullable: true) ?? new BoxedSlot(type);

        return TryCreateTyped(unwrapped, nullable: false) ?? new BoxedSlot(type);
    }

    private static ColumnSlot TryCreateTyped(ClickHouseType type, bool nullable)
    {
        // Order matters, and not only for speed: FrameworkType is not safe to evaluate on every column type.
        // AggregateFunctionType throws AggregateFunctionException from it (deliberately — you are meant to
        // learn you need xMerge() when you read the value, not when you open the reader), and the composite
        // types build a fresh Type object on each call. Slots are created for every column in the ctor, so
        // reading FrameworkType before this bail-out would turn merely *selecting* an AggregateFunction
        // column into a failure to construct the reader at all.
        if (type is not ITypedReader)
            return null;

        return Binders.TryGetValue(type.FrameworkType, out var bind) ? bind(type, nullable) : null;
    }

    // Binds only when the type's typed reader is for its *own* FrameworkType — the CLR type its boxed Read
    // returns. A type offering extra representations (a DateTime column also readable as DateTimeOffset, a
    // Decimal column as either decimal or ClickHouseDecimal) must not have a slot bound to one the boxed path
    // would not have produced, or GetValue would start handing back a different CLR type.
    private static ColumnSlot Bind<T>(ClickHouseType type, bool nullable)
        => type is not ITypedReader<T> typedReader ? null
            : nullable ? new NullableSlot<T>(typedReader) : new ValueSlot<T>(typedReader);
}
