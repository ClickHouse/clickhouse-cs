using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
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
/// change <c>FrameworkType</c> — <c>ReadStringsAsByteArrays</c> (string vs byte[]), <c>UseBigDecimal</c>
/// (decimal vs ClickHouseDecimal) — are handled without any of the cache-key hazards a per-query-shape cache
/// would have.</para>
/// </summary>
internal static class ColumnSlotFactory
{
    private static readonly MethodInfo CreateValueSlotMethod =
        typeof(ColumnSlotFactory).GetMethod(nameof(CreateValueSlot), BindingFlags.NonPublic | BindingFlags.Static);

    private static readonly MethodInfo CreateNullableSlotMethod =
        typeof(ColumnSlotFactory).GetMethod(nameof(CreateNullableSlot), BindingFlags.NonPublic | BindingFlags.Static);

    // Concrete ClickHouseType class -> the CLR types it can read box-free. Keyed on the class, not the
    // instance, because the implemented interface list is fixed per class. Bounded by the number of
    // ClickHouseType subclasses (~60), and it is what keeps SlotConstructors below bounded too: without it,
    // a composite column's FrameworkType (long[], Dictionary<K,V>, Tuple<...>, ...) would be an unbounded
    // family of keys, none of which could ever produce a typed slot.
    private static readonly ConcurrentDictionary<Type, Type[]> TypedReadTargets = new();

    // CLR type -> closed-generic slot constructors. Bounded by the union of every ITypedReader<T>'s T
    // (~25 types), because a key only ever gets here after passing the TypedReadTargets check.
    private static readonly ConcurrentDictionary<Type, SlotConstructors> ConstructorCache = new();

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
        var targets = TypedReadTargets.GetOrAdd(type.GetType(), FindTypedReadTargets);
        if (targets.Length == 0)
            return null;

        var clrType = type.FrameworkType;
        if (Array.IndexOf(targets, clrType) < 0)
            return null;

        var constructors = ConstructorCache.GetOrAdd(clrType, BuildSlotConstructors);
        return nullable ? constructors.Nullable(type) : constructors.Value(type);
    }

    private static Type[] FindTypedReadTargets(Type clickHouseTypeClass) => clickHouseTypeClass
        .GetInterfaces()
        .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ITypedReader<>))
        .Select(i => i.GetGenericArguments()[0])
        .ToArray();

    private static SlotConstructors BuildSlotConstructors(Type clrType) => new(
        (Func<ClickHouseType, ColumnSlot>)CreateValueSlotMethod
            .MakeGenericMethod(clrType)
            .CreateDelegate(typeof(Func<ClickHouseType, ColumnSlot>)),
        (Func<ClickHouseType, ColumnSlot>)CreateNullableSlotMethod
            .MakeGenericMethod(clrType)
            .CreateDelegate(typeof(Func<ClickHouseType, ColumnSlot>)));

    // The `is ITypedReader<T>` here is the real check; TypedReadTargets is only a cheap pre-filter that keeps
    // the constructor cache bounded, so these still return null rather than assuming the cast succeeds.
    private static ColumnSlot CreateValueSlot<T>(ClickHouseType type)
        => type is ITypedReader<T> typedReader ? new ValueSlot<T>(typedReader) : null;

    private static ColumnSlot CreateNullableSlot<T>(ClickHouseType type)
        => type is ITypedReader<T> typedReader ? new NullableSlot<T>(typedReader) : null;

    private readonly struct SlotConstructors(Func<ClickHouseType, ColumnSlot> value, Func<ClickHouseType, ColumnSlot> nullable)
    {
        public Func<ClickHouseType, ColumnSlot> Value { get; } = value;

        public Func<ClickHouseType, ColumnSlot> Nullable { get; } = nullable;
    }
}
