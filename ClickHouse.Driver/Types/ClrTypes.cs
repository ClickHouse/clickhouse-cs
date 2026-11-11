using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Numerics;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Builds <c>T?</c>, <c>T[]</c> and <c>new T[n]</c> for a CLR type known only at runtime.
/// <para>
/// Native AOT can create an array of a reference type at runtime, but not an array of a value type it did
/// not compile ahead of time. Each value type a <see cref="ClickHouseType"/> can produce is therefore listed
/// here, which compiles its array code. <c>NativeAotFallbackTests.ClrTypes_EveryValueTypeOfTheTypeMatrix_IsListed</c>
/// checks that the list covers every type.
/// </para>
/// </summary>
internal static class ClrTypes
{
    private static readonly Dictionary<Type, ValueTypeEntry> ValueTypes = BuildValueTypes();

    /// <summary>
    /// Gets <c>Nullable&lt;T&gt;</c> for a value type <paramref name="valueType"/>.
    /// </summary>
    public static Type MakeNullable(Type valueType) =>
        ValueTypes.TryGetValue(valueType, out var entry) && entry.Nullable != null
            ? entry.Nullable
            : MakeNullableType(valueType);

    /// <summary>
    /// Gets the one-dimensional array type of <paramref name="elementType"/>.
    /// </summary>
    public static Type MakeArrayType(Type elementType) =>
        ValueTypes.TryGetValue(elementType, out var entry) ? entry.Array : MakeArrayTypeCore(elementType);

    /// <summary>
    /// Creates a one-dimensional, zero-based array of <paramref name="elementType"/>.
    /// </summary>
    /// <exception cref="NotSupportedException">
    /// Native AOT, for a value type not listed here.
    /// </exception>
    public static Array CreateArray(Type elementType, int length) =>
        CreateArray(elementType, length, RuntimeFeature.IsDynamicCodeSupported);

    // dynamicCode is RuntimeFeature.IsDynamicCodeSupported, a parameter so tests can pass false
    internal static Array CreateArray(Type elementType, int length, bool dynamicCode)
    {
        if (ValueTypes.TryGetValue(elementType, out var entry))
            return entry.NewArray(length);

        if (elementType.IsValueType && !dynamicCode)
        {
            throw new NotSupportedException(
                $"Cannot create an array of {elementType} under native AOT: its array code was not compiled ahead of time.");
        }

        return CreateArrayCore(elementType, length);
    }

    private static Dictionary<Type, ValueTypeEntry> BuildValueTypes()
    {
        var types = new Dictionary<Type, ValueTypeEntry>();

        void Add<T>()
            where T : struct
        {
            types[typeof(T)] = new ValueTypeEntry(typeof(T?), typeof(T[]), static length => new T[length]);
            types[typeof(T?)] = new ValueTypeEntry(null, typeof(T?[]), static length => new T?[length]);
        }

        Add<bool>();
        Add<sbyte>();
        Add<byte>();
        Add<short>();
        Add<ushort>();
        Add<int>();
        Add<uint>();
        Add<long>();
        Add<ulong>();
        Add<float>();
        Add<double>();
        Add<decimal>();
        Add<ClickHouseDecimal>();
        Add<BigInteger>();
        Add<Guid>();
        Add<DateTime>();
        Add<TimeSpan>();

        return types;
    }

    // Builds only the Type; nothing is instantiated from it. On .NET 10, native AOT built Nullable<Half> this way
    // in an app that never used Nullable<Half>. Only a value type missing from the list reaches here.
    [UnconditionalSuppressMessage("AotAnalysis", "IL3050", Justification = "Only the Type is built, never an instance.")]
    private static Type MakeNullableType(Type valueType) => typeof(Nullable<>).MakeGenericType(valueType);

    // Builds only the Type. Instances come from CreateArray, which refuses an unlisted value type under AOT.
    [UnconditionalSuppressMessage("AotAnalysis", "IL3050", Justification = "Only the Type is built, never an instance.")]
    private static Type MakeArrayTypeCore(Type elementType) => elementType.MakeArrayType();

    // Reached for a reference type, which native AOT can make an array of at runtime, or with dynamic code.
    [UnconditionalSuppressMessage("AotAnalysis", "IL3050", Justification = "CreateArray refuses an unlisted value type when dynamic code is unsupported.")]
    private static Array CreateArrayCore(Type elementType, int length) => Array.CreateInstance(elementType, length);

#pragma warning disable SA1313 // Positional record parameters are properties
    private sealed record ValueTypeEntry(Type Nullable, Type Array, Func<int, Array> NewArray);
#pragma warning restore SA1313
}
