using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Types.Converters;

/// <summary>
/// The conversion of one write leaf: from the CLR type to the canonical value, which is the value that goes on the
/// wire. A struct, so the JIT can inline it into the loop of the writer.
/// </summary>
/// <remarks>
/// The canonical value is an unmanaged type whose bytes, as stored in memory, are the wire bytes. So two canonical
/// values are equal exactly when they encode to the same bytes, and a LowCardinality dictionary can intern them with
/// their default equality. The floats are the reason for this rule: their canonical value is their bit pattern, not
/// the float, because +0 equals -0 and every NaN equals every other NaN as floats, and their bit patterns differ.
/// </remarks>
/// <typeparam name="T">The CLR type that the leaf writes.</typeparam>
/// <typeparam name="TCanon">The canonical value.</typeparam>
internal interface IWriteConversion<T, TCanon>
    where TCanon : unmanaged
{
    /// <summary>
    /// Whether <typeparamref name="T"/> has the memory layout of <typeparamref name="TCanon"/> and
    /// <see cref="Convert"/> only gives the same bits. Then the writer writes a run of values as it is stored.
    /// </summary>
    bool IsReinterpretation { get; }

    /// <summary>Converts one value.</summary>
    /// <param name="value">The CLR value.</param>
    /// <param name="position">The zero-based position of the value in the write, for error messages.</param>
    /// <returns>The canonical value.</returns>
    /// <exception cref="ArgumentException">The value cannot be stored in the ClickHouse type.</exception>
    /// <exception cref="OverflowException">The value is outside the range of the ClickHouse type.</exception>
    TCanon Convert(T value, int position);
}

/// <summary>Writes the value with no change.</summary>
/// <typeparam name="T">The CLR type, which is also the canonical value.</typeparam>
internal readonly struct IdentityWrite<T> : IWriteConversion<T, T>
    where T : unmanaged
{
    /// <inheritdoc/>
    public bool IsReinterpretation => true;

    /// <inheritdoc/>
    public T Convert(T value, int position) => value;
}

/// <summary>A <c>Float32</c> value as its bit pattern.</summary>
internal readonly struct Float32Bits : IWriteConversion<float, uint>
{
    /// <inheritdoc/>
    public bool IsReinterpretation => true;

    /// <inheritdoc/>
    public uint Convert(float value, int position) => BitConverter.SingleToUInt32Bits(value);
}

/// <summary>A <c>Float64</c> value as its bit pattern.</summary>
internal readonly struct Float64Bits : IWriteConversion<double, ulong>
{
    /// <inheritdoc/>
    public bool IsReinterpretation => true;

    /// <inheritdoc/>
    public ulong Convert(double value, int position) => BitConverter.DoubleToUInt64Bits(value);
}

/// <summary>A <see cref="float"/> as the 16 high bits that <c>BFloat16</c> keeps. The low 16 bits are lost.</summary>
internal readonly struct BFloat16Bits : IWriteConversion<float, ushort>
{
    /// <inheritdoc/>
    public bool IsReinterpretation => false;

    /// <inheritdoc/>
    public ushort Convert(float value, int position) => (ushort)(BitConverter.SingleToUInt32Bits(value) >> 16);
}

/// <summary>A <see cref="DateOnly"/> as the <c>Date</c> day count since 1970-01-01.</summary>
internal readonly struct DateDays : IWriteConversion<DateOnly, ushort>
{
    /// <inheritdoc/>
    public bool IsReinterpretation => false;

    /// <inheritdoc/>
    public ushort Convert(DateOnly value, int position)
    {
        int days = value.DayNumber - DateColumnCodecShared.UnixEpochDayNumber;
        if (days is < 0 or > ushort.MaxValue)
        {
            // The parameter name is the one that the Date codec reports for the same value.
#pragma warning disable CA2208 // Instantiate argument exceptions correctly
            throw new ArgumentOutOfRangeException("column", value, "Date is outside the range ClickHouse Date can hold (1970-01-01 to 2149-06-06).");
#pragma warning restore CA2208
        }

        return (ushort)days;
    }
}

/// <summary>A <see cref="DateOnly"/> as the <c>Date32</c> day count since 1970-01-01, which can be negative.</summary>
internal readonly struct Date32Days : IWriteConversion<DateOnly, int>
{
    private static readonly int MinDays = new DateOnly(1900, 1, 1).DayNumber - DateColumnCodecShared.UnixEpochDayNumber;
    private static readonly int MaxDays = new DateOnly(2299, 12, 31).DayNumber - DateColumnCodecShared.UnixEpochDayNumber;

    /// <inheritdoc/>
    public bool IsReinterpretation => false;

    /// <inheritdoc/>
    public int Convert(DateOnly value, int position)
    {
        int days = value.DayNumber - DateColumnCodecShared.UnixEpochDayNumber;
        if (days < MinDays || days > MaxDays)
        {
            // The parameter name is the one that the Date32 codec reports for the same value.
#pragma warning disable CA2208 // Instantiate argument exceptions correctly
            throw new ArgumentOutOfRangeException("column", value, "Date32 is outside the range ClickHouse Date32 can hold (1900-01-01 to 2299-12-31).");
#pragma warning restore CA2208
        }

        return days;
    }
}

/// <summary>
/// A <see cref="Guid"/> as the 16 <c>UUID</c> wire bytes: two little-endian 64-bit halves. The canonical value holds
/// those bytes in wire order.
/// </summary>
internal readonly struct UuidBytes : IWriteConversion<Guid, UInt128>
{
    // result[i] = source[control[i]]: from the in-memory bytes of a Guid to the wire order.
    private static readonly Vector128<byte> GuidToWire = Vector128.Create((byte)6, 7, 4, 5, 0, 1, 2, 3, 15, 14, 13, 12, 11, 10, 9, 8);

    /// <inheritdoc/>
    public bool IsReinterpretation => false;

    /// <inheritdoc/>
    public UInt128 Convert(Guid value, int position)
    {
        ReadOnlySpan<byte> guidBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateReadOnlySpan(ref value, 1));
        UInt128 wire = default;
        Span<byte> wireBytes = MemoryMarshal.AsBytes(MemoryMarshal.CreateSpan(ref wire, 1));
        if (Vector128.IsHardwareAccelerated)
        {
            Vector128.Shuffle(Vector128.Create(guidBytes), GuidToWire).CopyTo(wireBytes);
        }
        else
        {
            for (int i = 0; i < wireBytes.Length; i++)
            {
                wireBytes[i] = guidBytes[GuidToWire[i]];
            }
        }

        return wire;
    }
}

/// <summary>An IPv4 <see cref="IPAddress"/> as the little-endian <c>IPv4</c> number.</summary>
internal readonly struct IPv4Number : IWriteConversion<IPAddress, uint>
{
    /// <inheritdoc/>
    public bool IsReinterpretation => false;

    /// <inheritdoc/>
    public uint Convert(IPAddress value, int position)
    {
        Span<byte> network = stackalloc byte[4];
        if (value?.AddressFamily != AddressFamily.InterNetwork || !value.TryWriteBytes(network, out _))
        {
            throw new ArgumentException($"An IPv4 column requires IPv4 addresses; got '{value}'.", nameof(value));
        }

        return BinaryPrimitives.ReadUInt32BigEndian(network);
    }
}

/// <summary>
/// An <see cref="IPAddress"/> as the 16 <c>IPv6</c> bytes in network order. An IPv4 address becomes its mapped IPv6
/// form. The canonical value holds the bytes in wire order.
/// </summary>
internal readonly struct IPv6Bytes : IWriteConversion<IPAddress, UInt128>
{
    /// <inheritdoc/>
    public bool IsReinterpretation => false;

    /// <inheritdoc/>
    public UInt128 Convert(IPAddress value, int position)
    {
        Span<byte> network = stackalloc byte[16];
        IPAddress address = value?.AddressFamily == AddressFamily.InterNetwork ? value.MapToIPv6() : value;
        if (address?.AddressFamily != AddressFamily.InterNetworkV6
            || !address.TryWriteBytes(network, out int written)
            || written != network.Length)
        {
            throw new ArgumentException($"An IPv6 column requires IPv6 addresses; got '{value}'.", nameof(value));
        }

        return MemoryMarshal.Read<UInt128>(network);
    }
}

/// <summary>A decimal value as its mantissa at the declared scale, checked against the declared precision.</summary>
/// <typeparam name="TValue">The CLR value type (<see cref="decimal"/> or <see cref="ClickHouseTcpDecimal"/>).</typeparam>
/// <typeparam name="TMantissa">The mantissa that goes on the wire.</typeparam>
internal readonly struct DecimalMantissa<TValue, TMantissa> : IWriteConversion<TValue, TMantissa>
    where TMantissa : unmanaged, IComparable<TMantissa>
{
    private readonly Func<TValue, int, TMantissa> encode;
    private readonly int precision;
    private readonly int scale;
    private readonly TMantissa minMantissa;
    private readonly TMantissa maxMantissa;
    private readonly string typeName;

    public DecimalMantissa(DecimalColumnCodec<TMantissa, TValue> codec)
    {
        encode = codec.Encode;
        precision = codec.Precision;
        scale = codec.Scale;
        minMantissa = codec.MinMantissa;
        maxMantissa = codec.MaxMantissa;
        typeName = codec.TypeName;
    }

    /// <inheritdoc/>
    public bool IsReinterpretation => false;

    /// <inheritdoc/>
    public TMantissa Convert(TValue value, int position)
    {
        TMantissa mantissa = encode(value, scale);
        if (mantissa.CompareTo(minMantissa) < 0 || mantissa.CompareTo(maxMantissa) > 0)
        {
            throw new OverflowException($"Value at index {position} exceeds the declared precision {precision} of decimal type '{typeName}'.");
        }

        return mantissa;
    }
}

/// <summary>A <see cref="DateTimeOffset"/> as the <c>DateTime</c> seconds since 1970-01-01 UTC.</summary>
internal readonly struct DateTimeFromOffset : IWriteConversion<DateTimeOffset, uint>
{
    /// <inheritdoc/>
    public bool IsReinterpretation => false;

    /// <inheritdoc/>
    public uint Convert(DateTimeOffset value, int position) => LeafWriteConversions.ToUnixSeconds(value.UtcDateTime);
}

/// <summary>
/// A <see cref="DateTime"/> as the <c>DateTime</c> seconds since 1970-01-01 UTC. A value of kind
/// <see cref="DateTimeKind.Unspecified"/> is a wall clock in the timezone of the column.
/// </summary>
internal readonly struct DateTimeFromDateTime : IWriteConversion<DateTime, uint>
{
    private readonly ResolvedTimeZone timeZone;

    public DateTimeFromDateTime(ResolvedTimeZone timeZone) => this.timeZone = timeZone;

    /// <inheritdoc/>
    public bool IsReinterpretation => false;

    /// <inheritdoc/>
    public uint Convert(DateTime value, int position) => LeafWriteConversions.ToUnixSeconds(DateTimeColumnCodec.ToUtc(value, timeZone));
}

/// <summary>A <see cref="DateTimeOffset"/> as the <c>DateTime64</c> count at the declared scale.</summary>
internal readonly struct DateTime64FromOffset : IWriteConversion<DateTimeOffset, long>
{
    private readonly int scale;
    private readonly string typeName;

    public DateTime64FromOffset(int scale, string typeName)
    {
        this.scale = scale;
        this.typeName = typeName;
    }

    /// <inheritdoc/>
    public bool IsReinterpretation => false;

    /// <inheritdoc/>
    public long Convert(DateTimeOffset value, int position) => LeafWriteConversions.DateTime64Count(value, scale, typeName);
}

/// <summary>
/// A <see cref="DateTime"/> as the <c>DateTime64</c> count at the declared scale. A value of kind
/// <see cref="DateTimeKind.Unspecified"/> is a wall clock in the timezone of the column.
/// </summary>
internal readonly struct DateTime64FromDateTime : IWriteConversion<DateTime, long>
{
    private readonly int scale;
    private readonly ResolvedTimeZone timeZone;
    private readonly string typeName;

    public DateTime64FromDateTime(int scale, ResolvedTimeZone timeZone, string typeName)
    {
        this.scale = scale;
        this.timeZone = timeZone;
        this.typeName = typeName;
    }

    /// <inheritdoc/>
    public bool IsReinterpretation => false;

    /// <inheritdoc/>
    public long Convert(DateTime value, int position)
        => LeafWriteConversions.DateTime64Count(new DateTimeOffset(DateTimeColumnCodec.ToUtc(value, timeZone)), scale, typeName);
}

/// <summary>A <see cref="TimeSpan"/> as the <c>Time</c> whole seconds, truncated toward zero.</summary>
internal readonly struct TimeFromTimeSpan : IWriteConversion<TimeSpan, int>
{
    /// <inheritdoc/>
    public bool IsReinterpretation => false;

    /// <inheritdoc/>
    public int Convert(TimeSpan value, int position)
    {
        long seconds = value.Ticks / TimeSpan.TicksPerSecond;
        if (seconds is < -LeafWriteConversions.MaxTimeSeconds or > LeafWriteConversions.MaxTimeSeconds)
        {
            throw new ArgumentOutOfRangeException(nameof(value), value, "Time is outside the range ClickHouse Time can hold ([-999:59:59, 999:59:59]).");
        }

        return (int)seconds;
    }
}

/// <summary>A <see cref="TimeOnly"/> as the <c>Time</c> whole seconds. Every time of day is in the range.</summary>
internal readonly struct TimeFromTimeOnly : IWriteConversion<TimeOnly, int>
{
    /// <inheritdoc/>
    public bool IsReinterpretation => false;

    /// <inheritdoc/>
    public int Convert(TimeOnly value, int position) => (int)(value.Ticks / TimeSpan.TicksPerSecond);
}

/// <summary>A <see cref="TimeSpan"/> as the <c>Time64</c> count at the declared scale, truncated toward zero.</summary>
internal readonly struct Time64FromTimeSpan : IWriteConversion<TimeSpan, long>
{
    private readonly int scale;

    public Time64FromTimeSpan(int scale) => this.scale = scale;

    /// <inheritdoc/>
    public bool IsReinterpretation => false;

    /// <inheritdoc/>
    public long Convert(TimeSpan value, int position)
    {
        long seconds = value.Ticks / TimeSpan.TicksPerSecond;
        if (seconds is < -LeafWriteConversions.MaxTimeSeconds or > LeafWriteConversions.MaxTimeSeconds)
        {
            throw new ArgumentOutOfRangeException(nameof(value), value, "Time64 is outside the range ClickHouse Time64 can hold ([-999:59:59, 999:59:59]).");
        }

        return FixedPointScaling.ShiftDecimalPlaces(value.Ticks, scale - LeafWriteConversions.DotNetTickScale);
    }
}

/// <summary>A <see cref="TimeOnly"/> as the <c>Time64</c> count at the declared scale, truncated toward zero.</summary>
internal readonly struct Time64FromTimeOnly : IWriteConversion<TimeOnly, long>
{
    private readonly int scale;

    public Time64FromTimeOnly(int scale) => this.scale = scale;

    /// <inheritdoc/>
    public bool IsReinterpretation => false;

    /// <inheritdoc/>
    public long Convert(TimeOnly value, int position)
        => FixedPointScaling.ShiftDecimalPlaces(value.Ticks, scale - LeafWriteConversions.DotNetTickScale);
}

/// <summary>An enum label as the ordinal that the type declares for it.</summary>
/// <typeparam name="T">The ordinal type (<see cref="sbyte"/> or <see cref="short"/>).</typeparam>
internal readonly struct EnumFromLabel<T> : IWriteConversion<string, T>
    where T : unmanaged
{
    private readonly IReadOnlyDictionary<string, T> labelToOrdinal;
    private readonly EnumMemberTable members;

    public EnumFromLabel(EnumColumnCodec<T> codec)
    {
        labelToOrdinal = codec.LabelToOrdinal;
        members = codec.Members;
    }

    /// <inheritdoc/>
    public bool IsReinterpretation => false;

    /// <inheritdoc/>
    public T Convert(string value, int position)
        => value is not null && labelToOrdinal.TryGetValue(value, out T ordinal)
            ? ordinal
            : throw members.NoSuchLabel(value, "label");
}

/// <summary>The checks and the arithmetic that more than one write conversion uses.</summary>
internal static class LeafWriteConversions
{
    /// <summary>The largest <c>Time</c> or <c>Time64</c> value in whole seconds: 999:59:59.</summary>
    public const int MaxTimeSeconds = (999 * 3600) + (59 * 60) + 59;

    /// <summary>The scale of a .NET tick: 100 ns is 10^-7 s.</summary>
    public const int DotNetTickScale = 7;

    private static readonly long UnixEpochTicks = DateTime.UnixEpoch.Ticks;

    /// <summary>The seconds from 1970-01-01 UTC to <paramref name="utc"/>, checked against the <c>DateTime</c> range.</summary>
    /// <param name="utc">A UTC instant.</param>
    /// <returns>The second count.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The instant is before 1970-01-01 or after 2106-02-07 06:28:15 UTC.</exception>
    public static uint ToUnixSeconds(DateTime utc)
    {
        long seconds = (utc - DateTime.UnixEpoch).Ticks / TimeSpan.TicksPerSecond;
        if (seconds < 0 || seconds > uint.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(utc),
                utc,
                "DateTime is outside the range ClickHouse DateTime can hold (1970-01-01 to 2106-02-07 06:28:15 UTC).");
        }

        return (uint)seconds;
    }

    /// <summary>
    /// The <c>DateTime64</c> count of <paramref name="value"/> at <paramref name="scale"/>. A scale of 7 or more is
    /// exact. A coarser scale must divide the tick count with no remainder, so no digits are lost.
    /// </summary>
    /// <param name="value">The instant.</param>
    /// <param name="scale">The declared scale (0 to 9).</param>
    /// <param name="typeName">The ClickHouse type, for the messages.</param>
    /// <returns>The count.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The count does not fit in an <see cref="long"/>.</exception>
    /// <exception cref="ArgumentException">The scale cannot hold the value without loss.</exception>
    public static long DateTime64Count(DateTimeOffset value, int scale, string typeName)
    {
        long dotNetTicksSinceEpoch = value.UtcDateTime.Ticks - UnixEpochTicks;
        int places = scale - DotNetTickScale;
        if (places >= 0)
        {
            long scaleUp = FixedPointScaling.Pow10(places);
            if (dotNetTicksSinceEpoch > long.MaxValue / scaleUp || dotNetTicksSinceEpoch < long.MinValue / scaleUp)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(value),
                    value,
                    $"{value:o} cannot be written to {typeName} (scale {scale}): the count of sub-second units since 1970-01-01 does not fit in an Int64.");
            }

            return dotNetTicksSinceEpoch * scaleUp;
        }

        long factor = FixedPointScaling.Pow10(-places);
        if (dotNetTicksSinceEpoch % factor != 0)
        {
            throw new ArgumentException($"{value:o} cannot be written to {typeName} (scale {scale}) without losing precision.", nameof(value));
        }

        return dotNetTicksSinceEpoch / factor;
    }
}
