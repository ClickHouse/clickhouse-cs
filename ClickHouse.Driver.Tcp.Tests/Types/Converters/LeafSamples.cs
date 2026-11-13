using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// Sample values for every leaf of the converter table: the column that a read decodes, and the values that a write
/// encodes. Each set holds edge values (bounds, NaN payloads, signed zeros, invalid UTF-8) that the current path
/// accepts, so the comparison with it covers them.
/// </summary>
internal static class LeafSamples
{
    private static readonly string[] IntervalUnits =
        { "Nanosecond", "Microsecond", "Millisecond", "Second", "Minute", "Hour", "Day", "Week", "Month", "Quarter", "Year" };

    /// <summary>The type strings that the tests derive: at least one for each leaf, and each parameter shape.</summary>
    public static IReadOnlyList<string> Types { get; } = new[]
    {
        "UInt8", "Int8", "UInt16", "Int16", "UInt32", "Int32", "UInt64", "Int64",
        "UInt128", "Int128", "UInt256", "Int256", "Bool",
        "Float32", "Float64", "BFloat16",
        "String", "FixedString(4)", "JSON",
        "Date", "Date32", "UUID", "IPv4", "IPv6", "Nothing",
        "Time", "Time64(0)", "Time64(3)", "Time64(9)",
        "DateTime", "DateTime('UTC')", "DateTime('Asia/Kolkata')",
        "DateTime64(0, 'Europe/Berlin')", "DateTime64(3)", "DateTime64(9, 'UTC')",
        "Enum8('a' = -1, 'b' = 127, 'c' = 5)", "Enum16('x' = -32768, 'y' = 32767)",
        "Enum('p' = 1, 'q' = 2)", "Enum('big' = 1000, 'small' = -1)",
        "Decimal(9, 2)", "Decimal(18, 4)", "Decimal(38, 10)", "Decimal(76, 20)",
        "Decimal32(3)", "Decimal64(6)", "Decimal128(10)", "Decimal256(20)",
    }.Concat(IntervalUnits.Select(unit => "Interval" + unit)).ToArray();

    /// <summary>
    /// The decoded column that a read of <paramref name="type"/> as <paramref name="target"/> starts from. The
    /// values are in range for the target, so the current read succeeds.
    /// </summary>
    public static Task<IColumn> DecodedAsync(string type, Type target)
        => type == "Nothing" ? ConverterHarness.DecodeNothingAsync(4) : ConverterHarness.DecodeAsync(type, Source(type, target));

    /// <summary>Values of <paramref name="clrType"/> that the current write of <paramref name="type"/> accepts.</summary>
    public static Array Writable(string type, Type clrType)
    {
        if (clrType == ConverterHarness.Codec(type).ElementType)
        {
            return Stored(type, clrType);
        }

        string name = TypeParser.Parse(type).Name;
        if (clrType == typeof(byte[]) && name == "String")
        {
            return RawStrings();
        }

        if (clrType == typeof(string) && name.StartsWith("Enum", StringComparison.Ordinal))
        {
            return Labels(type);
        }

        if (clrType == typeof(DateTimeOffset))
        {
            return name == "DateTime64" && type.Contains("(0", StringComparison.Ordinal)
                ? new[] { DateTimeOffset.UnixEpoch, new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(5)), new DateTimeOffset(1950, 3, 1, 0, 0, 0, TimeSpan.Zero) }
                : name == "DateTime64"
                    ? new[] { DateTimeOffset.UnixEpoch, new DateTimeOffset(2024, 1, 15, 10, 30, 0, 123, TimeSpan.FromHours(5)), new DateTimeOffset(1950, 3, 1, 0, 0, 0, TimeSpan.FromHours(-3)) }
                    : new[] { DateTimeOffset.UnixEpoch, new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(5)), DateTimeOffset.FromUnixTimeSeconds(uint.MaxValue) };
        }

        if (clrType == typeof(DateTime))
        {
            // Unspecified is a wall clock in the column timezone, and 2024-11-03 01:30 occurs two times in New York.
            // Local depends on the host, which is the same for both paths.
            var values = new List<DateTime>
            {
                DateTime.UnixEpoch,
                new(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc),
                new(2024, 7, 1, 12, 0, 0, DateTimeKind.Unspecified),
                new(2024, 11, 3, 1, 30, 0, DateTimeKind.Unspecified),
                new(2024, 7, 1, 12, 0, 0, DateTimeKind.Local),
            };
            return values.ToArray();
        }

        if (clrType == typeof(TimeSpan))
        {
            return new[] { TimeSpan.Zero, TimeSpan.FromSeconds(-1), new TimeSpan(999, 59, 59), -new TimeSpan(999, 59, 59), TimeSpan.FromTicks(12_345_678) };
        }

        if (clrType == typeof(TimeOnly))
        {
            return new[] { TimeOnly.MinValue, new TimeOnly(12, 34, 56, 789), TimeOnly.MaxValue, new TimeOnly(0, 0, 1) };
        }

        throw new ArgumentException($"No write samples of {clrType} for '{type}'.");
    }

    // The column to decode for a read as the target: the codec's own type, with values in range for the target.
    private static IColumn Source(string type, Type target)
    {
        IColumnCodec codec = ConverterHarness.Codec(type);
        string name = TypeParser.Parse(type).Name;
        Array values = name switch
        {
            // Raw bytes, so a byte sequence that UTF-8 cannot express reaches the decoded column.
            "String" => RawStrings(),
            "Time" when target == typeof(TimeOnly) => new[] { 0, 1, 43_200, 86_399 },
            "Time64" when target == typeof(TimeOnly) => Time64OfDay(type),
            _ => Stored(type, codec.ElementType),
        };

        return (IColumn)Activator.CreateInstance(typeof(ArrayColumn<>).MakeGenericType(values.GetType().GetElementType()), "c", type, values);
    }

    // Values of the type that the decoded column stores.
    private static Array Stored(string type, Type stored)
    {
        string name = TypeParser.Parse(type).Name;
        if (name.StartsWith("Interval", StringComparison.Ordinal))
        {
            return new[] { long.MinValue, -5L, 0L, long.MaxValue };
        }

        switch (name)
        {
            case "UInt8": return new byte[] { 0, 1, 128, 255 };
            case "Int8": return new sbyte[] { -128, -1, 0, 127 };
            case "UInt16": return new ushort[] { 0, 258, ushort.MaxValue };
            case "Int16": return new short[] { short.MinValue, -1, 0, short.MaxValue };
            case "UInt32": return new uint[] { 0, 1, uint.MaxValue };
            case "Int32": return new[] { int.MinValue, -1, 0, int.MaxValue };
            case "UInt64": return new ulong[] { 0, 1, ulong.MaxValue };
            case "Int64": return new[] { long.MinValue, -1, 0, long.MaxValue };
            case "UInt128": return new[] { UInt128.Zero, UInt128.One, UInt128.MaxValue };
            case "Int128": return new[] { Int128.MinValue, -Int128.One, Int128.Zero, Int128.MaxValue };
            case "UInt256": return new[] { UInt256.Zero, UInt256.FromBigInteger(BigInteger.Pow(2, 200)), UInt256.FromBigInteger(BigInteger.Pow(2, 256) - 1) };
            case "Int256": return new[] { Int256.FromBigInteger(-BigInteger.Pow(2, 255)), Int256.FromBigInteger(-1), Int256.Zero, Int256.FromBigInteger(BigInteger.Pow(2, 255) - 1) };
            case "Bool": return new[] { false, true, true, false };
            case "Float32":
                return new[]
                {
                    0f, -0f, 1.5f, float.MinValue, float.MaxValue, float.NaN, BitConverter.UInt32BitsToSingle(0x7FC0_0001),
                    BitConverter.UInt32BitsToSingle(0xFFC0_0002), float.PositiveInfinity, float.NegativeInfinity, float.Epsilon,
                };
            case "Float64":
                return new[]
                {
                    0d, -0d, -1.5e100, double.MinValue, double.MaxValue, double.NaN, BitConverter.UInt64BitsToDouble(0x7FF8_0000_0000_0001),
                    BitConverter.UInt64BitsToDouble(0xFFF8_0000_0000_0002), double.PositiveInfinity, double.Epsilon,
                };
            case "BFloat16":
                // Values whose low 16 bits are zero, so the write keeps them exactly.
                return new[] { 0f, -0f, 1.5f, -2f, float.PositiveInfinity, BitConverter.UInt32BitsToSingle(0x7FC1_0000), BitConverter.UInt32BitsToSingle(0x3F81_0000) };
            case "FixedString":
                return new[] { new byte[] { 0, 0, 0, 0 }, new byte[] { 1, 2, 3, 4 }, new byte[] { 0xFF, 0x00, 0x61, 0x00 }, Encoding.UTF8.GetBytes("aé.") };
            case "JSON": return new[] { "{}", "{\"a\":1}", "{\"b\":\"x\"}" };
            case "Date": return new[] { new DateOnly(1970, 1, 1), new DateOnly(2024, 1, 15), new DateOnly(2149, 6, 6) };
            case "Date32": return new[] { new DateOnly(1900, 1, 1), new DateOnly(1969, 12, 31), new DateOnly(2024, 1, 15), new DateOnly(2299, 12, 31) };
            case "UUID": return new[] { Guid.Empty, new Guid("00112233-4455-6677-8899-aabbccddeeff"), new Guid("ffffffff-ffff-ffff-ffff-ffffffffffff") };
            case "IPv4": return new[] { "0.0.0.0", "127.0.0.1", "192.168.1.1", "255.255.255.255" }.Select(IPAddress.Parse).ToArray();
            case "IPv6": return new[] { "::", "::1", "2001:db8::1", "::ffff:1.2.3.4", "2001:db8:85a3:8d3:1319:8a2e:370:7348" }.Select(IPAddress.Parse).ToArray();
            case "Time": return new[] { -3_599_999, -1, 0, 3_600, 90_000, 3_599_999 };
            case "Time64": return new[] { 0L, -1L, 1L, 1_234_567L, -999_999_999L, 3_599_999L };
            case "DateTime": return new uint[] { 0, 1, 1_705_314_600, 1_720_000_000, uint.MaxValue };
            case "DateTime64": return DateTime64Counts(type);
            case "Enum8": return new sbyte[] { -1, 127, 5, -1 };
            case "Enum16": return new short[] { -32768, 32767, -32768 };
            case "Enum": return type.Contains("big", StringComparison.Ordinal) ? new short[] { 1000, -1, 1000 } : new sbyte[] { 1, 2, 2 };
            case "String": return new[] { string.Empty, "a", "héllo✓", "a\0b" };
        }

        if (stored == typeof(decimal))
        {
            return type switch
            {
                "Decimal(9, 2)" => new[] { 0m, 1.25m, -9999999.99m, 1234567.89m },
                "Decimal32(3)" => new[] { 0m, 1.234m, -999999.999m },
                "Decimal(18, 4)" => new[] { 0m, 12345678901234.5678m, -1.0001m },
                _ => new[] { 0m, 123456789012.345678m, -0.000001m },
            };
        }

        if (stored == typeof(ClickHouseTcpDecimal))
        {
            int scale = type.Contains("20", StringComparison.Ordinal) ? 20 : 10;
            return new[]
            {
                new ClickHouseTcpDecimal(BigInteger.Zero, scale),
                new ClickHouseTcpDecimal(BigInteger.Parse("123456789012345678901234567") * BigInteger.Pow(10, 0), scale),
                new ClickHouseTcpDecimal(-BigInteger.Pow(10, scale) - 1, scale),
            };
        }

        throw new ArgumentException($"No stored samples for '{type}'.");
    }

    private static byte[][] RawStrings() => new[]
    {
        Array.Empty<byte>(),
        "a"u8.ToArray(),
        Encoding.UTF8.GetBytes("héllo✓"),
        new byte[] { 0xFF, 0x00, 0x61 },
        Encoding.UTF8.GetBytes(new string('x', 300)),
    };

    private static long[] Time64OfDay(string type)
    {
        int scale = int.Parse(TypeParser.Parse(type).Arguments[0].Name, System.Globalization.CultureInfo.InvariantCulture);
        long perSecond = (long)Math.Pow(10, scale);
        return new[] { 0L, 1L, (86_399 * perSecond) + perSecond - 1, 43_200 * perSecond };
    }

    private static long[] DateTime64Counts(string type) => type switch
    {
        // Whole seconds, in the DateTimeOffset range.
        "DateTime64(0, 'Europe/Berlin')" => new[] { 0L, 1_700_000_000L, -2_000_000_000L, 4_102_444_800L },
        "DateTime64(3)" => new[] { 0L, 1_700_000_000_123L, -6_000_000_000_000L },

        // At scale 9 the whole Int64 range is in the DateTimeOffset range.
        _ => new[] { 0L, 1_700_000_000_123_456_789L, -1_000_000_001L, long.MaxValue, long.MinValue },
    };

    private static string[] Labels(string type)
    {
        string[] labels = ConverterHarness.Codec(type) switch
        {
            Tcp.Types.Codecs.EnumColumnCodec<sbyte> e => e.LabelToOrdinal.Keys.ToArray(),
            Tcp.Types.Codecs.EnumColumnCodec<short> e => e.LabelToOrdinal.Keys.ToArray(),
            _ => throw new ArgumentException($"'{type}' is not an enum."),
        };

        return labels.Concat(Enumerable.Reverse(labels)).ToArray();
    }
}
