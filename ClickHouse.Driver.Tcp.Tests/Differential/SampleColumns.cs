using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Text;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// Builds a column of sample values for a ClickHouse type, in the type's canonical CLR type, for the cases that
/// name only a type.
/// </summary>
/// <remarks>
/// The values of a leaf type are valid for every reading of that type: for example, every <c>Time</c> value is a
/// time of day, so the <see cref="TimeOnly"/> reading gives values too. A <c>Nullable</c> column has NULL in row 1
/// and row 4, a <c>LowCardinality</c> column repeats values, and an <c>Array</c> or a <c>Map</c> has 0, 1 or 2
/// elements in a row.
/// </remarks>
internal static class SampleColumns
{
    /// <summary>The number of rows of a sample column.</summary>
    public const int RowCount = 5;

    /// <summary>Builds a sample column.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="columnType">The ClickHouse type.</param>
    /// <returns>An array column of the type's canonical CLR type.</returns>
    /// <exception cref="NotSupportedException">The generator has no sample values for a type in <paramref name="columnType"/>.</exception>
    public static IColumn Build(string name, string columnType)
    {
        TypeNode node = TypeParser.Parse(columnType);
        Type elementType = ElementType(node);
        Array values = Array.CreateInstance(elementType, RowCount);
        for (int row = 0; row < RowCount; row++)
        {
            values.SetValue(Value(node, row), row);
        }

        return (IColumn)Activator.CreateInstance(typeof(ArrayColumn<>).MakeGenericType(elementType), name, columnType, values);
    }

    private static Type ElementType(TypeNode node) => ColumnCodecRegistry.Default.ResolveNode(node, DifferentialEngine.Context).ElementType;

    private static object Value(TypeNode node, int row)
    {
        switch (node.Name)
        {
            case "Nullable":
                return row % 3 == 1 ? null : Value(node.Arguments[0], row);

            case "LowCardinality":
                // Rows 0 and 3 have one value, and rows 1 and 4 another, so the dictionary has fewer entries than the
                // column has rows.
                return Value(node.Arguments[0], row % 3);

            case "Array":
            {
                TypeNode inner = node.Arguments[0];
                Array elements = Array.CreateInstance(ElementType(inner), row % 3);
                for (int i = 0; i < elements.Length; i++)
                {
                    elements.SetValue(Value(inner, (row * 3) + i), i);
                }

                return elements;
            }

            case "Map":
            {
                Type pairType = typeof(KeyValuePair<,>).MakeGenericType(ElementType(node.Arguments[0]), ElementType(node.Arguments[1]));
                Array pairs = Array.CreateInstance(pairType, row % 3);
                for (int i = 0; i < pairs.Length; i++)
                {
                    // The keys of a row are rows 0 and 1 of the key type, which are different values.
                    pairs.SetValue(Activator.CreateInstance(pairType, Value(node.Arguments[0], i), Value(node.Arguments[1], (row * 3) + i)), i);
                }

                return pairs;
            }

            case "Tuple":
                return Activator.CreateInstance(ElementType(node), node.Arguments.Select((field, i) => Value(field, row + i)).ToArray());

            case "Variant":
                // Rows 1 and 4 are NULL. Rows 0 and 3 take the first alternative, and row 2 takes the second.
                return row % 3 == 1 ? null : Value(node.Arguments[row % 3 == 0 ? 0 : Math.Min(1, node.Arguments.Count - 1)], row);

            case "Dynamic":
                return (row % 3) switch
                {
                    0 => (object)(ulong)(row + 40),
                    1 => null,
                    _ => $"dynamic-{row}",
                };

            default:
                return Leaf(node, row);
        }
    }

    private static object Leaf(TypeNode node, int row)
    {
        switch (node.Name)
        {
            case "UInt8": return Pick(row, (byte)0, (byte)1, (byte)128, byte.MaxValue, (byte)7);
            case "Int8": return Pick(row, sbyte.MinValue, (sbyte)-1, (sbyte)0, sbyte.MaxValue, (sbyte)5);
            case "UInt16": return Pick(row, (ushort)0, (ushort)258, ushort.MaxValue, (ushort)1, (ushort)300);
            case "Int16": return Pick(row, short.MinValue, (short)-1, (short)0, short.MaxValue, (short)300);
            case "UInt32": return Pick(row, 0u, 1u, uint.MaxValue, 7u, 100_000u);
            case "Int32": return Pick(row, int.MinValue, -1, 0, int.MaxValue, 42);
            case "UInt64": return Pick(row, 0UL, 1UL, ulong.MaxValue, 7UL, 1UL << 40);
            case "Int64": return Pick(row, long.MinValue, -1L, 0L, long.MaxValue, 42L);
            case "UInt128": return Pick(row, UInt128.Zero, UInt128.One, UInt128.MaxValue, (UInt128)7, (UInt128)ulong.MaxValue + 1);
            case "Int128": return Pick(row, Int128.MinValue, Int128.NegativeOne, Int128.Zero, Int128.MaxValue, (Int128)42);
            case "UInt256": return Pick(row, UInt256.Zero, UInt256.FromBigInteger(1), UInt256.FromBigInteger(BigInteger.Pow(2, 200)), UInt256.FromBigInteger(BigInteger.Pow(2, 256) - 1), UInt256.FromBigInteger(7));
            case "Int256": return Pick(row, Int256.FromBigInteger(-BigInteger.Pow(2, 255)), Int256.FromBigInteger(-1), Int256.Zero, Int256.FromBigInteger(BigInteger.Pow(2, 255) - 1), Int256.FromBigInteger(42));
            case "Float32": return Pick(row, 0f, -0f, 1.5f, float.NaN, float.NegativeInfinity);
            case "Float64": return Pick(row, 0d, -0d, -1.5e100, double.NaN, double.PositiveInfinity);
            case "BFloat16": return Pick(row, 0f, -0f, 1f, -2f, 0.5f);
            case "Bool": return row % 2 == 0;
            case "String": return Pick(row, string.Empty, "a", "héllo✓", "a\0b", "zz");
            case "FixedString": return FixedString(int.Parse(node.Arguments[0].Name, System.Globalization.CultureInfo.InvariantCulture), row);
            case "Date": return Pick(row, new DateOnly(1970, 1, 1), new DateOnly(2024, 1, 15), new DateOnly(2149, 6, 6), new DateOnly(2000, 2, 29), new DateOnly(1999, 12, 31));
            case "Date32": return Pick(row, new DateOnly(1900, 1, 1), new DateOnly(1970, 1, 1), new DateOnly(2299, 12, 31), new DateOnly(2024, 1, 15), new DateOnly(1969, 7, 20));
            case "DateTime": return Pick(row, 1_700_000_000u, 0u, 599_916_153u, 4_000_000_000u, 1_000_000_000u);
            case "DateTime64": return Count(Pick(row, 1_700_000_000L, 0L, 599_916_153L, 4_000_000_000L, 1_000_000_000L), Scale(node), row);
            case "Time": return Pick(row, 0, 3_661, 45_296, 86_399, 1);
            case "Time64": return Count(Pick(row, 0L, 3_661L, 45_296L, 86_399L, 1L), Scale(node), row);
            case "UUID": return Pick(row, Guid.Empty, new Guid("00112233-4455-6677-8899-aabbccddeeff"), new Guid("ffffffff-ffff-ffff-ffff-ffffffffffff"), new Guid("01234567-89ab-cdef-0123-456789abcdef"), new Guid("10000000-0000-0000-0000-000000000001"));
            case "IPv4": return IPAddress.Parse(Pick(row, "0.0.0.0", "127.0.0.1", "192.168.1.1", "255.255.255.255", "10.0.0.1"));
            case "IPv6": return IPAddress.Parse(Pick(row, "::", "::1", "2001:db8::1", "fe80::1", "2001:db8:85a3:8d3:1319:8a2e:370:7348"));
            case "Decimal": return Decimal(node, row);
            case "Enum8": return (sbyte)EnumOrdinal(node, row);
            case "Enum16": return (short)EnumOrdinal(node, row);
            case "JSON": return Pick(row, "{}", "{\"a\":1}", "{\"b\":\"x\"}", "{\"a\":2,\"b\":\"y\"}", "{}");
            default:
                throw new NotSupportedException($"SampleColumns has no sample values for '{node}'. Add them to SampleColumns.Leaf.");
        }
    }

    private static T Pick<T>(int row, params T[] values) => values[row % values.Length];

    private static int Scale(TypeNode node) => int.Parse(node.Arguments[0].Name, System.Globalization.CultureInfo.InvariantCulture);

    // A count at the given scale: whole seconds, plus a fraction of a second in the odd rows.
    private static long Count(long seconds, int scale, int row)
    {
        long unit = (long)Math.Pow(10, scale);
        long fraction = row % 2 == 1 ? 123_456_789L / (long)Math.Pow(10, 9 - scale) : 0;
        return (seconds * unit) + fraction;
    }

    // UTF-8 text, zero-padded to the width, so a read as text and a write from that text keep the bytes.
    private static byte[] FixedString(int width, int row)
    {
        string text = Pick(row, "abcdefgh", string.Empty, "ab", "zzzzzzzz", "x");
        var bytes = new byte[width];
        Encoding.UTF8.GetBytes(text.AsSpan(0, Math.Min(text.Length, width)), bytes);
        return bytes;
    }

    private static object Decimal(TypeNode node, int row)
    {
        int scale = int.Parse(node.Arguments[1].Name, System.Globalization.CultureInfo.InvariantCulture);
        long mantissa = Pick(row, 0L, 123L, -123L, 99_999L, -1L);
        return ElementType(node) == typeof(decimal)
            ? new decimal((int)Math.Abs(mantissa), 0, 0, mantissa < 0, (byte)scale)
            : new ClickHouseTcpDecimal(new BigInteger(mantissa), scale);
    }

    private static long EnumOrdinal(TypeNode node, int row)
    {
        long[] ordinals = node.Arguments.Select(member => EnumColumnCodec.ParseMember(member.Name, node).Ordinal).ToArray();
        return ordinals[row % ordinals.Length];
    }
}
