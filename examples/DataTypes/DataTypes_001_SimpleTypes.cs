using System.Numerics;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates the simple/scalar data types supported by ClickHouse and their .NET mappings.
/// Covers numeric types (integers, floats, decimals) and booleans.
/// </summary>
public static class SimpleTypes
{
    public static async Task Run()
    {
        using var client = new ClickHouseClient("Host=localhost");

        Console.WriteLine("Simple Data Types Examples\n");

        await IntegerTypes(client);
        await FloatingPointTypes(client);
        await DecimalTypes(client);
        await BooleanType(client);
    }

    /// <summary>
    /// Demonstrates all integer types from 8-bit to 256-bit, signed and unsigned.
    /// </summary>
    private static async Task IntegerTypes(ClickHouseClient client)
    {
        Console.WriteLine("1. Integer Types:");
        Console.WriteLine("   ClickHouse Type    .NET Type       Example Value");
        Console.WriteLine("   --------------    ---------       -------------");

        // Signed integers
        var int8 = await client.ExecuteScalarAsync("SELECT toInt8(-128)");
        Console.WriteLine($"   Int8               sbyte           {int8}");

        var int16 = await client.ExecuteScalarAsync("SELECT toInt16(-32768)");
        Console.WriteLine($"   Int16              short           {int16}");

        var int32 = await client.ExecuteScalarAsync("SELECT toInt32(-2147483648)");
        Console.WriteLine($"   Int32              int             {int32}");

        var int64 = await client.ExecuteScalarAsync("SELECT toInt64(-9223372036854775808)");
        Console.WriteLine($"   Int64              long            {int64}");

        var int128 = await client.ExecuteScalarAsync("SELECT toInt128(-170141183460469231731687303715884105728)");
        Console.WriteLine($"   Int128             BigInteger      {int128}");

        var int256 = await client.ExecuteScalarAsync("SELECT toInt256(-57896044618658097711785492504343953926634992332820282019728792003956564819968)");
        Console.WriteLine($"   Int256             BigInteger      {((BigInteger)int256!).ToString().Substring(0, 20)}...");

        // Unsigned integers
        var uint8 = await client.ExecuteScalarAsync("SELECT toUInt8(255)");
        Console.WriteLine($"   UInt8              byte            {uint8}");

        var uint16 = await client.ExecuteScalarAsync("SELECT toUInt16(65535)");
        Console.WriteLine($"   UInt16             ushort          {uint16}");

        var uint32 = await client.ExecuteScalarAsync("SELECT toUInt32(4294967295)");
        Console.WriteLine($"   UInt32             uint            {uint32}");

        var uint64 = await client.ExecuteScalarAsync("SELECT toUInt64(18446744073709551615)");
        Console.WriteLine($"   UInt64             ulong           {uint64}");

        var uint128 = await client.ExecuteScalarAsync("SELECT toUInt128(340282366920938463463374607431768211455)");
        Console.WriteLine($"   UInt128            BigInteger      {((BigInteger)uint128!).ToString().Substring(0, 20)}...");

        Console.WriteLine();
    }

    /// <summary>
    /// Demonstrates floating point types: Float32, Float64.
    /// </summary>
    private static async Task FloatingPointTypes(ClickHouseClient client)
    {
        Console.WriteLine("2. Floating Point Types:");
        Console.WriteLine("   ClickHouse Type    .NET Type       Example Value");
        Console.WriteLine("   --------------    ---------       -------------");

        var float32 = await client.ExecuteScalarAsync("SELECT toFloat32(3.14159)");
        Console.WriteLine($"   Float32            float           {float32}");

        var float64 = await client.ExecuteScalarAsync("SELECT toFloat64(3.141592653589793)");
        Console.WriteLine($"   Float64            double          {float64}");

        // Special values
        var inf = await client.ExecuteScalarAsync("SELECT toFloat64(1) / toFloat64(0)");
        Console.WriteLine($"   Float64             double          {inf} (toFloat64(1) / toFloat64(0))");

        var nan = await client.ExecuteScalarAsync("SELECT toFloat64(0) / toFloat64(0)");
        Console.WriteLine($"   Float64             double          {nan} (toFloat64(0) / toFloat64(0))");

        Console.WriteLine();
    }

    /// <summary>
    /// Demonstrates decimal types with various precisions.
    /// ClickHouseDecimal preserves full precision for large decimals.
    /// </summary>
    private static async Task DecimalTypes(ClickHouseClient client)
    {
        Console.WriteLine("3. Decimal Types:");
        Console.WriteLine("   ClickHouse Type    .NET Type            Example Value");
        Console.WriteLine("   --------------    ---------            -------------");

        // Decimal32 - up to 9 digits of precision
        var decimal32 = await client.ExecuteScalarAsync("SELECT toDecimal32(123.456, 3)");
        Console.WriteLine($"   Decimal32(3)       ClickHouseDecimal    {decimal32}");

        // Decimal64 - up to 18 digits of precision
        var decimal64 = await client.ExecuteScalarAsync("SELECT toDecimal64(123456789.123456789, 9)");
        Console.WriteLine($"   Decimal64(9)       ClickHouseDecimal    {decimal64}");

        // Decimal128 - up to 38 digits of precision
        var decimal128 = await client.ExecuteScalarAsync("SELECT toDecimal128(1234567890123456789.12345678901234567890, 20)");
        Console.WriteLine($"   Decimal128(20)     ClickHouseDecimal    {decimal128}");

        // Using ClickHouseDecimal for precise operations
        Console.WriteLine("\n   ClickHouseDecimal can be converted to decimal when precision allows:");
        var chDecimal = (ClickHouseDecimal)decimal64!;
        var netDecimal = chDecimal.ToDecimal(System.Globalization.CultureInfo.InvariantCulture);
        Console.WriteLine($"   ClickHouseDecimal → decimal: {netDecimal}");

        Console.WriteLine();
    }

    /// <summary>
    /// Demonstrates the Bool type.
    /// </summary>
    private static async Task BooleanType(ClickHouseClient client)
    {
        Console.WriteLine("4. Boolean Type:");
        Console.WriteLine("   ClickHouse Type    .NET Type       Example Value");
        Console.WriteLine("   --------------    ---------       -------------");

        var boolTrue = await client.ExecuteScalarAsync("SELECT true");
        Console.WriteLine($"   Bool               bool            {boolTrue}");

        var boolFalse = await client.ExecuteScalarAsync("SELECT false");
        Console.WriteLine($"   Bool               bool            {boolFalse}");

        // Bool from expression
        var boolExpr = await client.ExecuteScalarAsync("SELECT (1 > 0)::Bool");
        Console.WriteLine($"   Bool (expr)        bool            {boolExpr} (1 > 0)");

        Console.WriteLine();
    }
}
