using System.Globalization;
using System.Net;
using System.Numerics;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Shows the CLR values used for representative ClickHouse scalar types.</summary>
public static class TcpScalarTypes
{
    private const string TableName = "example_tcp_scalar_types";
    private const string Columns =
        "u8, i64, u128, i128, u256, i256, f32, f64, bf16, d64, d128, " +
        "flag, text, fixed5, id, ip4, ip6, colour";

    public static async Task Run()
    {
        var builder = ExampleConfig.TcpBuilder();

        // BFloat16 is setting-gated on older supported ClickHouse versions.
        builder["set_allow_experimental_bfloat16_type"] = 1;

        await using var client = new ClickHouseTcpClient(builder.ToOptions());
        await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");

        try
        {
            await client.ExecuteAsync($"""
                CREATE TABLE {TableName}
                (
                    u8 UInt8,
                    i64 Int64,
                    u128 UInt128,
                    i128 Int128,
                    u256 UInt256,
                    i256 Int256,
                    f32 Float32,
                    f64 Float64,
                    bf16 BFloat16,
                    d64 Decimal64(4),
                    d128 Decimal128(20),
                    flag Bool,
                    text String,
                    fixed5 FixedString(5),
                    id UUID,
                    ip4 IPv4,
                    ip6 IPv6,
                    colour Enum8('red' = 1, 'green' = 2)
                )
                ENGINE = MergeTree
                ORDER BY id
                """);

            await client.InsertAsync(
                $"INSERT INTO {TableName} ({Columns}) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("u8", new byte[] { 200 }),
                    ClickHouseTcpColumn.Create("i64", new[] { -42L }),
                    ClickHouseTcpColumn.Create("u128", new[] { UInt128.MaxValue }),
                    ClickHouseTcpColumn.Create("i128", new[] { Int128.MinValue }),
                    ClickHouseTcpColumn.Create(
                        "u256",
                        new[] { UInt256.FromBigInteger(BigInteger.Pow(2, 255)) }),
                    ClickHouseTcpColumn.Create(
                        "i256",
                        new[] { Int256.FromBigInteger(-BigInteger.Pow(2, 255)) }),
                    ClickHouseTcpColumn.Create("f32", new[] { 1.5f }),
                    ClickHouseTcpColumn.Create("f64", new[] { -2.25 }),
                    ClickHouseTcpColumn.Create("bf16", new[] { 0.1f }),
                    ClickHouseTcpColumn.Create("d64", new[] { 1.2345m }),

                    // Decimal128 has 38-digit precision, so it uses ClickHouseTcpDecimal.
                    ClickHouseTcpColumn.Create(
                        "d128",
                        new[]
                        {
                            new ClickHouseTcpDecimal(
                                BigInteger.Parse(
                                    "123456789012345678901234567890",
                                    CultureInfo.InvariantCulture),
                                20),
                        }),
                    ClickHouseTcpColumn.Create("flag", new[] { true }),
                    ClickHouseTcpColumn.Create("text", new[] { "hello" }),

                    // FixedString is binary data; byte[] preserves zeros and non-UTF-8 bytes.
                    ClickHouseTcpColumn.Create(
                        "fixed5",
                        new[] { new byte[] { 0x61, 0x00, 0x62, 0xFF, 0x10 } }),
                    ClickHouseTcpColumn.Create(
                        "id",
                        new[] { Guid.Parse("61f0c404-5cb3-11e7-907b-a6006ad3dba0") }),
                    ClickHouseTcpColumn.Create("ip4", new[] { IPAddress.Parse("192.168.0.1") }),
                    ClickHouseTcpColumn.Create("ip6", new[] { IPAddress.Parse("2001:db8::1") }),

                    // Enum columns use their signed integer storage type on the block tier.
                    ClickHouseTcpColumn.Create("colour", new sbyte[] { 2 }),
                });

            await foreach (Block block in client.StreamAsync($"SELECT {Columns} FROM {TableName}"))
            {
                foreach (IColumn column in block.Columns)
                {
                    Console.WriteLine(
                        $"{column.Name,-7} {column.TypeName,-28} " +
                        $"-> {FriendlyName(column.ElementType),-20} {Render(column.GetValue(0))}");
                }
            }
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        }
    }

    private static string FriendlyName(Type type) => type switch
    {
        _ when type == typeof(byte[]) => "byte[]",
        _ => type.Name,
    };

    private static string Render(object? value) => value switch
    {
        byte[] bytes => "0x" + Convert.ToHexString(bytes),
        string text => $"\"{text}\"",
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        null => "NULL",
        _ => value.ToString() ?? "NULL",
    };
}
