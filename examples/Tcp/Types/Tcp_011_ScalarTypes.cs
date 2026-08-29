using System.Globalization;
using System.Net;
using System.Numerics;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// What CLR type each ClickHouse scalar becomes, and the four families where the answer is not the obvious one:
/// the 256-bit integers, the decimals, <c>BFloat16</c>, and the enums.
///
/// <para>
/// One rule underlies all of it: the client hands back <b>the value the wire carried</b>, in the narrowest CLR
/// type that holds it without loss. So <c>UInt8</c> is a <see cref="byte"/> and not an <see cref="int"/>,
/// <c>FixedString(N)</c> is a <see cref="byte"/>[] and not a <see cref="string"/>, and an <c>Enum8</c> is its
/// ordinal and not its label. The same type is what an insert column must hold, in both directions.
/// </para>
///
/// <para>
/// <c>Tcp_012</c> covers the date and time family, <c>Tcp_013</c> the composites. This one assumes the block tier
/// from <c>Tcp_006</c>.
/// </para>
/// </summary>
public static class TcpScalarTypes
{
    private const string TableName = "example_tcp_scalar_types";

    // Every column of the table above, in one list, so the DDL, the insert and the read agree.
    private const string Columns =
        "u8, i8, u16, i16, u32, i32, u64, i64, u128, i128, u256, i256, " +
        "f32, f64, bf16, d32, d64, d128, d256, flag, text, fixed5, id, ip4, ip6, e8, e16";

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();

        try
        {
            await Seed(client);
            await TheWholeMap(client);
            await WideIntegers(client);
            await Decimals(client);
            await Floats(client);
            await StringsAndBytes(client);
            await Enums(client);
            await Nothing(client);
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
            Console.WriteLine($"\nDropped '{TableName}'");
        }
    }

    private static async Task Seed(ClickHouseTcpClient client)
    {
        await client.ExecuteAsync($@"
            CREATE TABLE {TableName}
            (
                u8 UInt8, i8 Int8, u16 UInt16, i16 Int16, u32 UInt32, i32 Int32, u64 UInt64, i64 Int64,
                u128 UInt128, i128 Int128, u256 UInt256, i256 Int256,
                f32 Float32, f64 Float64, bf16 BFloat16,
                d32 Decimal32(2), d64 Decimal64(4), d128 Decimal128(20), d256 Decimal256(40),
                flag Bool, text String, fixed5 FixedString(5),
                id UUID, ip4 IPv4, ip6 IPv6,
                e8 Enum8('red' = 1, 'green' = 2), e16 Enum16('small' = 100, 'big' = 3000)
            )
            ENGINE = MergeTree()
            ORDER BY u64");

        // One row, written column by column. Each array's element type is the CLR type that column accepts, so
        // this list is the write-side answer to the same question the read side answers below.
        await client.InsertAsync(
            $"INSERT INTO {TableName} ({Columns}) VALUES",
            new IColumn[]
            {
                ClickHouseTcpColumn.Create("u8", new byte[] { 200 }),
                ClickHouseTcpColumn.Create("i8", new sbyte[] { -100 }),
                ClickHouseTcpColumn.Create("u16", new ushort[] { 60000 }),
                ClickHouseTcpColumn.Create("i16", new short[] { -30000 }),
                ClickHouseTcpColumn.Create("u32", new uint[] { 4000000000 }),
                ClickHouseTcpColumn.Create("i32", new[] { -2000000000 }),
                ClickHouseTcpColumn.Create("u64", new ulong[] { ulong.MaxValue }),
                ClickHouseTcpColumn.Create("i64", new[] { long.MinValue }),
                ClickHouseTcpColumn.Create("u128", new[] { UInt128.MaxValue }),
                ClickHouseTcpColumn.Create("i128", new[] { Int128.MinValue }),

                // The only two numeric types the driver defines itself: .NET stops at 128 bits.
                ClickHouseTcpColumn.Create("u256", new[] { UInt256.FromBigInteger(BigInteger.Pow(2, 255)) }),
                ClickHouseTcpColumn.Create("i256", new[] { Int256.FromBigInteger(-BigInteger.Pow(2, 255)) }),

                ClickHouseTcpColumn.Create("f32", new[] { 1.5f }),
                ClickHouseTcpColumn.Create("f64", new[] { -2.25 }),

                // BFloat16 has no CLR type of its own, so it is written from and read as a float.
                ClickHouseTcpColumn.Create("bf16", new[] { 0.1f }),

                // Precision decides the CLR type: 2 and 4 digits fit a decimal, 20 and 40 do not.
                ClickHouseTcpColumn.Create("d32", new[] { 1.25m }),
                ClickHouseTcpColumn.Create("d64", new[] { 1.2345m }),
                ClickHouseTcpColumn.Create("d128", new[] { new ClickHouseTcpDecimal(BigInteger.Parse("123456789012345678901234567890", CultureInfo.InvariantCulture), 20) }),
                ClickHouseTcpColumn.Create("d256", new[] { new ClickHouseTcpDecimal(BigInteger.Pow(10, 45) + 7, 40) }),

                ClickHouseTcpColumn.Create("flag", new[] { true }),
                ClickHouseTcpColumn.Create("text", new[] { "hello" }),

                // FixedString is bytes, and exactly N of them.
                ClickHouseTcpColumn.Create("fixed5", new[] { new byte[] { 0x61, 0x00, 0x62, 0xFF, 0x10 } }),

                ClickHouseTcpColumn.Create("id", new[] { Guid.Parse("61f0c404-5cb3-11e7-907b-a6006ad3dba0") }),
                ClickHouseTcpColumn.Create("ip4", new[] { IPAddress.Parse("192.168.0.1") }),
                ClickHouseTcpColumn.Create("ip6", new[] { IPAddress.Parse("2001:db8::1") }),

                // An enum is written as its ordinal, never as its label.
                ClickHouseTcpColumn.Create("e8", new sbyte[] { 2 }),
                ClickHouseTcpColumn.Create("e16", new short[] { 3000 }),
            });

        Console.WriteLine($"Seeded '{TableName}' with one row of every scalar type, written column by column.");
        Console.WriteLine("Each Create<T> above states the CLR type that column accepts; the table below is the");
        Console.WriteLine("same answer read back.");
    }

    private static async Task TheWholeMap(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n1. The whole scalar map\n");
        Console.WriteLine("   ClickHouse type                       IColumn<T> is         The value read back");
        Console.WriteLine("   ------------------------------------  --------------------  -------------------");

        await foreach (Block block in client.StreamAsync($"SELECT {Columns} FROM {TableName}"))
        {
            foreach (IColumn column in block.Columns)
            {
                object value = column.GetValue(0);
                Console.WriteLine($"   {column.TypeName,-36}  {Describe(column.ElementType),-20}  {Render(value)}");
            }

            break;
        }

        Console.WriteLine();
        Console.WriteLine("   Nothing in that table is a conversion. ElementType is the type the wire's bytes are,");
        Console.WriteLine("   so a read costs a copy at most, and the same type is what an insert column must hold.");
    }

    private static async Task WideIntegers(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n2. The wide integers: two from .NET, two from this driver\n");
        Console.WriteLine("   Int128 and UInt128 are BCL types, so UInt128.MaxValue and Int128.MinValue work as you");
        Console.WriteLine("   expect. Int256 and UInt256 have no BCL counterpart, so the driver defines them:\n");

        await foreach (Block block in client.StreamAsync($"SELECT u128, i128, u256, i256 FROM {TableName}"))
        {
            Console.WriteLine($"     UInt128 = {block.Column<UInt128>("u128")[0]}");
            Console.WriteLine($"     Int128  = {block.Column<Int128>("i128")[0]}");

            UInt256 wide = block.Column<UInt256>("u256")[0];
            Int256 signed = block.Column<Int256>("i256")[0];
            Console.WriteLine($"     UInt256 = {wide}");
            Console.WriteLine($"     Int256  = {signed}   IsNegative {signed.IsNegative}");

            Console.WriteLine();
            Console.WriteLine("   They are 32-byte value types, four ulong limbs least significant first, and they");
            Console.WriteLine("   carry exactly what a wire value needs — no arithmetic:");
            Console.WriteLine($"     Int256.Size                     {Int256.Size} bytes");
            Console.WriteLine($"     Int256.Zero                     {Int256.Zero}");
            Console.WriteLine($"     new Int256(0, 1, 0, 0)          {new Int256(0, 1, 0, 0)}   (limb 1 is 2^64)");
            Console.WriteLine($"     signed.ToBigInteger() == -2^255 {signed.ToBigInteger() == -BigInteger.Pow(2, 255)}");

            // Round-tripping through BigInteger is how arithmetic is done: the struct has comparison operators
            // but no +, -, * or /.
            Int256 doubled = Int256.FromBigInteger(Int256.FromBigInteger(21).ToBigInteger() * 2);
            Console.WriteLine($"     21 * 2 via BigInteger           {doubled}   (there is no Int256 operator *)");

            Span<byte> raw = stackalloc byte[Int256.Size];
            signed.WriteLittleEndian(raw);
            Console.WriteLine($"     WriteLittleEndian               {Convert.ToHexString(raw)}");
            Console.WriteLine($"     ReadLittleEndian round trip     {Int256.ReadLittleEndian(raw) == signed}");

            break;
        }
    }

    private static async Task Decimals(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n3. Decimals: the declared precision decides the CLR type\n");
        Console.WriteLine("   A Decimal(P, S) is a signed integer mantissa of a width P chooses, and the value is");
        Console.WriteLine("   mantissa / 10^S. P up to 18 fits a System.Decimal; wider does not, so it surfaces as");
        Console.WriteLine("   ClickHouseTcpDecimal. That is decided by P alone, never by the value:\n");

        await foreach (Block block in client.StreamAsync(
            @"SELECT CAST('1.25', 'Decimal(18, 2)') AS at_18, CAST('1.25', 'Decimal(19, 2)') AS at_19"))
        {
            foreach (IColumn column in block.Columns)
            {
                Console.WriteLine($"     {column.TypeName,-16} -> {Describe(column.ElementType),-22} value {column.GetValue(0)}");
            }

            Console.WriteLine("     Both hold 1.25. Only the declared precision differs.");
            break;
        }

        Console.WriteLine();
        Console.WriteLine("   ClickHouseTcpDecimal is the mantissa and the scale, unchanged from the wire:\n");

        await foreach (Block block in client.StreamAsync($"SELECT d128, d256 FROM {TableName}"))
        {
            foreach (IColumn column in block.Columns)
            {
                var value = (ClickHouseTcpDecimal)column.GetValue(0);
                bool narrows = value.TryToDecimal(out decimal narrowed);
                Console.WriteLine($"     {column.Name} {column.TypeName}");
                Console.WriteLine($"       Mantissa {value.Mantissa}");
                Console.WriteLine($"       Scale {value.Scale}, Sign {value.Sign}, ToString() {value}");
                Console.WriteLine($"       TryToDecimal {narrows}{(narrows ? $" -> {narrowed}" : " (out of a System.Decimal's range)")}");
            }

            break;
        }

        Console.WriteLine();
        Console.WriteLine("   A System.Decimal holds a 96-bit mantissa and a scale of 0 to 28, so TryToDecimal fails");
        Console.WriteLine("   on either count — and ToDecimal() throws where TryToDecimal returns false:");

        await foreach (Block block in client.StreamAsync(
            @"SELECT CAST('1.25', 'Decimal(20, 2)') AS fits,
                     CAST('1234567890123456789012345678901.5', 'Decimal(38, 1)') AS mantissa_too_wide,
                     CAST('1.2345678901234567890123456789012345', 'Decimal(38, 34)') AS scale_too_deep"))
        {
            foreach (IColumn column in block.Columns)
            {
                var value = (ClickHouseTcpDecimal)column.GetValue(0);
                Console.WriteLine($"     {column.Name,-18} {column.TypeName,-16} TryToDecimal {value.TryToDecimal(out _),-5}  {value}");
            }

            break;
        }

        // Two values of different scale can be the same number, and comparison says so.
        var oneDotZero = new ClickHouseTcpDecimal((Int128)10, 1);
        var oneDotZeroZero = new ClickHouseTcpDecimal((Int128)100, 2);
        Console.WriteLine();
        Console.WriteLine("   Equality and ordering compare the value, not the representation:");
        Console.WriteLine($"     ClickHouseTcpDecimal(10, 1) == ClickHouseTcpDecimal(100, 2)  {oneDotZero == oneDotZeroZero}   ('{oneDotZero}' and '{oneDotZeroZero}')");
        Console.WriteLine($"     FromDecimal(1.2500m) keeps the trailing zeros: '{ClickHouseTcpDecimal.FromDecimal(1.2500m)}', Scale {ClickHouseTcpDecimal.FromDecimal(1.2500m).Scale}");
        Console.WriteLine();
        Console.WriteLine("   ToString() is always the invariant fixed-point rendering with exactly Scale digits.");
        Console.WriteLine("   The type implements IFormattable, but the format and the provider are ignored:");
        Console.WriteLine($"     ToString(\"F3\", InvariantCulture) = '{oneDotZero.ToString("F3", CultureInfo.InvariantCulture)}'   (not 1.000)");
    }

    private static async Task Floats(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n4. Floats, and BFloat16's missing mantissa\n");
        Console.WriteLine("   Float32 is a float and Float64 a double. BFloat16 is a float too — it is a float32 with");
        Console.WriteLine("   the low 16 mantissa bits cut off, so widening it is exact and there is nothing narrower");
        Console.WriteLine("   to hand back. What you lose is precision, on the way in:\n");

        await foreach (Block block in client.StreamAsync($"SELECT f32, f64, bf16 FROM {TableName}"))
        {
            Console.WriteLine($"     Float32  wrote 1.5f   read {block.Column<float>("f32")[0]}");
            Console.WriteLine($"     Float64  wrote -2.25  read {block.Column<double>("f64")[0]}");
            Console.WriteLine($"     BFloat16 wrote 0.1f   read {block.Column<float>("bf16")[0]:R}");
            Console.WriteLine("       7 stored mantissa bits, so 0.1 is not representable and the nearest value comes back.");
            break;
        }
    }

    private static async Task StringsAndBytes(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n5. String is text, FixedString(N) is bytes\n");

        await foreach (Block block in client.StreamAsync($"SELECT text, fixed5, id, ip4, ip6 FROM {TableName}"))
        {
            byte[] fixedBytes = block.Column<byte[]>("fixed5")[0];
            Console.WriteLine($"     String         -> string    \"{block.Column<string>("text")[0]}\"");
            Console.WriteLine($"     FixedString(5) -> byte[]    {Convert.ToHexString(fixedBytes)}  ({fixedBytes.Length} bytes)");
            Console.WriteLine("       No decoding and no trimming: an embedded 0x00 and a byte that is not valid UTF-8");
            Console.WriteLine("       both survive, which a string could not carry.");
            Console.WriteLine($"     UUID           -> Guid      {block.Column<Guid>("id")[0]}");
            Console.WriteLine($"     IPv4           -> IPAddress {block.Column<IPAddress>("ip4")[0]}");
            Console.WriteLine($"     IPv6           -> IPAddress {block.Column<IPAddress>("ip6")[0]}");
            Console.WriteLine("       One CLR type for both, told apart by AddressFamily.");
            break;
        }

        await foreach (Block block in client.StreamAsync(
            "SELECT toIPv4('10.0.0.1') AS four, toIPv6('10.0.0.1') AS six"))
        {
            Console.WriteLine();
            Console.WriteLine("       The same address in each column, and the family is what differs:");
            foreach (IColumn column in block.Columns)
            {
                var address = (IPAddress)column.GetValue(0)!;
                Console.WriteLine($"         {column.TypeName,-5} {address,-18} AddressFamily {address.AddressFamily}");
            }

            Console.WriteLine("       An IPv4 address in an IPv6 column is the mapped form, ::ffff:a.b.c.d.");
            break;
        }

        Console.WriteLine();
        Console.WriteLine("   A String is decoded as UTF-8, so a String column carrying arbitrary bytes is lossy:");

        await foreach (Block block in client.StreamAsync(
            "SELECT CAST(unhex('C3A9') AS String) AS valid, CAST(unhex('FFFE') AS String) AS invalid"))
        {
            foreach (IColumn column in block.Columns)
            {
                string text = (string)column.GetValue(0);
                Console.WriteLine($"     {column.Name,-8} = \"{text}\"  -> re-encoded {Convert.ToHexString(System.Text.Encoding.UTF8.GetBytes(text))}");
            }

            Console.WriteLine("     0xFFFE came back as two replacement characters. Use FixedString(N) for bytes.");
            break;
        }

        Console.WriteLine();
        Console.WriteLine("   An insert supplies exactly N bytes. Shorter is refused rather than padded:");

        try
        {
            await client.InsertAsync(
                $"INSERT INTO {TableName} (u64, fixed5) VALUES",
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("u64", new ulong[] { 2 }),
                    ClickHouseTcpColumn.Create("fixed5", new[] { new byte[] { 0x61, 0x62 } }),
                });
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine($"     {Wrap(ex.Message.Split(" (Parameter")[0])}");
        }
    }

    private static async Task Enums(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n6. An enum is its ordinal; the labels live in the type string\n");

        await foreach (Block block in client.StreamAsync($"SELECT e8, e16 FROM {TableName}"))
        {
            foreach (IColumn column in block.Columns)
            {
                Console.WriteLine($"     {column.Name,-4} {column.TypeName}");
                Console.WriteLine($"          reads as {Describe(column.ElementType)} = {column.GetValue(0)}");
            }

            break;
        }

        Console.WriteLine();
        Console.WriteLine("   Enum8 is an Int8 ordinal and Enum16 an Int16 one, and no tier maps either back to its");
        Console.WriteLine("   label. The definition is in IColumn.TypeName, so the map is recoverable — but you parse");
        Console.WriteLine("   it, or you ask the server:");

        object label = await client.ExecuteScalarAsync($"SELECT toString(e8) FROM {TableName} LIMIT 1");
        Console.WriteLine($"     SELECT toString(e8) -> '{label}'   (the server's own reverse lookup)");

        Console.WriteLine();
        Console.WriteLine("   The POCO tier refuses a string property over an enum column rather than guessing:");

        try
        {
            await foreach (EnumRow _ in client.QueryAsync<EnumRow>($"SELECT e8 AS Colour FROM {TableName}"))
            {
                break;
            }
        }
        catch (InvalidOperationException ex)
        {
            Console.WriteLine($"     {Wrap(ex.Message)}");
        }

        Console.WriteLine();
        Console.WriteLine("   And an insert takes the ordinal, so map your own enum to its numeric value:");
        Console.WriteLine("     ClickHouseTcpColumn.Create(\"e8\", new sbyte[] { (sbyte)Colour.Green })");
    }

    private static async Task Nothing(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n7. Nothing: the type of a value that has no type\n");
        Console.WriteLine("   The server gives an untyped NULL and an untyped empty array the Nothing type. It cannot");
        Console.WriteLine("   be a column of a table, so you only ever meet it in an expression's result:\n");

        await foreach (Block block in client.StreamAsync("SELECT NULL AS nothing_at_all, [] AS empty_array"))
        {
            foreach (IColumn column in block.Columns)
            {
                Console.WriteLine($"     {column.Name,-14} {column.TypeName,-18} reads as {Describe(column.ElementType),-10} value {Render(column.GetValue(0))}");
            }

            break;
        }

        Console.WriteLine();
        Console.WriteLine("   object is the element type because there is no value to have a type. The server refuses");
        Console.WriteLine("   to store one at all:");

        try
        {
            await client.ExecuteAsync($"CREATE TABLE {TableName}_nothing (c Nothing) ENGINE = MergeTree ORDER BY tuple()");
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}_nothing");
            Console.WriteLine("     accepted, which this example did not expect");
        }
        catch (ClickHouseTcpServerException ex)
        {
            Console.WriteLine($"     {FirstLine(ex.Message)}");
        }

        Console.WriteLine();
        Console.WriteLine("   So a query whose column may be Nothing wants a CAST: SELECT CAST(NULL, 'Nullable(Int32)').");
    }

    // A POCO whose property type is deliberately wrong for the column, to show what the mapping reports.
    private sealed class EnumRow
    {
        public string Colour { get; set; } = string.Empty;
    }

    // The C# spelling of a CLR type, which is how a reader will write it.
    private static string Describe(Type type) => type switch
    {
        _ when type == typeof(byte) => "byte",
        _ when type == typeof(sbyte) => "sbyte",
        _ when type == typeof(ushort) => "ushort",
        _ when type == typeof(short) => "short",
        _ when type == typeof(uint) => "uint",
        _ when type == typeof(int) => "int",
        _ when type == typeof(ulong) => "ulong",
        _ when type == typeof(long) => "long",
        _ when type == typeof(float) => "float",
        _ when type == typeof(double) => "double",
        _ when type == typeof(decimal) => "decimal",
        _ when type == typeof(bool) => "bool",
        _ when type == typeof(string) => "string",
        _ when type == typeof(byte[]) => "byte[]",
        _ when type == typeof(object) => "object",
        _ when type == typeof(object[]) => "object[]",
        _ => type.Name,
    };

    private static string Render(object? value) => value switch
    {
        null => "NULL",
        byte[] bytes => "0x" + Convert.ToHexString(bytes),
        string text => $"\"{text}\"",
        bool flag => flag ? "true" : "false",
        object[] { Length: 0 } => "[]",
        float single => single.ToString("R", CultureInfo.InvariantCulture),
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "NULL",
    };

    private static string FirstLine(string message)
    {
        int newline = message.IndexOf('\n');
        string line = newline < 0 ? message : message[..newline];
        return line.StartsWith("DB::Exception: ", StringComparison.Ordinal) ? line["DB::Exception: ".Length..] : line;
    }

    // Reflows a long driver message so the console output stays readable.
    private static string Wrap(string message)
    {
        var lines = new List<string>();
        var line = new System.Text.StringBuilder();
        foreach (string word in message.Split(' '))
        {
            if (line.Length + word.Length + 1 > 88)
            {
                lines.Add(line.ToString());
                line.Clear();
            }

            line.Append(line.Length == 0 ? word : " " + word);
        }

        lines.Add(line.ToString());
        return string.Join("\n     ", lines);
    }
}
