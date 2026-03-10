using System.Text;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Guide to String and FixedString handling in ClickHouse.Driver.
/// Covers:
/// - String and FixedString types (which are binary, not necessarily UTF-8)
/// - Reading strings as byte[] for binary data
/// - Writing binary data via byte[], Stream, and ReadOnlyMemory
/// - The ReadStringsAsByteArrays connection setting
/// </summary>
public static class StringHandling
{
    public static async Task Run()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        Console.WriteLine("String and FixedString Handling Examples\n");
        Console.WriteLine("=".PadRight(60, '='));

        // Example 1: Basic String and FixedString
        Console.WriteLine("\n1. Basic String and FixedString Types:");
        await Example1_BasicStringTypes(connection);

        // Example 2: FixedString padding behavior
        Console.WriteLine("\n2. FixedString Padding Behavior:");
        await Example2_FixedStringPadding(connection);

        // Example 3: Writing binary data
        Console.WriteLine("\n3. Writing Binary Data to String Columns, using byte[], Stream, or ReadOnlyMemory:");
        await Example3_WritingBinaryData(connection);

        // Example 4: Reading strings as byte arrays
        Console.WriteLine("\n4. Reading Strings as Byte Arrays (ReadStringsAsByteArrays):");
        await Example4_ReadStringsAsByteArrays();

        Console.WriteLine("\n" + "=".PadRight(60, '='));
        Console.WriteLine("All String handling examples completed!");
    }

    /// <summary>
    /// String is a variable-length type. FixedString(N) is fixed-length.
    /// Both are binary types in ClickHouse - they can store any bytes, not just UTF-8.
    /// By default, both return as .NET string, decoded using UTF8.
    /// </summary>
    private static async Task Example1_BasicStringTypes(ClickHouseConnection connection)
    {
        var tableName = "example_string_basic";

        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt32,
                str String,
                fixed_str FixedString(10)
            )
            ENGINE = Memory
        ");

        // Insert string values
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $@"
                INSERT INTO {tableName} (id, str, fixed_str)
                VALUES ({{id:UInt32}}, {{str:String}}, {{fixed:FixedString(10)}})
            ";

            command.AddParameter("id", 1);
            command.AddParameter("str", "Hello, World!");
            command.AddParameter("fixed", "Test");  // Will be padded to 10 bytes

            await command.ExecuteNonQueryAsync();
        }

        // Read values - both return as string by default
        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT str, fixed_str FROM {tableName}");

        if (reader.Read())
        {
            var str = reader.GetString(0);
            var fixedStr = reader.GetString(1);

            Console.WriteLine($"   String: \"{str}\"");
            Console.WriteLine($"   FixedString(10): \"{fixedStr}\" (length: {fixedStr.Length} chars)");
            Console.WriteLine("   Note: FixedString is padded with null bytes to reach the specified length.");
        }

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// FixedString(N) always stores exactly N bytes.
    /// Shorter strings are padded with null bytes (0x00).
    /// </summary>
    private static async Task Example2_FixedStringPadding(ClickHouseConnection connection)
    {
        var tableName = "example_fixedstring_padding";

        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                fixed_str FixedString(5)
            )
            ENGINE = Memory
        ");

        using var client = new ClickHouseClient("Host=localhost");

        // Insert strings of different lengths
        var columns = new[] { "fixed_str" };
        var data = new List<object[]>
        {
            new object[] { "AB" },    // 2 bytes -> padded to 5
            new object[] { "ABCDE" }, // 5 bytes -> exact fit
        };

        await client.InsertBinaryAsync(tableName, columns, data);

        // Read back and show the actual bytes
        var cb = new ClickHouseConnectionStringBuilder("Host=localhost")
        {
            ReadStringsAsByteArrays = true
        };
        using var conn2 = new ClickHouseConnection(cb.ToString());

        using var reader = await conn2.ExecuteReaderAsync($"SELECT fixed_str FROM {tableName}");

        Console.WriteLine("   Stored bytes:");
        while (reader.Read())
        {
            var bytes = (byte[])reader.GetValue(0);
            Console.WriteLine($"     [{string.Join(", ", bytes.Select(b => $"0x{b:X2}"))}] = \"{Encoding.UTF8.GetString(bytes).TrimEnd('\0')}\"");
        }

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// String and FixedString can store any binary data, not just valid UTF-8.
    /// You can write binary data using byte[], ReadOnlyMemory&lt;byte&gt;, or Stream.
    /// </summary>
    private static async Task Example3_WritingBinaryData(ClickHouseConnection connection)
    {
        var tableName = "example_binary_data";

        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt32,
                data String
            )
            ENGINE = Memory
        ");

        using var client = new ClickHouseClient("Host=localhost");

        // Write binary data that is NOT valid UTF-8
        var binaryData = new byte[] { 0xFF, 0xFE, 0x00, 0x01, 0x02 };

        var columns = new[] { "id", "data" };
        var data = new List<object[]>
        {
            new object[] { 1u, binaryData },  // Write as byte[]
            new object[] { 2u, new ReadOnlyMemory<byte>(binaryData) },  // Write as ReadOnlyMemory<byte>,
            new object[] { 3u, new MemoryStream(binaryData) }, // Write as Stream
        };

        await client.InsertBinaryAsync(tableName, columns, data);
        Console.WriteLine($"   Inserted {data.Count} rows with binary data");

        // Read back as byte[] to preserve the binary data
        var cb = new ClickHouseConnectionStringBuilder("Host=localhost")
        {
            ReadStringsAsByteArrays = true
        };
        using var conn2 = new ClickHouseConnection(cb.ToString());

        using var reader = await conn2.ExecuteReaderAsync($"SELECT id, data FROM {tableName} ORDER BY id");

        Console.WriteLine("   Read back as byte[]:");
        while (reader.Read())
        {
            var id = reader.GetFieldValue<uint>(0);
            var bytes = (byte[])reader.GetValue(1);
            Console.WriteLine($"     Row {id}: [{string.Join(", ", bytes.Select(b => $"0x{b:X2}"))}]");
        }

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// The ReadStringsAsByteArrays connection setting controls how String/FixedString
    /// columns are returned when reading. By default (false), they return as string.
    /// When true, they return as byte[].
    /// </summary>
    private static async Task Example4_ReadStringsAsByteArrays()
    {
        // Default behavior: returns string
        Console.WriteLine("   Default (ReadStringsAsByteArrays=false):");
        using (var connection = new ClickHouseConnection("Host=localhost"))
        {
            var result = await connection.ExecuteScalarAsync("SELECT 'Hello'");
            Console.WriteLine($"     Type: {result.GetType().Name}, Value: \"{result}\"");
        }

        // With setting enabled: returns byte[]
        Console.WriteLine("   With ReadStringsAsByteArrays=true:");
        using (var connection = new ClickHouseConnection("Host=localhost;ReadStringsAsByteArrays=true"))
        {
            var result = await connection.ExecuteScalarAsync("SELECT 'Hello'");
            var bytes = (byte[])result;
            Console.WriteLine($"     Type: {result.GetType().Name}, Value: [{string.Join(", ", bytes.Select(b => $"0x{b:X2}"))}]");
        }

    }
}
