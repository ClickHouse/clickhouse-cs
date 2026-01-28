using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Comprehensive guide to DateTime handling in ClickHouse.Driver.
/// Covers:
/// - DateTime and DateTime64 types with and without timezones
/// - Date and Date32 types
/// - DateTime.Kind behavior when reading
/// - Writing DateTime values via parameters and bulk copy
/// - Working with DateTimeOffset
/// - Important timezone gotchas
/// </summary>
public static class DateTimeHandling
{
    public static async Task Run()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        Console.WriteLine("DateTime Handling Examples\n");
        Console.WriteLine("=".PadRight(60, '='));

        // Example 1: Date types (date-only, no time component)
        Console.WriteLine("\n1. Date Types (Date and Date32):");
        await Example1_DateTypes(connection);

        // Example 2: DateTime without timezone
        Console.WriteLine("\n2. DateTime Without Timezone:");
        await Example2_DateTimeNoTimezone(connection);

        // Example 3: DateTime with UTC timezone
        Console.WriteLine("\n3. DateTime With UTC Timezone:");
        await Example3_DateTimeUtc(connection);

        // Example 4: DateTime with non-UTC timezone
        Console.WriteLine("\n4. DateTime With Non-UTC Timezone (Europe/Amsterdam):");
        await Example4_DateTimeNonUtc(connection);

        // Example 5: DateTime64 with sub-second precision
        Console.WriteLine("\n5. DateTime64 With Sub-Second Precision:");
        await Example5_DateTime64(connection);

        // Example 6: Writing DateTime via parameters
        Console.WriteLine("\n6. Writing DateTime Via Parameters:");
        await Example6_WriteViaParameters(connection);

        // Example 7: Writing DateTime via bulk copy
        Console.WriteLine("\n7. Writing DateTime Via Bulk Copy:");
        await Example7_WriteViaBulkCopy(connection);

        // Example 8: Working with DateTimeOffset
        Console.WriteLine("\n8. Working With DateTimeOffset:");
        await Example8_DateTimeOffset(connection);

        // Example 9: Getting the server timezone
        Console.WriteLine("\n9. Getting the Server Timezone:");
        await Example9_ServerTimezone(connection);

        // Example 10: Important gotchas
        Console.WriteLine("\n10. Important Gotchas and Best Practices:");
        await Example10_Gotchas(connection);

        Console.WriteLine("\n" + "=".PadRight(60, '='));
        Console.WriteLine("All DateTime handling examples completed!");
    }

    /// <summary>
    /// Date and Date32 are date-only types (no time component).
    /// - Date: stores dates from 1970-01-01 to 2149-06-06 (2 bytes)
    /// - Date32: stores dates from 1900-01-01 to 2299-12-31 (4 bytes)
    /// Both return DateOnly in .NET 6+ or DateTime with time set to 00:00:00.
    /// </summary>
    private static async Task Example1_DateTypes(ClickHouseConnection connection)
    {
        var tableName = "example_date_types";

        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt32,
                date_col Date,
                date32_col Date32
            )
            ENGINE = Memory
        ");

        // Insert date values
        using (var command = connection.CreateCommand())
        {
            command.CommandText = $@"
                INSERT INTO {tableName} (id, date_col, date32_col)
                VALUES ({{id:UInt32}}, {{date:Date}}, {{date32:Date32}})
            ";

            command.AddParameter("id", 1);
            command.AddParameter("date", new DateOnly(2024, 6, 15));
            command.AddParameter("date32", new DateOnly(1950, 1, 1)); // Before 1970, requires Date32

            await command.ExecuteNonQueryAsync();
        }

        // Read date values
        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            $"SELECT date_col, date32_col FROM {tableName}");

        if (reader.Read())
        {
            var date = reader.GetFieldValue<DateTime>(0);
            var date32 = reader.GetFieldValue<DateTime>(1);

            Console.WriteLine($"   Date: {date:yyyy-MM-dd}");
            Console.WriteLine($"   Date32: {date32:yyyy-MM-dd} (supports dates before 1970)");
        }

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// DateTime without a timezone stores wall-clock time without timezone information.
    /// When read, returns DateTime with Kind=Unspecified.
    /// </summary>
    private static async Task Example2_DateTimeNoTimezone(ClickHouseConnection connection)
    {
        // Query a DateTime without timezone specification
        var result = (DateTime)await connection.ExecuteScalarAsync(
            "SELECT toDateTime('2024-06-15 14:30:00')");

        Console.WriteLine($"   Value: {result:yyyy-MM-dd HH:mm:ss}");
        Console.WriteLine($"   Kind: {result.Kind}");
        Console.WriteLine("   Note: DateTime without timezone returns Kind=Unspecified");
        Console.WriteLine("   The value represents wall-clock time with no timezone assumption.");
    }

    /// <summary>
    /// DateTime('UTC') stores time in UTC timezone.
    /// When read, returns DateTime with Kind=Utc.
    /// </summary>
    private static async Task Example3_DateTimeUtc(ClickHouseConnection connection)
    {
        var result = (DateTime)await connection.ExecuteScalarAsync(
            "SELECT toDateTime('2024-06-15 14:30:00', 'UTC')");

        Console.WriteLine($"   Value: {result:yyyy-MM-dd HH:mm:ss}");
        Console.WriteLine($"   Kind: {result.Kind}");
        Console.WriteLine("   Note: DateTime('UTC') returns Kind=Utc");
    }

    /// <summary>
    /// DateTime with a non-UTC timezone (e.g., 'Europe/Amsterdam') stores time in that timezone.
    /// When read as DateTime, returns Kind=Unspecified (the wall-clock time in that timezone).
    /// When read as DateTimeOffset, returns the correct offset for that timezone.
    /// </summary>
    private static async Task Example4_DateTimeNonUtc(ClickHouseConnection connection)
    {
        // January 15 - Amsterdam is UTC+1 (CET - winter time)
        var winterResult = (DateTime)await connection.ExecuteScalarAsync(
            "SELECT toDateTime('2024-01-15 14:30:00', 'Europe/Amsterdam')");

        Console.WriteLine($"   Winter (Jan 15):");
        Console.WriteLine($"     Value: {winterResult:yyyy-MM-dd HH:mm:ss}");
        Console.WriteLine($"     Kind: {winterResult.Kind}");

        // June 15 - Amsterdam is UTC+2 (CEST - summer time)
        var summerResult = (DateTime)await connection.ExecuteScalarAsync(
            "SELECT toDateTime('2024-06-15 14:30:00', 'Europe/Amsterdam')");

        Console.WriteLine($"   Summer (Jun 15):");
        Console.WriteLine($"     Value: {summerResult:yyyy-MM-dd HH:mm:ss}");
        Console.WriteLine($"     Kind: {summerResult.Kind}");

        // Read as DateTimeOffset to get the actual offset
        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            "SELECT toDateTime('2024-01-15 14:30:00', 'Europe/Amsterdam'), " +
            "toDateTime('2024-06-15 14:30:00', 'Europe/Amsterdam')");

        if (reader.Read())
        {
            var winterOffset = reader.GetDateTimeOffset(0);
            var summerOffset = reader.GetDateTimeOffset(1);

            Console.WriteLine($"   As DateTimeOffset:");
            Console.WriteLine($"     Winter: {winterOffset} (offset: {winterOffset.Offset})");
            Console.WriteLine($"     Summer: {summerOffset} (offset: {summerOffset.Offset})");
        }
    }

    /// <summary>
    /// DateTime64 provides sub-second precision up to nanoseconds.
    /// The precision parameter (0-9) determines decimal places.
    /// .NET DateTime supports up to 7 decimal places (100 nanosecond ticks).
    /// </summary>
    private static async Task Example5_DateTime64(ClickHouseConnection connection)
    {
        Console.WriteLine("   Precision examples:");

        // Precision 3 (milliseconds)
        var ms = (DateTime)await connection.ExecuteScalarAsync(
            "SELECT toDateTime64('2024-06-15 14:30:45.123', 3, 'UTC')");
        Console.WriteLine($"     Precision 3 (ms): {ms:yyyy-MM-dd HH:mm:ss.fff}");

        // Precision 6 (microseconds)
        var us = (DateTime)await connection.ExecuteScalarAsync(
            "SELECT toDateTime64('2024-06-15 14:30:45.123456', 6, 'UTC')");
        Console.WriteLine($"     Precision 6 (us): {us:yyyy-MM-dd HH:mm:ss.ffffff}");

        // Precision 7 (100 nanoseconds - .NET max)
        var ticks = (DateTime)await connection.ExecuteScalarAsync(
            "SELECT toDateTime64('2024-06-15 14:30:45.1234567', 7, 'UTC')");
        Console.WriteLine($"     Precision 7 (ticks): {ticks:yyyy-MM-dd HH:mm:ss.fffffff}");

        Console.WriteLine("   Note: .NET DateTime supports up to 7 decimal places (100ns ticks).");
        Console.WriteLine("   ClickHouse DateTime64 can go up to 9 (nanoseconds), but excess precision is truncated.");
    }

    /// <summary>
    /// Writing DateTime values via command parameters.
    /// The DateTime.Kind property determines how the value is interpreted:
    /// - Utc: The instant is preserved (converted to target timezone if needed)
    /// - Local: Converted to UTC first, then to target timezone
    /// - Unspecified: Treated as wall-clock time in the target timezone
    /// </summary>
    private static async Task Example6_WriteViaParameters(ClickHouseConnection connection)
    {
        var tableName = "example_datetime_params";

        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt32,
                dt_utc DateTime('UTC'),
                dt_amsterdam DateTime('Europe/Amsterdam'),
                dt_no_tz DateTime
            )
            ENGINE = Memory
        ");

        // Insert with different DateTime.Kind values
        using (var command = connection.CreateCommand())
        {
            // UTC DateTime - instant is preserved
            var utcTime = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc);

            // Unspecified DateTime - treated as wall-clock time
            var unspecifiedTime = new DateTime(2024, 6, 15, 14, 0, 0, DateTimeKind.Unspecified);

            command.CommandText = $@"
                INSERT INTO {tableName} (id, dt_utc, dt_amsterdam, dt_no_tz)
                VALUES (1, {{utc:DateTime}}, {{amsterdam:DateTime('Europe/Amsterdam')}}, {{notz:DateTime}})
            ";

            command.AddParameter("utc", utcTime);
            command.AddParameter("amsterdam", unspecifiedTime);  // 14:00 Amsterdam time
            command.AddParameter("notz", unspecifiedTime);

            await command.ExecuteNonQueryAsync();
        }

        // Read back the values
        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            $"SELECT dt_utc, dt_amsterdam, dt_no_tz FROM {tableName}");

        if (reader.Read())
        {
            var dtUtc = reader.GetDateTime(0);
            var dtAmsterdam = reader.GetDateTime(1);
            var dtNoTz = reader.GetDateTime(2);

            Console.WriteLine("   Inserted values read back:");
            Console.WriteLine($"     UTC column: {dtUtc:HH:mm:ss} (Kind={dtUtc.Kind})");
            Console.WriteLine($"     Amsterdam column: {dtAmsterdam:HH:mm:ss} (Kind={dtAmsterdam.Kind})");
            Console.WriteLine($"     No-TZ column: {dtNoTz:HH:mm:ss} (Kind={dtNoTz.Kind})");

            var dtoAmsterdam = reader.GetDateTimeOffset(1);
            var dtoNoTz = reader.GetDateTimeOffset(2);
            Console.WriteLine($"     Amsterdam as DateTimeOffset: {dtoAmsterdam}");
            Console.WriteLine($"     No-TZ column as DateTimeOffset: {dtoNoTz}");
        }

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// Writing DateTime values via bulk copy.
    /// Bulk copy knows the target column's timezone, so it can correctly
    /// interpret DateTime.Kind=Unspecified as wall-clock time in that timezone.
    /// </summary>
    private static async Task Example7_WriteViaBulkCopy(ClickHouseConnection connection)
    {
        var tableName = "example_datetime_bulk";

        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt32,
                dt_utc DateTime('UTC'),
                dt_amsterdam DateTime('Europe/Amsterdam')
            )
            ENGINE = Memory
        ");

        using (var bulkCopy = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = tableName,
        })
        {
            // Unspecified DateTime values are treated as wall-clock time in the column's timezone
            var data = new List<object[]>
            {
                // Row 1: Same wall-clock time (12:00) in both columns
                new object[] { 1u,
                    new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Unspecified), // 12:00 UTC
                    new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Unspecified)  // 12:00 Amsterdam
                },
                // Row 2: UTC DateTime - instant is preserved
                new object[] { 2u,
                    new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc), // 12:00 UTC
                    new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc)  // 12:00 UTC = 14:00 Amsterdam
                },
            };

            await bulkCopy.WriteToServerAsync(data);
            Console.WriteLine($"   Inserted {bulkCopy.RowsWritten} rows via bulk copy");
        }

        // Read back
        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            $"SELECT id, dt_utc, dt_amsterdam FROM {tableName} ORDER BY id");

        Console.WriteLine("   Results:");
        while (reader.Read())
        {
            var id = reader.GetFieldValue<uint>(0);
            var utc = reader.GetDateTime(1);
            var amsterdam = reader.GetDateTime(2);
            var amsterdamOffset = reader.GetDateTimeOffset(2);

            Console.WriteLine($"     Row {id}:");
            Console.WriteLine($"       UTC: {utc:HH:mm:ss}");
            Console.WriteLine($"       Amsterdam: {amsterdam:HH:mm:ss} ({amsterdamOffset})");
        }

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// DateTimeOffset is fully supported for both reading and writing.
    /// It always preserves the correct instant.
    /// </summary>
    private static async Task Example8_DateTimeOffset(ClickHouseConnection connection)
    {
        var tableName = "example_datetimeoffset";

        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt32,
                dt_utc DateTime('UTC')
            )
            ENGINE = Memory
        ");

        // Insert DateTimeOffset values
        using (var command = connection.CreateCommand())
        {
            // 15:00 at +03:00 offset = 12:00 UTC
            var dto = new DateTimeOffset(2024, 6, 15, 15, 0, 0, TimeSpan.FromHours(3));

            command.CommandText = $"INSERT INTO {tableName} (id, dt_utc) VALUES (1, {{dt:DateTime}})";
            command.AddParameter("dt", dto);
            await command.ExecuteNonQueryAsync();
        }

        // Read back
        var result = (DateTime)await connection.ExecuteScalarAsync($"SELECT dt_utc FROM {tableName}");
        Console.WriteLine($"   Input: 15:00 at +03:00 offset");
        Console.WriteLine($"   Stored in UTC: {result:HH:mm:ss} (correctly converted to 12:00 UTC)");

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
    }

    /// <summary>
    /// The server timezone is available from ClickHouseCommand.ServerTimezone after any query.
    /// This is extracted from the X-ClickHouse-Timezone response header.
    /// </summary>
    private static async Task Example9_ServerTimezone(ClickHouseConnection connection)
    {
        // The server timezone is available on any command after execution
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";
        await command.ExecuteNonQueryAsync();

        Console.WriteLine($"   Server timezone: {command.ServerTimezone}");
        Console.WriteLine("   Note: ServerTimezone is available on ClickHouseCommand after any query execution.");
        Console.WriteLine("   It's extracted from the X-ClickHouse-Timezone response header for free.");
    }

    /// <summary>
    /// Important gotchas and best practices for DateTime handling.
    /// </summary>
    private static async Task Example10_Gotchas(ClickHouseConnection connection)
    {
        Console.WriteLine("   GOTCHA #1: Parameter type hints and timezones");
        Console.WriteLine("   " + "-".PadRight(50, '-'));

        var tableName = "example_gotcha";

        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                dt_amsterdam DateTime('Europe/Amsterdam')
            )
            ENGINE = Memory
        ");

        // This is the common mistake:
        // When inserting testTime via a parameter WITHOUT timezone in the type hint:
        // command.CommandText = "INSERT INTO example_gotcha (dt_amsterdam) VALUES ({dt:DateTime})"
        // The string value is interpreted in UTC, NOT the column's timezone!
        // To have the value interpreted in the column's timezone, specify it:
        // command.CommandText = "INSERT INTO example_gotcha (dt_amsterdam) VALUES ({dt:DateTime('Europe/Amsterdam')})"

        var testTime = new DateTime(2024, 6, 15, 14, 0, 0, DateTimeKind.Unspecified);

        // Without timezone hint (interpreted as UTC)
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = $"INSERT INTO {tableName} VALUES ({{dt:DateTime}})";
            cmd.AddParameter("dt", testTime);
            await cmd.ExecuteNonQueryAsync();
        }

        // With timezone hint (interpreted as Amsterdam time)
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = $"INSERT INTO {tableName} VALUES ({{dt:DateTime('Europe/Amsterdam')}})";
            cmd.AddParameter("dt", testTime);
            await cmd.ExecuteNonQueryAsync();
        }

        var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            $"SELECT dt_amsterdam FROM {tableName}");

        var results = new List<string>();
        while (reader.Read())
        {
            var dto = reader.GetDateTimeOffset(0);
            results.Add(dto.ToString());
        }

        Console.WriteLine($"\n   Same input (14:00 Unspecified), different results:");
        Console.WriteLine($"     Without TZ hint: {results[0]} (14:00 treated as UTC -> 16:00 Amsterdam)");
        Console.WriteLine($"     With TZ hint:    {results[1]} (14:00 treated as Amsterdam -> 14:00 Amsterdam)");

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");

        // Best practices:
        // 1. Use DateTime('UTC') columns when you need to store instants
        // 2. Use DateTime('Your/Timezone') when you need wall-clock times
        // 3. Always specify timezone in parameter type hints for non-UTC columns
        // 4. Use bulk copy for large inserts - it handles timezones automatically
        // 5. Read as DateTimeOffset when you need the timezone offset
    }
}
