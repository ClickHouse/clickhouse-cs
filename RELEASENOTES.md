v?
---

**Breaking Changes:**
* **Removed feature discovery query from `OpenAsync`.** The connection's `OpenAsync()` method no longer executes `SELECT version()` to discover server capabilities. This makes connection opening instantaneous (no network round-trip) but removes the `SupportedFeatures` property from `ClickHouseConnection`. The `ServerVersion` property now throws `InvalidOperationException`.

  **Migration guidance:** If you need to check the server version:
  ```csharp
  using var reader = await connection.ExecuteReaderAsync("SELECT version()");
  reader.Read();
  var version = reader.GetString(0);
  ```

* **DateTime reading behavior changed for columns without explicit timezone.** Previously, `DateTime` columns without a timezone (e.g., `DateTime` vs `DateTime('Europe/Amsterdam')`) would use the server timezone (with `UseServerTimezone=true`) or client timezone to interpret the stored value. Now, these columns return `DateTime` with `Kind=Unspecified`, preserving the wall-clock time exactly as stored without making assumptions about timezone.

  | Column Type | Old Behavior | New Behavior |
  |-------------|--------------|--------------|
  | `DateTime` (no timezone) | Returned with server/client timezone applied | `DateTime` with `Kind=Unspecified` |
  | `DateTime('UTC')` | `DateTime` with `Kind=Utc` | `DateTime` with `Kind=Utc` (unchanged) |
  | `DateTime('Europe/Amsterdam')` | `DateTime` with `Kind=Unspecified` | `DateTime` with `Kind=Unspecified` (unchanged). Reading as DateTimeOffset has correct offset applied. |

  **Migration guidance:** If you need timezone-aware behavior, either:
  1. Use explicit timezones in your column definitions: `DateTime('UTC')` or `DateTime('Europe/Amsterdam')`
  2. Apply the timezone yourself after reading.

* **DateTime writing now respects `DateTime.Kind` property.** Previously, all `DateTime` values were treated as wall-clock time in the target column's timezone regardless of their `Kind` property. The new behavior:

  | DateTime.Kind | Old Behavior | New Behavior |
  |---------------|--------------|--------------|
  | `Utc` | Treated as wall-clock time in column timezone | Preserved as-is (instant is maintained) |
  | `Local` | Treated as wall-clock time in column timezone | Instant is maintained (inserted as UTC timestamp) |
  | `Unspecified` | Treated as wall-clock time in column timezone | Treated as wall-clock time in column timezone (unchanged) |

  Migration guidance: If you were relying on the old behavior where UTC `DateTime` values were reinterpreted in the column timezone, you should change these to `DateTimeKind.Unspecified`:
  ```csharp
  // Old code (worked by accident):
  var utcTime = DateTime.UtcNow;  // Would be reinterpreted in column timezone

  // New code (explicit intent):
  var wallClockTime = DateTime.SpecifyKind(myTime, DateTimeKind.Unspecified);
  ```

    **Important:** When using parameters, you must specify the timezone in the parameter type hint to have string values interpreted in the column timezone:
  ```csharp
  // Correct: timezone in type hint ensures proper interpretation
  command.AddParameter("dt", myDateTime, "DateTime('Europe/Amsterdam')");
  command.CommandText = "INSERT INTO table (dt_column) VALUES ({dt:DateTime('Europe/Amsterdam')})";

  // Gotcha: without timezone hint, UTC is used for interpretation
  command.AddParameter("dt", myDateTime);
  command.CommandText = "INSERT INTO table (dt_column) VALUES ({dt:DateTime})";
  // ^ String value interpreted in UTC, not column timezone!
  ```

  This differs from bulk copy operations where the column timezone is known and used automatically.

* **Removed `UseServerTimezone` setting.** This setting has been removed from the connection string, `ClickHouseClientSettings`, and `ClickHouseConnectionStringBuilder`. It no longer has any effect since columns without timezones now return `Unspecified` DateTime values without any timezone changes applied to what is returned from the server.
* **Moved `ServerTimezone` property from `ClickHouseConnection` to `ClickHouseCommand`.** The server timezone is now available on `ClickHouseCommand.ServerTimezone` after any query execution (the timezone is now extracted from the `X-ClickHouse-Timezone` response header instead of requiring a separate query).
* **Helper and extension methods made internal:** DateTimeConversions, DataReaderExtensions, DictionaryExtensions, EnumerableExtensions, MathUtils, StringExtensions.

* **JSON writing behavior changed for string and JsonNode inputs.** When writing `string` or `JsonNode` values to JSON columns, the driver now sends the JSON as a plain string for server-side parsing. This requires the server setting `input_format_binary_read_json_as_string=1`. Objects are serialized using binary encoding with type hints.

**New Features/Improvements:**
 * Added POCO serialization support for JSON columns. When writing POCOs to JSON columns with typed hints (e.g., `JSON(id Int64, name String)`), the driver now serializes properties using the hinted types for full type fidelity. Properties without a corresponding hinted path will have their ClickHouse types inferred automatically. Two attributes are available: `[ClickHouseJsonPath("path")]` for custom JSON paths and `[ClickHouseJsonIgnore]` to exclude properties. Property name matching to hint paths is case-sensitive (matching ClickHouse behavior which allows paths like `userName` and `UserName` to coexist).
 * Added support for QBit data type. QBit is a transposed vector column, designed to allow the user to choose a desired quantization level at runtime, speeding up approximate similarity searches. See the GitHub repo for usage examples.
 * Added support for setting roles at the connection and command levels.
 * Added support for custom headers at the connection level.
 * Added support for JWT/Bearer token authentication at both connection and command levels.
 * Added `InsertRawStreamAsync` method to `ClickHouseConnection` for inserting raw data streams (CSV, JSON, Parquet, etc.) directly from files or memory. Check out the examples on GitHub for usage examples of all the above.
 * When the query id has not been set, it will now be automatically generated by the client.
 * Added support for writing `byte[]` values to String type columns via BulkCopy.
 * Added `PingAsync` method to `ClickHouseConnection` for checking server availability via the `/ping` endpoint.
 * Added support for detecting mid-stream exceptions via the `X-ClickHouse-Exception-Tag` header (ClickHouse 25.11+). When `http_write_exception_in_output_format` is set to 0 on the server, exceptions that occur while streaming results are now properly detected and thrown as `ClickHouseServerException` (which includes the exception message) instead of `EndOfStreamException`.
 * Added support for writing to `Dynamic` type columns via BulkCopy. Values are automatically type-inferred from their .NET types and serialized with the appropriate binary type header. Supports all common types including integers, floating point, strings, booleans, DateTime, Guid, decimal, arrays, lists, and dictionaries.

**Bug Fixes:**
 * Fixed a crash when reading a Map with duplicate keys. The current behavior is to return only the last value for a given key.
