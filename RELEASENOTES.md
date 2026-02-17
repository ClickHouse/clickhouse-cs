v1.0.0
---

**Documentation and Usage Examples:**
Coinciding with the 1.0.0 release of the driver, we have greatly expanded the documentation and usage examples.
* Documentation: https://clickhouse.com/docs/integrations/csharp
* Usage examples: https://github.com/ClickHouse/clickhouse-cs/tree/main/examples

---

**New: ClickHouseClient - Simplified Primary API**

`ClickHouseClient` is the new recommended way to interact with ClickHouse. Thread-safe, singleton-friendly, and simpler than ADO.NET classes.

```csharp
using var client = new ClickHouseClient("Host=localhost");
```

| Method | Description                                                  |
|--------|--------------------------------------------------------------|
| `ExecuteNonQueryAsync` | Execute DDL/DML (CREATE, INSERT, ALTER, DROP)                |
| `ExecuteScalarAsync` | Return first column of first row                             |
| `ExecuteReaderAsync` | Stream results via `ClickHouseDataReader`                    |
| `InsertBinaryAsync` | High-performance bulk insert (replaces `ClickHouseBulkCopy`) |
| `ExecuteRawResultAsync` | Get raw result stream bypassing the parser                   |
| `InsertRawStreamAsync` | Insert from stream (CSV, JSON, Parquet, etc.)                |
| `PingAsync` | Check server connectivity                                    |
| `CreateConnection()` | Get `ClickHouseConnection` for ORM compatibility             |

**Per-query configuration** via `QueryOptions`.

**Parameters** via `ClickHouseParameterCollection`:
```csharp
var parameters = new ClickHouseParameterCollection();
parameters.Add("id", 42UL);
await client.ExecuteReaderAsync("SELECT * FROM t WHERE id = {id:UInt64}", parameters);
```

**Deprecation:** `ClickHouseBulkCopy` is deprecated. Use `client.InsertBinaryAsync(table, columns, rows)` instead.

---

**Breaking Changes:**
* **Dropped support for .NET Framework and .NET Standard.** The library now targets only `net6.0`, `net8.0`, `net9.0`, and `net10.0`. Removed support for `net462`, `net48`, and `netstandard2.1`. If you are using .NET Framework, you will need to stay on the previous version or migrate to .NET 6.0+.

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
  command.AddParameter("dt", myDateTime);
  
  // Correct: timezone in type hint ensures proper interpretation
  command.CommandText = "INSERT INTO table (dt_column) VALUES ({dt:DateTime('Europe/Amsterdam')})";

  // Gotcha: without timezone hint, UTC is used for interpretation
  command.CommandText = "INSERT INTO table (dt_column) VALUES ({dt:DateTime})";
  // ^ String value interpreted in UTC, not column timezone!
  ```

  This differs from bulk copy operations where the column timezone is known and used automatically.

* **Removed `UseServerTimezone` setting.** This setting has been removed from the connection string, `ClickHouseClientSettings`, and `ClickHouseConnectionStringBuilder`. It no longer has any effect since columns without timezones now return `Unspecified` DateTime values without any timezone changes applied to what is returned from the server.
* **Moved `ServerTimezone` property from `ClickHouseConnection` to `ClickHouseCommand`.** The server timezone is now available on `ClickHouseCommand.ServerTimezone` after any query execution (the timezone is now extracted from the `X-ClickHouse-Timezone` response header instead of requiring a separate query).
* **Helper and extension methods made internal:** DateTimeConversions, DataReaderExtensions, DictionaryExtensions, EnumerableExtensions, MathUtils, StringExtensions.

* **JSON writing default behavior changed.** The default `JsonWriteMode` has changed from `Binary` to `String`. This affects how JSON data is written to ClickHouse:

  | Input Type | Old Default (Binary) | New Default (String) |
  |------------|---------------------|----------------------|
  | `JsonObject` / `JsonNode` | Binary encoding | Serialized via `JsonSerializer.Serialize()` |
  | `string` | Binary encoding (parsed client-side) | Passed through directly |
  | POCO (registered) | Binary encoding with type hints | Serialized via `JsonSerializer.Serialize()` |
  | POCO (unregistered) | Exception | Serialized via `JsonSerializer.Serialize()` |

  **Impact if you don't modify your code:**
    - JSON writing will still work, but uses string serialization instead of binary encoding; the JSON string will be parsed on the server instead of the client. This could lead to subtle changes in paths without type hints, e.g., values previously parsed as ints may be parsed as longs.
    - `ClickHouseJsonPath` and `ClickHouseJsonIgnore` attributes are ignored in String mode (they only work in Binary mode). Serialization happens via `System.Text.Json`, so you can use those attributes instead.
    - Server setting `input_format_binary_read_json_as_string=1` is automatically set when using String write mode

**New Features/Improvements:**

* **Automatic parameter type extraction from SQL.** Types specified in the SQL query using `{name:Type}` syntax are now automatically used for parameter formatting, eliminating the need to specify the type twice:
  ```csharp
  // Before: type specified twice
  command.CommandText = "SELECT {dt:DateTime('Europe/Amsterdam')}";
  command.AddParameter("dt", "DateTime('Europe/Amsterdam')", value);

  // After: type extracted from SQL automatically
  command.CommandText = "SELECT {dt:DateTime('Europe/Amsterdam')}";
  command.AddParameter("dt", value);
  ```
  The `AddParameter(name, type, value)` overload is now marked obsolete. Use `AddParameterWithTypeOverride()` if you need to explicitly override the SQL type hint.

* **POCO serialization support for JSON columns.** When writing POCOs to JSON columns with typed hints (e.g., `JSON(id Int64, name String)`), the driver serializes properties using the hinted types for full type fidelity. Properties without a corresponding hinted path will have their ClickHouse types inferred automatically. Two attributes are available: `[ClickHouseJsonPath("path")]` for custom JSON paths and `[ClickHouseJsonIgnore]` to exclude properties. Property name matching to hint paths is case-sensitive (matching ClickHouse behavior which allows paths like `userName` and `UserName` to coexist). Register types via `client.RegisterJsonSerializationType<T>()`.

* **`JsonReadMode` and `JsonWriteMode` connection string settings** for configurable JSON handling:
    - `JsonReadMode.Binary` (default): Returns `System.Text.Json.Nodes.JsonObject`
    - `JsonReadMode.String`: Returns raw JSON string. Sets server setting `output_format_binary_write_json_as_string=1`.
    - `JsonWriteMode.String` (default): Accepts `JsonObject`, `JsonNode`, strings, and any object (serialized via `System.Text.Json.JsonSerializer`). Sets server setting `input_format_binary_read_json_as_string=1`.
    - `JsonWriteMode.Binary`: Only accepts registered POCO types with full type hint support and custom path attributes. Writing `string` or `JsonNode` values with `JsonWriteMode.Binary` throws an exception.

* **QBit data type support.** QBit is a transposed vector column, designed to allow the user to choose a desired quantization level at runtime, speeding up approximate similarity searches. See the GitHub repo for usage examples.

* **Dynamic type binary writing support** via `InsertBinaryAsync`. Values are automatically type-inferred from their .NET types and serialized with the appropriate binary type header. Supports all common types including integers, floating point, strings, booleans, DateTime, Guid, decimal, arrays, lists, and dictionaries.

* **Binary data in String/FixedString columns.** Write `byte[]`, `ReadOnlyMemory<byte>`, or `Stream` values to String and FixedString columns via `InsertBinaryAsync`. Read binary data back using the `ReadStringsAsByteArrays` connection string setting, which returns String columns as `byte[]` instead of `string`. Useful for storing binary data that may not be valid UTF-8.

* **First-class support for roles**, with query-level override.

* **Custom HTTP headers** at the connection level for proxy/infrastructure integration.

* **Support for JWT authentication**, with query-level override.

* **Mid-stream exception detection** via `X-ClickHouse-Exception-Tag` header (ClickHouse 25.11+). When `http_write_exception_in_output_format` is set to 0 on the server, exceptions that occur while streaming results are now properly detected and thrown as `ClickHouseServerException` (which includes the exception message) instead of `EndOfStreamException`.

* **Query ID auto-generation.** When the query ID has not been set, it will now be automatically generated by the client.

* **`AddParameter()` convenience method** for `ClickHouseParameterCollection`, simplifying parameter creation.

**Bug Fixes:**
* Fixed a crash when reading a Map with duplicate keys. The current behavior is to return only the last value for a given key.
