v1.2.0
---

**New Features:**
* `IParameterTypeResolver`: configurable default type mapping for `@`-style parameterized queries. Set `ParameterTypeResolver` on `ClickHouseClientSettings` to override how .NET types are mapped to ClickHouse types (e.g., `DateTime` → `DateTime64(3)`, `decimal` → `Decimal64(4)`). Includes one implementation, `DictionaryParameterTypeResolver` for simple type→type mappings, and supports custom implementations for value-aware or name-based resolution. Can also be set per-query via `QueryOptions.ParameterTypeResolver`.

**Internal Improvements:**
* Centralized parameter type resolution into `ParameterTypeResolution`, replacing previously scattered logic in `ClickHouseDbParameter.QueryForm` and `HttpParameterFormatter`. Each parameter's type is now resolved exactly once per request, ensuring consistency between SQL placeholder generation and HTTP value formatting.

**Bug Fixes:**
* `JsonReadMode` and `JsonWriteMode` will now correcly set the corresponding settings when set to `Binary` mode.

v1.1.1
---

**Improvements:**
* Type inference now inspects `IPAddress.AddressFamily` to correctly distinguish between IPv4 and IPv6 types. Previously, all `IPAddress` values were inferred as IPv4. This also works for collections, tuples, and maps containing `IPAddress` values.

v1.1.0
---

**New Features:**
* **POCO binary inserts**: new `InsertBinaryAsync<T>` overload on `ClickHouseClient` accepts `IEnumerable<T>` directly, mapping public properties to columns automatically. Register types upfront with `RegisterBinaryInsertType<T>()`. Customize column names and ClickHouse types with `[ClickHouseColumn(Name = "...", Type = "...")]`, or exclude properties with `[ClickHouseNotMapped]`. When all properties specify explicit types via the attribute, the schema probe is skipped entirely.
* `InsertOptions.ColumnTypes`: provide a dictionary of column name → ClickHouse type string to skip the schema probe query (`SELECT ... WHERE 1=0`) entirely. Ideal when the table schema is known at compile time.
* `InsertOptions.UseSchemaCache`: when `true`, the full table schema is cached per (database, table) for the lifetime of the `ClickHouseClient` instance. Subsequent inserts to the same table reuse the cached schema regardless of which columns are selected, eliminating redundant round-trips.

**Breaking Changes:**
* `InsertBinaryAsync` now throws `InvalidOperationException` when sessions are enabled and `MaxDegreeOfParallelism > 1`. ClickHouse only allows one concurrent query per session, so parallel batch inserts would cause `SESSION_IS_LOCKED` errors and partial writes. This also affects the deprecated `ClickHouseBulkCopy`, which defaults to `MaxDegreeOfParallelism = 4`. To fix, set `MaxDegreeOfParallelism` to 1, or disable sessions for the insert via `InsertOptions.UseSession = false`.

**Bug Fixes:**
* Fixed `IndexOutOfRangeException` when reading NULL values from `Variant` columns. The Variant `None` discriminator (used for NULLs) was not handled, causing an out-of-bounds array access instead of returning `DBNull.Value`.
* Fixed writing NULL values to `Variant` columns. Writing null/DBNull now correctly emits the `None` discriminator (`0xFF`) for binary writes, and null marker `\N` when using HTTP parameters.  Note: null Variant HTTP parameter parsing is broken in server versions prior to 26.3.

v1.0.2
---

**Bug Fixes:**
* Fixed `QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING` errors when using `InsertBinaryAsync` with a `QueryId`. The schema probe and all batch inserts were sharing the same query ID. The schema probe now uses the base query ID, and each batch insert receives a unique suffixed ID (`{queryId}-1`, `{queryId}-2`, etc.).

v1.0.1
---

 * Marked ClickHouseConnection.ServerVersion property as Obsolete.

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


v0.9.0
---

**Breaking Changes:**
 * FixedString is now returned as byte[] rather than String. FixedStrings are not necessarily valid UTF-8 strings, and the string transformation caused loss of information in some cases. Use Encoding.UTF8.GetString() on the resulting byte[] array to emulate the old behavior. String can still be used as a parameter or when inserting using BulkCopy into a FixedString column. When part of a json object, FixedString is still returned as a string.
 * Removed obsolete MySQL compatibility mapping TIME -> Int64.
 * Json serialization of bool arrays now uses the Boolean type instead of UInt8 (it is now consistent with how bool values outside arrays were handled).
 * GEOMETRY is no longer an alias for String.

**New Features/Improvements:**
 * Sessions can now be used with custom HttpClient or HttpClientFactory. Previously this combination was not allowed. Note that when sessions are enabled, ClickHouseConnection will allow only one request at a time, and responses are fully buffered before returning to ensure proper request serialization.
 * Added support for BFloat16. It is converted to and from a 32-bit float.
 * Added support for Time and Time64, which are converted to and from TimeSpan. The types are available since ClickHouse 25.6 and using them requires the enable_time_time64_type flag to be set.
 * The Dynamic type now offers full support for all underlying types.
 * Added support for LineString and MultiLineString geo types.
 * Added support for the Geometry type, which can hold any geo subtype (Point, Ring, LineString, Polygon, MultiLineString, MultiPolygon). Available since ClickHouse 25.11. Requires allow_suspicious_variant_types to be set to 1.
 * Json support has been improved in many ways:
   * Now supports parsing Json that includes Maps; they are read into JsonObjects.
   * Added support for decoding BigInteger types, UUID, IPv4, IPv6, and ClickHouseDecimal types (they are handled as strings).
   * Expanded binary parsing to cover all types.
   * Improved handling of numeric types when writing Json using BulkCopy: now properly detects and preserves Int32/In64 in addition to double (previously all numeric types were handled as double).
   * Parsing null values in arrays is now handled properly.
 * ClickHouseConnection.ConnectionString can now be set after creating the connection, to support cases where passing the connection string to the constructor is not possible.
 * ClickHouseConnection.CreateCommand() now has an optional argument for the command text.
 * Fixed a NullReferenceException when adding a parameter with null value and no provided type. The driver now simply sends '\N' (null value special character) when encountering this scenario. 

**Bug Fixes:**
 * Fixed a bug where serializing to json with an array of bools with both true and false elements would fail.


v0.8.1
---

**Improvements:**
 * Fixed NuGet readme file.

v0.8.0
---

**Breaking Changes:**
 * Trying to set ClickHouseConnection.ConnectionString will now throw a NotSupportedException. Create a new connection with the desired settings instead.
 * When a default database is not provided, the client no longer uses "default" (now uses empty string). This allows default user database settings to function as expected.
 * ClickHouseDataSource.Logger (ILogger) property changed to LoggerFactory (ILoggerFactory).
 * Removed support for loading configuration from environment variables (CLICKHOUSE_DB, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD). Use connection strings or ClickHouseClientSettings instead.
 * The default PooledConnectionIdleTimeout has been changed to 5 seconds, to prevent issues with half-open connections when using ClickHouse Cloud (where the default server-side idle timetout is 10s).

**New Features:**
 * Added .NET 10 as a target.
 * The NuGet package is now signed.
 * Enabled strong naming for the library.
 * Added a new way to configure ClickHouseConnection: the ClickHouseClientSettings class. You can initialize it from a connection string by calling ClickHouseClientSettings.FromConnectionString(), or simply by setting its properties.
 * Added settings validation to prevent incorrect configurations.
 * Added logging in the library, enable it by passing a LoggerFactory through the settings. Logging level configuration is configured through the factory. For more info, see the documentation: https://clickhouse.com/docs/integrations/csharp#logging-and-diagnostics
 * Added EnableDebugMode setting to ClickHouseClientSettings for low-level .NET network tracing (.NET 5+). When enabled, traces System.Net events (HTTP, Sockets, DNS, TLS) to help diagnose network issues. Requires ILoggerFactory with Trace-level logging enabled. WARNING: Significant performance impact - not recommended for production use.
 * AddClickHouseDataSource now automatically injects ILoggerFactory from the service provider when not explicitly provided.
 * Improvements to ActivitySource for tracing: stopped adding tags when it was not necessary, and made it configurable through ClickHouseDiagnosticsOptions.
 * Added new AddClickHouseDataSource extension methods that accept ClickHouseClientSettings for strongly-typed configuration in DI scenarios.
 * Added new AddClickHouseDataSource extension method that accepts IHttpClientFactory for better DI integration.
 * Optimized response header parsing.
 * Added list type conversion, so List<T> can now be passed to the library (converts to Array() in ClickHouse). Thanks to @jorgeparavicini.
 * Optimized EnumType value lookups.
 * Avoid unnecessarily parsing the X-ClickHouse-Summary headers twice. Thanks to @verdie-g.
 * Added the ability to pass a query id to ClickHouseConnection.PostStreamAsync(). Thanks to @dorki.
 * The user agent string now also contains information on the host operating system, .NET version, and processor architecture.

**Bug fixes:**
 * Fixed a crash when processing a tuple with an enum in it.
 * Fixed a potential sync-over-async issue in the connection. Thanks to @verdie-g.
 * Fixed a bug with parsing table definitions with parametrized json fields. Thanks to @dorki.
