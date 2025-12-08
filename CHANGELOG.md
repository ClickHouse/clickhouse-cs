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