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