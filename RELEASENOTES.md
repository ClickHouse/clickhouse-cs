v0.8.0

Breaking Changes:
 * Trying to set ClickHouseConnection.ConnectionString will now throw a NotSupportedException. Create a new connection with the desired settings instead.
 * When a default database is not provided, the client no longer uses "default" (now uses empty string). This allows default user database settings to function as expected.
 * ClickHouseDataSource.Logger (ILogger) property changed to LoggerFactory (ILoggerFactory).
 * Removed support for loading configuration from environment variables (CLICKHOUSE_DB, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD). Use connection strings or ClickHouseClientSettings instead.

New Features:
 * The NuGet package is now signed.
 * Enabled strong naming for the library.
 * Added a new way to configure ClickHouseConnection: the ClickHouseClientSettings class. You can initialize it from a connection string by calling ClickHouseClientSettings.FromConnectionString(), or simply by setting its properties.
 * Added settings validation to prevent incorrect configurations.
 * Added logging in the library, enable it by passing a LoggerFactory through the settings. Logging level configuration is configured through the factory.
 * Added new AddClickHouseDataSource extension methods that accept ClickHouseClientSettings for strongly-typed configuration in DI scenarios.
 * Added new AddClickHouseDataSource extension method that accepts IHttpClientFactory for better DI integration.
 * AddClickHouseDataSource now automatically injects ILoggerFactory from the service provider when not explicitly provided.
 * Optimized response header parsing.
 * Added list type conversion, so List<T> can now be passed to the library (converts to Array() in ClickHouse). Thanks to @jorgeparavicini.
 * Improvements to ActivitySource for tracing: stopped adding tags when it was not necessary, and made it configurable through ClickHouseDiagnosticsOptions.
 * Optimized EnumType value lookups.
 * Avoid unnecessarily parsing the X-ClickHouse-Summary headers twice. Thanks to @verdie-g.

Bug fixes:
 * Fixed a crash when processing a tuple with an enum in it.
 * Fixed a potential sync-over-async issue in the connection. Thanks to @verdie-g.
