v0.8.0

Breaking Changes:
 * Trying to set ClickHouseConnection.ConnectionString will now throw a NotSupportedException. Create a new connection with the desired settings instead.
 * When a default database is not provided, the client no longer uses "default". This allows default user database settings to function as expected.
 * ClickHouseDataSource.Logger property changed from ILogger to ILoggerFactory for better DI integration.

New Features:
 * Added a new way to configure ClickHouseConnection, the ClickHouseClientSettings class. You can initialize it from a connection string by calling ClickHouseClientSettings.FromConnectionString(), or simply by setting its properties.
 * Added new AddClickHouseDataSource extension methods that accept ClickHouseClientSettings for strongly-typed configuration in DI scenarios.
 * AddClickHouseDataSource now automatically injects ILoggerFactory from the service provider when not explicitly provided.
 * Optimized response header parsing.
 * Added list type conversion, so List<T> can now be passed to the library (converts to Array() in ClickHouse). Thanks to @jorgeparavicini.

Bug fixes:
 * Fixed a crash when processing a tuple with an enum in it.
