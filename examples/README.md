# ClickHouse C# Driver Examples

This directory contains examples demonstrating various features and usage patterns of the ClickHouse C# driver.

## Overview

We aim to cover various scenarios of driver usage with these examples. You should be able to run any of these examples by following the instructions in the [How to run](#how-to-run) section below.

If something is missing, or you found a mistake in one of these examples, please open an issue or a pull request. [AGENTS.md](AGENTS.md) has the checklist for adding one.

Examples are grouped by transport. Everything under [Http/](Http) uses `ClickHouseClient` or `ClickHouseConnection` over HTTP.

## Examples

### Core Usage & Configuration

- [Core_001_BasicUsage.cs](Http/Core/Core_001_BasicUsage.cs) - Creating a client, tables, and performing basic insert/select operations (using ClickHouseClientSettings)
- [Core_002_ConnectionStringConfiguration.cs](Http/Core/Core_002_ConnectionStringConfiguration.cs) - Various connection string formats and configuration options
- [Core_003_DependencyInjection.cs](Http/Core/Core_003_DependencyInjection.cs) - Using ClickHouse with dependency injection and config binding
- [Core_004_HttpClientConfiguration.cs](Http/Core/Core_004_HttpClientConfiguration.cs) - Providing custom HttpClient or IHttpClientFactory for SSL/TLS, proxy, timeouts, and more control over connection settings

### ASP.NET Integration

- [AspNet_001_HealthChecks.cs](Http/AspNet/AspNet_001_HealthChecks.cs) - Implementing ASP.NET health checks for ClickHouse

### Authentication

- [Auth_001_JwtAuthentication.cs](Http/Core/Auth_001_JwtAuthentication.cs) - Using JWT/Bearer token authentication with ClickHouse.

### Creating Tables

- [Tables_001_CreateTableSingleNode.cs](Http/Tables/Tables_001_CreateTableSingleNode.cs) - Creating tables with different engines and data types on a single-node deployment
- [Tables_002_CreateTableCluster.cs](Http/Tables/Tables_002_CreateTableCluster.cs) - Creating ReplicatedMergeTree tables on an on-premises ClickHouse cluster with ON CLUSTER and macros
- [Tables_003_CreateTableCloud.cs](Http/Tables/Tables_003_CreateTableCloud.cs) - Creating tables on ClickHouse Cloud (automatic replication, no ENGINE needed)

### Inserting Data

- [Insert_001_SimpleDataInsert.cs](Http/Insert/Insert_001_SimpleDataInsert.cs) - Basic data insertion using parameterized queries
- [Insert_002_BulkInsert.cs](Http/Insert/Insert_002_BulkInsert.cs) - High-performance bulk data insertion using `InsertBinaryAsync()`
- [Insert_003_AsyncInsert.cs](Http/Insert/Insert_003_AsyncInsert.cs) - Server-side batching with async inserts for high-concurrency workloads
- [Insert_004_RawStreamInsert.cs](Http/Insert/Insert_004_RawStreamInsert.cs) - Inserting raw data streams from files or memory (CSV, JSON, Parquet, etc.)
- [Insert_005_InsertFromSelect.cs](Http/Insert/Insert_005_InsertFromSelect.cs) - Using INSERT FROM SELECT for ETL, data transformation, and loading from external sources (S3, URL, remote servers)
- [Insert_006_EphemeralColumns.cs](Http/Insert/Insert_006_EphemeralColumns.cs) - Using EPHEMERAL columns to transform input data before storage
- [Insert_007_UpsertsWithReplacingMergeTree.cs](Http/Insert/Insert_007_UpsertsWithReplacingMergeTree.cs) - Upsert patterns using ReplacingMergeTree with version and deleted columns
- [Insert_008_SchemaOptimization.cs](Http/Insert/Insert_008_SchemaOptimization.cs) - Skipping the schema probe query with `ColumnTypes` or `UseSchemaCache`
- [Insert_009_PocoInsert.cs](Http/Insert/Insert_009_PocoInsert.cs) - Inserting strongly-typed POCO objects with `InsertBinaryAsync<T>`, attribute mapping, and schema probe optimization

### Selecting Data

- [Select_001_BasicSelect.cs](Http/Select/Select_001_BasicSelect.cs) - Basic SELECT queries and reading the results
- [Select_002_SelectMetadata.cs](Http/Select/Select_002_SelectMetadata.cs) - Column metadata overview
- [Select_003_SelectWithParameterBinding.cs](Http/Select/Select_003_SelectWithParameterBinding.cs) - Parameterized queries for safe and dynamic SQL construction
- [Select_004_ExportToFile.cs](Http/Select/Select_004_ExportToFile.cs) - Exporting query results to files (JSONEachRow, Parquet, etc.)
- [Select_005_CompressedRawExport.cs](Http/Select/Select_005_CompressedRawExport.cs) - Per-query `AcceptEncoding` override to stream a compressed raw export (e.g. LZ4-compressed Parquet) straight to a file with `ExecuteRawResultAsync`
- [Select_006_PocoSelect.cs](Http/Select/Select_006_PocoSelect.cs) - Reading query results into strongly-typed POCO objects with `QueryAsync<T>` (streaming) and `reader.MapTo<T>` (per-row), including `[ClickHouseColumn(Name)]` aliases and `[ClickHouseNotMapped]` exclusion
- [Select_007_ResponseCompression.cs](Http/Select/Select_007_ResponseCompression.cs) - Transport compression of responses: the codec the driver negotiates by default, how to override it with `AcceptEncoding`, and why raw exports are exempt

### Data Types

- [DataTypes_001_SimpleTypes.cs](Http/DataTypes/DataTypes_001_SimpleTypes.cs) - Simple/scalar data types: integers (Int8-Int256), floats, decimals (ClickHouseDecimal), boolean
- [DataTypes_002_DateTimeHandling.cs](Http/DataTypes/DataTypes_002_DateTimeHandling.cs) - Comprehensive guide to DateTime, DateTime64, Date, Date32, timezones, DateTime.Kind behavior, and DateTimeOffset
- [DataTypes_003_ComplexTypes.cs](Http/DataTypes/DataTypes_003_ComplexTypes.cs) - Working with complex data types: Arrays, Maps, Tuples, IP addresses, and Nested structures
- [DataTypes_004_StringHandling.cs](Http/DataTypes/DataTypes_004_StringHandling.cs) - String and FixedString handling, binary data, ReadStringsAsByteArrays setting, and writing from Streams
- [DataTypes_005_JsonType.cs](Http/DataTypes/DataTypes_005_JsonType.cs) - Working with JSON type: reading as JsonObject or string, writing from various sources, and configuring JsonReadMode/JsonWriteMode
- [DataTypes_006_Geometry.cs](Http/DataTypes/DataTypes_006_Geometry.cs) - Geometry types: Point, Polygon, WKT parsing, H3 geospatial indexing, point-in-polygon checks, and great circle distance calculations
- [Vector_001_QBitSimilaritySearch.cs](Http/DataTypes/Vector_001_QBitSimilaritySearch.cs) - Vector similarity search using quantized binary embeddings

### ORM Integration

- [ORM_001_Dapper.cs](Http/ORM/ORM_001_Dapper.cs) - Using Dapper and Dapper.Contrib with ClickHouse: queries, inserts, type handlers, and known limitations
- [ORM_002_Linq2Db.cs](Http/ORM/ORM_002_Linq2Db.cs) - Using linq2db with ClickHouse: LINQ queries, inserts, BulkCopy, and entity mapping

### Advanced Features

- [Advanced_001_QueryIdUsage.cs](Http/Advanced/Advanced_001_QueryIdUsage.cs) - Using Query IDs to track and monitor query execution
- [Advanced_002_SessionIdUsage.cs](Http/Advanced/Advanced_002_SessionIdUsage.cs) - Using Session IDs for temporary tables and session state (with important limitations)
- [Advanced_003_LongRunningQueries.cs](Http/Advanced/Advanced_003_LongRunningQueries.cs) - Strategies for handling long-running queries (progress headers and fire-and-forget patterns)
- [Advanced_004_CustomSettings.cs](Http/Advanced/Advanced_004_CustomSettings.cs) - Using custom ClickHouse server settings for resource limits and query optimization
- [Advanced_005_QueryStatistics.cs](Http/Advanced/Advanced_005_QueryStatistics.cs) - Accessing and using query statistics for performance monitoring and optimization decisions
- [Advanced_006_Roles.cs](Http/Advanced/Advanced_006_Roles.cs) - Using ClickHouse roles to control permissions at connection and command levels
- [Advanced_007_CustomHeaders.cs](Http/Advanced/Advanced_007_CustomHeaders.cs) - Using custom HTTP headers for proxy authentication, distributed tracing, etc
- [Advanced_008_QueryCancellation.cs](Http/Advanced/Advanced_008_QueryCancellation.cs) - Using CancellationToken to cancel long-running queries
- [Advanced_009_ReadOnlyUsers.cs](Http/Advanced/Advanced_009_ReadOnlyUsers.cs) - Working with READONLY = 1 users and their limitations
- [Advanced_010_RetriesAndDeduplication.cs](Http/Advanced/Advanced_010_RetriesAndDeduplication.cs) - Retry patterns with Polly and ReplacingMergeTree for exactly-once insert semantics
- [Advanced_011_Compression.cs](Http/Advanced/Advanced_011_Compression.cs) - Understanding the UseCompression setting: how it works, when to disable it, and custom HttpClient requirements
- [Advanced_012_ParameterTypeResolver.cs](Http/Advanced/Advanced_012_ParameterTypeResolver.cs) - Customizing default type mappings for @-style parameters (DateTime→DateTime64, decimal precision, custom resolvers)
- [Advanced_013_ParameterFormatter.cs](Http/Advanced/Advanced_013_ParameterFormatter.cs) - Customizing how parameter values are serialized for HTTP transport (custom DateTime format, array-element formatting, escaping caveats for string-like types in composites)
- [Advanced_014_ReadValueConverter.cs](Http/Advanced/Advanced_014_ReadValueConverter.cs) - Transforming values returned by the data reader (DateTime.Kind, string normalization, per-query overrides)

### Troubleshooting

- [Troubleshooting_001_LoggingConfiguration.cs](Http/Troubleshooting/Troubleshooting_001_LoggingConfiguration.cs) - Setting up logging with Microsoft.Extensions.Logging to view diagnostic information
- [Troubleshooting_002_NetworkTracing.cs](Http/Troubleshooting/Troubleshooting_002_NetworkTracing.cs) - Enabling low-level .NET network tracing for debugging connection issues (HTTP, Sockets, DNS, TLS)
- [Troubleshooting_003_OpenTelemetryTracing.cs](Http/Troubleshooting/Troubleshooting_003_OpenTelemetryTracing.cs) - Collecting OpenTelemetry traces from the driver for distributed tracing and observability

### Testing

- [Testing_001_Testcontainers.cs](Http/Testing/Testing_001_Testcontainers.cs) - Using Testcontainers to spin up ephemeral ClickHouse instances for integration testing

## How to run

### Prerequisites

- .NET 9.0 SDK or later
- ClickHouse server (local or remote)
  - For local runs, you can use Docker:
    ```bash
    docker run -d --name clickhouse-server -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server
    ```

### Running examples

Navigate to the examples directory and run the example program:

```bash
cd examples

# Run all examples
dotnet run

# List available examples
dotnet run -- --list

# Run specific example(s) using a filter
dotnet run -- --filter basicusage

# Shorthand (positional argument)
dotnet run -- basicusage
```

The filter matches the example's class name, which `--list` prints. A class name is the topic without the file's category prefix: `Core_001_BasicUsage.cs` declares `class BasicUsage`. Matching ignores case and underscores and accepts any substring, so `basicusage`, `basic` and `usage` all match it. The file's `core001` prefix does not.

### Connection configuration

By default, examples connect to ClickHouse at `localhost:8123` with the `default` user and no password. If your setup is different, you can:

1. Modify the connection strings in the examples
2. Set up a local ClickHouse instance with default settings
3. Use environment variables or configuration files (see [Core_002_ConnectionStringConfiguration.cs](Http/Core/Core_002_ConnectionStringConfiguration.cs))

### ClickHouse Cloud

If you want to use ClickHouse Cloud:

1. Create a ClickHouse Cloud instance
2. Update the connection string in the examples:
   ```csharp
   var connection = new ClickHouseConnection(
       "Host=your-instance.clickhouse.cloud;Port=8443;Protocol=https;Username=default;Password=your_password;Database=default"
   );
   ```

## Additional resources

- [ClickHouse C# Driver Documentation](https://clickhouse.com/docs/integrations/csharp)
- [ClickHouse Documentation](https://clickhouse.com/docs)
- [ClickHouse SQL Reference](https://clickhouse.com/docs/en/sql-reference)
