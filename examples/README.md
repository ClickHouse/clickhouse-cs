# ClickHouse C# Driver Examples

This directory contains examples demonstrating various features and usage patterns of the ClickHouse C# driver.

## Overview

We aim to cover various scenarios of driver usage with these examples. You should be able to run any of these examples by following the instructions in the [How to run](#how-to-run) section below.

If something is missing, or you found a mistake in one of these examples, please open an issue or a pull request.

## Examples

### Core Usage & Configuration

- [Core_001_BasicUsage.cs](Core_001_BasicUsage.cs) - Creating a client, tables, and performing basic insert/select operations (using ClickHouseClientSettings)
- [Core_002_ConnectionStringConfiguration.cs](Core_002_ConnectionStringConfiguration.cs) - Various connection string formats and configuration options
- [Core_003_DependencyInjection.cs](Core_003_DependencyInjection.cs) - Using ClickHouse with dependency injection and config binding
- [Core_004_HttpClientConfiguration.cs](Core_004_HttpClientConfiguration.cs) - Providing custom HttpClient or IHttpClientFactory for SSL/TLS, proxy, timeouts, and more control over connection settings

### Creating Tables

- [Tables_001_CreateTableSingleNode.cs](Tables_001_CreateTableSingleNode.cs) - Creating tables with different engines and data types on a single-node deployment

### Inserting Data

- [Insert_001_SimpleDataInsert.cs](Insert_001_SimpleDataInsert.cs) - Basic data insertion using parameterized queries
- [Insert_002_BulkInsert.cs](Insert_002_BulkInsert.cs) - High-performance bulk data insertion using `ClickHouseBulkCopy`

### Selecting Data

- [Select_001_BasicSelect.cs](Select_001_BasicSelect.cs) - Basic SELECT queries and reading the results
- [Select_002_SelectMetadata.cs](Select_002_SelectMetadata.cs) - Column metadata overview
- [Select_003_SelectWithParameterBinding.cs](Select_003_SelectWithParameterBinding.cs) - Parameterized queries for safe and dynamic SQL construction

### Data Types

- DataTypes_001_SimpleTypes.cs
- DataTypes_002_DatesAndTimes.cs
- [DataTypes_003_ComplexTypes.cs](DataTypes_003_ComplexTypes.cs) - Working with complex data types: Arrays, Maps, Tuples, IP addresses, and Nested structures

### Advanced Features

- [Advanced_001_QueryIdUsage.cs](Advanced_001_QueryIdUsage.cs) - Using Query IDs to track and monitor query execution
- [Advanced_002_SessionIdUsage.cs](Advanced_002_SessionIdUsage.cs) - Using Session IDs for temporary tables and session state (with important limitations)
- [Advanced_003_LongRunningQueries.cs](Advanced_003_LongRunningQueries.cs) - Strategies for handling long-running queries (progress headers and fire-and-forget patterns)
- [Advanced_004_CustomSettings.cs](Advanced_004_CustomSettings.cs) - Using custom ClickHouse server settings for resource limits and query optimization
- [Advanced_005_QueryStatistics.cs](Advanced_005_QueryStatistics.cs) - Accessing and using query statistics for performance monitoring and optimization decisions
- [Advanced_006_Roles.cs](Advanced_006_Roles.cs) - Using ClickHouse roles to control permissions at connection and command levels

### Troubleshooting

- [Troubleshooting_001_LoggingConfiguration.cs](Troubleshooting_001_LoggingConfiguration.cs) - Setting up logging with Microsoft.Extensions.Logging to view diagnostic information
- [Troubleshooting_002_NetworkTracing.cs](Troubleshooting_002_NetworkTracing.cs) - Enabling low-level .NET network tracing for debugging connection issues (HTTP, Sockets, DNS, TLS)

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

The filter uses fuzzy matching - it matches against any substring of the example filename, ignoring case and underscores. For example, `core001`, `core_001`, `basicusage`, and `Basic` would all match `Core_001_BasicUsage`.

### Connection configuration

By default, examples connect to ClickHouse at `localhost:8123` with the `default` user and no password. If your setup is different, you can:

1. Modify the connection strings in the examples
2. Set up a local ClickHouse instance with default settings
3. Use environment variables or configuration files (see [Core_002_ConnectionStringConfiguration.cs](Core_002_ConnectionStringConfiguration.cs))

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
