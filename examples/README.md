# ClickHouse C# Driver Examples

This directory contains examples demonstrating various features and usage patterns of the ClickHouse C# driver.

## Overview

We aim to cover various scenarios of driver usage with these examples. You should be able to run any of these examples by following the instructions in the [How to run](#how-to-run) section below.

If something is missing, or you found a mistake in one of these examples, please open an issue or a pull request.

## Examples

### Core Usage & Configuration

- [Core_001_BasicUsage.cs](Core_001_BasicUsage.cs) - Creating a client, tables, and performing basic insert/select operations
- [Core_002_ConnectionStringConfiguration.cs](Core_002_ConnectionStringConfiguration.cs) - Various connection string formats and configuration options

### Creating Tables

- [Tables_001_CreateTableSingleNode.cs](Tables_001_CreateTableSingleNode.cs) - Creating tables with different engines and data types on a single-node deployment

### Inserting Data

- [Insert_001_SimpleDataInsert.cs](Insert_001_SimpleDataInsert.cs) - Basic data insertion using parameterized queries
- [Insert_002_BulkInsert.cs](Insert_002_BulkInsert.cs) - High-performance bulk data insertion using `ClickHouseBulkCopy`

### Selecting Data

- [Select_001_BasicSelect.cs](Select_001_BasicSelect.cs) - Basic SELECT queries and reading the results
- [Select_002_SelectMetadata.cs](Select_002_SelectMetadata.cs) - Column metadata overview
- [Select_003_SelectWithParameterBinding.cs](Select_003_SelectWithParameterBinding.cs) - Parameterized queries for safe and dynamic SQL construction

### Troubleshooting

- [Troubleshooting_001_LoggingConfiguration.cs](Troubleshooting_001_LoggingConfiguration.cs) - Setting up logging with Microsoft.Extensions.Logging to view diagnostic information

## How to run

### Prerequisites

- .NET 9.0 SDK or later
- ClickHouse server (local or remote)
  - For local runs, you can use Docker:
    ```bash
    docker run -d --name clickhouse-server -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server
    ```

### Running examples

Navigate to the examples directory and run the example program (which will execute all examples):
   ```bash
   cd examples
   dotnet run
   ```

Alternatively, you can modify `Program.cs` to run specific examples:
   ```csharp
   // Run only specific examples
   await Core_001_BasicUsage.Run();
   await Insert_002_BulkInsert.Run();
   ```

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
