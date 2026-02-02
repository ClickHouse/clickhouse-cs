using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates INSERT FROM SELECT patterns for copying, transforming, and loading data.
/// This is a very useful pattern for ETL operations and data migrations.
///
/// See also Advanced_003_LongRunningQueries.cs for dealing with issues that can be encountered
/// when running large INSERT FROM SELECT queries.
///
/// INSERT FROM SELECT can also be used with integration engines to load external data:
/// See here for a full list: https://clickhouse.com/docs/sql-reference/table-functions
///
/// -- Load from S3:
/// INSERT INTO trips
///    SELECT *
///    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_*.gz', 'TabSeparatedWithNames')
///    LIMIT 1000000;
///
/// -- Load from URL:
/// INSERT INTO my_table
///    SELECT *
///    FROM url('https://example.com/data.csv', 'CSV');
///
/// -- Load from another ClickHouse server:
/// INSERT INTO local_table
///    SELECT *
///    FROM remote('other-server:9000', 'database', 'table', 'user', 'password');
///
/// -- Load from MySQL:
/// INSERT INTO clickhouse_table
///    SELECT *
///    FROM mysql('host:3306', 'database', 'table', 'user', 'password');
///
/// See https://clickhouse.com/docs/en/sql-reference/statements/insert-into for more details.
/// </summary>
public static class InsertFromSelect
{
    public static async Task Run()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        Console.WriteLine("INSERT FROM SELECT Examples\n");

        await BasicCopy(connection);
        await TransformAndAggregate(connection);

        Console.WriteLine("All INSERT FROM SELECT examples completed!");
    }

    /// <summary>
    /// Basic example: Copy data from one table to another.
    /// </summary>
    private static async Task BasicCopy(ClickHouseConnection connection)
    {
        Console.WriteLine("1. Basic copy from one table to another:");

        var sourceTable = "example_insert_select_source";
        var targetTable = "example_insert_select_target";

        // Create source table with data
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {sourceTable}
            (
                id UInt64,
                name String,
                value Float32
            )
            ENGINE = MergeTree()
            ORDER BY id
        ");

        await connection.ExecuteStatementAsync($@"
            INSERT INTO {sourceTable} VALUES
            (1, 'Alpha', 10.5),
            (2, 'Beta', 20.3),
            (3, 'Gamma', 30.7),
            (4, 'Delta', 40.2),
            (5, 'Epsilon', 50.9)
        ");

        // Create target table with same schema
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {targetTable}
            (
                id UInt64,
                name String,
                value Float32
            )
            ENGINE = MergeTree()
            ORDER BY id
        ");

        // Copy all data from source to target
        await connection.ExecuteStatementAsync($@"
            INSERT INTO {targetTable}
            SELECT * FROM {sourceTable}
        ");

        var count = await connection.ExecuteScalarAsync($"SELECT count() FROM {targetTable}");
        Console.WriteLine($"   Copied {count} rows from {sourceTable} to {targetTable}");

        // Cleanup
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {sourceTable}");
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        Console.WriteLine($"   Tables dropped\n");
    }

    /// <summary>
    /// Transform data during insertion with aggregations.
    /// </summary>
    private static async Task TransformAndAggregate(ClickHouseConnection connection)
    {
        Console.WriteLine("2. Transform and aggregate data during insertion:");

        var ordersTable = "example_orders";
        var summaryTable = "example_order_summary";

        // Create orders table with sample data
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {ordersTable}
            (
                order_id UInt64,
                customer_id UInt32,
                product String,
                quantity UInt32,
                price Float32,
                order_date Date
            )
            ENGINE = MergeTree()
            ORDER BY (order_date, order_id)
        ");

        await connection.ExecuteStatementAsync($@"
            INSERT INTO {ordersTable} VALUES
            (1, 100, 'Widget', 5, 10.00, '2024-01-15'),
            (2, 100, 'Gadget', 2, 25.00, '2024-01-16'),
            (3, 101, 'Widget', 3, 10.00, '2024-01-15'),
            (4, 101, 'Gizmo', 1, 50.00, '2024-01-17'),
            (5, 102, 'Widget', 10, 10.00, '2024-01-15'),
            (6, 100, 'Gizmo', 2, 50.00, '2024-01-18')
        ");

        // Create summary table for aggregated data
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {summaryTable}
            (
                customer_id UInt32,
                total_orders UInt64,
                total_quantity UInt64,
                total_spent Float64
            )
            ENGINE = MergeTree()
            ORDER BY customer_id
        ");

        // Insert aggregated data
        await connection.ExecuteStatementAsync($@"
            INSERT INTO {summaryTable}
            SELECT
                customer_id,
                count() AS total_orders,
                sum(quantity) AS total_quantity,
                sum(quantity * price) AS total_spent
            FROM {ordersTable}
            GROUP BY customer_id
            ORDER BY customer_id
        ");

        // Display results
        Console.WriteLine("   Customer order summary:");
        using (var reader = await connection.ExecuteReaderAsync($"SELECT * FROM {summaryTable} ORDER BY customer_id"))
        {
            Console.WriteLine("   Customer ID | Orders | Quantity | Total Spent");
            Console.WriteLine("   ------------|--------|----------|------------");
            while (reader.Read())
            {
                Console.WriteLine($"   {reader.GetFieldValue<uint>(0),-12} | {reader.GetFieldValue<ulong>(1),-6} | {reader.GetFieldValue<ulong>(2),-8} | ${reader.GetDouble(3):F2}");
            }
        }

        // Cleanup
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {ordersTable}");
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {summaryTable}");
        Console.WriteLine($"\n   Tables dropped\n");
    }
}
