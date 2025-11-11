using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates basic SELECT queries in ClickHouse.
/// Shows various ways to query and read data.
/// </summary>
public static class BasicSelect
{
    public static async Task Run()
    {
        using var connection = new ClickHouseConnection("Host=localhost");
        await connection.OpenAsync();

        var tableName = "example_select_basic";

        // Create and populate a test table
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt64,
                name String,
                department String,
                salary Float32,
                hire_date Date,
                is_active Boolean
            )
            ENGINE = MergeTree()
            ORDER BY (id)
        ");

        // Insert sample data
        await connection.ExecuteStatementAsync($@"
            INSERT INTO {tableName} VALUES
            (1, 'Alice Johnson', 'Engineering', 95000, '2020-01-15', true),
            (2, 'Bob Smith', 'Sales', 75000, '2019-06-20', true),
            (3, 'Carol White', 'Engineering', 105000, '2018-03-10', true),
            (4, 'David Brown', 'Marketing', 68000, '2021-09-05', true),
            (5, 'Eve Davis', 'Engineering', 88000, '2020-11-12', false),
            (6, 'Frank Miller', 'Sales', 82000, '2019-02-28', true),
            (7, 'Grace Lee', 'Marketing', 71000, '2022-01-08', true)
        ");

        Console.WriteLine($"Created and populated table '{tableName}'\n");

        // Example 1: SELECT with WHERE clause
        Console.WriteLine("\n1. SELECT with WHERE clause (Engineering department only):");
        using (var reader = await connection.ExecuteReaderAsync(
            $"SELECT name, salary FROM {tableName} WHERE department = 'Engineering' ORDER BY salary DESC"))
        {
            Console.WriteLine("Name\t\t\tSalary");
            Console.WriteLine("----\t\t\t------");

            while (reader.Read())
            {
                Console.WriteLine($"{reader.GetString(0),-20}\t${reader.GetFloat(1):F2}");
            }
        }

        
        // Example 2: SELECT with aggregations
        Console.WriteLine("\n2. SELECT with aggregations (average salary by department):");
        using (var reader = await connection.ExecuteReaderAsync($@"
            SELECT
                department,
                count() as employee_count,
                avg(salary) as avg_salary,
                min(salary) as min_salary,
                max(salary) as max_salary
            FROM {tableName}
            GROUP BY department
            ORDER BY avg_salary DESC
        "))
        {
            Console.WriteLine("Department\tCount\tAvg Salary\tMin Salary\tMax Salary");
            Console.WriteLine("----------\t-----\t----------\t----------\t----------");

            while (reader.Read())
            {
                Console.WriteLine($"{reader.GetString(0),-15}\t{reader.GetFieldValue<ulong>(1)}\t${reader.GetDouble(2),-10:F2}\t${reader.GetFloat(3),-10:F2}\t${reader.GetFloat(4),-10:F2}");
            }
        }

        
        // Example 3: Using ExecuteScalarAsync for single value
        Console.WriteLine("\n3. Using ExecuteScalarAsync for single value:");
        var totalEmployees = await connection.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
        Console.WriteLine($"   Total employees: {totalEmployees}");

        
        // Example 4: Reading data with GetFieldValue<T>
        Console.WriteLine("\n7. Using GetFieldValue<T> for type-safe reading:");
        using (var reader = await connection.ExecuteReaderAsync(
            $"SELECT id, name, salary FROM {tableName} WHERE id = 1"))
        {
            if (reader.Read())
            {
                var id = reader.GetFieldValue<ulong>(0);
                var name = reader.GetFieldValue<string>(1);
                var salary = reader.GetFieldValue<float>(2);
                Console.WriteLine($"   Employee ID: {id}, Name: {name}, Salary: ${salary:F2}");
            }
        }

        // Clean up
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine($"\nTable '{tableName}' dropped");
    }
}
