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
        using var client = new ClickHouseClient("Host=localhost");

        var tableName = "example_select_basic";

        // Create and populate a test table
        await client.ExecuteNonQueryAsync($@"
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

        // Insert sample data using InsertBinaryAsync
        var rows = new List<object[]>
        {
            new object[] { 1UL, "Alice Johnson", "Engineering", 95000f, new DateOnly(2020, 1, 15), true },
            new object[] { 2UL, "Bob Smith", "Sales", 75000f, new DateOnly(2019, 6, 20), true },
            new object[] { 3UL, "Carol White", "Engineering", 105000f, new DateOnly(2018, 3, 10), true },
            new object[] { 4UL, "David Brown", "Marketing", 68000f, new DateOnly(2021, 9, 5), true },
            new object[] { 5UL, "Eve Davis", "Engineering", 88000f, new DateOnly(2020, 11, 12), false },
            new object[] { 6UL, "Frank Miller", "Sales", 82000f, new DateOnly(2019, 2, 28), true },
            new object[] { 7UL, "Grace Lee", "Marketing", 71000f, new DateOnly(2022, 1, 8), true }
        };
        var columns = new[] { "id", "name", "department", "salary", "hire_date", "is_active" };
        await client.InsertBinaryAsync(tableName, columns, rows);

        Console.WriteLine($"Created and populated table '{tableName}'\n");

        // Example 1: SELECT with WHERE clause
        Console.WriteLine("\n1. SELECT with WHERE clause (Engineering department only):");
        using (var reader = await client.ExecuteReaderAsync(
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
        using (var reader = await client.ExecuteReaderAsync($@"
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
        var totalEmployees = await client.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
        Console.WriteLine($"   Total employees: {totalEmployees}");


        // Example 4: Reading data with GetFieldValue<T>
        Console.WriteLine("\n4. Using GetFieldValue<T> for type-safe reading:");
        using (var reader = await client.ExecuteReaderAsync(
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
        await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        Console.WriteLine($"\nTable '{tableName}' dropped");
    }
}
