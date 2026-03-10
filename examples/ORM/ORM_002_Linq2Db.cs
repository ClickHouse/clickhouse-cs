using ClickHouse.Driver.Numerics;
using LinqToDB;
using LinqToDB.Async;
using LinqToDB.Data;
using LinqToDB.DataProvider.ClickHouse;
using LinqToDB.Mapping;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates using linq2db with the ClickHouse driver.
/// linq2db has native ClickHouse support and works well with this driver.
/// </summary>
public static class Linq2DbExample
{
    public static async Task Run()
    {
        // Connect using linq2db's DataConnection with ClickHouseDriver provider
        var connectionString = "Host=localhost";
        var options = new DataOptions().UseClickHouse(connectionString, ClickHouseProvider.ClickHouseDriver);

        await using var db = new DataConnection(options);

        Console.WriteLine("Connected to ClickHouse via linq2db\n");

        // Create table using linq2db
        await CreateTable(db);

        // Insert data using linq2db
        await InsertData(db);

        // Query with LINQ
        await QueryWithLinq(db);

        // BulkCopy for high-performance inserts
        await BulkCopyExample(db);

        // Cleanup
        await Cleanup(db);
    }

    private static async Task CreateTable(DataConnection db)
    {
        Console.WriteLine("1. Creating table with linq2db:");

        // Drop if exists and create fresh
        await db.DropTableAsync<User>(throwExceptionIfNotExists: false);
        await db.CreateTableAsync<User>();

        Console.WriteLine("   Table 'example_linq2db_users' created\n");
    }

    private static async Task InsertData(DataConnection db)
    {
        Console.WriteLine("2. Inserting data with linq2db:");

        // Insert single row
        await db.InsertAsync(new User
        {
            Id = 1,
            Name = "Alice",
            Email = "alice@example.com",
            Balance = 1000.50m,
            Created = DateTime.UtcNow
        });

        // Insert multiple rows
        await db.InsertAsync(new User { Id = 2, Name = "Bob", Email = "bob@example.com", Balance = 2500.75m, Created = DateTime.UtcNow });
        await db.InsertAsync(new User { Id = 3, Name = "Carol", Email = "carol@example.com", Balance = 750.25m, Created = DateTime.UtcNow });

        Console.WriteLine("   Inserted 3 rows\n");
    }

    private static async Task QueryWithLinq(DataConnection db)
    {
        Console.WriteLine("3. Querying with LINQ:");

        var users = db.GetTable<User>();

        // Simple select all
        var allUsers = await users.OrderBy(u => u.Id).ToListAsync();
        Console.WriteLine($"   Total users: {allUsers.Count}");

        // Filtered query with LINQ
        var highBalance = await users
            .Where(u => u.Balance > 1000)
            .OrderByDescending(u => u.Balance)
            .ToListAsync();

        Console.WriteLine("   Users with balance > 1000:");
        foreach (var user in highBalance)
        {
            Console.WriteLine($"   - {user.Name}: {user.Balance:F2}");
        }

        // Count query
        var totalCount = await users.CountAsync();
        Console.WriteLine($"   Total user count: {totalCount}\n");
    }

    private static async Task BulkCopyExample(DataConnection db)
    {
        Console.WriteLine("4. BulkCopy for high-performance inserts:");

        var bulkUsers = Enumerable.Range(100, 10).Select(i => new User
        {
            Id = i,
            Name = $"BulkUser{i}",
            Email = $"bulk{i}@example.com",
            Balance = i * 10.5m,
            Created = DateTime.UtcNow
        });

        var options = new BulkCopyOptions
        {
            MaxBatchSize = 100,
            MaxDegreeOfParallelism = 1,
        };

        var result = await db.BulkCopyAsync(options, bulkUsers);
        Console.WriteLine($"   BulkCopy inserted {result.RowsCopied} rows\n");

        // Verify total count
        var count = await db.GetTable<User>().CountAsync();
        Console.WriteLine($"   Total rows in table: {count}\n");
    }

    private static async Task Cleanup(DataConnection db)
    {
        await db.DropTableAsync<User>();
        Console.WriteLine("Table 'example_linq2db_users' dropped");
    }

    /// <summary>
    /// Entity class for linq2db.
    /// Use [Table] and [Column] attributes to control mapping.
    /// </summary>
    [Table("example_linq2db_users")]
    private class User
    {
        [Column, PrimaryKey]
        public int Id { get; set; }

        [Column]
        public string Name { get; set; } = string.Empty;

        [Column]
        public string Email { get; set; } = string.Empty;

        [Column]
        public ClickHouseDecimal Balance { get; set; }

        [Column]
        public DateTime Created { get; set; }
    }
}
