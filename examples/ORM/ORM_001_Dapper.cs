using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Dapper;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Utility;
using Dapper;
using Dapper.Contrib.Extensions;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates using Dapper ORM with the ClickHouse driver.
/// Dapper provides a simple object mapping layer over ADO.NET connections.
///
/// Connection Lifetime Pattern:
/// - Use ClickHouseDataSource as a singleton (register in DI, or create once and reuse)
/// - Create short-lived connections per operation using dataSource.CreateConnection()
/// - The DataSource manages connection pooling internally
///
/// Call ClickHouseDapper.Register() once at startup to register type handlers (decimal,
/// DateTimeOffset, ITuple, BigInteger, IPAddress) and the Dapper.Contrib SQL adapter.
/// </summary>
public static class DapperExample
{
    private const string TableName = "example_dapper";

    static DapperExample()
    {
        ClickHouseDapper.Register();
    }

    public static async Task Run()
    {
        // Create a DataSource - in a real app, this would be a singleton (register in DI)
        // The DataSource manages HttpClient pooling internally
        var dataSource = new ClickHouseDataSource("Host=localhost");

        // Create a connection from the DataSource
        // Connections are lightweight - create them per operation
        await using var connection = dataSource.CreateConnection();
        await connection.OpenAsync();

        await SetupTable(connection);

        // Insert data using Dapper's anonymous object parameters
        await InsertWithAnonymousParameters(connection);

        // Query with strongly-typed results
        await QueryWithStrongTyping(connection);

        // Query with dynamic results
        await QueryWithDynamicResults(connection);

        // Dapper.Contrib usage
        await DapperContribExample(connection);

        await Cleanup(connection);
    }

    private static async Task SetupTable(ClickHouseConnection connection)
    {
        await connection.ExecuteStatementAsync($@"
            CREATE TABLE IF NOT EXISTS {TableName} (
                id UInt32,
                name String,
                email String,
                balance Decimal64(2),
                created DateTime DEFAULT now()
            ) ENGINE = Memory
        ");

        Console.WriteLine($"Table '{TableName}' created\n");
    }

    /// <summary>
    /// Demonstrates inserting data using Dapper's anonymous object parameter binding.
    /// </summary>
    private static async Task InsertWithAnonymousParameters(ClickHouseConnection connection)
    {
        Console.WriteLine("1. Inserting data with Dapper anonymous parameters:");

        var sql = $"INSERT INTO {TableName} (id, name, email, balance) VALUES (@id, @name, @email, @balance)";

        await connection.ExecuteAsync(sql, new { id = 1, name = "Alice", email = "alice@example.com", balance = 1000.50m });
        await connection.ExecuteAsync(sql, new { id = 2, name = "Bob", email = "bob@example.com", balance = 2500.75m });
        await connection.ExecuteAsync(sql, new { id = 3, name = "Carol", email = "carol@example.com", balance = 750.25m });

        Console.WriteLine("   Inserted 3 rows using Dapper\n");
    }

    /// <summary>
    /// Demonstrates querying data and mapping to strongly-typed objects.
    /// Dapper maps columns to properties by name (case-insensitive).
    /// </summary>
    private static async Task QueryWithStrongTyping(ClickHouseConnection connection)
    {
        Console.WriteLine("2. Querying with strongly-typed results:");

        var users = (await connection.QueryAsync<User>(
            $"SELECT id, name, email, balance, created FROM {TableName} ORDER BY id")).ToList();

        Console.WriteLine("   ID  Name       Email                    Balance     Created");
        Console.WriteLine("   --  ----       -----                    -------     -------");
        foreach (var user in users)
        {
            Console.WriteLine($"   {user.Id,-3} {user.Name,-10} {user.Email,-24} {user.Balance,10:F2} {user.Created:HH:mm:ss}");
        }
        Console.WriteLine();
    }

    /// <summary>
    /// Demonstrates querying with dynamic results when you don't need a typed class.
    /// </summary>
    private static async Task QueryWithDynamicResults(ClickHouseConnection connection)
    {
        Console.WriteLine("3. Querying with dynamic results:");

        var rows = await connection.QueryAsync($"SELECT id, name, balance FROM {TableName} WHERE balance > 1000 ORDER BY balance DESC");

        Console.WriteLine("   Users with balance > 1000:");
        foreach (var row in rows)
        {
            Console.WriteLine($"   - {row.name}: {row.balance:F2}");
        }
        Console.WriteLine();
    }

    /// <summary>
    /// Demonstrates Dapper.Contrib's Get, GetAll, and Insert methods.
    /// Insert returns 0 because ClickHouse has no auto-increment; the caller supplies the key.
    /// </summary>
    private static async Task DapperContribExample(ClickHouseConnection connection)
    {
        Console.WriteLine("4. Dapper.Contrib - Get, GetAll, Insert:");

        var allUsers = (await connection.GetAllAsync<ContribUser>()).ToList();
        Console.WriteLine($"   GetAll returned {allUsers.Count} users");

        var user = await connection.GetAsync<ContribUser>(1);
        Console.WriteLine($"   Get(1) returned: {user?.name ?? "null"}");

        var newUser = new ContribUser { id = 99, name = "Dave", email = "dave@example.com", balance = 42.00m };
        var key = await connection.InsertAsync(newUser);
        Console.WriteLine($"   InsertAsync returned key={key} (always 0 — ClickHouse has no auto-increment)");

        Console.WriteLine();
    }

    private static async Task Cleanup(ClickHouseConnection connection)
    {
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {TableName}");
        Console.WriteLine($"Table '{TableName}' dropped");
    }

    private class User
    {
        public uint Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
        public ClickHouseDecimal Balance { get; set; }
        public DateTime Created { get; set; }
    }

    [Table(TableName)]
    private class ContribUser
    {
        [ExplicitKey]
        public uint id { get; set; }
        public string name { get; set; } = string.Empty;
        public string email { get; set; } = string.Empty;
        public ClickHouseDecimal balance { get; set; }
        public DateTime created { get; set; }
    }
}
