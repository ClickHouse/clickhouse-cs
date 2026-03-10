using System.Data;
using System.Globalization;
using ClickHouse.Driver.ADO;
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
/// NOTE: Dapper's @parameter syntax does NOT work with ClickHouse's {param:Type} syntax.
/// The following will NOT work:
///     connection.QueryAsync&lt;string&gt;("SELECT {p1:Int32}", new { p1 = 42 });
/// Use standard Dapper @parameter syntax for INSERTs or explicit column selection.
/// </summary>
public static class DapperExample
{
    private const string TableName = "example_dapper";

    static DapperExample()
    {
        // Register custom type handlers for ClickHouse-specific types
        SqlMapper.AddTypeHandler(new ClickHouseDecimalHandler());
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
    /// Uses @parameter syntax which Dapper translates to ADO.NET parameters.
    /// </summary>
    private static async Task InsertWithAnonymousParameters(ClickHouseConnection connection)
    {
        Console.WriteLine("1. Inserting data with Dapper anonymous parameters:");

        var sql = $"INSERT INTO {TableName} (id, name, email, balance) VALUES (@id, @name, @email, @balance)";

        // Insert multiple rows using anonymous objects
        // Use decimal literals (m suffix) for Decimal64 columns
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
    /// Demonstrates Dapper.Contrib's GetAll and Get methods.
    /// NOTE: Dapper.Contrib's Insert does not work with ClickHouse due to SQL dialect differences.
    /// </summary>
    private static async Task DapperContribExample(ClickHouseConnection connection)
    {
        Console.WriteLine("4. Dapper.Contrib - GetAll and Get:");

        // GetAll retrieves all rows and maps to the entity type
        var allUsers = (await connection.GetAllAsync<ContribUser>()).ToList();
        Console.WriteLine($"   GetAll returned {allUsers.Count} users");

        // Get retrieves a single row by primary key
        var user = await connection.GetAsync<ContribUser>(1);
        Console.WriteLine($"   Get(1) returned: {user?.name ?? "null"}");

        Console.WriteLine();
        Console.WriteLine("   NOTE: Dapper.Contrib's Insert<T>() does not work with ClickHouse");
        Console.WriteLine("   (generates SQL Server syntax with square brackets and SCOPE_IDENTITY)");
        Console.WriteLine("   Use standard Dapper ExecuteAsync with INSERT statements instead.\n");
    }

    private static async Task Cleanup(ClickHouseConnection connection)
    {
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {TableName}");
        Console.WriteLine($"Table '{TableName}' dropped");
    }

    /// <summary>
    /// Simple user class for Dapper mapping.
    /// Property names match column names (case-insensitive).
    /// Dapper requires a parameterless constructor for materialization.
    /// Note: Balance uses ClickHouseDecimal to match Decimal64 column type.
    /// </summary>
    private class User
    {
        public uint Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
        public ClickHouseDecimal Balance { get; set; }
        public DateTime Created { get; set; }
    }

    /// <summary>
    /// Entity class for Dapper.Contrib.
    /// Uses [Table] attribute to specify the table name.
    /// Uses [ExplicitKey] for non-auto-increment primary keys (ClickHouse doesn't have auto-increment).
    /// IMPORTANT: Property names must match ClickHouse column names exactly (case-sensitive).
    /// </summary>
    [Table(TableName)]
    private class ContribUser
    {
        // Property names must be lowercase to match ClickHouse column names
        [ExplicitKey]
        public uint id { get; set; }
        public string name { get; set; } = string.Empty;
        public string email { get; set; } = string.Empty;
        public ClickHouseDecimal balance { get; set; }
        public DateTime created { get; set; }
    }

    /// <summary>
    /// Custom type handler for ClickHouseDecimal.
    /// Register this handler to support Decimal64/Decimal128/Decimal256 columns.
    /// </summary>
    private class ClickHouseDecimalHandler : SqlMapper.TypeHandler<ClickHouseDecimal>
    {
        public override void SetValue(IDbDataParameter parameter, ClickHouseDecimal value)
        {
            parameter.Value = value.ToString(CultureInfo.InvariantCulture);
        }

        public override ClickHouseDecimal Parse(object value)
        {
            return value switch
            {
                ClickHouseDecimal chd => chd,
                IConvertible ic => Convert.ToDecimal(ic),
                _ => throw new ArgumentException($"Cannot convert {value?.GetType()} to ClickHouseDecimal", nameof(value))
            };
        }
    }
}
