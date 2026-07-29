using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.ADO;

namespace ClickHouse.Driver.Tests;

[TestFixture]
public abstract class AbstractConnectionTestFixture : IDisposable
{
    protected readonly ClickHouseConnection connection;
    protected readonly ClickHouseClient client;

    private readonly ConcurrentQueue<string> createdTables = new();
    private bool disposed;

    protected AbstractConnectionTestFixture()
    {
        client = TestUtilities.GetTestClickHouseClient();
        connection = client.CreateConnection();
    }

    protected static string SanitizeTableName(string input) => TestUtilities.SanitizeTableName(input);

    /// <summary>
    /// Builds a unique, database-qualified table name and registers it to be dropped when this
    /// fixture tears down. See <see cref="TestUtilities.CreateTableName"/> for the naming scheme.
    /// </summary>
    /// <param name="prefix">
    /// Readable part of the name; defaults to the calling test's name. Sanitized, so interpolating
    /// test-case values into it is safe.
    /// </param>
    /// <param name="database">
    /// Database to qualify the name with, <see cref="TestUtilities.TestDatabase"/> by default. Pass
    /// <c>null</c> for an unqualified name.
    /// </param>
    /// <param name="testName">Do not pass explicitly; filled in by the compiler.</param>
    protected string CreateTableName(string prefix = null, string database = TestUtilities.TestDatabase, [CallerMemberName] string testName = null)
    {
        var name = TestUtilities.CreateTableName(prefix, database, testName);
        createdTables.Enqueue(name);
        return name;
    }

    /// <summary>
    /// Drops every table handed out by <see cref="CreateTableName"/>. Best-effort: a table that
    /// cannot be dropped must not fail an otherwise passing fixture.
    /// </summary>
    private void DropCreatedTables()
    {
        while (createdTables.TryDequeue(out var table))
        {
            try
            {
                client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}").GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                TestContext.Progress.WriteLine($"Failed to drop test table {table}: {e.Message}");
            }
        }
    }

    [OneTimeTearDown]
    public void Dispose()
    {
        // NUnit both invokes [OneTimeTearDown] and disposes the fixture instance, so this can run twice.
        if (disposed)
            return;
        disposed = true;

        DropCreatedTables();
        connection?.Dispose();
        client?.Dispose();
    }
}
