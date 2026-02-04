using System;
using System.Text;
using ClickHouse.Driver.ADO;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

[TestFixture]
public abstract class AbstractConnectionTestFixture : IDisposable
{
    protected readonly ClickHouseConnection connection;
    protected readonly ClickHouseClient client;

    protected AbstractConnectionTestFixture()
    {
        client = TestUtilities.GetTestClickHouseClient();
        connection = client.CreateConnection();
        client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test;").GetAwaiter().GetResult();
    }

    protected static string SanitizeTableName(string input)
    {
        var builder = new StringBuilder();
        foreach (var c in input)
        {
            if (char.IsLetterOrDigit(c) || c == '_')
                builder.Append(c);
        }

        return builder.ToString();
    }

    [OneTimeTearDown]
    public void Dispose() => connection?.Dispose();
}
