using System;
using System.Text;
using ClickHouse.Driver.ADO;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests;

[TestFixture]
public abstract class AbstractConnectionTestFixture : IDisposable
{
    protected readonly ClickHouseConnection connection;

    protected AbstractConnectionTestFixture()
    {
        connection = TestUtilities.GetTestClickHouseConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "CREATE DATABASE IF NOT EXISTS test;";
        command.ExecuteScalar();
    }

    protected static string SanitizeTableName(string input)
    {
        var builder = new StringBuilder();
        foreach (var c in input)
        {
            if (char.IsLetterOrDigit(c) || c == '_')
                builder.Append(c);
        }

        // When running in parallel, we need to avoid false failures due to running against the same tables
        var frameworkSuffix = GetFrameworkSuffix();
        if (!string.IsNullOrEmpty(frameworkSuffix))
            builder.Append('_').Append(frameworkSuffix);

        return builder.ToString();
    }

    private static string GetFrameworkSuffix()
    {
#if NET462
        return "net462";
#elif NET48
        return "net48";
#elif NET6_0
        return "net6";
#elif NET8_0
        return "net8";
#elif NET9_0
        return "net9";
#else
        return "";
#endif
    }

    [OneTimeTearDown]
    public void Dispose() => connection?.Dispose();
}
