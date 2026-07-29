using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

[Category("Cloud")]
public class SessionConnectionTest
{
    private static DbConnection CreateConnection(bool useSession, string sessionId = null)
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.UseSession = useSession;
        builder.Compression = true;
        if (sessionId != null)
            builder.SessionId = sessionId;
        return new ClickHouseConnection(builder.ToString());
    }

    private static ClickHouseConnection CreateConnectionWithHttpClient(HttpClient httpClient, bool useSession, string sessionId = null)
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        var settings = new ClickHouseClientSettings(builder)
        {
            UseSession = useSession,
            SessionId = sessionId,
            HttpClient = httpClient,
        };
        return new ClickHouseConnection(settings);
    }

    /// <summary>
    /// Builds a unique name for a temporary table. Temporary tables live outside any database, so the
    /// name has to stay unqualified, and they disappear with their session, so they need no cleanup.
    /// </summary>
    private static string CreateTempTableName([CallerMemberName] string testName = null) =>
        TestUtilities.CreateTableName(database: null, testName: testName);

    [Test]
    public async Task TempTableShouldBeCreatedSuccessfullyIfUseSessionEnabled()
    {
        var table = CreateTempTableName();
        using var connection = CreateConnection(true);
        await connection.ExecuteStatementAsync($"CREATE TEMPORARY TABLE {table} (value UInt8)");
        await connection.ExecuteScalarAsync($"SELECT COUNT(*) from {table}");
    }

    [Test]
    public async Task TempTableShouldBeCreatedSuccessfullyIfSessionIdPassed()
    {
        var table = CreateTempTableName();
        var sessionId = "TEST-" + Guid.NewGuid().ToString();
        using var connection = CreateConnection(true, sessionId);
        await connection.ExecuteStatementAsync($"CREATE TEMPORARY TABLE {table} (value UInt8)");
        await connection.ExecuteScalarAsync($"SELECT COUNT(*) from {table}");
    }
    
    [Test]
    public async Task QueryWithTempTable_SessionIdSetInClickHouseClientSettings_TableShouldBeCreatedSuccessfully()
    {
        var table = CreateTempTableName();
        var sessionId = "TEST-" + Guid.NewGuid().ToString();
        var builder = TestUtilities.GetConnectionStringBuilder();
        var settings = new ClickHouseClientSettings(builder)
        {
            SessionId = sessionId,
            UseSession = true,
        };

        var connection = new ClickHouseConnection(settings);
        await connection.ExecuteStatementAsync($"CREATE TEMPORARY TABLE {table} (value UInt8)");
        await connection.ExecuteScalarAsync($"SELECT COUNT(*) from {table}");
    }

    [Test]
    public async Task TempTableShouldFailIfSessionDisabled()
    {
        var table = CreateTempTableName();
        using var connection = CreateConnection(false);
        try
        {
            await connection.ExecuteStatementAsync($"CREATE TEMPORARY TABLE {table} (value UInt8)");
            await connection.ExecuteScalarAsync($"SELECT COUNT(*) from {table}");
            Assert.Fail("ClickHouse should not be able to use temp table if session is disabled");
        }
        catch (ClickHouseServerException e) when (e.ErrorCode == 60) // Error 60 means the table does not exist
        {
        }
    }

    [Test]
    public async Task TempTableShouldFailIfSessionDisabledAndSessionIdPassed()
    {
        var table = CreateTempTableName();
        using var connection = CreateConnection(false, "ASD");
        try
        {
            await connection.ExecuteStatementAsync($"CREATE TEMPORARY TABLE {table} (value UInt8)");
            await connection.ExecuteScalarAsync($"SELECT COUNT(*) from {table}");
            Assert.Fail("ClickHouse should not be able to use temp table if session is disabled");
        }
        catch (ClickHouseServerException e) when (e.ErrorCode == 60) // Error 60 means the table does not exist
        {
        }
    }

    [Test]
    public async Task Session_WithCustomHttpClient_ShouldWork()
    {
        var table = CreateTempTableName();
        var sessionId = "TEST-" + Guid.NewGuid().ToString();
        var handler = new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
        };
        
        using var httpClient = new HttpClient(handler);

        using var connection = CreateConnectionWithHttpClient(httpClient, useSession: true, sessionId);
        await connection.ExecuteStatementAsync($"CREATE TEMPORARY TABLE {table} (value UInt8)");
        await connection.ExecuteScalarAsync($"SELECT COUNT(*) from {table}");
    }
}
