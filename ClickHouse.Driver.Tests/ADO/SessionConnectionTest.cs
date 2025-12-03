using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Net;
using System.Net.Http;
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

    [Test]
    public async Task TempTableShouldBeCreatedSuccessfullyIfUseSessionEnabled()
    {
        using var connection = CreateConnection(true);
        await connection.ExecuteStatementAsync("CREATE TEMPORARY TABLE test_temp_table (value UInt8)");
        await connection.ExecuteScalarAsync("SELECT COUNT(*) from test_temp_table");
    }

    [Test]
    public async Task TempTableShouldBeCreatedSuccessfullyIfSessionIdPassed()
    {
        var sessionId = "TEST-" + Guid.NewGuid().ToString();
        using var connection = CreateConnection(true, sessionId);
        await connection.ExecuteStatementAsync("CREATE TEMPORARY TABLE test_temp_table (value UInt8)");
        await connection.ExecuteScalarAsync("SELECT COUNT(*) from test_temp_table");
    }
    
    [Test]
    public async Task QueryWithTempTable_SessionIdSetInClickHouseClientSettings_TableShouldBeCreatedSuccessfully()
    {
        var sessionId = "TEST-" + Guid.NewGuid().ToString();
        var builder = TestUtilities.GetConnectionStringBuilder();
        var settings = new ClickHouseClientSettings(builder)
        {
            SessionId = sessionId,
            UseSession = true,
        };

        var connection = new ClickHouseConnection(settings);
        await connection.ExecuteStatementAsync("CREATE TEMPORARY TABLE test_temp_table (value UInt8)");
        await connection.ExecuteScalarAsync("SELECT COUNT(*) from test_temp_table");
    }

    [Test]
    public async Task TempTableShouldFailIfSessionDisabled()
    {
        using var connection = CreateConnection(false);
        try
        {
            await connection.ExecuteStatementAsync("CREATE TEMPORARY TABLE test_temp_table (value UInt8)");
            await connection.ExecuteScalarAsync("SELECT COUNT(*) from test_temp_table");
            Assert.Fail("ClickHouse should not be able to use temp table if session is disabled");
        }
        catch (ClickHouseServerException e) when (e.ErrorCode == 60) // Error 60 means the table does not exist
        {
        }
    }

    [Test]
    public async Task TempTableShouldFailIfSessionDisabledAndSessionIdPassed()
    {
        using var connection = CreateConnection(false, "ASD");
        try
        {
            await connection.ExecuteStatementAsync("CREATE TEMPORARY TABLE test_temp_table (value UInt8)");
            await connection.ExecuteScalarAsync("SELECT COUNT(*) from test_temp_table");
            Assert.Fail("ClickHouse should not be able to use temp table if session is disabled");
        }
        catch (ClickHouseServerException e) when (e.ErrorCode == 60) // Error 60 means the table does not exist
        {
        }
    }

    [Test]
    public async Task Session_WithCustomHttpClient_ShouldWork()
    {
        var sessionId = "TEST-" + Guid.NewGuid().ToString();
        var handler = new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
        };
        
        using var httpClient = new HttpClient(handler);

        using var connection = CreateConnectionWithHttpClient(httpClient, useSession: true, sessionId);
        await connection.ExecuteStatementAsync("CREATE TEMPORARY TABLE test_temp_table (value UInt8)");
        await connection.ExecuteScalarAsync("SELECT COUNT(*) from test_temp_table");
    }

    [Test]
    public async Task Session_ConcurrentRequests_AreSerialized()
    {
        var sessionId = "TEST-" + Guid.NewGuid();
        var marker = Guid.NewGuid().ToString("N");

        using var connection = (ClickHouseConnection)CreateConnection(useSession: true, sessionId);

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Two 300ms sleep queries with markers we can find in query_log
        var task1 = connection.ExecuteScalarAsync($"SELECT sleep(0.3), 'marker1_{marker}'");
        var task2 = connection.ExecuteScalarAsync($"SELECT sleep(0.3), 'marker2_{marker}'");

        await Task.WhenAll(task1, task2);
        stopwatch.Stop();

        // Quick sanity check: should take >600ms if serialized
        Assert.That(stopwatch.ElapsedMilliseconds, Is.GreaterThan(600),
            $"Requests should be serialized. Expected >600ms but took {stopwatch.ElapsedMilliseconds}ms");
    }
}
