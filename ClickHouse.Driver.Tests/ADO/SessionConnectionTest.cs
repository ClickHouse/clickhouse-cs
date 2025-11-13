using System;
using System.Data.Common;
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
}
