using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Tests.Utilities;

namespace ClickHouse.Driver.Tests;

[TestFixture]
public class ClickHouseClientQueryOptionsTests : AbstractConnectionTestFixture
{
    private string CreateTestTableName([CallerMemberName] string testName = null)
        => SanitizeTableName($"test_opts_{testName}_{Guid.NewGuid():N}");

    private async Task<string> CreateSimpleTestTableAsync([CallerMemberName] string testName = null)
    {
        var tableName = $"test.{CreateTestTableName(testName)}";
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (id UInt64, value String)
            ENGINE = MergeTree() ORDER BY id");
        return tableName;
    }

    private async Task<string> CreateTableWithDefaultsAsync([CallerMemberName] string testName = null)
    {
        var tableName = $"test.{CreateTestTableName(testName)}";
        await client.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName}
            (
                id UInt64,
                name String,
                created_at DateTime DEFAULT now(),
                value Float32 DEFAULT 42.5
            )
            ENGINE = MergeTree() ORDER BY id");
        return tableName;
    }

    private static IEnumerable<object[]> GenerateTestRows(int count, ulong startId = 1)
    {
        for (ulong i = 0; i < (ulong)count; i++)
            yield return new object[] { startId + i, $"Value_{startId + i}" };
    }

    private (ClickHouseClient client, TrackingHandler handler) CreateClientWithTracking()
    {
        var handler = new TrackingHandler(new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
        });
        var httpClient = new HttpClient(handler);
        var builder = TestUtilities.GetConnectionStringBuilder();
        var settings = new ClickHouseClientSettings(builder)
        {
            HttpClient = httpClient,
        };
        return (new ClickHouseClient(settings), handler);
    }

    [Test]
    public async Task ExecuteScalarAsync_WithCustomQueryId_QueryIdAppearsInSystemQueryLog()
    {
        var customQueryId = $"test_query_id_{Guid.NewGuid():N}";
        var options = new QueryOptions { QueryId = customQueryId };

        await client.ExecuteScalarAsync("SELECT 1", options: options);

        // Wait for query_log flush
        await Task.Delay(500);
        await client.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");

        var count = await client.ExecuteScalarAsync(
            $"SELECT count() FROM system.query_log WHERE query_id = '{customQueryId}'");
        Assert.That(count, Is.GreaterThan(0UL));
    }

    [Test]
    public async Task ExecuteScalarAsync_WithoutQueryId_AutoGeneratesGuid()
    {
        // Use a unique marker to find this specific query
        var marker = $"auto_guid_test_{Guid.NewGuid():N}";
        await client.ExecuteScalarAsync($"SELECT 42 /* {marker} */");

        await client.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");

        // Find the query in system.query_log - should have a valid GUID as query_id
        using var reader = await client.ExecuteReaderAsync(
            $"SELECT query_id FROM system.query_log WHERE query LIKE '%{marker}%' AND type = 'QueryFinish' ORDER BY event_time DESC LIMIT 1");

        Assert.That(reader.Read(), Is.True, "Query should appear in query_log");
        var queryId = reader.GetString(0);
        Assert.That(Guid.TryParse(queryId, out _), Is.True, "Query ID should be a valid GUID");
    }

    [Test]
    public async Task ExecuteReaderAsync_WithQueryId_QueryIdPassedToServer()
    {
        var customQueryId = $"custom_reader_qid_{Guid.NewGuid():N}";
        var options = new QueryOptions { QueryId = customQueryId };

        using var reader = await client.ExecuteReaderAsync("SELECT 1", options: options);
        while (reader.Read()) { } // Consume results

        await client.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");

        var count = await client.ExecuteScalarAsync(
            $"SELECT count() FROM system.query_log WHERE query_id = '{customQueryId}'");
        Assert.That(count, Is.GreaterThan(0UL), "Custom query ID should appear in query_log");
    }

    [Test]
    public async Task ExecuteScalarAsync_WithDatabaseOverride_QueriesSpecifiedDatabase()
    {
        await client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test_secondary");

        var options = new QueryOptions { Database = "test_secondary" };
        var result = await client.ExecuteScalarAsync("SELECT currentDatabase()", options: options);

        Assert.That(result, Is.EqualTo("test_secondary"));
    }

    [Test]
    public async Task ExecuteNonQueryAsync_WithDatabaseOverride_CreatesTableInSpecifiedDatabase()
    {
        await client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test_secondary");
        var tableName = $"test_table_{Guid.NewGuid():N}"[..30];

        try
        {
            var options = new QueryOptions { Database = "test_secondary" };
            await client.ExecuteNonQueryAsync(
                $"CREATE TABLE {tableName} (id UInt64) ENGINE = MergeTree() ORDER BY id",
                options: options);

            // Verify table exists in test_secondary
            var exists = await client.ExecuteScalarAsync(
                $"SELECT count() FROM system.tables WHERE database = 'test_secondary' AND name = '{tableName}'");
            Assert.That(exists, Is.EqualTo(1UL));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test_secondary.{tableName}");
        }
    }

    [Test]
    public void ExecuteScalarAsync_WithNonExistentDatabase_ThrowsServerException()
    {
        var options = new QueryOptions { Database = "nonexistent_database_12345" };

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(async () =>
            await client.ExecuteScalarAsync("SELECT 1", options: options));

        Assert.That(ex!.ErrorCode, Is.EqualTo(81)); // UNKNOWN_DATABASE
    }

    [Test]
    public async Task ExecuteReaderAsync_WithDatabaseOverride_ReturnsDataFromSpecifiedDatabase()
    {
        await client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test_secondary");
        var tableName = $"test_data_{Guid.NewGuid():N}"[..30];

        try
        {
            // Create and populate table in test_secondary
            await client.ExecuteNonQueryAsync(
                $"CREATE TABLE test_secondary.{tableName} (id UInt64, value String) ENGINE = MergeTree() ORDER BY id");
            await client.ExecuteNonQueryAsync(
                $"INSERT INTO test_secondary.{tableName} VALUES (1, 'from_secondary')");

            var options = new QueryOptions { Database = "test_secondary" };
            using var reader = await client.ExecuteReaderAsync(
                $"SELECT value FROM {tableName}", options: options);

            Assert.That(reader.Read(), Is.True);
            Assert.That(reader.GetString(0), Is.EqualTo("from_secondary"));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test_secondary.{tableName}");
        }
    }

    [Test]
    public async Task ExecuteScalarAsync_WithMultipleRoles_CurrentRolesReturnsAllRoles()
    {
        if (TestUtilities.TestEnvironment != TestEnv.LocalSingleNode)
        {
            Assert.Ignore("Requires local_single_node environment with access storage");
        }

        var role1 = $"TEST_ROLE1_{Guid.NewGuid():N}"[..30];
        var role2 = $"TEST_ROLE2_{Guid.NewGuid():N}"[..30];
        var userName = $"test_user_{Guid.NewGuid():N}"[..30];
        var password = $"Pass_{Guid.NewGuid():N}";

        try
        {
            await client.ExecuteNonQueryAsync($"CREATE ROLE IF NOT EXISTS {role1}");
            await client.ExecuteNonQueryAsync($"CREATE ROLE IF NOT EXISTS {role2}");
            await client.ExecuteNonQueryAsync($"CREATE USER IF NOT EXISTS {userName} IDENTIFIED BY '{password}'");
            await client.ExecuteNonQueryAsync($"GRANT {role1}, {role2} TO {userName}");

            var builder = TestUtilities.GetConnectionStringBuilder();
            builder.Username = userName;
            builder.Password = password;
            using var userClient = new ClickHouseClient(builder.ConnectionString);

            var options = new QueryOptions { Roles = new[] { role1, role2 } };
            var result = await userClient.ExecuteScalarAsync("SELECT currentRoles()", options: options);

            Assert.That(result, Is.InstanceOf<string[]>());
            var roles = (string[])result;
            Assert.That(roles, Contains.Item(role1));
            Assert.That(roles, Contains.Item(role2));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP USER IF EXISTS {userName}");
            await client.ExecuteNonQueryAsync($"DROP ROLE IF EXISTS {role1}");
            await client.ExecuteNonQueryAsync($"DROP ROLE IF EXISTS {role2}");
        }
    }

    [Test]
    public async Task ExecuteNonQueryAsync_WithRoleThatCanInsert_InsertSucceeds()
    {
        if (TestUtilities.TestEnvironment != TestEnv.LocalSingleNode)
        {
            Assert.Ignore("Requires local_single_node environment with access storage");
        }

        var roleName = $"INSERT_ROLE_{Guid.NewGuid():N}"[..30];
        var userName = $"test_user_{Guid.NewGuid():N}"[..30];
        var password = $"Pass_{Guid.NewGuid():N}";
        var tableName = await CreateSimpleTestTableAsync();

        try
        {
            await client.ExecuteNonQueryAsync($"CREATE ROLE IF NOT EXISTS {roleName}");
            await client.ExecuteNonQueryAsync($"GRANT INSERT ON {tableName} TO {roleName}");
            await client.ExecuteNonQueryAsync($"CREATE USER IF NOT EXISTS {userName} IDENTIFIED BY '{password}'");
            await client.ExecuteNonQueryAsync($"GRANT {roleName} TO {userName}");

            var builder = TestUtilities.GetConnectionStringBuilder();
            builder.Username = userName;
            builder.Password = password;
            using var userClient = new ClickHouseClient(builder.ConnectionString);

            var options = new QueryOptions { Roles = new[] { roleName } };
            await userClient.ExecuteNonQueryAsync(
                $"INSERT INTO {tableName} VALUES (1, 'test')", options: options);

            var count = await client.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
            Assert.That(count, Is.EqualTo(1UL));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            await client.ExecuteNonQueryAsync($"DROP USER IF EXISTS {userName}");
            await client.ExecuteNonQueryAsync($"DROP ROLE IF EXISTS {roleName}");
        }
    }

    [Test]
    public async Task ExecuteScalarAsync_QueryRolesOverrideConnectionRoles_QueryRolesUsed()
    {
        if (TestUtilities.TestEnvironment != TestEnv.LocalSingleNode)
            Assert.Ignore("Requires local_single_node environment with access storage");

        var connectionRole = $"CONN_ROLE_{Guid.NewGuid():N}"[..30];
        var queryRole = $"QUERY_ROLE_{Guid.NewGuid():N}"[..30];
        var userName = $"test_user_{Guid.NewGuid():N}"[..30];
        var password = $"Pass_{Guid.NewGuid():N}";

        try
        {
            await client.ExecuteNonQueryAsync($"CREATE ROLE IF NOT EXISTS {connectionRole}");
            await client.ExecuteNonQueryAsync($"CREATE ROLE IF NOT EXISTS {queryRole}");
            await client.ExecuteNonQueryAsync($"CREATE USER IF NOT EXISTS {userName} IDENTIFIED BY '{password}'");
            await client.ExecuteNonQueryAsync($"GRANT {connectionRole}, {queryRole} TO {userName}");

            var builder = TestUtilities.GetConnectionStringBuilder();
            builder.Username = userName;
            builder.Password = password;
            var settings = new ClickHouseClientSettings(builder) { Roles = new[] { connectionRole } };
            using var userClient = new ClickHouseClient(settings);

            // Query-level roles should override connection-level
            var options = new QueryOptions { Roles = new[] { queryRole } };
            var result = await userClient.ExecuteScalarAsync("SELECT currentRoles()", options: options);

            var roles = (string[])result;
            Assert.That(roles, Contains.Item(queryRole));
            Assert.That(roles, Does.Not.Contain(connectionRole));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP USER IF EXISTS {userName}");
            await client.ExecuteNonQueryAsync($"DROP ROLE IF EXISTS {connectionRole}");
            await client.ExecuteNonQueryAsync($"DROP ROLE IF EXISTS {queryRole}");
        }
    }

    [Test]
    public async Task ExecuteScalarAsync_WithMaxMemoryUsageSetting_SettingIsApplied()
    {
        var options = new QueryOptions
        {
            CustomSettings = new Dictionary<string, object> { { "max_memory_usage", 1000000000 } }
        };

        var result = await client.ExecuteScalarAsync("SELECT getSetting('max_memory_usage')", options: options);
        Assert.That(result, Is.EqualTo(1000000000UL));
    }

    [Test]
    public async Task ExecuteScalarAsync_WithMultipleSettings_AllSettingsApplied()
    {
        var options = new QueryOptions
        {
            CustomSettings = new Dictionary<string, object>
            {
                { "max_threads", 2 },
                { "max_block_size", 5000 }
            }
        };

        using var reader = await client.ExecuteReaderAsync(
            "SELECT getSetting('max_threads'), getSetting('max_block_size')", options: options);

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetValue(0), Is.EqualTo(2UL));
        Assert.That(reader.GetValue(1), Is.EqualTo(5000UL));
    }

    [Test]
    public async Task ExecuteScalarAsync_QueryCustomSettingsOverrideConnectionSettings_QuerySettingsWin()
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder["set_max_threads"] = 8;
        using var settingsClient = new ClickHouseClient(builder.ConnectionString);

        var options = new QueryOptions
        {
            CustomSettings = new Dictionary<string, object> { { "max_threads", 2 } }
        };

        // Query-level max_threads=2 should override connection-level max_threads=8
        var result = await settingsClient.ExecuteScalarAsync("SELECT getSetting('max_threads')", options: options);
        Assert.That(result, Is.EqualTo(2UL));
    }

    [Test]
    public async Task ExecuteScalarAsync_WithCustomHeader_HeaderIsSent()
    {
        var (trackedClient, handler) = CreateClientWithTracking();
        using (trackedClient)
        {
            var options = new QueryOptions
            {
                CustomHeaders = new Dictionary<string, string> { { "X-Test-Header", "test-value" } }
            };

            await trackedClient.ExecuteScalarAsync("SELECT 1", options: options);

            var request = handler.Requests.Last();
            Assert.That(request.Headers.Contains("X-Test-Header"), Is.True);
            Assert.That(request.Headers.GetValues("X-Test-Header").First(), Is.EqualTo("test-value"));
        }
    }

    [Test]
    public async Task ExecuteScalarAsync_WithBlockedHeader_HeaderIsIgnored()
    {
        var (trackedClient, handler) = CreateClientWithTracking();
        using (trackedClient)
        {
            var options = new QueryOptions
            {
                CustomHeaders = new Dictionary<string, string> { { "Authorization", "Bearer evil-token" } }
            };

            await trackedClient.ExecuteScalarAsync("SELECT 1", options: options);

            var request = handler.Requests.Last();
            // Authorization should be Basic (default), not Bearer
            Assert.That(request.Headers.Authorization?.Scheme, Is.EqualTo("Basic"));
        }
    }

    [Test]
    public async Task ExecuteScalarAsync_WithMultipleCustomHeaders_AllHeadersSent()
    {
        var (trackedClient, handler) = CreateClientWithTracking();
        using (trackedClient)
        {
            var options = new QueryOptions
            {
                CustomHeaders = new Dictionary<string, string>
                {
                    { "X-Header-One", "value1" },
                    { "X-Header-Two", "value2" },
                    { "X-Header-Three", "value3" }
                }
            };

            await trackedClient.ExecuteScalarAsync("SELECT 1", options: options);

            var request = handler.Requests.Last();
            Assert.That(request.Headers.GetValues("X-Header-One").First(), Is.EqualTo("value1"));
            Assert.That(request.Headers.GetValues("X-Header-Two").First(), Is.EqualTo("value2"));
            Assert.That(request.Headers.GetValues("X-Header-Three").First(), Is.EqualTo("value3"));
        }
    }

    [Test]
    public async Task ExecuteScalarAsync_CustomHeaderOverridesConnectionLevel_QueryHeaderWins()
    {
        var handler = new TrackingHandler(new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
        });
        var httpClient = new HttpClient(handler);
        var builder = TestUtilities.GetConnectionStringBuilder();
        var settings = new ClickHouseClientSettings(builder)
        {
            HttpClient = httpClient,
            CustomHeaders = new Dictionary<string, string> { { "X-Custom", "connection-level" } }
        };
        using var trackedClient = new ClickHouseClient(settings);

        var options = new QueryOptions
        {
            CustomHeaders = new Dictionary<string, string> { { "X-Custom", "query-level" } }
        };

        await trackedClient.ExecuteScalarAsync("SELECT 1", options: options);

        var request = handler.Requests.Last();
        Assert.That(request.Headers.GetValues("X-Custom").First(), Is.EqualTo("query-level"));
    }

    [Test]
    public async Task ExecuteNonQueryAsync_WithUseSessionTrue_TempTablePersistsAcrossQueries()
    {
        var sessionId = $"test_session_{Guid.NewGuid():N}";
        var options = new QueryOptions
        {
            UseSession = true,
            SessionId = sessionId
        };

        await client.ExecuteNonQueryAsync(
            "CREATE TEMPORARY TABLE temp_test_persist (id UInt8)", options: options);

        // Should be accessible with same session
        var count = await client.ExecuteScalarAsync(
            "SELECT count() FROM temp_test_persist", options: options);
        Assert.That(count, Is.EqualTo(0UL));
    }

    [Test]
    public async Task ExecuteNonQueryAsync_WithUseSessionFalse_TempTableNotAccessible()
    {
        // First create a temp table with session disabled
        var options = new QueryOptions { UseSession = false };

        await client.ExecuteNonQueryAsync(
            "CREATE TEMPORARY TABLE temp_test_nosession (id UInt8)", options: options);

        // Without session, temp table not accessible in next query
        var ex = Assert.ThrowsAsync<ClickHouseServerException>(async () =>
            await client.ExecuteScalarAsync("SELECT count() FROM temp_test_nosession", options: options));

        Assert.That(ex!.ErrorCode, Is.EqualTo(60)); // UNKNOWN_TABLE
    }

    [Test]
    public async Task ExecuteScalarAsync_WithSameSessionId_SharesSessionState()
    {
        var sessionId = $"shared_session_{Guid.NewGuid():N}";
        var options = new QueryOptions
        {
            UseSession = true,
            SessionId = sessionId
        };

        // Set a session variable
        await client.ExecuteNonQueryAsync("SET max_threads = 3", options: options);

        // Verify it's accessible in the same session
        var result = await client.ExecuteScalarAsync("SELECT getSetting('max_threads')", options: options);
        Assert.That(result, Is.EqualTo(3UL));
    }

    [Test]
    public async Task ExecuteScalarAsync_WithBearerToken_AuthorizationHeaderSet()
    {
        var (trackedClient, handler) = CreateClientWithTracking();
        using (trackedClient)
        {
            var options = new QueryOptions { BearerToken = "test_bearer_token_123" };

            // This will fail auth, but we can verify the header was set
            try
            {
                await trackedClient.ExecuteScalarAsync("SELECT 1", options: options);
            }
            catch (ClickHouseServerException)
            {
                // Expected - invalid token
            }

            var request = handler.Requests.Last();
            Assert.That(request.Headers.Authorization?.Scheme, Is.EqualTo("Bearer"));
            Assert.That(request.Headers.Authorization?.Parameter, Is.EqualTo("test_bearer_token_123"));
        }
    }

    [Test]
    public async Task ExecuteScalarAsync_BearerTokenOverridesBasicAuth_BearerUsed()
    {
        var handler = new TrackingHandler(new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
        });
        var httpClient = new HttpClient(handler);
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.Username = "some_user";
        builder.Password = "some_password";
        var settings = new ClickHouseClientSettings(builder)
        {
            HttpClient = httpClient,
        };
        using var trackedClient = new ClickHouseClient(settings);

        var options = new QueryOptions { BearerToken = "my_jwt_token" };

        try
        {
            await trackedClient.ExecuteScalarAsync("SELECT 1", options: options);
        }
        catch (ClickHouseServerException)
        {
            // Expected - invalid token
        }

        var request = handler.Requests.Last();
        // Should use Bearer, not Basic
        Assert.That(request.Headers.Authorization?.Scheme, Is.EqualTo("Bearer"));
        Assert.That(request.Headers.Authorization?.Parameter, Is.EqualTo("my_jwt_token"));
    }

    [Test]
    public async Task ExecuteScalarAsync_WithNullBearerToken_UsesClientLevelBearerToken()
    {
        var handler = new TrackingHandler(new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
        });
        var httpClient = new HttpClient(handler);
        var builder = TestUtilities.GetConnectionStringBuilder();
        var settings = new ClickHouseClientSettings(builder)
        {
            HttpClient = httpClient,
            BearerToken = "client_level_token"
        };
        using var trackedClient = new ClickHouseClient(settings);

        // QueryOptions with null BearerToken should fall back to client-level
        var options = new QueryOptions { BearerToken = null };

        try
        {
            await trackedClient.ExecuteScalarAsync("SELECT 1", options: options);
        }
        catch (ClickHouseServerException)
        {
            // Expected - invalid token
        }

        var request = handler.Requests.Last();
        Assert.That(request.Headers.Authorization?.Scheme, Is.EqualTo("Bearer"));
        Assert.That(request.Headers.Authorization?.Parameter, Is.EqualTo("client_level_token"));
    }

    [Test]
    public async Task ExecuteScalarAsync_WithNullCustomHeaders_UsesClientLevelHeaders()
    {
        var handler = new TrackingHandler(new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
        });
        var httpClient = new HttpClient(handler);
        var builder = TestUtilities.GetConnectionStringBuilder();
        var settings = new ClickHouseClientSettings(builder)
        {
            HttpClient = httpClient,
            CustomHeaders = new Dictionary<string, string> { { "X-Client-Header", "client-value" } }
        };
        using var trackedClient = new ClickHouseClient(settings);

        // QueryOptions with null CustomHeaders should preserve client-level headers
        var options = new QueryOptions { CustomHeaders = null };

        await trackedClient.ExecuteScalarAsync("SELECT 1", options: options);

        var request = handler.Requests.Last();
        Assert.That(request.Headers.Contains("X-Client-Header"), Is.True);
        Assert.That(request.Headers.GetValues("X-Client-Header").First(), Is.EqualTo("client-value"));
    }

    [Test]
    public async Task ExecuteScalarAsync_WithNullRoles_UsesClientLevelRoles()
    {
        if (TestUtilities.TestEnvironment != TestEnv.LocalSingleNode)
            Assert.Ignore("Requires local_single_node environment with access storage");

        var clientRole = $"CLIENT_ROLE_{Guid.NewGuid():N}"[..30];
        var userName = $"test_user_{Guid.NewGuid():N}"[..30];
        var password = $"Pass_{Guid.NewGuid():N}";

        try
        {
            await client.ExecuteNonQueryAsync($"CREATE ROLE IF NOT EXISTS {clientRole}");
            await client.ExecuteNonQueryAsync($"CREATE USER IF NOT EXISTS {userName} IDENTIFIED BY '{password}'");
            await client.ExecuteNonQueryAsync($"GRANT {clientRole} TO {userName}");

            var builder = TestUtilities.GetConnectionStringBuilder();
            builder.Username = userName;
            builder.Password = password;
            var settings = new ClickHouseClientSettings(builder) { Roles = new[] { clientRole } };
            using var userClient = new ClickHouseClient(settings);

            // Null roles in QueryOptions should use client-level roles
            var options = new QueryOptions { Roles = null };
            var result = await userClient.ExecuteScalarAsync("SELECT currentRoles()", options: options);

            var roles = (string[])result;
            Assert.That(roles, Contains.Item(clientRole));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP USER IF EXISTS {userName}");
            await client.ExecuteNonQueryAsync($"DROP ROLE IF EXISTS {clientRole}");
        }
    }

    [Test]
    public async Task ExecuteScalarAsync_WithNullCustomSettings_UsesClientLevelSettings()
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder["set_max_threads"] = 4;
        using var settingsClient = new ClickHouseClient(builder.ConnectionString);

        // Null CustomSettings should preserve client-level settings
        var options = new QueryOptions { CustomSettings = null };

        var result = await settingsClient.ExecuteScalarAsync("SELECT getSetting('max_threads')", options: options);
        Assert.That(result, Is.EqualTo(4UL));
    }

    [Test]
    public async Task ExecuteScalarAsync_WithMaxExecutionTime_SettingIsApplied()
    {
        var options = new QueryOptions { MaxExecutionTime = TimeSpan.FromSeconds(30) };

        var result = await client.ExecuteScalarAsync("SELECT getSetting('max_execution_time')", options: options);
        Assert.That(result, Is.EqualTo(30UL));
    }

    [Test]
    public void ExecuteScalarAsync_WithMaxExecutionTime_LongQueryTimesOut()
    {
        var options = new QueryOptions { MaxExecutionTime = TimeSpan.FromSeconds(1) };

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(async () =>
            await client.ExecuteScalarAsync("SELECT sleep(3)", options: options));

        // TIMEOUT_EXCEEDED = 159
        Assert.That(ex!.ErrorCode, Is.EqualTo(159));
    }

    [Test]
    public async Task InsertBinaryAsync_WithSmallBatchSize_InsertsInMultipleBatches()
    {
        var tableName = await CreateSimpleTestTableAsync();
        try
        {
            var options = new InsertOptions { BatchSize = 10 };
            var rows = GenerateTestRows(100).ToList();

            await client.InsertBinaryAsync(tableName, new[] { "id", "value" }, rows, options);

            // All rows should be inserted regardless of batch size
            var count = await client.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
            Assert.That(count, Is.EqualTo(100UL));

            // Verify data integrity
            var distinctCount = await client.ExecuteScalarAsync($"SELECT count(DISTINCT id) FROM {tableName}");
            Assert.That(distinctCount, Is.EqualTo(100UL));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void InsertBinaryAsync_WithInvalidBatchSize_ThrowsArgumentOutOfRangeException(int batchSize)
    {
        var options = new InsertOptions { BatchSize = batchSize };
        var rows = GenerateTestRows(10).ToList();

        var ex = Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await client.InsertBinaryAsync("test.dummy", new[] { "id", "value" }, rows, options));

        Assert.That(ex!.ParamName, Is.EqualTo("options"));
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void InsertBinaryAsync_WithInvalidParallelism_ThrowsArgumentOutOfRangeException(int parallelism)
    {
        var options = new InsertOptions { MaxDegreeOfParallelism = parallelism };
        var rows = GenerateTestRows(10).ToList();

        var ex = Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await client.InsertBinaryAsync("test.dummy", new[] { "id", "value" }, rows, options));

        Assert.That(ex!.ParamName, Is.EqualTo("options"));
    }

    [Test]
    public async Task InsertBinaryAsync_WithBatchesExceedingParallelism_ReturnsCorrectRowCount()
    {
        var tableName = await CreateSimpleTestTableAsync();
        try
        {
            var options = new InsertOptions
            {
                BatchSize = 10,
                MaxDegreeOfParallelism = 2
            };
            var rows = GenerateTestRows(50).ToList();

            var inserted = await client.InsertBinaryAsync(
                tableName, new[] { "id", "value" }, rows, options);

            // 50 rows in 5 batches with parallelism 2: must return 50, not 20
            Assert.That(inserted, Is.EqualTo(50));

            var count = await client.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
            Assert.That(count, Is.EqualTo(50UL));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithRowBinaryFormat_InsertsSuccessfully()
    {
        var tableName = await CreateSimpleTestTableAsync();
        try
        {
            var options = new InsertOptions { Format = RowBinaryFormat.RowBinary };
            var rows = GenerateTestRows(50).ToList();

            var inserted = await client.InsertBinaryAsync(
                tableName, new[] { "id", "value" }, rows, options);

            Assert.That(inserted, Is.EqualTo(50));

            var count = await client.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
            Assert.That(count, Is.EqualTo(50UL));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithRowBinaryWithDefaultsFormat_InsertsWithDefaults()
    {
        var tableName = await CreateTableWithDefaultsAsync();
        try
        {
            var options = new InsertOptions { Format = RowBinaryFormat.RowBinaryWithDefaults };
            var rows = new List<object[]>
            {
                new object[] { 1UL, "Name1", DateTime.UtcNow, 1.5f },
                new object[] { 2UL, "Name2", DateTime.UtcNow, 2.5f },
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, new[] { "id", "name", "created_at", "value" }, rows, options);

            Assert.That(inserted, Is.EqualTo(2));

            var count = await client.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
            Assert.That(count, Is.EqualTo(2UL));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithRowBinaryWithDefaultsFormat_OmittedColumnsUseDefaults()
    {
        var tableName = await CreateTableWithDefaultsAsync();
        try
        {
            var options = new InsertOptions { Format = RowBinaryFormat.RowBinaryWithDefaults };
            var rows = new List<object[]>
            {
                new object[] { 1UL, "Name1" },
                new object[] { 2UL, "Name2" },
            };

            // Insert only id and name columns - created_at and value should use defaults
            var inserted = await client.InsertBinaryAsync(
                tableName, new[] { "id", "name" }, rows, options);

            Assert.That(inserted, Is.EqualTo(2));

            // Verify default values were used
            using var reader = await client.ExecuteReaderAsync(
                $"SELECT id, name, value FROM {tableName} ORDER BY id");

            reader.Read();
            Assert.That(reader.GetValue(0), Is.EqualTo(1UL));
            Assert.That(reader.GetString(1), Is.EqualTo("Name1"));
            Assert.That(reader.GetFloat(2), Is.EqualTo(42.5f)); // Default value

            reader.Read();
            Assert.That(reader.GetValue(0), Is.EqualTo(2UL));
            Assert.That(reader.GetString(1), Is.EqualTo("Name2"));
            Assert.That(reader.GetFloat(2), Is.EqualTo(42.5f)); // Default value
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithDatabaseOverride_InsertsToSpecifiedDatabase()
    {
        await client.ExecuteNonQueryAsync("CREATE DATABASE IF NOT EXISTS test_secondary");
        var tableName = $"insert_test_{Guid.NewGuid():N}"[..30];

        try
        {
            await client.ExecuteNonQueryAsync(
                $"CREATE TABLE test_secondary.{tableName} (id UInt64, value String) ENGINE = MergeTree() ORDER BY id");

            var options = new InsertOptions { Database = "test_secondary" };
            var rows = GenerateTestRows(10).ToList();

            var inserted = await client.InsertBinaryAsync(
                tableName, new[] { "id", "value" }, rows, options);

            Assert.That(inserted, Is.EqualTo(10));

            var count = await client.ExecuteScalarAsync($"SELECT count() FROM test_secondary.{tableName}");
            Assert.That(count, Is.EqualTo(10UL));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test_secondary.{tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithQueryId_QueryIdApplied()
    {
        var tableName = await CreateSimpleTestTableAsync();
        try
        {
            var customQueryId = $"insert_qid_{Guid.NewGuid():N}";
            var options = new InsertOptions { QueryId = customQueryId };
            var rows = GenerateTestRows(10).ToList();

            await client.InsertBinaryAsync(tableName, new[] { "id", "value" }, rows, options);

            await client.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");

            var count = await client.ExecuteScalarAsync(
                $"SELECT count() FROM system.query_log WHERE query_id = '{customQueryId}'");
            Assert.That(count, Is.GreaterThan(0UL), "Custom query ID should appear in query_log");
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Test]
    public async Task InsertBinaryAsync_WithCustomSettings_SettingsApplied()
    {
        var tableName = await CreateSimpleTestTableAsync();
        try
        {
            var options = new InsertOptions
            {
                CustomSettings = new Dictionary<string, object>
                {
                    { "insert_quorum", 0 }
                }
            };
            var rows = GenerateTestRows(10).ToList();

            // Should succeed with the custom setting
            var inserted = await client.InsertBinaryAsync(
                tableName, new[] { "id", "value" }, rows, options);

            Assert.That(inserted, Is.EqualTo(10));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }
}
