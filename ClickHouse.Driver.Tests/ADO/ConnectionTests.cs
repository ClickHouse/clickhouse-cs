using System;
using System.Data;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Utilities;
using ClickHouse.Driver.Utility;
using Dapper;
using NSubstitute;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

[Category("Cloud")]
public class ConnectionTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task ShouldCreateConnectionWithProvidedHttpClient()
    {
        using var httpClientHandler = new HttpClientHandler() { AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate };
        using var httpClient = new HttpClient(httpClientHandler);
        using var conn = new ClickHouseConnection(TestUtilities.GetConnectionStringBuilder().ToString(), httpClient);
        await conn.OpenAsync();
        ClassicAssert.IsNotEmpty(conn.ServerVersion);
    }

    [Test]
    public void ShouldThrowExceptionOnInvalidHttpClient()
    {
        using var httpClient = new HttpClient(); // No decompression handler
        using var conn = new ClickHouseConnection(TestUtilities.GetConnectionStringBuilder().ToString(), httpClient);
        Assert.Throws<InvalidOperationException>(() => conn.Open());
    }

    [Test]
    public async Task ShouldCreateConnectionWithSkipServerCertificateValidation()
    {
        var connectionString = TestUtilities.GetConnectionStringBuilder().ToString();
        using var conn = new ClickHouseConnection(connectionString, skipServerCertificateValidation: true);
        await conn.OpenAsync();

        Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));
        Assert.That(conn.SkipServerCertificateValidation, Is.True);
    }

    [Test]
    public async Task ShouldBeAbleToSetConnectionStringAfterCreation() // Necessary to support this for some scenarios involving ClickHouseConnectionFactory
    {
        var conn = new ClickHouseConnection();
        conn.ConnectionString = TestUtilities.GetConnectionStringBuilder().ToString();
        await conn.OpenAsync();

        Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));
    }

    [Test]
    public async Task ShouldNotBeAbleToSetConnectionStringWhileOpen()
    {
        using var conn = new ClickHouseConnection(TestUtilities.GetConnectionStringBuilder().ToString());
        await conn.OpenAsync();

        Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));
        Assert.Throws<InvalidOperationException>(() => conn.ConnectionString = "Host=otherhost");
    }

    [Test]
    public void ShouldParseCustomParameter()
    {
        using var conn = new ClickHouseConnection("set_my_parameter=aaa");
        Assert.That(conn.CustomSettings["my_parameter"], Is.EqualTo("aaa"));
    }

    [Test]
    public void ShouldEmitCustomParameter()
    {
        using var conn = new ClickHouseConnection();
        conn.CustomSettings.Add("my_parameter", "aaa");
        Assert.That(conn.ConnectionString, Contains.Substring("set_my_parameter=aaa"));
    }

    [Test]
    public void ShouldConnectToServer()
    {
        using var conn = TestUtilities.GetTestClickHouseConnection();
        ClassicAssert.IsNotEmpty(conn.ServerVersion);
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));
        conn.Close();
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));
    }

    [Test]
    [TestCase("1.2.3.4.altinity")]
    [TestCase("1.2.3.4")]
    [TestCase("20")]
    [TestCase("20.1")]
    [TestCase("20.1.2")]
    public void ShoulParseVersion(string version) => _ = ClickHouseConnection.ParseVersion(version);

    [Test]
    public async Task TimeoutShouldCancelConnection()
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.UseSession = false;
        builder.Compression = true;
        builder.Timeout = TimeSpan.FromMilliseconds(5);
        var conn = new ClickHouseConnection(builder.ToString());
        try
        {
            var task = conn.ExecuteScalarAsync("SELECT sleep(1)");
            _ = await task;
            Assert.Fail("The task should have been cancelled before completion");
        }
        catch (TaskCanceledException)
        {
            /* Expected: task cancelled */
        }
    }

    [Test]
    public async Task ServerShouldSetQueryId()
    {
        var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";
        await command.ExecuteScalarAsync();
        ClassicAssert.IsFalse(string.IsNullOrWhiteSpace(command.QueryId));
    }

    [Test]
    public async Task ClientShouldSetQueryId()
    {
        string queryId = "MyQueryId123456";
        var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";
        command.QueryId = queryId;
        await command.ExecuteScalarAsync();
        Assert.That(command.QueryId, Is.EqualTo(queryId));
    }

    [Test]
    public async Task ClientShouldSetUserAgent()
    {
        var headers = new HttpRequestMessage().Headers;
        connection.AddDefaultHttpHeaders(headers);
        // Build assembly version defaults to 1.0.0
        Assert.That(headers.UserAgent.ToString().Contains("ClickHouse.Driver/1.0.0"), Is.True);
    }

    [Test]
    [Explicit("This test takes 3s, and can be flaky on loaded server")]
    public async Task ReplaceRunningQuerySettingShouldReplace()
    {
        connection.CustomSettings.Add("replace_running_query", 1);
        string queryId = "MyQueryId123456";

        var command1 = connection.CreateCommand();
        var command2 = connection.CreateCommand();

        command1.CommandText = "SELECT sleep(3) FROM system.numbers LIMIT 100";
        command2.CommandText = "SELECT 1";

        command1.QueryId = queryId;
        command2.QueryId = queryId;

        var asyncResult1 = command1.ExecuteScalarAsync();
        var asyncResult2 = command2.ExecuteScalarAsync();

        try
        {
            await asyncResult1;
            Assert.Fail("Query was not cancelled as expected");
        }
        catch (ClickHouseServerException ex) when (ex.ErrorCode == 394)
        {
            // Expected exception as next query replaced this one
        }
        await asyncResult2;
    }

    [Test]
    [Ignore("TODO")]
    public void ShouldFetchSchema()
    {
        var schema = connection.GetSchema();
        ClassicAssert.NotNull(schema);
    }

    [Test]
    [Ignore("TODO")]
    public void ShouldFetchSchemaTables()
    {
        var schema = connection.GetSchema("Tables");
        ClassicAssert.NotNull(schema);
    }

    [Test]
    public void ShouldFetchSchemaDatabaseColumns()
    {
        var schema = connection.GetSchema("Columns", ["system"]);
        ClassicAssert.NotNull(schema);
        Assert.That(new[] { "Database", "Table", "DataType", "ProviderType" }, Is.SubsetOf(GetColumnNames(schema)));
    }

    [Test]
    public void ShouldFetchSchemaTableColumns()
    {
        var schema = connection.GetSchema("Columns", ["system", "functions"]);
        ClassicAssert.NotNull(schema);
        Assert.That(new[] { "Database", "Table", "DataType", "ProviderType" }, Is.SubsetOf(GetColumnNames(schema)));
    }

    [Test]
    public void ChangeDatabaseShouldChangeDatabase()
    {
        // Using separate connection instance here to avoid conflicting with other tests
        using var conn = TestUtilities.GetTestClickHouseConnection();
        conn.ChangeDatabase("system");
        Assert.That(conn.Database, Is.EqualTo("system"));
        conn.ChangeDatabase("default");
        Assert.That(conn.Database, Is.EqualTo("default"));
    }

    [Test]
    public void ShouldExcludePasswordFromRedactedConnectionString()
    {
        const string MOCK = "verysecurepassword";
        var settings = new ClickHouseClientSettings()
        {
            Password = MOCK,
        };
        using var conn = new ClickHouseConnection(settings);
        Assert.Multiple(() =>
        {
            Assert.That(conn.ConnectionString, Contains.Substring($"Password={MOCK}"));
            Assert.That(conn.RedactedConnectionString, Is.Not.Contains($"Password={MOCK}"));
        });
    }

    [Test]
    [TestCase("https")]
    [TestCase("http")]
    public void ShouldSaveProtocolAtConnectionString(string protocol)
    {
        string protocolPart = $"Protocol={protocol}";
        string connString = new ClickHouseConnectionStringBuilder(protocolPart).ToString();
        using var conn = new ClickHouseConnection(connString);
        Assert.That(conn.ConnectionString, Contains.Substring(protocolPart));
    }

    [Test]
    public async Task ShouldPostDynamicallyGeneratedRawStream()
    {
        var targetTable = "test.raw_stream";

        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value Int32) ENGINE Null");
        await connection.PostStreamAsync($"INSERT INTO {targetTable} FORMAT CSV", async (stream, ct) =>
        {

            foreach (var i in Enumerable.Range(1, 1000))
            {
                var line = $"{i}\n";
                var bytes = Encoding.UTF8.GetBytes(line);
                await stream.WriteAsync(bytes, 0, bytes.Length, ct);
            }
        }, false, CancellationToken.None);
    }

    [Test]
    public async Task ShouldUseQueryIdForRawStream()
    {
        var queryId = Guid.NewGuid().ToString();
        var httpResponseMessage = await connection.PostStreamAsync("SELECT version()", (_, _) => Task.CompletedTask, false, CancellationToken.None, queryId);
        
        Assert.That(ClickHouseConnection.ExtractQueryId(httpResponseMessage), Is.EqualTo(queryId));
    }

    private static string[] GetColumnNames(DataTable table) => table.Columns.Cast<DataColumn>().Select(dc => dc.ColumnName).ToArray();

    [Test]
    public void Constructor_WithValidSettings_ShouldCreateConnection()
    {
        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            Port = 8123,
            Protocol = "http",
            Database = "default",
            Username = "default",
            Password = ""
        };

        using var conn = new ClickHouseConnection(settings);

        Assert.That(conn, Is.Not.Null);
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));
    }

    [Test]
    public void Constructor_WithNullSettings_ShouldThrowArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new ClickHouseConnection((ClickHouseClientSettings)null));
    }

    [Test]
    public void Constructor_WithInvalidSettings_ShouldThrowInvalidOperationException()
    {
        var settings = new ClickHouseClientSettings
        {
            Host = "" // Invalid: empty host
        };

        Assert.Throws<InvalidOperationException>(() => new ClickHouseConnection(settings));
    }

    [Test]
    public void Constructor_WithSettings_ShouldApplyAllProperties()
    {
        var settings = new ClickHouseClientSettings
        {
            Host = "testhost",
            Port = 9000,
            Protocol = "https",
            Database = "testdb",
            Username = "testuser",
            Password = "testpass",
            Path = "/custom",
            UseCompression = false,
            UseServerTimezone = false,
            UseCustomDecimals = false,
            Timeout = TimeSpan.FromMinutes(5)
        };

        using var conn = new ClickHouseConnection(settings);

        // Verify settings were applied by checking connection string
        var connString = conn.ConnectionString;
        Assert.Multiple(() =>
        {
            Assert.That(connString, Does.Contain("Host=testhost"));
            Assert.That(connString, Does.Contain("Port=9000"));
            Assert.That(connString, Does.Contain("Protocol=https"));
            Assert.That(connString, Does.Contain("Database=testdb"));
            Assert.That(connString, Does.Contain("Username=testuser"));
            Assert.That(connString, Does.Contain("Password=testpass"));
            Assert.That(connString, Does.Contain("Path=/custom"));
            Assert.That(connString, Does.Contain("Compression=False"));
            Assert.That(connString, Does.Contain("UseServerTimezone=False"));
            Assert.That(connString, Does.Contain("UseCustomDecimals=False"));
            Assert.That(connString, Does.Contain("Timeout=300"));
        });
    }

    [Test]
    public void Constructor_WithSettingsWithCustomSettings_ShouldApplyCustomSettings()
    {
        var settings = new ClickHouseClientSettings
        {
            Host = "localhost"
        };
        settings.CustomSettings["max_threads"] = 4;
        settings.CustomSettings["readonly"] = 1;

        using var conn = new ClickHouseConnection(settings);

        Assert.Multiple(() =>
        {
            Assert.That(conn.CustomSettings, Is.Not.Null);
            Assert.That(conn.CustomSettings.Count, Is.EqualTo(2));
            Assert.That(conn.CustomSettings["max_threads"], Is.EqualTo(4));
            Assert.That(conn.CustomSettings["readonly"], Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Constructor_WithSettingsWithHttpClient_ShouldUseProvidedHttpClient()
    {
        // Use a tracking handler to verify our HttpClient is actually used
        var trackingHandler = new TrackingHandler(new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
        });
        using var httpClient = new HttpClient(trackingHandler);

        var builder = TestUtilities.GetConnectionStringBuilder();
        var settings = new ClickHouseClientSettings(builder)
        {
            HttpClient = httpClient,
        };

        using var conn = new ClickHouseConnection(settings);

        // Open connection - should use the provided HttpClient
        await conn.OpenAsync();

        Assert.That(trackingHandler.RequestCount, Is.GreaterThan(0), "HttpClient should have been used");
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));
    }

    [Test]
    public async Task Constructor_WithSettingsWithHttpClientFactory_ShouldUseProvidedFactory()
    {
        var factory = new TestHttpClientFactory();

        var builder = TestUtilities.GetConnectionStringBuilder();
        var settings = new ClickHouseClientSettings(builder)
        {
            HttpClientFactory = factory,
            HttpClientName = "test-client",
        };

        using var conn = new ClickHouseConnection(settings);

        // Verify factory not called yet
        Assert.That(factory.CreateClientCallCount, Is.EqualTo(0));

        // Open connection - should use the provided factory
        await conn.OpenAsync();

        // Verify the provided factory was actually used
        Assert.Multiple(() =>
        {
            Assert.That(factory.CreateClientCallCount, Is.GreaterThan(0), "Provided HttpClientFactory was not used");
            Assert.That(factory.LastRequestedClientName, Is.EqualTo("test-client"));
            Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));
        });
    }

    [Test]
    public void Constructor_WithSettingsWithBothHttpClientAndFactory_ShouldThrow()
    {
        using var httpClient = new HttpClient();
        var factory = new TestHttpClientFactory();

        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            HttpClient = httpClient,
            HttpClientFactory = factory
        };

        Assert.Throws<InvalidOperationException>(() => new ClickHouseConnection(settings));
    }

    [Test]
    public async Task Constructor_WithSettings_ShouldConnectToServer()
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        var settings = builder.ToSettings();

        using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        Assert.Multiple(() =>
        {
            Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));
            Assert.That(conn.ServerVersion, Is.Not.Empty);
        });
    }

    [Test]
    public async Task Constructor_WithSettingsWithTimeout_ShouldRespectTimeout()
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.UseSession = false;
        builder.Compression = true;
        builder.Timeout = TimeSpan.FromMilliseconds(5);

        var settings = builder.ToSettings();
        using var conn = new ClickHouseConnection(settings);

        try
        {
            var task = conn.ExecuteScalarAsync("SELECT sleep(2)");
            _ = await task;
            Assert.Fail("The task should have been cancelled before completion");
        }
        catch (TaskCanceledException)
        {
            /* Expected: task cancelled */
        }
    }

    [Test]
    public void Constructor_WithSettingsFromConnectionString_ShouldMatchDirectConnectionString()
    {
        var connectionString = "Host=myhost;Port=9000;Database=mydb;Username=myuser;Password=mypass";

        // Create connection from connection string
        using var connection1 = new ClickHouseConnection(connectionString);

        // Create connection from settings parsed from same connection string
        var settings = ClickHouseClientSettings.FromConnectionString(connectionString);
        using var connection2 = new ClickHouseConnection(settings);

        // Both should have same effective configuration
        Assert.Multiple(() =>
        {
            Assert.That(connection2.ConnectionString, Does.Contain("Host=myhost"));
            Assert.That(connection2.ConnectionString, Does.Contain("Port=9000"));
            Assert.That(connection2.ConnectionString, Does.Contain("Database=mydb"));
            Assert.That(connection2.ConnectionString, Does.Contain("Username=myuser"));
            Assert.That(connection2.ConnectionString, Does.Contain("Password=mypass"));
        });
    }

    [Test]
    public void Constructor_WithSettingsWithSessionId_ShouldApplySessionId()
    {
        var settings = new ClickHouseClientSettings
        {
            Host = "localhost",
            UseSession = true,
            SessionId = "test-session-123"
        };

        using var conn = new ClickHouseConnection(settings);

        var connString = conn.ConnectionString;
        Assert.Multiple(() =>
        {
            Assert.That(connString, Does.Contain("UseSession=True"));
            Assert.That(connString, Does.Contain("SessionId=test-session-123"));
        });
    }

    [Test]
    public async Task Constructor_WithSettingsWithPath_ShouldApplyPath()
    {
        // Use a fake response so we don't need a real server at the custom path
        var fakeResponse = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("25.10\tUTC")
        };
        var trackingHandler = new TrackingHandler(fakeResponse);
        using var httpClient = new HttpClient(trackingHandler);

        var builder = TestUtilities.GetConnectionStringBuilder();
        var settings = new ClickHouseClientSettings(builder)
        {
            Path = "/custom/reverse/proxy/path",
            HttpClient = httpClient,
        };

        using var conn = new ClickHouseConnection(settings);

        // Open connection - this will make a request
        await conn.OpenAsync();

        // Verify the path was used in the request
        Assert.That(trackingHandler.Requests, Has.Count.GreaterThan(0), "HttpClient should have been used");
        Assert.That(trackingHandler.Requests[0].RequestUri.AbsolutePath, Does.StartWith("/custom/reverse/proxy/path"), "Path was not applied to request");
    }
}
