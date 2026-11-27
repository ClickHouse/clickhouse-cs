using NUnit.Framework;

namespace ClickHouse.Driver.TestRunner.Tests;

// Ported from args_test.go of the clickhouse-go harness.
public class OptionsTests
{
    [Test]
    public void Parse_SqlTestCommandLine_KeepsTheServerSettingsOnly()
    {
        // The command line that tests/clickhouse-test uses for a .sql test.
        var opts = Options.Parse(
        [
            "client", "--host=127.0.0.1", "--port=9000", "--no-secure", "--database=test_abc",
            "--send_logs_level=warning", "--multiquery", "--log_comment", "00001_select_1.sql-test_abc",
            "--max_block_size=1", "--allow_repeated_settings", "--client_logs_file=/tmp/x-fatal",
            "--param_CLICKHOUSE_DATABASE=test_abc",
        ]);
        Assert.That(opts.Host, Is.EqualTo("127.0.0.1"));
        Assert.That(opts.Port, Is.EqualTo(9000));
        Assert.That(opts.Database, Is.EqualTo("test_abc"));
        Assert.That(opts.Secure, Is.False);
        Assert.That(opts.Query, Is.Null);
        Assert.That(opts.Settings, Is.EqualTo(new[]
        {
            new KeyValuePair<string, string>("send_logs_level", "warning"),
            new KeyValuePair<string, string>("log_comment", "00001_select_1.sql-test_abc"),
        }));
        Assert.That(opts.Params, Is.EqualTo(new Dictionary<string, string> { ["CLICKHOUSE_DATABASE"] = "test_abc" }));
    }

    [Test]
    public void Parse_FeatureTagOverrides_GoToTheServer()
    {
        // tests/clickhouse-test adds these for the tags no-async-insert and no-parallel-replicas.
        var opts = Options.Parse(
        [
            "--log_comment", "x.sql-test_abc", "--async_insert", "0", "--enable_parallel_replicas", "0",
            "--async_insert_busy_timeout_ms=5", "--parallel_replicas_for_non_replicated_merge_tree=1",
        ]);
        Assert.That(opts.Settings, Is.EqualTo(new[]
        {
            new KeyValuePair<string, string>("log_comment", "x.sql-test_abc"),
            new KeyValuePair<string, string>("async_insert", "0"),
            new KeyValuePair<string, string>("enable_parallel_replicas", "0"),
        }));
        // The randomized parallel-replica mode turns the feature on with other settings that the shim drops.
        var random = Options.Parse(["--enable_parallel_replicas=1", "--automatic_parallel_replicas_mode=2", "--async_insert", "1"]);
        Assert.That(random.Settings, Is.Empty);
    }

    [Test]
    public void Parse_ShortOptions_SetTheirValues()
    {
        var opts = Options.Parse(["-h", "db", "-u", "me", "-d", "x", "-q", "SELECT 1", "-n", "-s"]);
        Assert.That(opts.Host, Is.EqualTo("db"));
        Assert.That(opts.User, Is.EqualTo("me"));
        Assert.That(opts.Database, Is.EqualTo("x"));
        Assert.That(opts.Query, Is.EqualTo("SELECT 1"));
        Assert.That(opts.Secure, Is.True);
    }

    [Test]
    public void Parse_SettingWithNegativeValue_TakesTheValue()
    {
        var opts = Options.Parse(["--max_threads", "-1", "--max-insert-threads", "2", "--query=SELECT 1"]);
        Assert.That(opts.Settings, Is.EqualTo(new[]
        {
            new KeyValuePair<string, string>("max_threads", "-1"),
            new KeyValuePair<string, string>("max_insert_threads", "2"),
        }));
    }

    [Test]
    public void Parse_BadValues_Throw()
    {
        Assert.Throws<ArgumentException>(() => Options.Parse(["--port=abc"]));
        Assert.Throws<ArgumentException>(() => Options.Parse(["--query"]));
    }

    [Test]
    [NonParallelizable]
    public void HttpPort_NativePort_MapsToTheHttpPort()
    {
        var tcp = Environment.GetEnvironmentVariable("CLICKHOUSE_PORT_TCP");
        var http = Environment.GetEnvironmentVariable("CLICKHOUSE_PORT_HTTP");
        try
        {
            Environment.SetEnvironmentVariable("CLICKHOUSE_PORT_TCP", null);
            Environment.SetEnvironmentVariable("CLICKHOUSE_PORT_HTTP", null);
            Assert.That(HttpBackend.HttpPort(0, false), Is.EqualTo(8123));
            Assert.That(HttpBackend.HttpPort(9000, false), Is.EqualTo(8123));
            Assert.That(HttpBackend.HttpPort(18123, false), Is.EqualTo(18123));
            Assert.That(HttpBackend.HttpPort(9440, true), Is.EqualTo(8443));
            Environment.SetEnvironmentVariable("CLICKHOUSE_PORT_TCP", "19000");
            Environment.SetEnvironmentVariable("CLICKHOUSE_PORT_HTTP", "18124");
            Assert.That(HttpBackend.HttpPort(19000, false), Is.EqualTo(18124));
            Assert.That(HttpBackend.HttpPort(9000, false), Is.EqualTo(9000));
        }
        finally
        {
            Environment.SetEnvironmentVariable("CLICKHOUSE_PORT_TCP", tcp);
            Environment.SetEnvironmentVariable("CLICKHOUSE_PORT_HTTP", http);
        }
    }

    [Test]
    public async Task RunAsync_ExtractFromConfig_AnswersListenHostOnly()
    {
        using var output = new MemoryStream();
        Assert.That(await Program.RunAsync(["extract-from-config", "--try", "--config", "x.xml", "--key", "listen_host"], Stream.Null, output, TextWriter.Null), Is.Zero);
        Assert.That(System.Text.Encoding.UTF8.GetString(output.ToArray()), Is.EqualTo("127.0.0.1\n"));
        output.SetLength(0);
        Assert.That(await Program.RunAsync(["extract-from-config", "--key=tcp_port"], Stream.Null, output, TextWriter.Null), Is.Zero);
        Assert.That(output.Length, Is.Zero);
    }

    [TestCase("USE db1;", "db1")]
    [TestCase("use `my db`", "my db")]
    [TestCase("-- c\nUSE DATABASE db2", "db2")]
    [TestCase("USE {CLICKHOUSE_DATABASE:Identifier};", "test_x")]
    [TestCase("USE \"db3\"", "db3")]
    [TestCase("USE db1, db2", null)]
    public void ParseUseDatabase_Statement_GivesTheDatabase(string sql, string? expected) =>
        Assert.That(Runner.ParseUseDatabase(sql, new Dictionary<string, string> { ["CLICKHOUSE_DATABASE"] = "test_x" }), Is.EqualTo(expected));

    [TestCase("SET max_threads = 1", true)]
    [TestCase("SET default_format = 'TSV'", true)]
    [TestCase("SET ROLE r1", true)]
    [TestCase("SET ROLE DEFAULT", true)]
    [TestCase("SET DEFAULT ROLE r1 TO u1;", false)]
    [TestCase("-- c\nset default role NONE to u1", false)]
    public void IsSessionSet_Statement_TellsIfTheSessionHoldsIt(string sql, bool expected) =>
        Assert.That(Runner.IsSessionSet(sql), Is.EqualTo(expected));

    [Test]
    public void ParseSetAssignments_Statement_GivesTheSettings() =>
        Assert.That(Runner.ParseSetAssignments("SET dialect = 'kusto', max_threads=1, x = (1, 2);"), Is.EqualTo(new Dictionary<string, string>
        {
            ["dialect"] = "kusto",
            ["max_threads"] = "1",
            ["x"] = "(1, 2)",
        }));

    [Test]
    public void RedactPassword_Argv_HidesThePasswordOnly() =>
        Assert.That(
            InvocationLog.RedactPassword(["--user", "u", "--password", "secret", "--password=secret2", "--query", "SELECT 1", "--password"]),
            Is.EqualTo(new[] { "--user", "u", "--password", "***", "--password=***", "--query", "SELECT 1", "--password" }));
}
