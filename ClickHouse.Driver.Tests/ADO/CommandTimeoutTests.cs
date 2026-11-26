using System;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;

namespace ClickHouse.Driver.Tests.ADO;

public class CommandTimeoutTests : AbstractConnectionTestFixture
{
    public enum ExecutionPath
    {
        Scalar,
        NonQuery,
        Reader,
        RawResult,
    }

    [Test]
    public async Task ExecuteScalarAsync_WithCommandTimeout_AppliesMaxExecutionTimeSetting()
    {
        using var command = connection.CreateCommand("SELECT getSetting('max_execution_time')");
        command.CommandTimeout = 30;

        Assert.That(await command.ExecuteScalarAsync(), Is.EqualTo(30UL));
    }

    [TestCase(ExecutionPath.Scalar)]
    [TestCase(ExecutionPath.NonQuery)]
    [TestCase(ExecutionPath.Reader)]
    [TestCase(ExecutionPath.RawResult)]
    public void ExecuteAsync_WithCommandTimeout_LongQueryIsCancelledByServer(ExecutionPath path)
    {
        using var command = connection.CreateCommand("SELECT sleep(3)");
        command.CommandTimeout = 1;

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(async () => await ExecuteAsync(command, path));

        // TIMEOUT_EXCEEDED = 159
        Assert.That(ex!.ErrorCode, Is.EqualTo(159));
    }

    [Test]
    public async Task ExecuteScalarAsync_WithDefaultCommandTimeout_LeavesMaxExecutionTimeUnchanged()
    {
        var serverDefault = await client.ExecuteScalarAsync("SELECT getSetting('max_execution_time')");

        using var command = connection.CreateCommand("SELECT getSetting('max_execution_time')");

        Assert.That(await command.ExecuteScalarAsync(), Is.EqualTo(serverDefault));
    }

    [Test]
    public async Task ExecuteScalarAsync_WithNegativeCommandTimeout_LeavesMaxExecutionTimeUnchanged()
    {
        var serverDefault = await client.ExecuteScalarAsync("SELECT getSetting('max_execution_time')");

        using var command = connection.CreateCommand("SELECT getSetting('max_execution_time')");
        command.CommandTimeout = -1;

        Assert.That(await command.ExecuteScalarAsync(), Is.EqualTo(serverDefault));
    }

    [TestCase(0)]
    [TestCase(-1)]
    public async Task ExecuteScalarAsync_WithoutPositiveCommandTimeout_KeepsConnectionCustomSetting(int commandTimeout)
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder["set_max_execution_time"] = 45;
        using var connectionWithSetting = new ClickHouseConnection(builder.ConnectionString);

        using var command = connectionWithSetting.CreateCommand("SELECT getSetting('max_execution_time')");
        command.CommandTimeout = commandTimeout;

        Assert.That(await command.ExecuteScalarAsync(), Is.EqualTo(45UL));
    }

    [Test]
    public async Task ExecuteScalarAsync_WithCommandTimeoutAndCommandCustomSetting_PrefersCustomSetting()
    {
        using var command = connection.CreateCommand("SELECT getSetting('max_execution_time')");
        command.CommandTimeout = 30;
        command.CustomSettings.Add("max_execution_time", 45);

        Assert.That(await command.ExecuteScalarAsync(), Is.EqualTo(45UL));
    }

    [Test]
    public async Task ExecuteScalarAsync_WithCommandTimeoutAndConnectionCustomSetting_PrefersCommandTimeout()
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder["set_max_execution_time"] = 45;
        using var connectionWithSetting = new ClickHouseConnection(builder.ConnectionString);

        using var command = connectionWithSetting.CreateCommand("SELECT getSetting('max_execution_time')");
        command.CommandTimeout = 30;

        Assert.That(await command.ExecuteScalarAsync(), Is.EqualTo(30UL));
    }

    private static async Task ExecuteAsync(ClickHouseCommand command, ExecutionPath path)
    {
        switch (path)
        {
            case ExecutionPath.Scalar:
                await command.ExecuteScalarAsync();
                break;
            case ExecutionPath.NonQuery:
                await command.ExecuteNonQueryAsync();
                break;
            case ExecutionPath.Reader:
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                    }
                }

                break;
            case ExecutionPath.RawResult:
                using (await command.ExecuteRawResultAsync(default))
                {
                }

                break;
        }
    }
}
