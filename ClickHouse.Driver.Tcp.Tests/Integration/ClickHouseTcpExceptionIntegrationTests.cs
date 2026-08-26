using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// Proves the error contract against a real server: which code a real failure carries, and that the three
// exception types are reachable through the one base a caller can catch. Codes were read off 26.7.1; they
// are part of the server's compatibility surface, so a change here is a server change worth noticing.
[TestFixture]
[Category("Integration")]
public class ClickHouseTcpExceptionIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [TestCase("SELECT * FROM table_that_does_not_exist_xyz", ClickHouseErrorCode.UnknownTable, 60)]
    [TestCase("SELECT * FROM system.tables WHERE", ClickHouseErrorCode.SyntaxError, 62)]
    [TestCase("SELECT notAFunction123(1)", ClickHouseErrorCode.UnknownFunction, 46)]
    [TestCase("SELECT * FROM db_that_does_not_exist_xyz.t", ClickHouseErrorCode.UnknownDatabase, 81)]
    public async Task ExecuteAsync_ServerRejectsTheQuery_MapsTheCodeToItsNamedConstant(
        string sql,
        ClickHouseErrorCode expected,
        int expectedRaw)
    {
        await using var client = TcpServerFixture.CreateClient();

        var thrown = Assert.ThrowsAsync<ClickHouseTcpServerException>(
            async () => await client.ExecuteAsync(sql, cancellationToken: None));

        Assert.Multiple(() =>
        {
            Assert.That(thrown.Code, Is.EqualTo(expected));
            Assert.That(thrown.RawCode, Is.EqualTo(expectedRaw));
            Assert.That(thrown.ErrorCode, Is.EqualTo(expectedRaw), "DbException.ErrorCode carries the same number.");
            Assert.That(thrown.Name, Is.Not.Empty);
            Assert.That(thrown.IsTransient, Is.False);
        });
    }

    [Test]
    public async Task ExecuteAsync_QueryExceedsMaxExecutionTime_ReportsATransientTimeout()
    {
        await using var client = TcpServerFixture.CreateClient();

        var thrown = Assert.ThrowsAsync<ClickHouseTcpServerException>(
            async () => await client.ExecuteAsync("SELECT sleep(3) SETTINGS max_execution_time = 1", cancellationToken: None));

        Assert.Multiple(() =>
        {
            Assert.That(thrown.Code, Is.EqualTo(ClickHouseErrorCode.TimeoutExceeded));
            Assert.That(thrown.IsTransient, Is.True, "the same query may well succeed on a less busy server.");
        });
    }

    [Test]
    public async Task ExecuteAsync_ServerRejectsTheQuery_IsCatchableAsTheSharedBaseType()
    {
        await using var client = TcpServerFixture.CreateClient();

        // The point of the base type: one catch covers server, protocol and transport failures alike.
        var thrown = Assert.ThrowsAsync<ClickHouseTcpServerException>(
            async () => await client.ExecuteAsync("SELECT * FROM table_that_does_not_exist_xyz", cancellationToken: None));

        Assert.That(thrown, Is.InstanceOf<ClickHouseTcpException>());
    }

    // A column type the client cannot resolve arrives as a type name in the block header, so the refusal is a
    // disagreement with the server and belongs under the same base as the rest. AggregateFunction is the one
    // type a real server produces that the client deliberately declines, so it is the only way to reach this
    // path without hand-building a block.
    [Test]
    public async Task QueryAsync_ColumnTypeTheClientCannotRead_ReportsAProtocolFailureKeepingTheHint()
    {
        await using var client = TcpServerFixture.CreateClient();

        var thrown = Assert.ThrowsAsync<ClickHouseTcpProtocolException>(
            async () => await client.QueryAsync("SELECT sumState(number) AS s FROM numbers(3)").ToListAsync());

        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.InstanceOf<ClickHouseTcpException>());
            Assert.That(thrown.Message, Does.Contain("'s'"), "the failing column is named.");
            Assert.That(thrown.Message, Does.Contain("sumMerge(column)"), "the actionable hint survives the wrapping.");
            Assert.That(thrown.InnerException, Is.InstanceOf<NotSupportedException>());
        });
    }

    [Test]
    public async Task CreateClient_WithTheWrongPassword_FailsAuthenticationDuringTheHandshake()
    {
        await using var client = TcpServerFixture.CreateClient(password: "definitely-not-the-password");

        var thrown = Assert.ThrowsAsync<ClickHouseTcpServerException>(
            async () => await client.ExecuteAsync("SELECT 1", cancellationToken: None));

        Assert.Multiple(() =>
        {
            Assert.That(thrown.Code, Is.EqualTo(ClickHouseErrorCode.AuthenticationFailed));
            Assert.That(thrown.IsTransient, Is.False, "retrying the same wrong password cannot help.");
        });
    }
}
