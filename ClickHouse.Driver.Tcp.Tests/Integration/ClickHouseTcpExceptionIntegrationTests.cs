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
[Category("Cloud")]
public class ClickHouseTcpExceptionIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [TestCase("SELECT * FROM table_that_does_not_exist_xyz", ClickHouseErrorCode.UnknownTable, 60)]
    [TestCase("SELECT * FROM system.tables WHERE", ClickHouseErrorCode.SyntaxError, 62)]
    [TestCase("SELECT notAFunction123(1)", ClickHouseErrorCode.UnknownFunction, 46)]
    [TestCase("SELECT * FROM db_that_does_not_exist_xyz.t", ClickHouseErrorCode.UnknownDatabase, 81)]
    [TestCase("SELECT CAST('(a, 1)', 'Tuple(String, UInt8)')", ClickHouseErrorCode.CannotParseQuotedString, 26)]
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

            // The server writes its class name into the message as well as into the name field. Only a real
            // server proves the prefix is there to strip.
            Assert.That(thrown.Message, Does.Not.StartWith(thrown.Name), "Name is not repeated at the head of Message.");
            Assert.That(thrown.Message, Is.Not.Empty, "stripping the prefix leaves the message text.");
        });
    }

    [Test]
    public async Task ExecuteAsync_QueryExceedsMaxExecutionTime_MapsTheCodeToTimeoutExceeded()
    {
        await using var client = TcpServerFixture.CreateClient();

        var thrown = Assert.ThrowsAsync<ClickHouseTcpServerException>(
            async () => await client.ExecuteAsync("SELECT sleep(3) SETTINGS max_execution_time = 1", cancellationToken: None));

        Assert.That(thrown.Code, Is.EqualTo(ClickHouseErrorCode.TimeoutExceeded));
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
    // disagreement with the server and belongs under the same base as the rest. AggregateFunction is one of the
    // two types a real server produces that the client deliberately declines — the other is a wide Tuple, below
    // — so between them this path is reachable without hand-building a block.
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

    // The typed TupleColumn shapes stop at seven elements, and a server will happily send more — a wide table
    // selected as a tuple, or any tuple() of eight. Read on a pool of one, so the follow-up query is also the
    // proof that declining a column type costs neither the connection nor its permit.
    [Test]
    public async Task QueryAsync_TupleWiderThanTheClientReads_ReportsAProtocolFailureAndKeepsThePoolUsable()
    {
        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options() with { MaxPoolSize = 1 });

        var thrown = Assert.ThrowsAsync<ClickHouseTcpProtocolException>(
            async () => await client.QueryAsync("SELECT tuple(1, 2, 3, 4, 5, 6, 7, 8) AS t").ToListAsync());

        object next = await client.ExecuteScalarAsync("SELECT toUInt64(7)", cancellationToken: None);

        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.InstanceOf<ClickHouseTcpException>());
            Assert.That(thrown.Message, Does.Contain("'t'"), "the failing column is named.");
            Assert.That(thrown.Message, Does.Contain("at most 7"), "and the limit, so a caller knows what it has to change.");
            Assert.That(thrown.InnerException, Is.InstanceOf<NotSupportedException>());
            Assert.That(next, Is.EqualTo(7UL), "the only pool slot came back.");
        });
    }

    [Test]
    public async Task CreateClient_WithTheWrongPassword_FailsAuthenticationDuringTheHandshake()
    {
        await using var client = TcpServerFixture.CreateClient(password: "definitely-not-the-password");

        var thrown = Assert.ThrowsAsync<ClickHouseTcpServerException>(
            async () => await client.ExecuteAsync("SELECT 1", cancellationToken: None));

        Assert.That(thrown.Code, Is.EqualTo(ClickHouseErrorCode.AuthenticationFailed));
    }
}
