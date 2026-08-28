using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

// Covers the public conveniences added with the R1-R4 surface pass: server info, ExecuteScalarAsync, and the
// ClickHouseTcpColumn factory. The factory is the only public way to build an insert column, so it needs a real
// insert to prove it: the columns it builds carry no ClickHouse type name, and the target type comes from the
// server's schema instead.
[TestFixture]
[Category("Integration")]
public class PublicSurfaceIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static string UniqueTableName() => $"tcp_public_surface_{Guid.NewGuid():N}";

    [Test]
    public async Task GetServerInfoAsync_AgainstRealServer_ReportsVersionMatchingSelectVersion()
    {
        await using var client = TcpServerFixture.CreateClient();

        ClickHouseTcpServerInfo info = await client.GetServerInfoAsync(None);
        var reported = (string)await client.ExecuteScalarAsync("SELECT version()", cancellationToken: None);
        var expected = Version.Parse(reported);

        Assert.Multiple(() =>
        {
            Assert.That(info.Name, Is.EqualTo("ClickHouse"));
            Assert.That(info.VersionMajor, Is.EqualTo(expected.Major));
            Assert.That(info.VersionMinor, Is.EqualTo(expected.Minor));
            Assert.That(info.VersionPatch, Is.EqualTo(expected.Build));
            Assert.That(info.Timezone, Is.Not.Empty);

            // Negotiated, so it is the client's ceiling whenever the server offers at least that much.
            Assert.That(info.ProtocolRevision, Is.EqualTo(NegotiatedProtocol.ClientTcpProtocolVersion));
        });
    }

    [Test]
    public async Task GetServerInfoAsync_OnASession_ReportsTheSameServerAsItsClient()
    {
        await using var client = TcpServerFixture.CreateClient();
        await using IClickHouseTcpSession session = await client.OpenSessionAsync(None);

        ClickHouseTcpServerInfo fromClient = await client.GetServerInfoAsync(None);
        ClickHouseTcpServerInfo fromSession = await session.GetServerInfoAsync(None);

        Assert.That(fromSession, Is.EqualTo(fromClient));
    }

    [Test]
    public async Task ExecuteScalarAsync_SelectCount_ReturnsTheFirstCellBoxed()
    {
        await using var client = TcpServerFixture.CreateClient();

        object value = await client.ExecuteScalarAsync("SELECT count() FROM numbers(7)", cancellationToken: None);

        Assert.That(value, Is.EqualTo(7UL));
    }

    [Test]
    public async Task ExecuteScalarAsync_MultipleRowsAndColumns_ReturnsTheFirstColumnOfTheFirstRow()
    {
        await using var client = TcpServerFixture.CreateClient();

        object value = await client.ExecuteScalarAsync(
            "SELECT number, number * 2 FROM numbers(1000) ORDER BY number",
            cancellationToken: None);

        Assert.That(value, Is.EqualTo(0UL));
    }

    [Test]
    public async Task ExecuteScalarAsync_EmptyResult_ReturnsNull()
    {
        await using var client = TcpServerFixture.CreateClient();

        object value = await client.ExecuteScalarAsync("SELECT 1 WHERE 0", cancellationToken: None);

        Assert.That(value, Is.Null);
    }

    [Test]
    public async Task ExecuteScalarAsync_AbandonsALargeResult_LeavesTheConnectionReusable()
    {
        // Stopping after the first row cancels the rest of the result. The connection goes back to the pool, so
        // the next operation on the same client must succeed rather than trip over leftover bytes.
        await using var client = TcpServerFixture.CreateClient();

        object first = await client.ExecuteScalarAsync(
            "SELECT number FROM numbers(5000000) ORDER BY number",
            cancellationToken: None);
        object second = await client.ExecuteScalarAsync("SELECT 42", cancellationToken: None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(0UL));
            Assert.That(second, Is.EqualTo((byte)42));
        });
    }

    [Test]
    public async Task ExecuteScalarAsync_NullCell_ReturnsNull()
    {
        await using var client = TcpServerFixture.CreateClient();

        object value = await client.ExecuteScalarAsync(
            "SELECT CAST(NULL, 'Nullable(Int32)')",
            cancellationToken: None);

        Assert.That(value, Is.Null);
    }

    [Test]
    public async Task Create_ColumnsForEveryShape_RoundTripThroughAnInsert()
    {
        // One case per shape the factory has to carry: fixed-width, string, nullable, and a jagged array row.
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();

        try
        {
            await client.ExecuteAsync(
                $"CREATE TABLE {table} (id Int32, name String, note Nullable(String), tags Array(UInt32)) ENGINE = Memory",
                cancellationToken: None);

            IColumn[] columns =
            {
                ClickHouseTcpColumn.Create("id", new[] { 1, 2, 3 }),
                ClickHouseTcpColumn.Create("name", new[] { "a", "b", "c" }),
                ClickHouseTcpColumn.Create("note", new[] { "x", null, "z" }),
                ClickHouseTcpColumn.Create("tags", new[] { new uint[] { 1, 2 }, Array.Empty<uint>(), new uint[] { 3 } }),
            };

            await client.InsertAsync($"INSERT INTO {table} (id, name, note, tags) VALUES", columns, cancellationToken: None);

            var rows = new List<object[]>();
            await foreach (object[] row in client.QueryAsync($"SELECT id, name, note, tags FROM {table} ORDER BY id", cancellationToken: None))
            {
                rows.Add(row);
            }

            Assert.Multiple(() =>
            {
                Assert.That(rows, Has.Count.EqualTo(3));
                Assert.That(rows[0][0], Is.EqualTo(1));
                Assert.That(rows[0][1], Is.EqualTo("a"));
                Assert.That(rows[1][2], Is.Null);
                Assert.That(rows[0][3], Is.EqualTo(new uint[] { 1, 2 }));
                Assert.That(rows[1][3], Is.EqualTo(Array.Empty<uint>()));
                Assert.That(rows[2][3], Is.EqualTo(new uint[] { 3 }));
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task Create_ColumnsNamedOutOfOrderAndCoveringASubset_MatchTheTargetByName()
    {
        // The factory takes no type name, so name matching is the whole contract: order is free and an unnamed
        // column must fall back to its server-side default.
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();

        try
        {
            await client.ExecuteAsync(
                $"CREATE TABLE {table} (id Int32, name String DEFAULT 'unset') ENGINE = Memory",
                cancellationToken: None);

            IColumn[] columns = { ClickHouseTcpColumn.Create("id", new[] { 7 }) };
            await client.InsertAsync($"INSERT INTO {table} (id) VALUES", columns, cancellationToken: None);

            var rows = new List<object[]>();
            await foreach (object[] row in client.QueryAsync($"SELECT id, name FROM {table}", cancellationToken: None))
            {
                rows.Add(row);
            }

            Assert.Multiple(() =>
            {
                Assert.That(rows, Has.Count.EqualTo(1));
                Assert.That(rows[0][0], Is.EqualTo(7));
                Assert.That(rows[0][1], Is.EqualTo("unset"));
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task Create_ClrTypeTheTargetDoesNotAccept_ThrowsNamingTheColumn()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();

        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id Int32) ENGINE = Memory", cancellationToken: None);

            IColumn[] columns = { ClickHouseTcpColumn.Create("id", new[] { "not an int" }) };

            var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
                await client.InsertAsync($"INSERT INTO {table} (id) VALUES", columns, cancellationToken: None));

            Assert.That(ex.Message, Does.Contain("id").And.Contain("Int32"));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task Create_ReadBackColumnReinserted_NeedsNoFactory()
    {
        // The read shape is already an insert shape, which is why the factory does not need a dense overload.
        await using var client = TcpServerFixture.CreateClient();
        string source = UniqueTableName();
        string destination = UniqueTableName();

        try
        {
            await client.ExecuteAsync($"CREATE TABLE {source} (v Array(Int64)) ENGINE = Memory", cancellationToken: None);
            await client.ExecuteAsync($"CREATE TABLE {destination} (v Array(Int64)) ENGINE = Memory", cancellationToken: None);
            await client.ExecuteAsync($"INSERT INTO {source} VALUES ([1, 2]), ([]), ([3])", cancellationToken: None);

            await foreach (Block block in client.StreamAsync($"SELECT v FROM {source} ORDER BY v", cancellationToken: None))
            {
                await client.InsertAsync(
                    $"INSERT INTO {destination} (v) VALUES",
                    new[] { block[0] },
                    cancellationToken: None);
            }

            object copied = await client.ExecuteScalarAsync($"SELECT count() FROM {destination}", cancellationToken: None);
            Assert.That(copied, Is.EqualTo(3UL));
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {source}", cancellationToken: None);
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {destination}", cancellationToken: None);
        }
    }

    [Test]
    public void Create_NullNameOrValues_Throws()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpColumn.Create(null, new[] { 1 }));
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpColumn.Create<int>("id", (int[])null));
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpColumn.Create<int>("id", (IEnumerable<int>)null));
        });
    }

    [Test]
    public async Task Create_FromASequence_EnumeratesOnceAndInsertsTheSameRows()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = UniqueTableName();

        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id Int32) ENGINE = Memory", cancellationToken: None);

            int enumerations = 0;
            IEnumerable<int> Sequence()
            {
                enumerations++;
                yield return 10;
                yield return 20;
            }

            IColumn[] columns = { ClickHouseTcpColumn.Create("id", Sequence()) };
            await client.InsertAsync($"INSERT INTO {table} (id) VALUES", columns, cancellationToken: None);

            object total = await client.ExecuteScalarAsync($"SELECT sum(id) FROM {table}", cancellationToken: None);

            Assert.Multiple(() =>
            {
                Assert.That(total, Is.EqualTo(30L));
                Assert.That(enumerations, Is.EqualTo(1));
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }
}
