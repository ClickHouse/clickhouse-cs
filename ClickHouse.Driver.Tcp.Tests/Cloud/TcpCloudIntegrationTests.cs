using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Cloud;

/// <summary>
/// What only a real ClickHouse Cloud service shows: that the native protocol works end to end inside a TLS
/// tunnel against a publicly trusted certificate, through a load balancer, on a pooled client.
///
/// <para>
/// Deliberately a smoke set rather than the whole integration suite. Those tests build <c>Memory</c> tables,
/// which live on one replica, while a pooled client spreads an insert and its read-back across connections that
/// need not land on the same one — so running them here would fail for reasons that have nothing to do with the
/// transport. Per-type and per-feature coverage stays on the container suite; this proves the transport.
/// </para>
/// </summary>
[TestFixture]
[Category("Cloud")]
public class TcpCloudIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private readonly List<string> tables = [];

    private string UniqueTableName()
    {
        // Cloud services outlive the run, so every table made here is dropped again in the teardown below.
        string name = $"tcp_cloud_test_{Guid.NewGuid():N}";
        tables.Add(name);
        return name;
    }

    [OneTimeTearDown]
    public async Task DropCreatedTablesAsync()
    {
        if (tables.Count == 0)
        {
            return;
        }

        await using ClickHouseTcpClient client = TcpCloudFixture.CreateClient();
        foreach (string table in tables)
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    [Test]
    public async Task PingAsync_OverTls_CompletesTheHandshakeAndReportsTheServerIdentity()
    {
        await using ClickHouseTcpClient client = TcpCloudFixture.CreateClient();

        // Ping is the shortest exchange that proves the tunnel carries protocol packets in both directions.
        await client.PingAsync(None);

        var version = new List<object[]>();
        await foreach (object[] row in client.QueryAsync("SELECT version()", cancellationToken: None))
        {
            version.Add(row);
        }

        Assert.Multiple(() =>
        {
            Assert.That(version, Has.Count.EqualTo(1));
            Assert.That(version[0][0], Is.InstanceOf<string>().And.Not.Empty);
        });
    }

    [Test]
    public async Task QueryAsync_OverTls_ReturnsEveryRow()
    {
        // Enough rows to cross the TLS record size, so the read path is exercised over several records rather
        // than one that happens to fit.
        await using ClickHouseTcpClient client = TcpCloudFixture.CreateClient();

        var numbers = new List<ulong>();
        await foreach (object[] row in client.QueryAsync("SELECT number FROM system.numbers LIMIT 50000", cancellationToken: None))
        {
            numbers.Add((ulong)row[0]);
        }

        Assert.Multiple(() =>
        {
            Assert.That(numbers, Has.Count.EqualTo(50000));
            Assert.That(numbers[0], Is.EqualTo(0UL));
            Assert.That(numbers[^1], Is.EqualTo(49999UL));
        });
    }

    [Test]
    public async Task InsertRowsAsync_MergeTreeTable_RoundTripsThroughSelect()
    {
        // MergeTree, not Memory: Cloud turns it into SharedMergeTree, whose data every replica can read, so the
        // read-back does not depend on landing on the connection that inserted.
        await using ClickHouseTcpClient client = TcpCloudFixture.CreateClient();
        string table = UniqueTableName();
        await client.ExecuteAsync($"CREATE TABLE {table} (id UInt64, name String) ENGINE = MergeTree ORDER BY id", cancellationToken: None);

        await client.InsertRowsAsync(
            $"INSERT INTO {table} (id, name) VALUES",
            Enumerable.Range(0, 1000).Select(i => new object[] { (ulong)i, $"row-{i}" }).ToArray(),
            cancellationToken: None);

        var rows = new List<(ulong Id, string Name)>();
        await foreach (object[] row in client.QueryAsync($"SELECT id, name FROM {table} ORDER BY id", cancellationToken: None))
        {
            rows.Add(((ulong)row[0], (string)row[1]));
        }

        Assert.Multiple(() =>
        {
            Assert.That(rows, Has.Count.EqualTo(1000));
            Assert.That(rows[0], Is.EqualTo((0UL, "row-0")));
            Assert.That(rows[^1], Is.EqualTo((999UL, "row-999")));
        });
    }

    [Test]
    public async Task QueryAsync_SequentialQueriesOnOneClient_ReuseThePooledTlsConnection()
    {
        // A returned SslStream connection has to be usable again, and asserting on results alone would not show
        // it: five queries pass identically whether the pool reuses one connection or redials every time. A
        // temporary table lives in the session, and a native connection *is* the session, so reading one back on
        // a later query is only possible on the same connection.
        await using ClickHouseTcpClient client = new(TcpCloudFixture.Options() with { MaxPoolSize = 1 });
        string temporary = $"tcp_cloud_session_{Guid.NewGuid():N}";

        await client.ExecuteAsync($"CREATE TEMPORARY TABLE {temporary} (id UInt64)", cancellationToken: None);
        await client.ExecuteAsync($"INSERT INTO {temporary} VALUES (7)", cancellationToken: None);

        var ids = new List<ulong>();
        await foreach (object[] row in client.QueryAsync($"SELECT id FROM {temporary}", cancellationToken: None))
        {
            ids.Add((ulong)row[0]);
        }

        // No DROP: a temporary table goes when the session does, which is the point being proved.
        Assert.That(ids, Is.EqualTo(new ulong[] { 7 }));
    }

    [Test]
    public async Task QueryAsync_ConcurrentQueriesOverTls_EachReturnItsOwnResultUncrossed()
    {
        await using ClickHouseTcpClient client = new(TcpCloudFixture.Options() with { MaxPoolSize = 4 });

        ulong[] results = await Task.WhenAll(Enumerable.Range(0, 4).Select(async i =>
        {
            ulong total = 0;
            await foreach (object[] row in client.QueryAsync($"SELECT sum(number) FROM numbers({(i + 1) * 1000})", cancellationToken: None))
            {
                total = (ulong)row[0];
            }

            return total;
        }));

        // sum(0..n-1) == n(n-1)/2, so each task must get its own answer back rather than another task's.
        Assert.That(results, Is.EqualTo(Enumerable.Range(0, 4).Select(i =>
        {
            ulong n = (ulong)(i + 1) * 1000;
            return n * (n - 1) / 2;
        }).ToArray()));
    }
}
