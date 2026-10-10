using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// The scatter tiers of the POCO read plan against a real server. <see cref="PocoReadIntegrationTests"/> reads through
/// the tier that the runtime chooses; this fixture reads one result through each tier. The other cases of the
/// <see cref="PocoScatterTier.Fill"/> tier run offline, in the differential tests and in the plan tests.
/// </summary>
[TestFixture]
[Category("Integration")]
[Category("Cloud")]
public class PocoReadTierIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task Materialize_EveryScatterTier_ReadsTheSameRows()
    {
        // The tiers are compared here as well as in the unit tests because these values come off a real server: the
        // compiled loop and the bulk read with a setter for each row have to agree on decoded storage, not just on
        // columns a test built. QueryAsync<T> leaves the tier to the runtime, so the plan is built directly to name
        // one; each tier reads its own block, since materializing one block twice would let the first tier fill the
        // caches the second reads.
        const string sql = "SELECT toUInt64(number) AS Id, toString(number) AS Name FROM numbers(3)";
        await using var client = new ClickHouseTcpClient(TcpServerFixture.Options());
        var byTier = new Dictionary<PocoScatterTier, List<Numbered>>();

        foreach (PocoScatterTier tier in Enum.GetValues<PocoScatterTier>())
        {
            var read = new List<Numbered>();
            await foreach (Block block in client.StreamAsync(sql, cancellationToken: None))
            {
                var rows = new Numbered[block.RowCount];
                PocoReadPlan<Numbered>.Build(PocoTypeDescriptor<Numbered>.Build(), block, tier)
                    .Materialize(block, rows, read.Count);
                read.AddRange(rows);
            }

            byTier[tier] = read;
        }

        Assert.Multiple(() =>
        {
            foreach ((PocoScatterTier tier, List<Numbered> rows) in byTier)
            {
                Assert.That(rows.ConvertAll(row => row.Id), Is.EqualTo(new ulong[] { 0, 1, 2 }), $"{tier}: Id");
                Assert.That(rows.ConvertAll(row => row.Name), Is.EqualTo(new[] { "0", "1", "2" }), $"{tier}: Name");
            }
        });
    }

    [Test]
    public async Task QueryAsync_EveryScatterTierOverSeveralWindows_ReadsTheExpectedRows()
    {
        // One block of 600 rows is read in three windows of the client. Each column has its own decoded storage, and
        // each has a NULL, a dictionary entry or a new length in every window, so a tier that loses the window offset
        // reads the values of another row.
        const int rowCount = 600;
        string sql =
            "SELECT number AS Id, "
            + "if(number % 7 = 3, NULL, toInt64(number) - 300) AS Count, "
            + "toLowCardinality(if(number % 5 = 0, NULL, toString(number % 13))) AS Label, "
            + "arrayMap(x -> toInt32(x), range(number % 4)) AS Lengths, "
            + "map(toString(number % 3), number) AS Tags, "
            + "addMilliseconds(toDateTime64(1700000000 + number, 3, 'UTC'), number % 1000) AS Stamp "
            + $"FROM numbers({rowCount})";
        Mixed[] expected = Enumerable.Range(0, rowCount).Select(n => new Mixed
        {
            Id = (ulong)n,
            Count = n % 7 == 3 ? null : n - 300L,
            Label = n % 5 == 0 ? null : (n % 13).ToString(CultureInfo.InvariantCulture),
            Lengths = Enumerable.Range(0, n % 4).ToArray(),
            Tags = new[] { new KeyValuePair<string, ulong>((n % 3).ToString(CultureInfo.InvariantCulture), (ulong)n) },
            Stamp = DateTime.UnixEpoch.AddSeconds(1700000000 + n).AddMilliseconds(n % 1000),
        }).ToArray();

        foreach (PocoScatterTier tier in Enum.GetValues<PocoScatterTier>())
        {
            await using var client = new ClickHouseTcpClient(TcpServerFixture.Options())
            {
                PocoTypes = new PocoTypeRegistry { ForcedTier = tier },
            };
            List<Mixed> rows = await client.QueryAsync<Mixed>(sql, cancellationToken: None).ToListAsync();

            Assert.That(rows, Has.Count.EqualTo(rowCount), $"{tier}: rows");
            Assert.Multiple(() =>
            {
                Assert.That(rows.Select(row => row.Id), Is.EqualTo(expected.Select(row => row.Id)), $"{tier}: Id");
                Assert.That(rows.Select(row => row.Count), Is.EqualTo(expected.Select(row => row.Count)), $"{tier}: Count");
                Assert.That(rows.Select(row => row.Label), Is.EqualTo(expected.Select(row => row.Label)), $"{tier}: Label");
                Assert.That(rows.Select(row => row.Lengths), Is.EqualTo(expected.Select(row => row.Lengths)), $"{tier}: Lengths");
                Assert.That(rows.Select(row => row.Tags), Is.EqualTo(expected.Select(row => row.Tags)), $"{tier}: Tags");
                Assert.That(rows.Select(row => row.Stamp), Is.EqualTo(expected.Select(row => row.Stamp)), $"{tier}: Stamp");
            });
        }
    }

    private sealed class Mixed
    {
        public ulong Id { get; set; }

        public long? Count { get; set; }

        public string Label { get; set; }

        public int[] Lengths { get; set; }

        public KeyValuePair<string, ulong>[] Tags { get; set; }

        public DateTime Stamp { get; set; }
    }

    private sealed class Numbered
    {
        public ulong Id { get; set; }

        public string Name { get; set; }
    }
}
