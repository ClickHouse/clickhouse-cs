using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// The scatter tiers of the POCO read plan against a real server. <see cref="PocoReadIntegrationTests"/> and
/// <see cref="PocoReadFillIntegrationTests"/> read through one tier each; this fixture compares the tiers on one result.
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

    private sealed class Numbered
    {
        public ulong Id { get; set; }

        public string Name { get; set; }
    }
}
