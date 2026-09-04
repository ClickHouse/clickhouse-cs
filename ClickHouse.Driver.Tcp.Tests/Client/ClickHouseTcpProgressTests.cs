namespace ClickHouse.Driver.Tcp.Tests.Client;

// The accumulator exists because every consumer of OnProgress has to add the packets up, and a client that made
// them discover that for themselves would have most of them reporting the last step as the total.
[TestFixture]
public class ClickHouseTcpProgressTests
{
    [Test]
    public void OperatorPlus_TwoIncrements_SumsEveryCounter()
    {
        var first = new ClickHouseTcpProgress(rows: 1, bytes: 2, totalRows: 3, wroteRows: 4, wroteBytes: 5, elapsedNs: 6);
        var second = new ClickHouseTcpProgress(rows: 10, bytes: 20, totalRows: 30, wroteRows: 40, wroteBytes: 50, elapsedNs: 60);

        ClickHouseTcpProgress total = first + second;

        Assert.Multiple(() =>
        {
            Assert.That(total.Rows, Is.EqualTo(11UL));
            Assert.That(total.Bytes, Is.EqualTo(22UL));
            Assert.That(total.TotalRows, Is.EqualTo(33UL));
            Assert.That(total.WroteRows, Is.EqualTo(44UL));
            Assert.That(total.WroteBytes, Is.EqualTo(55UL));
            Assert.That(total.ElapsedNs, Is.EqualTo(66UL));
        });
    }

    [Test]
    public void Add_TwoIncrements_MatchesTheOperator()
    {
        var first = new ClickHouseTcpProgress(1, 2, 3, 4, 5, 6);
        var second = new ClickHouseTcpProgress(10, 20, 30, 40, 50, 60);

        Assert.That(ClickHouseTcpProgress.Add(first, second), Is.EqualTo(first + second));
    }

    [Test]
    public void OperatorPlus_DefaultSeed_IsTheIdentity()
    {
        // So a caller can fold a sequence starting from default without a special first case.
        var increment = new ClickHouseTcpProgress(1, 2, 3, 4, 5, 6);

        Assert.That(default(ClickHouseTcpProgress) + increment, Is.EqualTo(increment));
    }
}
