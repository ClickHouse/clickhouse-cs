using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Tests.Client;

[TestFixture]
public class ClickHouseTcpInsertOptionsTests
{
    [Test]
    public void MaxRowsPerBlock_NotSet_DefaultsToTheBlockRowCap()
    {
        Assert.That(new ClickHouseTcpInsertOptions().MaxRowsPerBlock, Is.EqualTo(1_000_000));
    }

    [Test]
    public void MaxRowsPerBlock_SetToNull_StaysNullRatherThanFallingBackToTheDefault()
    {
        // Null means "one block, whatever the row count", which is a different instruction from "not specified".
        // InsertAsync must therefore read the value off a default instance instead of coalescing it — a ?? would
        // turn this back into the default and silently re-enable splitting.
        Assert.That(new ClickHouseTcpInsertOptions { MaxRowsPerBlock = null }.MaxRowsPerBlock, Is.Null);
    }

    [Test]
    public void ResolveMaxRowsPerBlock_NoOptions_UsesTheDefaultCap()
    {
        Assert.That(ClickHouseTcpClient.ResolveMaxRowsPerBlock(null), Is.EqualTo(1_000_000));
    }

    [Test]
    public void ResolveMaxRowsPerBlock_OptionsWithoutTheCapSet_UsesTheDefaultCap()
    {
        Assert.That(ClickHouseTcpClient.ResolveMaxRowsPerBlock(new ClickHouseTcpInsertOptions()), Is.EqualTo(1_000_000));
    }

    [Test]
    public void ResolveMaxRowsPerBlock_ExplicitNullCap_StaysNullSoTheInsertIsOneBlock()
    {
        // The case a ?? would get wrong: "not specified" must take the default, but an explicit null must survive.
        Assert.That(ClickHouseTcpClient.ResolveMaxRowsPerBlock(new ClickHouseTcpInsertOptions { MaxRowsPerBlock = null }), Is.Null);
    }

    [Test]
    public void ResolveMaxRowsPerBlock_ExplicitCap_IsPassedThrough()
    {
        Assert.That(ClickHouseTcpClient.ResolveMaxRowsPerBlock(new ClickHouseTcpInsertOptions { MaxRowsPerBlock = 32 }), Is.EqualTo(32));
    }

    [Test]
    public void InsertOptions_IsUsableAsQueryOptions_SoTheSettingsMergeSeesThem()
    {
        var options = new ClickHouseTcpInsertOptions
        {
            QueryId = "insert-1",
            Settings = new Dictionary<string, string> { ["max_threads"] = "4" },
            MaxRowsPerBlock = 32,
        };

        ClickHouseTcpQueryOptions asQueryOptions = options;
        var merged = ClickHouseTcpClient.MergeSettings(clientSettings: null, asQueryOptions.Settings);

        Assert.Multiple(() =>
        {
            Assert.That(asQueryOptions.QueryId, Is.EqualTo("insert-1"));
            Assert.That(merged["max_threads"], Is.EqualTo("4"));
        });
    }
}
