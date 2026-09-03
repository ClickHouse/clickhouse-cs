using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Tests.Client;

[TestFixture]
public class ClickHouseTcpInsertOptionsTests
{
    [Test]
    public void MaxRowsPerBlock_NotSet_DefaultsToTheBlockRowCap()
    {
        Assert.That(new ClickHouseTcpInsertOptions().MaxRowsPerBlock, Is.EqualTo(50_000));
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
        Assert.That(ClickHouseTcpClient.ResolveMaxRowsPerBlock(null), Is.EqualTo(50_000));
    }

    [Test]
    public void ResolveMaxRowsPerBlock_OptionsWithoutTheCapSet_UsesTheDefaultCap()
    {
        Assert.That(ClickHouseTcpClient.ResolveMaxRowsPerBlock(new ClickHouseTcpInsertOptions()), Is.EqualTo(50_000));
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
    public void With_OnAQueryOptionsTypedReference_KeepsTheRuntimeTypeAndTheInsertOnlyKnobs()
    {
        // `with` clones through the record's virtual clone method, so it copies the runtime type rather than the
        // static one. Deriving a per-query variant through a base-typed reference must therefore not quietly
        // downgrade an insert options instance and reset MaxRowsPerBlock to its default.
        ClickHouseTcpQueryOptions asQueryOptions = new ClickHouseTcpInsertOptions
        {
            QueryId = "insert-1",
            MaxRowsPerBlock = 32,
        };

        var derived = asQueryOptions with { QueryId = "insert-2" };

        Assert.Multiple(() =>
        {
            Assert.That(derived, Is.InstanceOf<ClickHouseTcpInsertOptions>());
            Assert.That(derived.QueryId, Is.EqualTo("insert-2"));
            Assert.That(((ClickHouseTcpInsertOptions)derived).MaxRowsPerBlock, Is.EqualTo(32));
        });
    }

    [Test]
    public void With_SettingTheCapToNull_KeepsTheExplicitNullRatherThanTheDefault()
    {
        // The explicit-null instruction ("one block, whatever the row count") must survive a clone, since a default
        // MaxRowsPerBlock would silently re-enable splitting.
        var derived = new ClickHouseTcpInsertOptions { QueryId = "q" } with { MaxRowsPerBlock = null };

        Assert.That(ClickHouseTcpClient.ResolveMaxRowsPerBlock(derived), Is.Null);
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
