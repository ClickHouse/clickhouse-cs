using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Tests.Client;

[TestFixture]
public class ClickHouseTcpClientSettingsTests
{
    private const string FlattenedSetting = "output_format_native_use_flattened_dynamic_and_json_serialization";

    [Test]
    public void MergeSettings_NoSettings_InjectsFlattenedSerializationByDefault()
    {
        var merged = ClickHouseTcpClient.MergeSettings(clientSettings: null, perQuerySettings: null);

        Assert.That(merged[FlattenedSetting], Is.EqualTo("1"));
    }

    [Test]
    public void MergeSettings_CallerSetsFlattened_CallerValueWins()
    {
        var perQuery = new Dictionary<string, string> { [FlattenedSetting] = "0" };

        var merged = ClickHouseTcpClient.MergeSettings(clientSettings: null, perQuery);

        Assert.That(merged[FlattenedSetting], Is.EqualTo("0"));
    }

    [Test]
    public void MergeSettings_ClientSetsFlattened_ClientValueNotOverwritten()
    {
        var client = new Dictionary<string, string> { [FlattenedSetting] = "0" };

        var merged = ClickHouseTcpClient.MergeSettings(client, perQuerySettings: null);

        Assert.That(merged[FlattenedSetting], Is.EqualTo("0"));
    }

    [Test]
    public void MergeSettings_PerQueryOverridesClientLevelForSameKey()
    {
        var client = new Dictionary<string, string> { ["max_threads"] = "4" };
        var perQuery = new Dictionary<string, string> { ["max_threads"] = "8" };

        var merged = ClickHouseTcpClient.MergeSettings(client, perQuery);

        Assert.That(merged["max_threads"], Is.EqualTo("8"));
    }

    [Test]
    public void MergeSettings_UnionsClientAndPerQueryKeys()
    {
        var client = new Dictionary<string, string> { ["max_threads"] = "4" };
        var perQuery = new Dictionary<string, string> { ["max_block_size"] = "1000" };

        var merged = ClickHouseTcpClient.MergeSettings(client, perQuery);

        Assert.Multiple(() =>
        {
            Assert.That(merged["max_threads"], Is.EqualTo("4"));
            Assert.That(merged["max_block_size"], Is.EqualTo("1000"));
            Assert.That(merged[FlattenedSetting], Is.EqualTo("1"));
        });
    }
}
