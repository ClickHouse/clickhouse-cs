using System;
using System.Collections.Generic;

namespace ClickHouse.Driver.Tcp.Tests.Client;

[TestFixture]
public class ClickHouseTcpClientSettingsTests
{
    private const string FlattenedSetting = "output_format_native_use_flattened_dynamic_and_json_serialization";
    private const string JsonAsStringSetting = "output_format_native_write_json_as_string";

    // The client injects both serialization settings so a caller never has to know about them: the flattened one
    // for Dynamic, the text one for JSON. Each is overridable at either level — turning one off is how a caller
    // asks for an encoding this client does not decode, so the override has to reach the server rather than being
    // silently forced back on.
    [TestCase(FlattenedSetting)]
    [TestCase(JsonAsStringSetting)]
    public void MergeSettings_NoSettings_InjectsTheSerializationSettingByDefault(string setting)
    {
        var merged = ClickHouseTcpClient.MergeSettings(clientSettings: null, perQuerySettings: null);

        Assert.That(merged[setting], Is.EqualTo("1"));
    }

    [TestCase(FlattenedSetting)]
    [TestCase(JsonAsStringSetting)]
    public void MergeSettings_PerQuerySetsTheSerializationSetting_CallerValueWins(string setting)
    {
        var perQuery = new Dictionary<string, string> { [setting] = "0" };

        var merged = ClickHouseTcpClient.MergeSettings(clientSettings: null, perQuery);

        Assert.That(merged[setting], Is.EqualTo("0"));
    }

    [TestCase(FlattenedSetting)]
    [TestCase(JsonAsStringSetting)]
    public void MergeSettings_ClientSetsTheSerializationSetting_ClientValueNotOverwritten(string setting)
    {
        var client = new Dictionary<string, string> { [setting] = "0" };

        var merged = ClickHouseTcpClient.MergeSettings(client, perQuerySettings: null);

        Assert.That(merged[setting], Is.EqualTo("0"));
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
    public void MergeSettings_PerQuerySettingWithEmptyName_ThrowsArgumentException()
    {
        // Per-query settings bypass the construction-time validation of the client-level ones, so the merge is the
        // last point before the Query packet where an empty name can be rejected instead of truncating the wire
        // settings list.
        var perQuery = new Dictionary<string, string> { [string.Empty] = "1" };

        Assert.Throws<ArgumentException>(() => ClickHouseTcpClient.MergeSettings(clientSettings: null, perQuery));
    }

    [Test]
    public void MergeSettings_PerQuerySettingWithNullValue_ThrowsArgumentException()
    {
        var perQuery = new Dictionary<string, string> { ["max_threads"] = null };

        Assert.Throws<ArgumentException>(() => ClickHouseTcpClient.MergeSettings(clientSettings: null, perQuery));
    }

    [Test]
    public void MergeSettings_PerQuerySettingWithEmptyValue_IsAccepted()
    {
        var perQuery = new Dictionary<string, string> { ["some_flag"] = string.Empty };

        var merged = ClickHouseTcpClient.MergeSettings(clientSettings: null, perQuery);

        Assert.That(merged["some_flag"], Is.EqualTo(string.Empty));
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
