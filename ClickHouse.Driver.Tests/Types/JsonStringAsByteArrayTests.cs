using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Utility;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace ClickHouse.Driver.Tests.Types;

/// <summary>
/// Covers <c>ReadStringsAsByteArrays</c> combined with a <c>JSON</c> column. String leaves used to
/// fall through to <c>JsonSerializer.SerializeToElement</c>, which renders a <c>byte[]</c> as base64,
/// so <c>GetValue&lt;string&gt;()</c> silently returned <c>"aW5mbw=="</c> instead of <c>"info"</c>.
/// </summary>
[TestFixture]
public class JsonStringAsByteArrayTests : AbstractConnectionTestFixture
{
    // Strings at top level, in a sub-object and in an array, including punctuation that must survive
    // escaping, non-ASCII text, and a base64-shaped value that must pass through verbatim.
    private const string SampleJson =
        "{\"event\":\"info\",\"props\":{\"region\":\"NYIp\",\"note\":\"|auN8}W2\"}," +
        "\"tags\":[\"p+q/r==\",\"li6Wu)\"],\"unicode\":\"héllo 世界\"," +
        "\"count\":42,\"ratio\":0.5,\"ok\":true}";

    private static ClickHouseClientSettings GetByteArraySettings(JsonReadMode readMode = JsonReadMode.Binary) =>
        new(TestUtilities.GetTestClickHouseClientSettings(jsonReadMode: readMode)) { ReadStringsAsByteArrays = true };

    private static ClickHouseConnection CreateByteArrayConnection(JsonReadMode readMode = JsonReadMode.Binary)
    {
        var connection = new ClickHouseConnection(GetByteArraySettings(readMode));
        connection.Open();
        return connection;
    }

    private static async Task<JsonObject> SelectJsonAsync(ClickHouseConnection connection, string sql)
    {
        using var reader = await connection.ExecuteReaderAsync(sql);
        ClassicAssert.IsTrue(reader.Read());
        return (JsonObject)reader.GetValue(0);
    }

    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task ReadJson_WithDynamicStringPathsAndReadStringsAsByteArrays_ReturnsDecodedStrings()
    {
        using var byteArrayConnection = CreateByteArrayConnection();

        var result = await SelectJsonAsync(byteArrayConnection, $"SELECT '{SampleJson}'::Json");

        Assert.Multiple(() =>
        {
            Assert.That(result["event"].GetValue<string>(), Is.EqualTo("info"));
            Assert.That(result["props"]["region"].GetValue<string>(), Is.EqualTo("NYIp"));
            Assert.That(result["props"]["note"].GetValue<string>(), Is.EqualTo("|auN8}W2"));
            Assert.That(result["unicode"].GetValue<string>(), Is.EqualTo("héllo 世界"));
            Assert.That(
                result["tags"].AsArray().Select(node => node.GetValue<string>()),
                Is.EqualTo(new[] { "p+q/r==", "li6Wu)" }));

            // Non-string leaves were never affected; pinned so they cannot be rerouted either.
            Assert.That(result["count"].GetValue<long>(), Is.EqualTo(42));
            Assert.That(result["ratio"].GetValue<double>(), Is.EqualTo(0.5));
            Assert.That(result["ok"].GetValue<bool>(), Is.True);
        });
    }

    /// <summary>
    /// The backing CLR type discriminates the two paths exactly: a properly decoded leaf is backed by
    /// <see cref="string"/>, one that fell through to <c>JsonSerializer</c> by a <c>JsonElement</c>.
    /// </summary>
    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task ReadJson_WithReadStringsAsByteArrays_BacksStringLeavesWithString()
    {
        using var byteArrayConnection = CreateByteArrayConnection();

        var result = await SelectJsonAsync(byteArrayConnection, $"SELECT '{SampleJson}'::Json");

        Assert.Multiple(() =>
        {
            Assert.That(result["event"].GetValue<object>(), Is.TypeOf<string>());
            Assert.That(result["props"]["region"].GetValue<object>(), Is.TypeOf<string>());
            Assert.That(result["tags"][0].GetValue<object>(), Is.TypeOf<string>());
        });
    }

    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task ReadJson_WithTypedStringPathsAndReadStringsAsByteArrays_ReturnsDecodedStrings()
    {
        const string json = "{\"plain\":\"a\",\"low\":\"b\",\"maybe\":\"c\",\"fixed\":\"exact\",\"list\":[\"d\",\"e\"]}";
        const string typeDefinition =
            "plain String, low LowCardinality(String), maybe Nullable(String), " +
            "fixed FixedString(5), list Array(String)";

        using var byteArrayConnection = CreateByteArrayConnection();

        var result = await SelectJsonAsync(byteArrayConnection, $"SELECT '{json}'::Json({typeDefinition})");

        Assert.Multiple(() =>
        {
            Assert.That(result["plain"].GetValue<string>(), Is.EqualTo("a"));
            Assert.That(result["low"].GetValue<string>(), Is.EqualTo("b"));
            Assert.That(result["maybe"].GetValue<string>(), Is.EqualTo("c"));
            Assert.That(result["fixed"].GetValue<string>(), Is.EqualTo("exact"));
            Assert.That(
                result["list"].AsArray().Select(node => node.GetValue<string>()),
                Is.EqualTo(new[] { "d", "e" }));
        });
    }

    /// <summary>
    /// Map keys are read separately from values and were cast straight to <see cref="string"/>, so
    /// before the fix they threw <c>InvalidCastException</c> rather than corrupting.
    /// </summary>
    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task ReadJson_WithTypedMapOfStringsAndReadStringsAsByteArrays_ReturnsDecodedKeysAndValues()
    {
        const string json = "{\"attrs\":{\"colour\":\"green\",\"size\":\"large\"}}";

        using var byteArrayConnection = CreateByteArrayConnection();

        var result = await SelectJsonAsync(byteArrayConnection, $"SELECT '{json}'::Json(attrs Map(String, String))");

        var expected = new JsonObject { ["colour"] = "green", ["size"] = "large" };
        Assert.That(JsonNode.DeepEquals(result["attrs"], expected), Is.True,
            $"Expected: {expected.ToJsonString()}, Actual: {result["attrs"]?.ToJsonString()}");
    }

    /// <summary>
    /// <c>None</c> differs from <c>Binary</c> only in not sending the server-side format setting, so it
    /// decodes through the same path and carried the same bug.
    /// </summary>
    [Test]
    [RequiredFeature(Feature.Json)]
    [TestCase(JsonReadMode.Binary, TestName = "BinaryReadMode")]
    [TestCase(JsonReadMode.None, TestName = "NoneReadMode")]
    public async Task ReadJson_WithReadStringsAsByteArrays_DecodesStringsInEveryStructuredReadMode(JsonReadMode readMode)
    {
        using var byteArrayConnection = CreateByteArrayConnection(readMode);

        var result = await SelectJsonAsync(byteArrayConnection, $"SELECT '{SampleJson}'::Json");

        Assert.Multiple(() =>
        {
            Assert.That(result["event"].GetValue<string>(), Is.EqualTo("info"));
            Assert.That(result["props"]["region"].GetValue<string>(), Is.EqualTo("NYIp"));
            Assert.That(result["tags"][0].GetValue<string>(), Is.EqualTo("p+q/r=="));
        });
    }

    /// <summary>
    /// <see cref="JsonReadMode.String"/> returns the whole document via <c>ReadString()</c> before any
    /// per-path type dispatch, so the setting cannot reach it — it has always returned text. Pinned
    /// because it is the precedent for this change, not just a footnote.
    /// </summary>
    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task ReadJson_InStringReadMode_IsUnaffectedByReadStringsAsByteArrays()
    {
        using var byteArrayConnection = CreateByteArrayConnection(JsonReadMode.String);

        using var reader = await byteArrayConnection.ExecuteReaderAsync($"SELECT '{SampleJson}'::Json");
        ClassicAssert.IsTrue(reader.Read());
        var value = reader.GetValue(0);

        Assert.That(value, Is.TypeOf<string>());
        Assert.That(JsonNode.Parse((string)value)["event"].GetValue<string>(), Is.EqualTo("info"));
    }

    /// <summary>
    /// <c>Array(UInt8)</c> also materializes as a <c>byte[]</c>, so decoding must key off the ClickHouse
    /// type. Asserted against literal values rather than flag-on-vs-flag-off: these paths read
    /// identically under both settings, so the equality test below cannot catch a regression here. The
    /// expected base64 is the pre-existing output for a byte array reached through a wrapper.
    /// </summary>
    [Test]
    [RequiredFeature(Feature.Json | Feature.Variant)]
    [TestCase("{\"v\":[1,2]}", "v Variant(Array(UInt8), String)", "AQI=", TestName = "VariantOfByteArrayAndString")]
    [TestCase("{\"v\":[1,2]}", "v Variant(Array(UInt8), UInt64)", "AQI=", TestName = "VariantOfByteArrayAndInt")]
    [TestCase("{\"v\":[255,254]}", "v Variant(Array(UInt8), String)", "//4=", TestName = "VariantOfByteArrayNotValidUtf8")]
    [TestCase("{\"v\":[1,2]}", "v SimpleAggregateFunction(anyLast, Array(UInt8))", "AQI=", TestName = "SimpleAggregateFunctionOfByteArray")]
    public async Task ReadJson_WithNonTextByteArrayPath_IsNotDecodedAsText(string json, string typeDefinition, string expected)
    {
        using var byteArrayConnection = CreateByteArrayConnection();
        var sql = $"SELECT '{json}'::Json({typeDefinition})";

        var withFlag = await SelectJsonAsync(byteArrayConnection, sql);
        var withoutFlag = await SelectJsonAsync(connection, sql);

        Assert.Multiple(() =>
        {
            Assert.That(withFlag["v"].GetValue<string>(), Is.EqualTo(expected));
            Assert.That(withoutFlag["v"].GetValue<string>(), Is.EqualTo(expected));
        });
    }

    /// <summary>
    /// Every shape that can carry a string, hinted and dynamic. An empty type definition means dynamic
    /// paths, whose types come from <c>BinaryTypeDecoder</c> rather than <c>TypeConverter</c> — two
    /// construction sites, each honouring the setting independently. Seven of the ten fail without the
    /// fix; <c>FixedString</c> (already correct), <c>NullString</c> (null arm) and <c>EmptyString</c>
    /// (base64 of zero bytes is also empty) are regression guards that cannot discriminate.
    /// </summary>
    public static IEnumerable<TestCaseData> StringBearingJsonShapes()
    {
        yield return new TestCaseData(SampleJson, "").SetName("DynamicPaths");
        yield return new TestCaseData("{\"s\":\"plain\"}", "s String").SetName("String");
        yield return new TestCaseData("{\"s\":\"\"}", "s String").SetName("EmptyString");
        yield return new TestCaseData("{\"s\":null}", "s Nullable(String)").SetName("NullString");
        yield return new TestCaseData("{\"s\":\"low\"}", "s LowCardinality(String)").SetName("LowCardinalityString");
        yield return new TestCaseData("{\"s\":\"exact\"}", "s FixedString(5)").SetName("FixedString");
        yield return new TestCaseData("{\"a\":[\"x\",\"y\"]}", "a Array(String)").SetName("ArrayOfString");
        yield return new TestCaseData("{\"a\":[[\"x\",\"y\"],[\"z\"]]}", "a Array(Array(String))").SetName("NestedArrayOfString");
        yield return new TestCaseData("{\"m\":{\"k1\":\"v1\",\"k2\":\"v2\"}}", "m Map(String, String)").SetName("MapOfString");
        yield return new TestCaseData("{\"m\":{\"k\":[\"v1\",\"v2\"]}}", "m Map(String, Array(String))").SetName("MapOfArrayOfString");
    }

    /// <summary>
    /// The broadest assertion, and the one that generalizes past the enumerated shapes: the setting is
    /// not supposed to reshape a JSON document, so flag-on and flag-off output must be identical.
    /// </summary>
    [Test]
    [RequiredFeature(Feature.Json)]
    [TestCaseSource(nameof(StringBearingJsonShapes))]
    public async Task ReadJson_WithReadStringsAsByteArrays_MatchesReadWithoutIt(string json, string typeDefinition)
    {
        var sql = $"SELECT '{json}'::Json({typeDefinition})";

        using var byteArrayConnection = CreateByteArrayConnection();
        var withFlag = (await SelectJsonAsync(byteArrayConnection, sql)).ToJsonString();
        var withoutFlag = (await SelectJsonAsync(connection, sql)).ToJsonString();

        Assert.That(withFlag, Is.EqualTo(withoutFlag));
    }

    /// <summary>
    /// Read-then-write is where the read defect became persisted damage: base64 entered on the read,
    /// then a caller writing the document back stored it server-side.
    /// </summary>
    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task InsertJson_ReadWithReadStringsAsByteArrays_RoundTripsWithoutCorruption()
    {
        var targetTable = CreateTableName();
        await connection.ExecuteStatementAsync(
            $"CREATE TABLE {targetTable} (id UInt32, data JSON) ENGINE = Memory");
        await connection.ExecuteStatementAsync($"INSERT INTO {targetTable} VALUES (1, '{SampleJson}')");

        JsonObject read;
        using (var byteArrayClient = new ClickHouseClient(GetByteArraySettings()))
        {
            read = (JsonObject)await byteArrayClient.ExecuteScalarAsync(
                $"SELECT data FROM {targetTable} WHERE id = 1");
        }

        // Write the freshly read document straight back, exactly as an application would.
        await client.InsertBinaryAsync(targetTable, ["id", "data"], [new object[] { 2u, read }]);

        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT data FROM {targetTable} ORDER BY id");
        ClassicAssert.IsTrue(reader.Read());
        var original = (JsonObject)reader.GetValue(0);
        ClassicAssert.IsTrue(reader.Read());
        var roundTripped = (JsonObject)reader.GetValue(0);

        Assert.That(JsonNode.DeepEquals(roundTripped, original), Is.True,
            $"Expected: {original.ToJsonString()}, Actual: {roundTripped.ToJsonString()}");
    }
}
