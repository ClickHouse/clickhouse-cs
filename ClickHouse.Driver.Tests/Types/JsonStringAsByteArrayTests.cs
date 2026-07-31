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
/// Covers <c>ReadStringsAsByteArrays</c> combined with a <c>JSON</c> column.
/// </summary>
/// <remarks>
/// The flag exists because a ClickHouse <c>String</c> column is arbitrary bytes that need not be
/// valid UTF-8. A string *inside a JSON document* is different: JSON defines its strings as text
/// (RFC 8259), so the decoder always produces a <see cref="JsonValue"/> backed by
/// <see cref="string"/> regardless of the flag — same call the driver already makes for
/// <c>FixedString</c> paths. Before that was true, every string leaf fell through to
/// <c>JsonSerializer.SerializeToElement</c>, which renders a <c>byte[]</c> as base64, so
/// <c>GetValue&lt;string&gt;()</c> silently returned <c>"aW5mbw=="</c> instead of <c>"info"</c>.
/// </remarks>
[TestFixture]
public class JsonStringAsByteArrayTests : AbstractConnectionTestFixture
{
    // Strings in every position a JSON document can hold one: top level, inside a sub-object, and
    // inside an array. Deliberately includes punctuation that has to survive escaping, non-ASCII text
    // (which only round-trips if the bytes are decoded as UTF-8 rather than reinterpreted), and a
    // base64-shaped value that must pass through verbatim rather than being decoded.
    private const string SampleJson =
        "{\"event\":\"info\",\"props\":{\"region\":\"NYIp\",\"note\":\"|auN8}W2\"}," +
        "\"tags\":[\"p+q/r==\",\"li6Wu)\"],\"unicode\":\"héllo 世界\"," +
        "\"count\":42,\"ratio\":0.5,\"ok\":true}";

    private static ClickHouseClientSettings GetByteArraySettings() =>
        new(TestUtilities.GetTestClickHouseClientSettings()) { ReadStringsAsByteArrays = true };

    private static ClickHouseConnection CreateByteArrayConnection()
    {
        var connection = new ClickHouseConnection(GetByteArraySettings());
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
        using var connection = CreateByteArrayConnection();

        var result = await SelectJsonAsync(connection, $"SELECT '{SampleJson}'::Json");

        Assert.Multiple(() =>
        {
            Assert.That(result["event"].GetValue<string>(), Is.EqualTo("info"));
            Assert.That(result["props"]["region"].GetValue<string>(), Is.EqualTo("NYIp"));
            Assert.That(result["props"]["note"].GetValue<string>(), Is.EqualTo("|auN8}W2"));
            Assert.That(result["unicode"].GetValue<string>(), Is.EqualTo("héllo 世界"));
            Assert.That(
                result["tags"].AsArray().Select(node => node.GetValue<string>()),
                Is.EqualTo(new[] { "p+q/r==", "li6Wu)" }));

            // Non-string leaves were never affected by the flag; assert them so a future change to the
            // type switch cannot quietly reroute them through the JsonSerializer fallback either.
            Assert.That(result["count"].GetValue<long>(), Is.EqualTo(42));
            Assert.That(result["ratio"].GetValue<double>(), Is.EqualTo(0.5));
            Assert.That(result["ok"].GetValue<bool>(), Is.True);
        });
    }

    /// <summary>
    /// The inner CLR type is the exact discriminator between the two code paths: a string leaf decoded
    /// properly is backed by <see cref="string"/>, whereas one that fell through to the
    /// <c>JsonSerializer</c> fallback is backed by a <c>JsonElement</c>.
    /// </summary>
    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task ReadJson_WithReadStringsAsByteArrays_BacksStringLeavesWithString()
    {
        using var connection = CreateByteArrayConnection();

        var result = await SelectJsonAsync(connection, $"SELECT '{SampleJson}'::Json");

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

        using var connection = CreateByteArrayConnection();

        var result = await SelectJsonAsync(connection, $"SELECT '{json}'::Json({typeDefinition})");

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
    /// Map keys are read separately from map values, so they need their own case. Before the fix this
    /// threw <c>InvalidCastException</c> on the key rather than corrupting it — the key read casts the
    /// decoded value straight to <see cref="string"/>.
    /// </summary>
    [Test]
    [RequiredFeature(Feature.Json)]
    public async Task ReadJson_WithTypedMapOfStringsAndReadStringsAsByteArrays_ReturnsDecodedKeysAndValues()
    {
        const string json = "{\"attrs\":{\"colour\":\"green\",\"size\":\"large\"}}";

        using var connection = CreateByteArrayConnection();

        var result = await SelectJsonAsync(connection, $"SELECT '{json}'::Json(attrs Map(String, String))");

        var expected = new JsonObject { ["colour"] = "green", ["size"] = "large" };
        Assert.That(JsonNode.DeepEquals(result["attrs"], expected), Is.True,
            $"Expected: {expected.ToJsonString()}, Actual: {result["attrs"]?.ToJsonString()}");
    }

    /// <summary>
    /// Every JSON shape that can carry a string, in both hinted and dynamic form. An empty type
    /// definition means dynamic (unhinted) paths, where the path type arrives as a binary type code
    /// (<c>BinaryTypeDecoder</c>) instead of being parsed from the column definition
    /// (<c>TypeConverter</c>) — two separate construction sites, each honouring the flag
    /// independently, so both need covering.
    /// </summary>
    /// <remarks>
    /// Seven of these ten cases fail without the fix. The other three are regression guards rather
    /// than demonstrations of the bug, and it is worth knowing which is which: <c>FixedString</c> was
    /// already handled correctly, <c>NullString</c> short-circuits on the null arm, and
    /// <c>EmptyString</c> cannot discriminate at all because the base64 of zero bytes is also the
    /// empty string.
    /// </remarks>
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
    /// The broadest assertion available, and the one that generalizes past the shapes anyone thought
    /// to enumerate: reading the same row with the flag on and off must produce byte-identical JSON,
    /// because the flag is not supposed to reshape a JSON document at all.
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
    /// Read-then-write is where the read-path defect became persisted damage: base64 entered through
    /// the read, then a caller writing the same document back stored the base64 text server-side.
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
