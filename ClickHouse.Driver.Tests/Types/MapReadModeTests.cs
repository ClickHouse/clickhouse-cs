using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

/// <summary>
/// A ClickHouse Map is physically an Array(Tuple(K, V)) and may repeat a key. Only
/// <see cref="MapReadMode.KeyValuePairs"/> can represent such a map; the default
/// <see cref="MapReadMode.Dictionary"/> keeps the last pair of each key.
/// </summary>
[TestFixture]
public class MapReadModeTests : AbstractConnectionTestFixture
{
    private static readonly List<KeyValuePair<string, string>> DuplicateKeyPairs =
    [
        new("key", "X"),
        new("key", "Y"),
    ];

    private static ClickHouseClientSettings KeyValuePairSettings()
    {
        var builder = TestUtilities.GetConnectionStringBuilder();
        builder.MapReadMode = MapReadMode.KeyValuePairs;
        return new ClickHouseClientSettings(builder);
    }

    /// <summary>
    /// Every test here reads in the same mode, so one client is shared: a client is thread-safe and
    /// owns its own connection pool, so one per test would only prevent connection reuse.
    /// </summary>
    private ClickHouseClient pairClient;

    [OneTimeSetUp]
    public void CreatePairClient() => pairClient = new ClickHouseClient(KeyValuePairSettings());

    [OneTimeTearDown]
    public void DisposePairClient()
    {
        pairClient?.Dispose();
        pairClient = null;
    }

    private static List<KeyValuePair<string, string>> AsPairs(object value) =>
        (List<KeyValuePair<string, string>>)value;

    [Test]
    public async Task ExecuteReaderAsync_MapWithDuplicateKeys_KeyValuePairsModeKeepsEveryPair()
    {
        using var reader = await pairClient.ExecuteReaderAsync("SELECT map('key', 'X', 'key', 'Y')");
        Assert.That(reader.Read(), Is.True);

        Assert.That(AsPairs(reader.GetValue(0)), Is.EqualTo(DuplicateKeyPairs));
    }

    [Test]
    public async Task GetFieldType_KeyValuePairsMode_ReportsListOfKeyValuePairs()
    {
        using var reader = await pairClient.ExecuteReaderAsync("SELECT map('key', 'X', 'key', 'Y')");
        Assert.That(reader.Read(), Is.True);

        Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(List<KeyValuePair<string, string>>)));
        Assert.That(reader.GetFieldValue<List<KeyValuePair<string, string>>>(0), Is.EqualTo(DuplicateKeyPairs));
    }

    [Test]
    public async Task ExecuteReaderAsync_NestedMapWithDuplicateKeys_KeyValuePairsModeKeepsEveryPair()
    {
        // A map is reached through any composite type, so the representation must follow it there
        using var reader = await pairClient.ExecuteReaderAsync(
            "SELECT [map('key', 'X', 'key', 'Y')], map('outer', map('key', 'X', 'key', 'Y')), tuple('t', map('key', 'X', 'key', 'Y'))");
        Assert.That(reader.Read(), Is.True);

        var inArray = (IList)reader.GetValue(0);
        Assert.That(AsPairs(inArray[0]), Is.EqualTo(DuplicateKeyPairs));

        var outerMap = (IList)reader.GetValue(1);
        var outerPair = (KeyValuePair<string, List<KeyValuePair<string, string>>>)outerMap[0];
        Assert.That(outerPair.Value, Is.EqualTo(DuplicateKeyPairs));

        var tuple = (ITuple)reader.GetValue(2);
        Assert.That(AsPairs(tuple[1]), Is.EqualTo(DuplicateKeyPairs));
    }

    [Test]
    public async Task ExecuteReaderAsync_MapWithNullValues_KeyValuePairsModeReturnsNullNotDBNull()
    {
        using var reader = await pairClient.ExecuteReaderAsync(
            "SELECT CAST(map('key', NULL, 'key', 'Y'), 'Map(String, Nullable(String))')");
        Assert.That(reader.Read(), Is.True);

        Assert.That(AsPairs(reader.GetValue(0)), Is.EqualTo(new List<KeyValuePair<string, string>>
        {
            new("key", null),
            new("key", "Y"),
        }));
    }

    [Test]
    public async Task ExecuteReaderAsync_MapWithValueTypeKeysAndNullableValues_KeyValuePairsModeKeepsEveryPair()
    {
        // Value-type keys and values go through a different unboxing conversion than strings
        using var reader = await pairClient.ExecuteReaderAsync(
            "SELECT CAST(map(1, 10, 1, NULL), 'Map(Int32, Nullable(Int32))'), CAST(map(), 'Map(Int32, Int32)')");
        Assert.That(reader.Read(), Is.True);

        Assert.That(reader.GetValue(0), Is.EqualTo(new List<KeyValuePair<int, int?>>
        {
            new(1, 10),
            new(1, null),
        }));
        Assert.That(reader.GetValue(1), Is.Empty);
    }

    [Test]
    [RequiredFeature(Feature.Dynamic)]
    public async Task ExecuteReaderAsync_MapInsideDynamicColumn_KeyValuePairsModeKeepsEveryPair()
    {
        // The Dynamic read path decodes the type from its binary description, not from a header
        using var reader = await pairClient.ExecuteReaderAsync("SELECT map('key', 'X', 'key', 'Y')::Dynamic");
        Assert.That(reader.Read(), Is.True);

        Assert.That(AsPairs(reader.GetValue(0)), Is.EqualTo(DuplicateKeyPairs));
    }

    [Test]
    public async Task InsertBinaryAsync_KeyValuePairsWithDuplicateKeys_RoundTripsEveryPair()
    {
        // The value read in KeyValuePairs mode must be writable back without losing a pair
        var table = CreateTableName();
        await pairClient.ExecuteNonQueryAsync($"CREATE TABLE {table} (m Map(String, String)) ENGINE Memory");

        await pairClient.InsertBinaryAsync(table, ["m"], [new object[] { DuplicateKeyPairs }]);

        using var reader = await pairClient.ExecuteReaderAsync($"SELECT m FROM {table}");
        Assert.That(reader.Read(), Is.True);
        Assert.That(AsPairs(reader.GetValue(0)), Is.EqualTo(DuplicateKeyPairs));
    }

    [Test]
    [RequiredFeature(Feature.Variant)]
    public async Task InsertBinaryAsync_VariantMapInKeyValuePairsMode_AcceptsDictionary()
    {
        // A Variant member is picked by CanWrite, so it must accept every representation
        // Write accepts - a dictionary stays writable in KeyValuePairs mode
        var table = CreateTableName();
        await pairClient.ExecuteNonQueryAsync($"CREATE TABLE {table} (v Variant(String, Map(String, String))) ENGINE Memory");

        await pairClient.InsertBinaryAsync(table, ["v"], [new object[] { new Dictionary<string, string> { ["key"] = "X" } }]);

        using var reader = await pairClient.ExecuteReaderAsync($"SELECT v FROM {table}");
        Assert.That(reader.Read(), Is.True);
        Assert.That(AsPairs(reader.GetValue(0)), Is.EqualTo(new List<KeyValuePair<string, string>> { new("key", "X") }));
    }

    [Test]
    [RequiredFeature(Feature.Variant)]
    public async Task InsertBinaryAsync_VariantMapInDictionaryMode_AcceptsKeyValuePairs()
    {
        // The mirror case - a key-value-pair sequence is writable in the default mode
        var table = CreateTableName();
        await client.ExecuteNonQueryAsync($"CREATE TABLE {table} (v Variant(String, Map(String, String))) ENGINE Memory");

        await client.InsertBinaryAsync(table, ["v"], [new object[] { DuplicateKeyPairs }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT v FROM {table}");
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetValue(0), Is.EqualTo(new Dictionary<string, string> { ["key"] = "Y" }));
    }

    [Test]
    [RequiredFeature(Feature.Variant)]
    public async Task InsertBinaryAsync_VariantWithTwoMapMembers_PicksTheMemberMatchingTheEntryTypes()
    {
        // Accepting both representations must not make every map match every map member.
        // Three members also exercise the type-keyed lookup, not only the linear scan
        var table = CreateTableName();
        await pairClient.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id UInt32, v Variant(String, Map(String, String), Map(String, Int64))) ENGINE Memory");

        await pairClient.InsertBinaryAsync(table, ["id", "v"], [
            new object[] { 1u, new Dictionary<string, long> { ["key"] = 1L } },
            new object[] { 2u, new List<KeyValuePair<string, long>> { new("key", 2L) } },
            new object[] { 3u, new Dictionary<string, string> { ["key"] = "X" } },
        ]);

        using var reader = await pairClient.ExecuteReaderAsync($"SELECT variantType(v) FROM {table} ORDER BY id");
        var members = new List<object>();
        while (reader.Read())
            members.Add(reader.GetValue(0));

        Assert.That(members, Is.EqualTo(new[] { "Map(String, Int64)", "Map(String, Int64)", "Map(String, String)" }));
    }

    [TestCase(MapReadMode.Dictionary)]
    [TestCase(MapReadMode.KeyValuePairs)]
    public void CanWrite_MapMemberOfVariant_AcceptsBothRepresentationsOfItsOwnEntryTypes(MapReadMode mode)
    {
        var settings = TypeSettings.Default with { mapReadMode = mode };
        var mapType = (MapType)TypeConverter.ParseClickHouseType("Map(String, String)", settings);

        Assert.That(mapType.CanWrite(new Dictionary<string, string>()), Is.True);
        Assert.That(mapType.CanWrite(DuplicateKeyPairs), Is.True);

        // A map with other entry types is still not this member, and neither is a non-map
        Assert.That(mapType.CanWrite(new Dictionary<string, long>()), Is.False);
        Assert.That(mapType.CanWrite(new List<KeyValuePair<string, long>>()), Is.False);
        Assert.That(mapType.CanWrite("string"), Is.False);
        Assert.That(mapType.CanWrite(null), Is.False);
    }

    [Test]
    public async Task ExecuteReaderAsync_KeyValuePairsParameterWithDuplicateKeys_SendsEveryPair()
    {
        using var connectionWithPairs = pairClient.CreateConnection();
        using var command = connectionWithPairs.CreateCommand();
        command.CommandText = "SELECT {map:Map(String, String)}";
        command.AddParameter("map", DuplicateKeyPairs);

        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);

        Assert.That(AsPairs(reader.GetValue(0)), Is.EqualTo(DuplicateKeyPairs));
    }

    [Test]
    public void ToClickHouseType_ListOfKeyValuePairs_InfersMap()
    {
        // Without this, a value read in KeyValuePairs mode cannot be bound as a parameter
        // unless the query carries an explicit Map type hint
        Assert.That(TypeConverter.ToClickHouseType(DuplicateKeyPairs).ToString(), Is.EqualTo("Map(String, String)"));
        Assert.That(TypeConverter.ToClickHouseType(new List<KeyValuePair<string, string>>()).ToString(), Is.EqualTo("Map(String, String)"));
    }

    [Test]
    public void MapReadMode_ConnectionString_IsReadAndWritten()
    {
        Assert.That(new ClickHouseConnectionStringBuilder("Host=localhost").MapReadMode, Is.EqualTo(MapReadMode.Dictionary));
        Assert.That(new ClickHouseConnectionStringBuilder("Host=localhost;MapReadMode=KeyValuePairs").MapReadMode, Is.EqualTo(MapReadMode.KeyValuePairs));

        var settings = new ClickHouseClientSettings("Host=localhost") { MapReadMode = MapReadMode.KeyValuePairs };
        Assert.That(ClickHouseConnectionStringBuilder.FromSettings(settings).ToSettings().MapReadMode, Is.EqualTo(MapReadMode.KeyValuePairs));
    }
}
