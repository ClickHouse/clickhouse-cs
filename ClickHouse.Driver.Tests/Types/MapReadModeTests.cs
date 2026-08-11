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

    private static ClickHouseClient CreateKeyValuePairClient() => new(KeyValuePairSettings());

    private static List<KeyValuePair<string, string>> AsPairs(object value) =>
        (List<KeyValuePair<string, string>>)value;

    [Test]
    public async Task ExecuteReaderAsync_MapWithDuplicateKeys_KeyValuePairsModeKeepsEveryPair()
    {
        using var pairClient = CreateKeyValuePairClient();

        using var reader = await pairClient.ExecuteReaderAsync("SELECT map('key', 'X', 'key', 'Y')");
        Assert.That(reader.Read(), Is.True);

        Assert.That(AsPairs(reader.GetValue(0)), Is.EqualTo(DuplicateKeyPairs));
    }

    [Test]
    public async Task GetFieldType_KeyValuePairsMode_ReportsListOfKeyValuePairs()
    {
        using var pairClient = CreateKeyValuePairClient();

        using var reader = await pairClient.ExecuteReaderAsync("SELECT map('key', 'X', 'key', 'Y')");
        Assert.That(reader.Read(), Is.True);

        Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(List<KeyValuePair<string, string>>)));
        Assert.That(reader.GetFieldValue<List<KeyValuePair<string, string>>>(0), Is.EqualTo(DuplicateKeyPairs));
    }

    [Test]
    public async Task ExecuteReaderAsync_NestedMapWithDuplicateKeys_KeyValuePairsModeKeepsEveryPair()
    {
        // A map is reached through any composite type, so the representation must follow it there
        using var pairClient = CreateKeyValuePairClient();

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
        using var pairClient = CreateKeyValuePairClient();

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
        using var pairClient = CreateKeyValuePairClient();

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
        using var pairClient = CreateKeyValuePairClient();

        using var reader = await pairClient.ExecuteReaderAsync("SELECT map('key', 'X', 'key', 'Y')::Dynamic");
        Assert.That(reader.Read(), Is.True);

        Assert.That(AsPairs(reader.GetValue(0)), Is.EqualTo(DuplicateKeyPairs));
    }

    [Test]
    public async Task InsertBinaryAsync_KeyValuePairsWithDuplicateKeys_RoundTripsEveryPair()
    {
        // The value read in KeyValuePairs mode must be writable back without losing a pair
        using var pairClient = CreateKeyValuePairClient();
        var table = CreateTableName();
        await pairClient.ExecuteNonQueryAsync($"CREATE TABLE {table} (m Map(String, String)) ENGINE Memory");

        await pairClient.InsertBinaryAsync(table, ["m"], [new object[] { DuplicateKeyPairs }]);

        using var reader = await pairClient.ExecuteReaderAsync($"SELECT m FROM {table}");
        Assert.That(reader.Read(), Is.True);
        Assert.That(AsPairs(reader.GetValue(0)), Is.EqualTo(DuplicateKeyPairs));
    }

    [Test]
    public async Task ExecuteReaderAsync_KeyValuePairsParameterWithDuplicateKeys_SendsEveryPair()
    {
        using var pairClient = CreateKeyValuePairClient();
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
