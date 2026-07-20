using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tests.Copy;

/// <summary>
/// Materializing a ClickHouse <c>Map(K,V)</c> column into a POCO property typed as a list/array of
/// <see cref="KeyValuePair{TKey,TValue}"/> (via the QueryAsync fast path), and the reverse on binary insert.
/// The list form preserves wire order (and duplicate keys); <see cref="Dictionary{TKey,TValue}"/> still works.
/// </summary>
[TestFixture]
public class PocoMapTupleTests : AbstractConnectionTestFixture
{
    private string CreateTestTableName([CallerMemberName] string testName = null)
        => SanitizeTableName($"test_pocomap_{testName}_{Guid.NewGuid():N}");

    // map(...) literal preserves argument order, so the on-wire order is 1, 2, 3.
    private const string MapProjection = "SELECT map(toInt64(1), 'a', toInt64(2), 'b', toInt64(3), 'c') AS Attrs";

    public class MapListPoco
    {
        public List<KeyValuePair<long, string>> Attrs { get; set; }
    }

    public class MapArrayPoco
    {
        public KeyValuePair<long, string>[] Attrs { get; set; }
    }

    public class MapDictionaryPoco
    {
        public Dictionary<long, string> Attrs { get; set; }
    }

    [Test]
    public async Task QueryAsync_MapColumn_MaterializesAsKeyValuePairList_PreservingOrder()
    {
        client.RegisterPocoType<MapListPoco>();

        MapListPoco row = null;
        await foreach (var r in client.QueryAsync<MapListPoco>(MapProjection))
            row = r;

        Assert.That(row.Attrs, Is.EqualTo(new[]
        {
            new KeyValuePair<long, string>(1, "a"),
            new KeyValuePair<long, string>(2, "b"),
            new KeyValuePair<long, string>(3, "c"),
        }));
    }

    [Test]
    public async Task QueryAsync_MapColumn_MaterializesAsKeyValuePairArray()
    {
        client.RegisterPocoType<MapArrayPoco>();

        MapArrayPoco row = null;
        await foreach (var r in client.QueryAsync<MapArrayPoco>(MapProjection))
            row = r;

        Assert.That(row.Attrs, Is.EqualTo(new[]
        {
            new KeyValuePair<long, string>(1, "a"),
            new KeyValuePair<long, string>(2, "b"),
            new KeyValuePair<long, string>(3, "c"),
        }));
    }

    [Test]
    public async Task QueryAsync_MapColumn_DictionaryPropertyStillWorks()
    {
        client.RegisterPocoType<MapDictionaryPoco>();

        MapDictionaryPoco row = null;
        await foreach (var r in client.QueryAsync<MapDictionaryPoco>(MapProjection))
            row = r;

        Assert.That(row.Attrs, Is.EquivalentTo(new Dictionary<long, string> { [1] = "a", [2] = "b", [3] = "c" }));
    }

    public class MapRoundTripPoco
    {
        public long Id { get; set; }
        public List<KeyValuePair<long, string>> Attrs { get; set; }
    }

    [Test]
    public async Task InsertBinaryAsync_KeyValuePairListProperty_RoundTripsToMapColumn()
    {
        var tableName = CreateTestTableName();
        try
        {
            await client.ExecuteNonQueryAsync(
                $"CREATE TABLE IF NOT EXISTS test.{tableName} (Id Int64, Attrs Map(Int64, String)) " +
                "ENGINE = MergeTree() ORDER BY Id");

            client.RegisterPocoType<MapRoundTripPoco>();

            var row = new MapRoundTripPoco
            {
                Id = 1,
                Attrs = new List<KeyValuePair<long, string>>
                {
                    new(10, "ten"),
                    new(20, "twenty"),
                },
            };

            var inserted = await client.InsertBinaryAsync(
                tableName, new[] { row }, new InsertOptions { Database = "test" });
            Assert.That(inserted, Is.EqualTo(1));

            MapRoundTripPoco readBack = null;
            await foreach (var r in client.QueryAsync<MapRoundTripPoco>(
                $"SELECT Id, Attrs FROM test.{tableName}"))
                readBack = r;

            Assert.That(readBack.Id, Is.EqualTo(1L));
            Assert.That(readBack.Attrs, Is.EquivalentTo(row.Attrs));
        }
        finally
        {
            await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS test.{tableName}");
        }
    }
}
