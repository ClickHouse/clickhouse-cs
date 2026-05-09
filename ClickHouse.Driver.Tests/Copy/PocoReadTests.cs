using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Copy;

[TestFixture]
public class PocoReadTests : AbstractConnectionTestFixture
{
    private string CreateTestTableName([CallerMemberName] string testName = null)
        => SanitizeTableName($"test_pocoread_{testName}_{Guid.NewGuid():N}");

    public class SimplePoco
    {
        public ulong Id { get; set; }
        public string Value { get; set; }
    }

    public class AliasedPoco
    {
        [ClickHouseColumn(Name = "id")]
        public ulong UserId { get; set; }

        [ClickHouseColumn(Name = "value")]
        public string UserName { get; set; }
    }

    public class ExtraPropertyPoco
    {
        public ulong Id { get; set; }
        public string Value { get; set; }
        public int Missing { get; set; } // not present in result
    }

    public class CaseSensitivePoco
    {
        // Lowercase property name, table column is uppercase 'Id' — ordinal match should fail.
        public ulong id { get; set; }
        public string Value { get; set; }
    }

    public class NullablePoco
    {
        public ulong Id { get; set; }
        public int? Score { get; set; }
    }

    public class NonNullablePoco
    {
        public ulong Id { get; set; }
        public int Score { get; set; }
    }

    public class DecimalPoco
    {
        public ulong Id { get; set; }
        public decimal Amount { get; set; }
    }

    public class StringIdPoco
    {
        // String property mapped to a UInt64 column — type mismatch.
        public string Id { get; set; }
        public string Value { get; set; }
    }

    public class UnregisteredPoco
    {
        public ulong Id { get; set; }
    }

    [Test]
    public async Task MapTo_ExactColumnNames_MapsProperties()
    {
        client.RegisterPocoType<SimplePoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(7) AS Id, 'hello' AS Value");

        Assert.That(reader.Read(), Is.True);
        var poco = reader.MapTo<SimplePoco>();

        Assert.That(poco.Id, Is.EqualTo(7UL));
        Assert.That(poco.Value, Is.EqualTo("hello"));
    }

    [Test]
    public async Task MapTo_DoesNotAdvanceReader()
    {
        client.RegisterPocoType<SimplePoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, 'a' AS Value");

        Assert.That(reader.Read(), Is.True);
        var first = reader.MapTo<SimplePoco>();
        var second = reader.MapTo<SimplePoco>();

        Assert.That(first.Id, Is.EqualTo(1UL));
        Assert.That(second.Id, Is.EqualTo(1UL));
        Assert.That(reader.Read(), Is.False);
    }

    [Test]
    public async Task MapTo_ClickHouseColumnNameAttribute_MapsAlias()
    {
        client.RegisterPocoType<AliasedPoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(42) AS id, 'alice' AS value");

        Assert.That(reader.Read(), Is.True);
        var poco = reader.MapTo<AliasedPoco>();

        Assert.That(poco.UserId, Is.EqualTo(42UL));
        Assert.That(poco.UserName, Is.EqualTo("alice"));
    }

    [Test]
    public async Task MapTo_MissingColumns_LeavesDefaults()
    {
        client.RegisterPocoType<ExtraPropertyPoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, 'present' AS Value");

        Assert.That(reader.Read(), Is.True);
        var poco = reader.MapTo<ExtraPropertyPoco>();

        Assert.That(poco.Id, Is.EqualTo(1UL));
        Assert.That(poco.Value, Is.EqualTo("present"));
        Assert.That(poco.Missing, Is.EqualTo(0)); // default
    }

    [Test]
    public async Task MapTo_ExtraColumns_IgnoresExtras()
    {
        client.RegisterPocoType<SimplePoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, 'kept' AS Value, 'extra' AS Extra");

        Assert.That(reader.Read(), Is.True);
        var poco = reader.MapTo<SimplePoco>();

        Assert.That(poco.Id, Is.EqualTo(1UL));
        Assert.That(poco.Value, Is.EqualTo("kept"));
    }

    [Test]
    public async Task MapTo_CaseMismatch_DoesNotMap()
    {
        client.RegisterPocoType<CaseSensitivePoco>();

        // Column "Id" (uppercase) does not match property "id" (lowercase) under StringComparer.Ordinal.
        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(99) AS Id, 'kept' AS Value");

        Assert.That(reader.Read(), Is.True);
        var poco = reader.MapTo<CaseSensitivePoco>();

        Assert.That(poco.id, Is.EqualTo(0UL)); // not mapped, default
        Assert.That(poco.Value, Is.EqualTo("kept"));
    }

    [Test]
    public async Task MapTo_UnregisteredType_ThrowsInvalidOperation()
    {
        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id");

        Assert.That(reader.Read(), Is.True);
        var ex = Assert.Throws<InvalidOperationException>(() => reader.MapTo<UnregisteredPoco>());

        Assert.That(ex.Message, Does.Contain("UnregisteredPoco"));
        Assert.That(ex.Message, Does.Contain("RegisterPocoType"));
    }

    [Test]
    public async Task MapTo_NullForReferenceProperty_AssignsNull()
    {
        client.RegisterPocoType<SimplePoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, CAST(NULL, 'Nullable(String)') AS Value");

        Assert.That(reader.Read(), Is.True);
        var poco = reader.MapTo<SimplePoco>();

        Assert.That(poco.Id, Is.EqualTo(1UL));
        Assert.That(poco.Value, Is.Null);
    }

    [Test]
    public async Task MapTo_NonNullForNullableProperty_AssignsValue()
    {
        client.RegisterPocoType<NullablePoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, toInt32(42) AS Score");

        Assert.That(reader.Read(), Is.True);
        var poco = reader.MapTo<NullablePoco>();

        Assert.That(poco.Id, Is.EqualTo(1UL));
        Assert.That(poco.Score, Is.EqualTo(42));
    }

    [Test]
    public async Task MapTo_NullForNullableProperty_AssignsNull()
    {
        client.RegisterPocoType<NullablePoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, CAST(NULL, 'Nullable(Int32)') AS Score");

        Assert.That(reader.Read(), Is.True);
        var poco = reader.MapTo<NullablePoco>();

        Assert.That(poco.Id, Is.EqualTo(1UL));
        Assert.That(poco.Score, Is.Null);
    }

    [Test]
    public async Task MapTo_NullForNonNullableValueType_ThrowsInvalidOperation()
    {
        client.RegisterPocoType<NonNullablePoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, CAST(NULL, 'Nullable(Int32)') AS Score");

        Assert.That(reader.Read(), Is.True);
        var ex = Assert.Throws<InvalidOperationException>(() => reader.MapTo<NonNullablePoco>());

        Assert.That(ex.Message, Does.Contain("Score"));
    }

    [Test]
    public async Task MapTo_TypeMismatch_ThrowsInvalidOperation()
    {
        client.RegisterPocoType<StringIdPoco>();

        // 'Id' column is UInt64 but property is string — strict v1 mismatches throw.
        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, 'v' AS Value");

        Assert.That(reader.Read(), Is.True);
        var ex = Assert.Throws<InvalidOperationException>(() => reader.MapTo<StringIdPoco>());

        Assert.That(ex.Message, Does.Contain("Id"));
        Assert.That(ex.Message, Does.Contain("System.String"));
    }

    [Test]
    public async Task MapTo_ClickHouseDecimalReturnedForDecimalProperty_ThrowsInvalidOperation()
    {
        // With UseCustomDecimals=true (default), ClickHouseDecimal columns surface as
        // ClickHouseDecimal, which is not assignable to System.Decimal. v1 must throw.
        using var customClient = TestUtilities.GetTestClickHouseClient();
        customClient.RegisterPocoType<DecimalPoco>();

        using var reader = await customClient.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, toDecimal128('123.45', 2) AS Amount");

        Assert.That(reader.Read(), Is.True);
        var ex = Assert.Throws<InvalidOperationException>(() => reader.MapTo<DecimalPoco>());

        Assert.That(ex.Message, Does.Contain("Amount"));
        Assert.That(ex.Message, Does.Contain(nameof(ClickHouseDecimal)));
    }

    [Test]
    public async Task QueryAsync_RegisteredPoco_StreamsRowsInOrder()
    {
        client.RegisterPocoType<SimplePoco>();

        var results = new List<SimplePoco>();
        await foreach (var row in client.QueryAsync<SimplePoco>(
            "SELECT toUInt64(number + 1) AS Id, concat('row_', toString(number)) AS Value FROM numbers(5)"))
        {
            results.Add(row);
        }

        Assert.That(results, Has.Count.EqualTo(5));
        for (var i = 0; i < 5; i++)
        {
            Assert.That(results[i].Id, Is.EqualTo((ulong)(i + 1)));
            Assert.That(results[i].Value, Is.EqualTo($"row_{i}"));
        }
    }

    [Test]
    public async Task QueryAsync_WithParameters_PassesParameters()
    {
        client.RegisterPocoType<SimplePoco>();

        var parameters = new ClickHouseParameterCollection();
        parameters.AddParameter("min", 3UL);

        var results = new List<SimplePoco>();
        await foreach (var row in client.QueryAsync<SimplePoco>(
            "SELECT toUInt64(number) AS Id, toString(number) AS Value FROM numbers(6) WHERE number >= {min:UInt64}",
            parameters))
        {
            results.Add(row);
        }

        Assert.That(results.Select(r => r.Id), Is.EqualTo(new ulong[] { 3, 4, 5 }));
    }

    [Test]
    public async Task QueryAsync_WithQueryOptions_PassesOptions()
    {
        client.RegisterPocoType<SimplePoco>();

        var queryId = $"poco-query-{Guid.NewGuid():N}";
        var options = new QueryOptions { QueryId = queryId };

        var results = new List<SimplePoco>();
        await foreach (var row in client.QueryAsync<SimplePoco>(
            "SELECT toUInt64(1) AS Id, 'a' AS Value", options: options))
        {
            results.Add(row);
        }

        Assert.That(results, Has.Count.EqualTo(1));

        // Verify the QueryId reached the server by looking it up in system.query_log.
        await client.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");
        var found = await client.ExecuteScalarAsync(
            "SELECT count() FROM system.query_log WHERE query_id = {qid:String}",
            new ClickHouseParameterCollection
            {
                new ClickHouseDbParameter { ParameterName = "qid", Value = queryId, ClickHouseType = "String" },
            });

        Assert.That(Convert.ToInt64(found), Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task QueryAsync_EnumerationStopsEarly_DisposesReader()
    {
        client.RegisterPocoType<SimplePoco>();

        var enumerator = client.QueryAsync<SimplePoco>(
            "SELECT toUInt64(number) AS Id, toString(number) AS Value FROM numbers(1000)").GetAsyncEnumerator();
        try
        {
            Assert.That(await enumerator.MoveNextAsync(), Is.True);
            Assert.That(enumerator.Current.Id, Is.EqualTo(0UL));
            // stop early
        }
        finally
        {
            await enumerator.DisposeAsync();
        }

        // After disposal we should be able to issue a follow-up query without resource issues.
        var count = await client.ExecuteScalarAsync("SELECT 1");
        Assert.That(Convert.ToInt32(count), Is.EqualTo(1));
    }

    [Test]
    public async Task ClickHouseConnection_RegisterPocoType_AllowsCommandReaderMapTo()
    {
        using var conn = TestUtilities.GetTestClickHouseConnection();
        conn.RegisterPocoType<SimplePoco>();

        using var cmd = conn.CreateCommand("SELECT toUInt64(11) AS Id, 'cmd' AS Value");
        using var reader = (ClickHouseDataReader)await cmd.ExecuteReaderAsync();

        Assert.That(reader.Read(), Is.True);
        var poco = reader.MapTo<SimplePoco>();

        Assert.That(poco.Id, Is.EqualTo(11UL));
        Assert.That(poco.Value, Is.EqualTo("cmd"));
    }
}
