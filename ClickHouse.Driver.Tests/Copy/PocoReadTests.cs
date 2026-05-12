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

    public class NullableTriplePoco
    {
        public ulong Id { get; set; }
        public int? NullableNull { get; set; }
        public int? NullableValue { get; set; }
        public int? Plain { get; set; }
    }

    [Test]
    public async Task MapTo_NullableProperty_HandlesNullableAndPlainColumns()
    {
        // Exercise all three rows of the int? property matrix in a single query:
        //   Nullable(Int32) NULL        → null
        //   Nullable(Int32) non-null    → unwrapped value boxed into int?
        //   Int32 (non-nullable)        → value boxed into int?
        client.RegisterPocoType<NullableTriplePoco>();

        using var reader = await client.ExecuteReaderAsync(@"
            SELECT toUInt64(1)                          AS Id,
                   CAST(NULL, 'Nullable(Int32)')        AS NullableNull,
                   CAST(toInt32(42), 'Nullable(Int32)') AS NullableValue,
                   toInt32(7)                           AS Plain");

        Assert.That(reader.Read(), Is.True);
        var poco = reader.MapTo<NullableTriplePoco>();

        Assert.That(poco.Id, Is.EqualTo(1UL));
        Assert.That(poco.NullableNull, Is.Null,
            "Nullable(Int32) NULL → int? should be null");
        Assert.That(poco.NullableValue, Is.EqualTo(42),
            "Nullable(Int32) non-null → int? should unwrap to the underlying value");
        Assert.That(poco.Plain, Is.EqualTo(7),
            "plain Int32 → int? should box and wrap into the nullable");
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
    public async Task MapTo_DecimalWithUseCustomDecimalsFalse_AssignsValue()
    {
        // With UseCustomDecimals=false, Decimal columns are returned as System.Decimal directly,
        // so a System.Decimal property is assignable. Complements the v1 "no implicit conversion"
        // rule: callers who want decimal interop today must opt out of ClickHouseDecimal.
        using var customClient = TestUtilities.GetTestClickHouseClient(customDecimals: false);
        customClient.RegisterPocoType<DecimalPoco>();

        using var reader = await customClient.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, toDecimal128('123.45', 2) AS Amount");
        Assert.That(reader.Read(), Is.True);

        var poco = reader.MapTo<DecimalPoco>();
        Assert.That(poco.Id, Is.EqualTo(1UL));
        Assert.That(poco.Amount, Is.EqualTo(123.45m));
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
    
    public class SecondPoco
    {
        public ulong Id { get; set; }
        public string Other { get; set; }
    }

    public class ObjectPropertyPoco
    {
        public ulong Id { get; set; }
        public object Value { get; set; }
    }

    public class WidenedIntPoco
    {
        // Column will be Int16; property is Int32 — strict type mapping disallows widening.
        public int Id { get; set; }
    }

    public class ReadBasePoco
    {
        public ulong Id { get; set; }
    }

    public class DerivedReadPoco : ReadBasePoco
    {
        public string OwnValue { get; set; }
    }

    public class MixedAccessorReadPoco
    {
        public int Id { get; set; }
        public string SkippedInit { get; init; }
        public string SkippedReadOnly { get; }
        public int SkippedPrivate { get; private set; }
    }

    public class NotMappedReadPoco
    {
        public ulong Id { get; set; }
        public string Value { get; set; }

        [ClickHouseNotMapped]
        public string IgnoreMe { get; set; }
    }

    public class IndexerReadPoco
    {
        public ulong Id { get; set; }
        public string Value { get; set; }

        public string this[int i] => null;
    }

    // Passes read validation (public non-init setter) but fails insert validation (only
    // property has a private getter). Exercises the read-side of registration atomicity.
    public class PrivateGetterOnlyReadPoco
    {
        public int Id { private get; set; }
    }

    private class UnregisteredQueryPoco
    {
        public ulong Id { get; set; }
    }

#if NET7_0_OR_GREATER
    public class EndToEndRequiredPoco
    {
        public required int Id { get; set; }
        public required string Name { get; set; }
    }
#endif

    [Test]
    public async Task MapTo_PocoWithIndexer_MaterializesProperties()
    {
        client.RegisterPocoType<IndexerReadPoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(42) AS Id, 'with_indexer' AS Value");
        Assert.That(reader.Read(), Is.True);

        var poco = reader.MapTo<IndexerReadPoco>();
        Assert.That(poco.Id, Is.EqualTo(42UL));
        Assert.That(poco.Value, Is.EqualTo("with_indexer"));
    }

    [Test]
    public void RegisterPocoType_FailedInsertValidation_LeavesReadUnregistered_QueryAsyncThrows()
    {
        // PrivateGetterOnlyReadPoco passes read validation but fails insert validation. The
        // failed RegisterPocoType<T> must build both mappings up front so the read commit never
        // happens. Prove it via QueryAsync: it must throw the "not registered for POCO read"
        // error, not silently succeed against a tentative read mapping.
        Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<PrivateGetterOnlyReadPoco>());

        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in client.QueryAsync<PrivateGetterOnlyReadPoco>(
                "SELECT toInt32(1) AS Id"))
            {
            }
        });

        Assert.That(ex.Message, Does.Contain("not registered for POCO read"));
    }

    [Test]
    public async Task MapTo_InitReadOnlyAndPrivateSetterProperties_AreSkipped()
    {
        client.RegisterPocoType<MixedAccessorReadPoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toInt32(7) AS Id, 'a' AS SkippedInit, 'b' AS SkippedReadOnly, toInt32(99) AS SkippedPrivate");
        Assert.That(reader.Read(), Is.True);

        var poco = reader.MapTo<MixedAccessorReadPoco>();
        Assert.That(poco.Id, Is.EqualTo(7));
        Assert.That(poco.SkippedInit, Is.Null,
            "init-only property must not be filled even when a matching column is present");
        Assert.That(poco.SkippedReadOnly, Is.Null,
            "get-only property must not be filled even when a matching column is present");
        Assert.That(poco.SkippedPrivate, Is.EqualTo(0),
            "private-setter property must not be filled even when a matching column is present");
    }

    [Test]
    public async Task MapTo_NumericWideningInt16ToInt32_ThrowsInvalidOperation()
    {
        client.RegisterPocoType<WidenedIntPoco>();

        using var reader = await client.ExecuteReaderAsync("SELECT toInt16(7) AS Id");
        Assert.That(reader.Read(), Is.True);

        var ex = Assert.Throws<InvalidOperationException>(() => reader.MapTo<WidenedIntPoco>());
        Assert.That(ex.Message, Does.Contain("Id"));
        Assert.That(ex.Message, Does.Contain("System.Int32"));
        Assert.That(ex.Message, Does.Contain("System.Int16"));
    }

    [Test]
    public async Task MapTo_StringIntoObjectProperty_AssignsValue()
    {
        client.RegisterPocoType<ObjectPropertyPoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, 'hello' AS Value");
        Assert.That(reader.Read(), Is.True);

        var poco = reader.MapTo<ObjectPropertyPoco>();
        Assert.That(poco.Id, Is.EqualTo(1UL));
        Assert.That(poco.Value, Is.EqualTo("hello"));
    }

    [Test]
    public async Task MapTo_DerivedTypeInheritedProperty_MapsCorrectly()
    {
        client.RegisterPocoType<DerivedReadPoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(42) AS Id, 'derived' AS OwnValue");
        Assert.That(reader.Read(), Is.True);

        var poco = reader.MapTo<DerivedReadPoco>();
        Assert.That(poco.Id, Is.EqualTo(42UL));
        Assert.That(poco.OwnValue, Is.EqualTo("derived"));
    }

    [Test]
    public async Task MapTo_MultipleTypesOnSameReader_MaterializeIndependently()
    {
        client.RegisterPocoType<SimplePoco>();
        client.RegisterPocoType<SecondPoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, 'a' AS Value, 'b' AS Other");
        Assert.That(reader.Read(), Is.True);

        var first = reader.MapTo<SimplePoco>();
        var second = reader.MapTo<SecondPoco>();

        Assert.That(first.Id, Is.EqualTo(1UL));
        Assert.That(first.Value, Is.EqualTo("a"));
        Assert.That(second.Id, Is.EqualTo(1UL));
        Assert.That(second.Other, Is.EqualTo("b"));
    }

    [Test]
    public async Task MapTo_BeforeRead_ThrowsInvalidOperation()
    {
        client.RegisterPocoType<SimplePoco>();

        using var reader = await client.ExecuteReaderAsync("SELECT toUInt64(1) AS Id, 'a' AS Value");

        // No Read() yet — there is no current row. MapTo must surface that precondition rather
        // than silently materializing a default-filled instance.
        var ex = Assert.Throws<InvalidOperationException>(() => reader.MapTo<SimplePoco>());
        Assert.That(ex.Message, Does.Contain("Read()"));
    }

    [Test]
    public async Task MapTo_AfterEndOfStream_ThrowsInvalidOperation()
    {
        client.RegisterPocoType<SimplePoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, 'a' AS Value");

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.Read(), Is.False);

        var ex = Assert.Throws<InvalidOperationException>(() => reader.MapTo<SimplePoco>());
        Assert.That(ex.Message, Does.Contain("Read()"));
    }

    [Test]
    public async Task MapTo_RequiredMembers_MaterializesInstance()
    {
#if NET7_0_OR_GREATER
        client.RegisterPocoType<EndToEndRequiredPoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toInt32(7) AS Id, 'alice' AS Name");

        Assert.That(reader.Read(), Is.True);
        var poco = reader.MapTo<EndToEndRequiredPoco>();

        Assert.That(poco.Id, Is.EqualTo(7));
        Assert.That(poco.Name, Is.EqualTo("alice"));
#else
        Assert.Ignore("`required` members require .NET 7+");
#endif
    }

    [Test]
    public async Task QueryAsync_UnregisteredType_ThrowsOnFirstYield()
    {
        var enumerator = client.QueryAsync<UnregisteredQueryPoco>("SELECT toUInt64(1) AS Id").GetAsyncEnumerator();
        try
        {
            Assert.ThrowsAsync<InvalidOperationException>(async () => await enumerator.MoveNextAsync());
        }
        finally
        {
            await enumerator.DisposeAsync();
        }
    }

    [Test]
    [Tests.Attributes.FromVersion(25, 11)]
    public void QueryAsync_ServerErrorMidStream_SurfacesServerException()
    {
        client.RegisterPocoType<SimplePoco>();

        // Enable mid-stream exception tagging so the server can flag the point at which the
        // query started failing. Without this, the failure surfaces as EndOfStreamException
        // (or silent truncation); the driver must propagate it as ClickHouseServerException.
        var options = new QueryOptions
        {
            CustomSettings = new Dictionary<string, object>
            {
                ["http_write_exception_in_output_format"] = 1,
            },
        };

        var ex = Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            await foreach (var _ in client.QueryAsync<SimplePoco>(
                "SELECT toUInt64(number) AS Id, throwIf(number = 10, 'boom') AS Value " +
                "FROM system.numbers LIMIT 10000000", options: options).ConfigureAwait(false))
            {
            }
        });

        Assert.That(ex.Message, Does.Contain("boom"));
    }
}
