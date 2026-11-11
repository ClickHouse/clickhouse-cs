using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Utility;
using NUnit.Framework.Legacy;

namespace ClickHouse.Driver.Tests.Types;

public class VariantTests : AbstractConnectionTestFixture
{
    [Test]
    [RequiredFeature(Feature.Variant)]
    public async Task Read_NoneDiscriminator_ShouldReturnDbNull()
    {
        var targetTable = CreateTableName();

        await connection.ExecuteStatementAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, val Variant(String, UInt64)) ENGINE = Memory");

        await connection.ExecuteStatementAsync(
            $"INSERT INTO {targetTable} VALUES (1, 'hello'), (2, 42), (3, NULL)");

        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT id, val FROM {targetTable} ORDER BY id");

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(1u));
        Assert.That(reader.GetValue(1), Is.EqualTo("hello"));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(2u));
        Assert.That(reader.GetValue(1), Is.EqualTo((ulong)42));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(3u));
        Assert.That(reader.GetValue(1), Is.EqualTo(DBNull.Value));

        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    [RequiredFeature(Feature.Variant)]
    public async Task Write_Null_ShouldRoundTrip()
    {
        var targetTable = CreateTableName();

        await client.ExecuteNonQueryAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, val Variant(String, UInt64)) ENGINE = Memory");

        await client.InsertBinaryAsync(targetTable, ["id", "val"], [
            new object[] { 1u, "hello" },
            new object[] { 2u, null },
        ]);

        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT id, val FROM {targetTable} ORDER BY id");

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(1u));
        Assert.That(reader.GetValue(1), Is.EqualTo("hello"));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(2u));
        Assert.That(reader.GetValue(1), Is.EqualTo(DBNull.Value));

        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    [RequiredFeature(Feature.Variant)]
    [FromVersion(26, 5)]
    public async Task ParameterizedSelect_Null_ShouldReturnDbNull()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {var:Variant(String, UInt64)}";
        command.AddParameter("var", DBNull.Value);

        using var reader = await command.ExecuteReaderAsync();
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(DBNull.Value));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    [RequiredFeature(Feature.Variant)]
    [FromVersion(25, 4)]
    public async Task ParameterizedSelect_ArrayWithNullElement_ShouldRoundTrip()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {var:Array(Variant(UInt64, String))}";
        command.AddParameter("var", new object[] { (ulong)1, null, "hello" });

        using var reader = await command.ExecuteReaderAsync();
        ClassicAssert.IsTrue(reader.Read());
        var result = (object[])reader.GetValue(0);
        Assert.That(result[0], Is.EqualTo((ulong)1));
        Assert.That(result[1], Is.Null);
        Assert.That(result[2], Is.EqualTo("hello"));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    [RequiredFeature(Feature.Variant)]
    [FromVersion(25, 4)]
    public async Task ParameterizedSelect_VariantStringDateTimeUtc_WithDateTimeValue_ShouldRoundTrip()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT {var:Variant(String, DateTime('UTC'))}, variantType({var:Variant(String, DateTime('UTC'))})";
        command.AddParameter("var", new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc));

        using var reader = await command.ExecuteReaderAsync();
        ClassicAssert.IsTrue(reader.Read());
        var result = (DateTime)reader.GetValue(0);
        Assert.That(result, Is.EqualTo(new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc)));
        Assert.That(result.Kind, Is.EqualTo(DateTimeKind.Utc));
        Assert.That(reader.GetString(1), Is.EqualTo("DateTime('UTC')"));
        ClassicAssert.IsFalse(reader.Read());
    }

    // Two-type variant: IPv4 and IPv6 share FrameworkType=IPAddress and must be disambiguated by
    // AddressFamily. With < 3 types the write path resolves the subtype via the linear scan
    // (VariantType.GetMatchingType, no lookup built). The 3+ variant below covers the lookup path.
    [Test]
    [RequiredFeature(Feature.Variant)]
    public async Task InsertBinaryAsync_VariantIPv4IPv6_ShouldRoundTrip()
    {
        var targetTable = CreateTableName();

        await client.ExecuteNonQueryAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, addr Variant(IPv4, IPv6)) ENGINE = Memory");

        var ipv4 = IPAddress.Parse("10.0.0.1");
        var ipv6 = IPAddress.Parse("2001:db8::1");

        await client.InsertBinaryAsync(targetTable, ["id", "addr"], [
            new object[] { 1u, ipv4 },
            new object[] { 2u, ipv6 },
            new object[] { 3u, null },
        ]);

        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT id, addr, variantType(addr) FROM {targetTable} ORDER BY id");

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(1u));
        Assert.That(reader.GetValue(1), Is.EqualTo(ipv4));
        Assert.That(reader.GetString(2), Is.EqualTo("IPv4"));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(2u));
        Assert.That(reader.GetValue(1), Is.EqualTo(ipv6));
        Assert.That(reader.GetString(2), Is.EqualTo("IPv6"));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(0), Is.EqualTo(3u));
        Assert.That(reader.GetValue(1), Is.EqualTo(DBNull.Value));
        Assert.That(reader.GetString(2), Is.EqualTo("None"));

        ClassicAssert.IsFalse(reader.Read());
    }

    // Tuples of 8+ elements read as LargeTuple whatever their element types, as do most smaller tuples under
    // native AOT, so the runtime type alone cannot pick the Variant member: arity and elements have to. The server
    // orders a Variant's members by name, so the String tuple below is the first member the write path tries.
    [TestCase("Tuple(UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8)", "(1, 2, 3, 4, 5, 6, 7, 8)")]
    [TestCase("Tuple(UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8)", "(1, 2, 3, 4, 5, 6, 7, 8, 9)")]
    [RequiredFeature(Feature.Variant)]
    public async Task InsertBinaryAsync_LargeTupleIntoVariantOfTuples_SelectsTheMemberItFits(string member, string tuple)
    {
        var targetTable = CreateTableName();
        const string first = "Tuple(String, String, String, String, String, String, String, String)";

        await client.ExecuteNonQueryAsync(
            $"CREATE TABLE {targetTable} (id UInt32, val Variant({first}, {member})) ENGINE = Memory");

        var value = (ITuple)await client.ExecuteScalarAsync($"SELECT {tuple}::{member}");
        Assert.That(value, Is.InstanceOf<LargeTuple>());

        await client.InsertBinaryAsync(targetTable, ["id", "val"], [new object[] { 1u, value }]);

        using var reader = await connection.ExecuteReaderAsync($"SELECT val, variantType(val) FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());
        Assert.Multiple(() =>
        {
            Assert.That(reader.GetString(1).Replace(" ", string.Empty), Is.EqualTo(member.Replace(" ", string.Empty)));
            Assert.That(Elements((ITuple)reader.GetValue(0)), Is.EqualTo(Elements(value)).AsCollection);
        });
    }

    // Map members whose key and value CLR types are the same can still be different ClickHouse types: IPv4 and
    // IPv6 are both IPAddress, and every 8+ element tuple is a LargeTuple. The entries have to pick the member.
    // The server orders a Variant's members by name, so the first member is the one the write path tries first.
    [TestCase("Map(String, IPv4)", "Map(String, IPv6)", "map('a', toIPv6('::1'))")]
    [TestCase(
        "Map(String, Tuple(String, String, String, String, String, String, String, String))",
        "Map(String, Tuple(UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8))",
        "map('a', (1, 2, 3, 4, 5, 6, 7, 8))")]
    [RequiredFeature(Feature.Variant)]
    public async Task InsertBinaryAsync_MapIntoVariantOfMapsWithSameEntryTypes_SelectsTheMemberItFits(
        string first, string member, string map)
    {
        var targetTable = CreateTableName();

        await client.ExecuteNonQueryAsync(
            $"CREATE TABLE {targetTable} (id UInt32, val Variant({first}, {member})) ENGINE = Memory");

        var value = await client.ExecuteScalarAsync($"SELECT {map}::{member}");
        await client.InsertBinaryAsync(targetTable, ["id", "val"], [new object[] { 1u, value }]);

        var written = await client.ExecuteScalarAsync($"SELECT variantType(val) FROM {targetTable}");
        Assert.That(((string)written).Replace(" ", string.Empty), Is.EqualTo(member.Replace(" ", string.Empty)));
    }

    private const string StringFields = "a String, b String, c String, d String, e String, f String, g String, h String";
    private const string UInt8Fields = "a UInt8, b UInt8, c UInt8, d UInt8, e UInt8, f UInt8, g UInt8, h UInt8";
    private const string StringTuple = "Tuple(String, String, String, String, String, String, String, String)";
    private const string UInt8Tuple = "Tuple(UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8)";
    private const string EightUInt8s = "(1, 2, 3, 4, 5, 6, 7, 8)";

    // first, member, value literal. The server orders a Variant's members by name, so where both members could
    // take the value, first is the member the write path tries first.
    private static readonly object[] MembersSharingAClrType =
    [
        // A member whose CLR type is shared, inside an array, a tuple, a Nested, or a map's array or LowCardinality
        // value: IPv4 and IPv6 are both IPAddress, and every 8+ element tuple is a LargeTuple.
        new object[] { "Array(IPv4)", "Array(IPv6)", "[toIPv6('::1')]" },
        new object[] { $"Array({StringTuple})", $"Array({UInt8Tuple})", $"[{EightUInt8s}]" },
        new object[] { "Tuple(IPv4, UInt8)", "Tuple(IPv6, UInt8)", "(toIPv6('::1'), 1)" },
        new object[] { $"Nested({StringFields})", $"Nested({UInt8Fields})", $"[{EightUInt8s}]" },
        new object[] { $"Nested({UInt8Fields})", UInt8Tuple, EightUInt8s },
        new object[] { "Map(String, Array(IPv4))", "Map(String, Array(IPv6))", "map('a', [toIPv6('::1')])" },
        new object[] { "Map(String, LowCardinality(IPv4))", "Map(String, LowCardinality(IPv6))", "map('a', toIPv6('::1'))" },
        new object[]
        {
            "Map(String, LowCardinality(Nullable(IPv4)))", "Map(String, LowCardinality(Nullable(IPv6)))", "map('a', toIPv6('::1'), 'b', NULL)",
        },

        // An element that does not identify the tuple makes every element be checked, a LowCardinality(Nullable)
        // value type among them.
        new object[] { "String", "Tuple(IPv4, LowCardinality(Nullable(UUID)))", "(toIPv4('1.2.3.4'), generateUUIDv4())" },

        // A Variant or Dynamic value reads as object, and must still accept its values, null among them.
        new object[] { "String", "Map(String, Dynamic)", "map('a', 1, 'b', NULL)" },
        new object[] { "String", "Map(String, Variant(String, UInt8))", "map('a', 1, 'b', NULL)" },
        new object[] { "String", "Array(Dynamic)", "[1, NULL]" },
    ];

    [TestCaseSource(nameof(MembersSharingAClrType))]
    [RequiredFeature(Feature.Variant)]
    public async Task InsertBinaryAsync_ValueIntoVariantOfMembersSharingAClrType_SelectsTheMemberItFits(
        string first, string member, string literal)
    {
        var targetTable = CreateTableName();
        var options = new QueryOptions
        {
            CustomSettings = new Dictionary<string, object>
            {
                ["allow_suspicious_low_cardinality_types"] = 1,
                ["flatten_nested"] = 0,
            },
        };

        await client.ExecuteNonQueryAsync(
            $"CREATE TABLE {targetTable} (id UInt32, val Variant({first}, {member})) ENGINE = Memory", options: options);

        var value = await client.ExecuteScalarAsync($"SELECT {literal}::{member}", options: options);
        await client.InsertBinaryAsync(targetTable, ["id", "val"], [new object[] { 1u, value }]);

        var written = await client.ExecuteScalarAsync($"SELECT variantType(val) FROM {targetTable}");
        Assert.That(((string)written).Replace(" ", string.Empty), Is.EqualTo(member.Replace(" ", string.Empty)));
    }

    // An inner array that is null is written as empty, so it fits either member; the other element picks it.
    [Test]
    [RequiredFeature(Feature.Variant)]
    public async Task InsertBinaryAsync_ArrayWithANullInnerArrayIntoVariant_SelectsTheMemberItFits()
    {
        var targetTable = CreateTableName();

        await client.ExecuteNonQueryAsync(
            $"CREATE TABLE {targetTable} (id UInt32, val Variant(Array(Array(IPv4)), Array(Array(IPv6)))) ENGINE = Memory");

        var value = new IPAddress[][] { null, [IPAddress.IPv6Loopback] };
        await client.InsertBinaryAsync(targetTable, ["id", "val"], [new object[] { 1u, value }]);

        var written = await client.ExecuteScalarAsync($"SELECT variantType(val) FROM {targetTable}");
        Assert.That(written, Is.EqualTo("Array(Array(IPv6))"));
    }

    private static IEnumerable<object> Elements(ITuple tuple) => Enumerable.Range(0, tuple.Length).Select(i => tuple[i]);

    // Three-type variant: exercises the write path's O(1) lookup (VariantType builds the lookup for
    // 3+ types). IPv4 and IPv6 land in the same FrameworkType=IPAddress bucket and must still be
    // disambiguated by AddressFamily, while String resolves from its own bucket.
    [Test]
    [RequiredFeature(Feature.Variant)]
    public async Task InsertBinaryAsync_ThreeTypeVariantIPv4IPv6String_ShouldRoundTrip()
    {
        var targetTable = CreateTableName();

        await client.ExecuteNonQueryAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, val Variant(IPv4, IPv6, String)) ENGINE = Memory");

        var ipv4 = IPAddress.Parse("10.0.0.1");
        var ipv6 = IPAddress.Parse("2001:db8::1");

        await client.InsertBinaryAsync(targetTable, ["id", "val"], [
            new object[] { 1u, ipv4 },
            new object[] { 2u, ipv6 },
            new object[] { 3u, "hello" },
            new object[] { 4u, null },
        ]);

        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT id, val, variantType(val) FROM {targetTable} ORDER BY id");

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(1), Is.EqualTo(ipv4));
        Assert.That(reader.GetString(2), Is.EqualTo("IPv4"));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(1), Is.EqualTo(ipv6));
        Assert.That(reader.GetString(2), Is.EqualTo("IPv6"));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(1), Is.EqualTo("hello"));
        Assert.That(reader.GetString(2), Is.EqualTo("String"));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(1), Is.EqualTo(DBNull.Value));
        Assert.That(reader.GetString(2), Is.EqualTo("None"));

        ClassicAssert.IsFalse(reader.Read());
    }

    // Multi-type variant with distinct FrameworkTypes: exercises the general lookup path (3 types,
    // one candidate per bucket, no shared bucket) to confirm each value resolves to the right subtype
    // on write. Uses at most one numeric-family type (Int64) so the variant is not "suspicious" — two
    // integer-like types (e.g. Int64 + IPv4/UUID) are rejected by the server without
    // allow_suspicious_variant_types on some versions.
    [Test]
    [RequiredFeature(Feature.Variant)]
    public async Task InsertBinaryAsync_MultiTypeVariant_ShouldRoundTrip()
    {
        var targetTable = CreateTableName();

        await client.ExecuteNonQueryAsync(
            $"CREATE OR REPLACE TABLE {targetTable} (id UInt32, val Variant(Int64, String, Array(Int64))) ENGINE = Memory");

        var array = new long[] { 10, 20, 30 };

        await client.InsertBinaryAsync(targetTable, ["id", "val"], [
            new object[] { 1u, 42L },
            new object[] { 2u, "hello" },
            new object[] { 3u, array },
        ]);

        using var reader = await connection.ExecuteReaderAsync(
            $"SELECT id, val, variantType(val) FROM {targetTable} ORDER BY id");

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(1), Is.EqualTo(42L));
        Assert.That(reader.GetString(2), Is.EqualTo("Int64"));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(1), Is.EqualTo("hello"));
        Assert.That(reader.GetString(2), Is.EqualTo("String"));

        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetValue(1), Is.EqualTo(array));
        Assert.That(reader.GetString(2), Is.EqualTo("Array(Int64)"));

        ClassicAssert.IsFalse(reader.Read());
    }
}
