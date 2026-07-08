using System;
using System.Net;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Utility;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace ClickHouse.Driver.Tests.Types;

public class VariantTests : AbstractConnectionTestFixture
{
    [Test]
    [RequiredFeature(Feature.Variant)]
    public async Task Read_NoneDiscriminator_ShouldReturnDbNull()
    {
        var targetTable = "test.test_variant_read_null";
        try
        {
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
        finally
        {
            await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    [Test]
    [RequiredFeature(Feature.Variant)]
    public async Task Write_Null_ShouldRoundTrip()
    {
        var targetTable = "test.test_variant_write_null";
        try
        {
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
        finally
        {
            await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        }
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
        var targetTable = "test.test_variant_ip";
        try
        {
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
        finally
        {
            await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        }
    }

    // Three-type variant: exercises the write path's O(1) lookup (VariantType builds the lookup for
    // 3+ types). IPv4 and IPv6 land in the same FrameworkType=IPAddress bucket and must still be
    // disambiguated by AddressFamily, while String resolves from its own bucket.
    [Test]
    [RequiredFeature(Feature.Variant)]
    public async Task InsertBinaryAsync_ThreeTypeVariantIPv4IPv6String_ShouldRoundTrip()
    {
        var targetTable = "test.test_variant_ip3";
        try
        {
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
        finally
        {
            await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        }
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
        var targetTable = "test.test_variant_multi";
        try
        {
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
        finally
        {
            await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {targetTable}");
        }
    }
}
