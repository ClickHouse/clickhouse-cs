using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

[Category("Cloud")]
public class DataReaderTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task ShouldReadFieldByIndex()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT 1 as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader[0], Is.EqualTo(1));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadFieldByName()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT 1 as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader["value"], Is.EqualTo(1));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadBoolean()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT 1 as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetBoolean(0), Is.EqualTo(true));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadByte()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT toUInt8(1) as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetByte(0), Is.EqualTo(1));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadFloat()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT toFloat32(1) as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetFloat(0), Is.EqualTo(1));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadDouble()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT toFloat64(1) as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetDouble(0), Is.EqualTo(1));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadInt16()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT toInt16(1) as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetInt16(0), Is.EqualTo(1));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadInt32()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT toInt32(1) as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetInt32(0), Is.EqualTo(1));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadInt64()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT toInt64(1) as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetInt64(0), Is.EqualTo(1));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadUInt16()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT toUInt16(1) as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetUInt16(0), Is.EqualTo(1));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadUInt32()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT toUInt32(1) as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetUInt32(0), Is.EqualTo(1));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadUInt64()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT toUInt64(1) as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetUInt64(0), Is.EqualTo(1));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadDecimal()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT toDecimal64(1,3) as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetDecimal(0), Is.EqualTo(1.000m));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadString()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT 'ASD' as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetString(0), Is.EqualTo("ASD"));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadNull()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT NULL as value");
        ClassicAssert.IsTrue(reader.Read());
        ClassicAssert.IsTrue(reader.IsDBNull(0));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadIPv4()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT toIPv4('1.2.3.4')");
        ClassicAssert.IsTrue(reader.Read());
        ClassicAssert.NotNull(reader.GetIPAddress(0));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadTuple()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT tuple(1,'a', NULL)");
        ClassicAssert.IsTrue(reader.Read());
        ClassicAssert.NotNull(reader.GetTuple(0));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadGuid()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT generateUUIDv4() as value");
        ClassicAssert.IsTrue(reader.Read());
        ClassicAssert.NotNull(reader.GetGuid(0));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldGetFieldValue()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT 'ASD' as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetFieldValue<string>(0), Is.EqualTo("ASD"));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldGetFieldValueByName()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT 'ASD' as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetFieldValue<string>("value"), Is.EqualTo("ASD"));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldThrowGettingFieldValueByUnknownName()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync("SELECT 'ASD' as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.Throws<ArgumentException>(() => reader.GetFieldValue<string>("nonexistent"));
    }

    [Test]
    public async Task ShouldGetDataTypeName()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT 'ASD' as value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.GetDataTypeName(0), Is.EqualTo("String"));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldEnumerateRows()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT * FROM system.numbers LIMIT 100");
        var rows = reader.Cast<IDataRecord>().Select(row => row[0]).ToList();
        Assert.That(rows, Is.EqualTo(Enumerable.Range(0, 100)).AsCollection);
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task TryGetEnumOrdinal_Enum8Column_ReturnsWireOrdinalWhileStandardAccessorsAreUnchanged()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            "SELECT CAST('Active', 'Enum8(''Active'' = 1, ''Inactive'' = 2)') AS value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.Multiple(() =>
        {
            // The opt-in accessor exposes the underlying ordinal that arrived on the wire.
            Assert.That(reader.TryGetEnumOrdinal(0, out var ordinal), Is.True);
            Assert.That(ordinal, Is.EqualTo(1));
            // The string form is unchanged (an enum column materializes as its label).
            Assert.That(reader.GetString(0), Is.EqualTo("Active"));
            Assert.That(reader.GetValue(0), Is.InstanceOf<string>());
            Assert.That(reader.GetFieldValue<string>(0), Is.EqualTo("Active"));
            // The standard numeric accessors keep ADO.NET's behavior for a string-backed column.
            Assert.Throws<InvalidCastException>(() => reader.GetInt32(0));
            Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<int>(0));
        });
        ClassicAssert.IsFalse(reader.Read());
    }

    [TestCase("Enum8('Active' = 1, 'Inactive' = 2)", "Active", 1)]
    [TestCase("Enum8('Active' = 1, 'Inactive' = 2)", "Inactive", 2)]
    [TestCase("Enum8('None' = -1, 'Active' = 1)", "None", -1)]        // negative ordinal
    [TestCase("Enum16('Low' = 1, 'High' = 1000)", "High", 1000)]      // beyond Enum8/byte range
    public async Task TryGetEnumOrdinal_KnownLabel_ReturnsRawSignedWireOrdinal(string enumType, string label, int expected)
    {
        var quotedType = enumType.Replace("'", "''"); // embed the enum type in the SQL string literal
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            $"SELECT CAST('{label}', '{quotedType}') AS value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.TryGetEnumOrdinal(0, out var ordinal), Is.True);
        Assert.That(ordinal, Is.EqualTo(expected));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task TryGetEnumOrdinal_NullableEnum8WithValue_ReturnsOrdinal()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            "SELECT CAST('Active', 'Nullable(Enum8(''Active'' = 1, ''Inactive'' = 2))') AS value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.That(reader.TryGetEnumOrdinal(0, out var ordinal), Is.True);
        Assert.That(ordinal, Is.EqualTo(1));
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task TryGetEnumOrdinal_NonEnumOrNullColumns_ReturnsFalseAndLeavesAccessorsUnchanged()
    {
        // Contrast cases: the new accessor only resolves live enum labels; everything else returns
        // false and the standard accessors behave exactly as they do on a build without it.
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            "SELECT 'plain' AS s, toInt32(42) AS i, CAST(NULL, 'Nullable(Enum8(''Active'' = 1))') AS n");
        ClassicAssert.IsTrue(reader.Read());
        Assert.Multiple(() =>
        {
            // Plain String column: not an enum -> false; a numeric cast still throws.
            Assert.That(reader.TryGetEnumOrdinal(0, out _), Is.False);
            Assert.Throws<InvalidCastException>(() => reader.GetInt32(0));
            // Int column: not an enum -> false; GetInt32 works exactly as before.
            Assert.That(reader.TryGetEnumOrdinal(1, out _), Is.False);
            Assert.That(reader.GetInt32(1), Is.EqualTo(42));
            // NULL Nullable(Enum): no ordinal -> false; callers use IsDBNull.
            Assert.That(reader.TryGetEnumOrdinal(2, out _), Is.False);
            ClassicAssert.IsTrue(reader.IsDBNull(2));
        });
        ClassicAssert.IsFalse(reader.Read());
    }
}
