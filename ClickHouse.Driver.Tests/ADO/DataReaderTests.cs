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
    public async Task ShouldReadEnum8ColumnViaNumericAccessors()
    {
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            "SELECT CAST('Active', 'Enum8(''Active'' = 1, ''Inactive'' = 2)') AS value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.Multiple(() =>
        {
            // Numeric accessors expose the underlying ordinal that arrived on the wire.
            Assert.That(reader.GetByte(0), Is.EqualTo(1));
            Assert.That(reader.GetSByte(0), Is.EqualTo(1));
            Assert.That(reader.GetInt16(0), Is.EqualTo(1));
            Assert.That(reader.GetInt32(0), Is.EqualTo(1));
            Assert.That(reader.GetInt64(0), Is.EqualTo(1));
            Assert.That(reader.GetUInt16(0), Is.EqualTo(1));
            Assert.That(reader.GetUInt32(0), Is.EqualTo(1));
            Assert.That(reader.GetUInt64(0), Is.EqualTo(1));
            Assert.That(reader.GetFieldValue<int>(0), Is.EqualTo(1));
            Assert.That(reader.GetFieldValue<byte>(0), Is.EqualTo(1));
            Assert.That(reader.GetFieldValue<sbyte>(0), Is.EqualTo(1));
            Assert.That(reader.GetFieldValue<long>(0), Is.EqualTo(1));
            // The string form is unchanged (backward compatible).
            Assert.That(reader.GetString(0), Is.EqualTo("Active"));
            Assert.That(reader.GetValue(0), Is.InstanceOf<string>());
            Assert.That(reader.GetFieldValue<string>(0), Is.EqualTo("Active"));
        });
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadNegativeEnum8OrdinalAcrossIntegerAccessors()
    {
        // A negative Enum8 ordinal round-trips through the signed accessors, but must surface a
        // clear error on byte/unsigned accessors rather than silently wrapping to a huge value.
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            "SELECT CAST('None', 'Enum8(''None'' = -1, ''Active'' = 1)') AS value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.Multiple(() =>
        {
            Assert.That(reader.GetSByte(0), Is.EqualTo(-1));
            Assert.That(reader.GetInt16(0), Is.EqualTo(-1));
            Assert.That(reader.GetInt32(0), Is.EqualTo(-1));
            Assert.That(reader.GetInt64(0), Is.EqualTo(-1));
            Assert.That(reader.GetFieldValue<sbyte>(0), Is.EqualTo(-1));
            Assert.That(reader.GetFieldValue<int>(0), Is.EqualTo(-1));
            Assert.Throws<OverflowException>(() => reader.GetByte(0));
            Assert.Throws<OverflowException>(() => reader.GetUInt16(0));
            Assert.Throws<OverflowException>(() => reader.GetUInt32(0));
            Assert.Throws<OverflowException>(() => reader.GetUInt64(0));
            Assert.Throws<OverflowException>(() => reader.GetFieldValue<uint>(0));
        });
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadEnum16OrdinalBeyondByteRangeAcrossIntegerAccessors()
    {
        // An Enum16 ordinal (1000) exceeds byte/sbyte range: the wider signed and unsigned
        // accessors return it, while byte/sbyte surface a clear error rather than truncating.
        using var reader = (ClickHouseDataReader)await connection.ExecuteReaderAsync(
            "SELECT CAST('High', 'Enum16(''Low'' = 1, ''High'' = 1000)') AS value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.Multiple(() =>
        {
            Assert.That(reader.GetInt16(0), Is.EqualTo(1000));
            Assert.That(reader.GetInt32(0), Is.EqualTo(1000));
            Assert.That(reader.GetInt64(0), Is.EqualTo(1000));
            Assert.That(reader.GetUInt16(0), Is.EqualTo(1000));
            Assert.That(reader.GetUInt32(0), Is.EqualTo(1000));
            Assert.That(reader.GetUInt64(0), Is.EqualTo(1000));
            Assert.That(reader.GetFieldValue<int>(0), Is.EqualTo(1000));
            Assert.That(reader.GetFieldValue<ushort>(0), Is.EqualTo(1000));
            Assert.Throws<OverflowException>(() => reader.GetByte(0));
            Assert.Throws<OverflowException>(() => reader.GetSByte(0));
            Assert.Throws<OverflowException>(() => reader.GetFieldValue<byte>(0));
        });
        ClassicAssert.IsFalse(reader.Read());
    }

    [Test]
    public async Task ShouldReadNullableEnum8AsInt()
    {
        using (var reader = await connection.ExecuteReaderAsync(
            "SELECT CAST('Active', 'Nullable(Enum8(''Active'' = 1, ''Inactive'' = 2))') AS value"))
        {
            ClassicAssert.IsTrue(reader.Read());
            Assert.That(reader.GetInt32(0), Is.EqualTo(1));
            ClassicAssert.IsFalse(reader.Read());
        }

        using (var reader = await connection.ExecuteReaderAsync(
            "SELECT CAST(NULL, 'Nullable(Enum8(''Active'' = 1, ''Inactive'' = 2))') AS value"))
        {
            ClassicAssert.IsTrue(reader.Read());
            ClassicAssert.IsTrue(reader.IsDBNull(0));
            ClassicAssert.IsFalse(reader.Read());
        }
    }

    [Test]
    public async Task ShouldThrowWhenReadingStringColumnAsInt32()
    {
        // Contrast case: enum-aware numeric accessors must not make a plain String
        // column leniently convertible to an integer.
        using var reader = await connection.ExecuteReaderAsync("SELECT 'Active' AS value");
        ClassicAssert.IsTrue(reader.Read());
        Assert.Multiple(() =>
        {
            Assert.Throws<InvalidCastException>(() => reader.GetInt32(0));
            Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<int>(0));
        });
        ClassicAssert.IsFalse(reader.Read());
    }
}
