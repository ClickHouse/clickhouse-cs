using System;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.SQL;

public class ParameterizedInsertTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task ShouldInsertParameterizedFloat64Array()
    {
        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.float_array");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.float_array (arr Array(Float64)) ENGINE Memory");

        var command = connection.CreateCommand();
        command.AddParameter("values", new[] { 1.0, 2.0, 3.0 });
        command.CommandText = "INSERT INTO test.float_array VALUES ({values:Array(Float32)})";
        await command.ExecuteNonQueryAsync();

        var count = await connection.ExecuteScalarAsync("SELECT COUNT(*) FROM test.float_array");
        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    public async Task ShouldInsertEnum8()
    {
        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.insert_enum8");
        await connection.ExecuteStatementAsync("CREATE TABLE IF NOT EXISTS test.insert_enum8 (enum Enum8('a' = -1, 'b' = 127)) ENGINE Memory");

        var command = connection.CreateCommand();
        command.AddParameter("value", "a");
        command.CommandText = "INSERT INTO test.insert_enum8 VALUES ({value:Enum8('a' = -1, 'b' = 127)})";
        await command.ExecuteNonQueryAsync();

        var count = await connection.ExecuteScalarAsync("SELECT COUNT(*) FROM test.insert_enum8");
        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    [RequiredFeature(Feature.UUIDParameters)]
    public async Task ShouldInsertParameterizedUUIDArray()
    {
        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.uuid_array");
        await connection.ExecuteStatementAsync(
            "CREATE TABLE IF NOT EXISTS test.uuid_array (arr Array(UUID)) ENGINE Memory");

        var command = connection.CreateCommand();
        command.AddParameter("values", new[] { Guid.NewGuid(), Guid.NewGuid(), });
        command.CommandText = "INSERT INTO test.uuid_array VALUES ({values:Array(UUID)})";
        await command.ExecuteNonQueryAsync();

        var count = await connection.ExecuteScalarAsync("SELECT COUNT(*) FROM test.uuid_array");
        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    public async Task ShouldInsertStringWithNewline()
    {
        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.string_with_newline");
        await connection.ExecuteStatementAsync(
            "CREATE TABLE IF NOT EXISTS test.string_with_newline (str_value String) ENGINE Memory");

        var command = connection.CreateCommand();

        var strValue = "Hello \n ClickHouse";

        command.AddParameter("str_value", strValue);
        command.CommandText = "INSERT INTO test.string_with_newline VALUES ({str_value:String})";
        await command.ExecuteNonQueryAsync();

        var count = await connection.ExecuteScalarAsync("SELECT COUNT(*) FROM test.string_with_newline");
        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    public async Task ShouldInsertWithExceptSyntax()
    {
        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.insert_except");
        await connection.ExecuteStatementAsync(@"
            CREATE TABLE IF NOT EXISTS test.insert_except (
                id Int32,
                name String,
                value Float64,
                created DateTime DEFAULT now(),
                updated DateTime DEFAULT now()
            ) ENGINE Memory
        ");

        // Insert using EXCEPT syntax to exclude default columns
        var command = connection.CreateCommand();
        command.AddParameter("id", 42);
        command.AddParameter("name", "test-except");
        command.AddParameter("value", 99.99);
        command.CommandText = "INSERT INTO test.insert_except (* EXCEPT (created, updated)) VALUES ({id:Int32}, {name:String}, {value:Float64})";
        await command.ExecuteNonQueryAsync();

        var count = await connection.ExecuteScalarAsync("SELECT COUNT(*) FROM test.insert_except");
        Assert.That(count, Is.EqualTo(1));

        // Verify all columns including defaults using SELECT *
        using var reader = await connection.ExecuteReaderAsync("SELECT * FROM test.insert_except");
        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.FieldCount, Is.EqualTo(5));
        Assert.That(reader.GetInt32(0), Is.EqualTo(42));
        Assert.That(reader.GetString(1), Is.EqualTo("test-except"));
        Assert.That(reader.GetDouble(2), Is.EqualTo(99.99));
        // Verify default timestamps were set
        Assert.That(reader.GetDateTime(3), Is.GreaterThan(DateTime.UtcNow.AddMinutes(-1)));
        Assert.That(reader.GetDateTime(4), Is.GreaterThan(DateTime.UtcNow.AddMinutes(-1)));
    }
}
