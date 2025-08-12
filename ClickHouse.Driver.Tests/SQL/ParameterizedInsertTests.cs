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
        var targetTable = $"test.{SanitizeTableName("float_array")}";
        await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (arr Array(Float64)) ENGINE Memory");

        var command = connection.CreateCommand();
        command.AddParameter("values", new[] { 1.0, 2.0, 3.0 });
        command.CommandText = $"INSERT INTO {targetTable} VALUES ({{values:Array(Float32)}})";
        await command.ExecuteNonQueryAsync();

        var count = await connection.ExecuteScalarAsync($"SELECT COUNT(*) FROM {targetTable}");
        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    public async Task ShouldInsertEnum8()
    {
        var targetTable = $"test.{SanitizeTableName("insert_enum8")}";
        await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (enum Enum8('a' = -1, 'b' = 127)) ENGINE Memory");

        var command = connection.CreateCommand();
        command.AddParameter("value", "a");
        command.CommandText = $"INSERT INTO {targetTable} VALUES ({{value:Enum8('a' = -1, 'b' = 127)}})";
        await command.ExecuteNonQueryAsync();

        var count = await connection.ExecuteScalarAsync($"SELECT COUNT(*) FROM {targetTable}");
        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    [RequiredFeature(Feature.UUIDParameters)]
    public async Task ShouldInsertParameterizedUUIDArray()
    {
        var targetTable = $"test.{SanitizeTableName("uuid_array")}";
        await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync(
            $"CREATE TABLE IF NOT EXISTS {targetTable} (arr Array(UUID)) ENGINE Memory");

        var command = connection.CreateCommand();
        command.AddParameter("values", new[] { Guid.NewGuid(), Guid.NewGuid(), });
        command.CommandText = $"INSERT INTO {targetTable} VALUES ({{values:Array(UUID)}})";
        await command.ExecuteNonQueryAsync();

        var count = await connection.ExecuteScalarAsync($"SELECT COUNT(*) FROM {targetTable}");
        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    public async Task ShouldInsertStringWithNewline()
    {
        var targetTable = $"test.{SanitizeTableName("string_with_newline")}";
        await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync(
            $"CREATE TABLE IF NOT EXISTS {targetTable} (str_value String) ENGINE Memory");

        var command = connection.CreateCommand();

        var strValue = "Hello \n ClickHouse";

        command.AddParameter("str_value", strValue);
        command.CommandText = $"INSERT INTO {targetTable} VALUES ({{str_value:String}})";
        await command.ExecuteNonQueryAsync();

        var count = await connection.ExecuteScalarAsync($"SELECT COUNT(*) FROM {targetTable}");
        Assert.That(count, Is.EqualTo(1));
    }
}
