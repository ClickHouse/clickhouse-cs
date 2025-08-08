
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace ClickHouse.Driver.Tests.Types;

[TestFixture]
public class JsonTypeTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task ShouldSelectDataWithComplexHintedJsonType()
    {
        var targetTable = "test.aggregate_test";

        await connection.ExecuteStatementAsync(
            $@"
            CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON(level1_int Int32, nested.level2_string String)
            ) ENGINE = Memory;");

        var json = "{\"level1_int\": 789, \"nested\": {\"level2_string\": \"nested_value\"}, \"unhinted_float\": 99.9}";
        await connection.ExecuteStatementAsync($"INSERT INTO {targetTable} VALUES (1, '{json}')");

        using var reader = await connection.ExecuteReaderAsync($"SELECT data FROM {targetTable}");
        ClassicAssert.IsTrue(reader.Read());

        var result = (JsonObject)reader.GetValue(0);

        // Assert hinted properties
        ClassicAssert.AreEqual(789, (int)result["level1_int"]);
        ClassicAssert.AreEqual("nested_value", (string)result["nested"]["level2_string"]);

        // Assert non-hinted property
        ClassicAssert.IsInstanceOf<JsonValue>(result["unhinted_float"]);
        ClassicAssert.AreEqual(99.9, (double)result["unhinted_float"]);
    }
}
