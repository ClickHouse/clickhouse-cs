
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
    [TestCase("")]
    [TestCase("level1_int Int32, nested.level2_string String")]
    [TestCase("level1_int Int32, nested.level2_string String, skip path.to.ignore")]
    [TestCase("level1_int Int32, nested.level2_string String, SKIP path.to.skip, SKIP REGEXP 'regex.path.*'")]
    [TestCase("max_dynamic_paths=10, level1_int Int32, nested.level2_string String")]
    [TestCase("max_dynamic_paths=10, level1_int Int32, nested.level2_string String, SKIP path.to.skip")]
    [TestCase("max_dynamic_types=3, level1_int Int32, nested.level2_string String")]
    [TestCase("skip_items Int32, nested.level2_string String, SKIP path.to.skip, skip path.to.ignore")]
    public async Task ShouldSelectDataWithComplexHintedJsonType(string jsonDefinition)
    {
        var targetTable = "test.select_data_complex_hinted_json";

        await connection.ExecuteStatementAsync(
            $@"
            CREATE OR REPLACE TABLE {targetTable} (
                id UInt32,
                data JSON({jsonDefinition})
            ) ENGINE = Memory;");

        var json = "{\"level1_int\": 789, \"skip_items\": 30, \"nested\": {\"level2_string\": \"nested_value\"}, \"unhinted_float\": 99.9}";
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
