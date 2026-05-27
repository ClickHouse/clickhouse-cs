using ClickHouse.Driver.Utility;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tests.BinaryImport;

internal class BinaryImportTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task ShouldExecuteInsertWithLessColumns()
    {
        var targetTable = $"test.bi_multiple_columns";

        await connection.ExecuteStatementAsync($"TRUNCATE TABLE IF EXISTS {targetTable}");
        await connection.ExecuteStatementAsync($"CREATE TABLE IF NOT EXISTS {targetTable} (value1 Nullable(UInt8), value2 Nullable(Float32), value3 Nullable(Int8)) ENGINE Memory");

        var import = await connection.ClickHouseClient.StartInsertAsync(targetTable, ["value2"]);
        using var batch = import.StartNewBatch();
        using var batch2 = import.StartNewBatch();

        var expectValues = new List<float>();
        for (int i = 0; i < 10; i++)
        {
            expectValues.Add(i);
        }

        for (int i = 0; i < 5; i++)
        {
            float value2 = expectValues[i];
            batch.WriteData(0, value2);
        }

        batch.CompleteWrite();

        for (int i = 5; i < 10; i++)
        {
            float value2 = expectValues[i];
            batch2.WriteData(0, value2);
        }

        batch2.CompleteWrite();

        var send1 = import.SendBatchAsync(batch, default);
        var send2 = import.SendBatchAsync(batch2, default);

        await Task.WhenAll(send1, send2);

        var actualValues = new List<float>();

        using (var reader = await connection.ExecuteReaderAsync($"SELECT * from {targetTable}"))
        {
            while (reader.Read())
            {
                var value = reader.GetFloat("value2");
                actualValues.Add(value);
            }
        }

        var actual = actualValues.OrderBy(o => o).ToArray();

        Assert.That(actual, Has.Length.EqualTo(expectValues.Count));
        for (int i = 0; i < actual.Length; i++)
        {
            var expectValue = expectValues[i];
            var actualValue = actual[i];
            Assert.That(actualValue, Is.EqualTo(expectValue));
        }
    }
}
