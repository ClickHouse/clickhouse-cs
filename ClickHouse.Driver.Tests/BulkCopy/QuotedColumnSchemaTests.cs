using System.Collections.Generic;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tests.BulkCopy;

[TestFixture]
public class QuotedColumnSchemaTests : AbstractConnectionTestFixture
{
    // Raw column name:
    //   `a\`b`
    //
    // The name starts and ends with backticks and contains a backslash
    // immediately before an interior backtick.
    private const string RawColumnName = "`a\\`b`";

    // SQL spelling used to create and read the column. Existing backslashes
    // must be escaped before backticks.
    private static string QuotedColumnName =>
        $"`{RawColumnName.Replace("\\", "\\\\").Replace("`", "\\`")}`";

    public static IEnumerable<TestCaseData> SchemaResolutionCases()
    {
        yield return new TestCaseData("DefaultSchemaProbe", new InsertOptions());

        yield return new TestCaseData(
            "SchemaCache",
            new InsertOptions { UseSchemaCache = true });

        yield return new TestCaseData(
            "ExplicitColumnTypes",
            new InsertOptions
            {
                ColumnTypes = new Dictionary<string, string>
                {
                    [RawColumnName] = "Nullable(UInt8)",
                },
            });
    }

    [Test]
    [TestCaseSource(nameof(SchemaResolutionCases))]
    public async Task InsertBinaryAsync_QuotedColumnWithBackslashBeforeBacktick_RoundTrips(
        string schemaMode,
        InsertOptions options)
    {
        var tableName = CreateTableName($"quoted_column_{schemaMode}");

        await client.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} ({QuotedColumnName} Nullable(UInt8)) ENGINE = Memory");

        await client.InsertBinaryAsync(
            tableName,
            new[] { RawColumnName },
            new[] { new object[] { (byte)5 } },
            options);

        using var reader = await client.ExecuteReaderAsync(
            $"SELECT {QuotedColumnName} FROM {tableName}");

        Assert.That(reader.Read(), Is.True);
        Assert.That(reader.GetValue(0), Is.EqualTo((byte)5));
        Assert.That(reader.Read(), Is.False);
    }
}
