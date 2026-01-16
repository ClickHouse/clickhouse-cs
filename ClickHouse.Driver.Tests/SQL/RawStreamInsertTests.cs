using System.IO;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.SQL;

public class RawStreamInsertTests : AbstractConnectionTestFixture
{
    private const string CsvDataNoHeader = """
        1,"Alice",10.5
        2,"Bob",20.3
        3,"Charlie",30.7
        """;

    private const string CsvDataWithHeader = """
        id,name,value
        1,"Alice",10.5
        2,"Bob",20.3
        3,"Charlie",30.7
        """;

    private const string JsonCompactEachRowData = """
        ["2022-04-30", 2021, "Sutton United", "Bradford City", 1, 4]
        ["2022-04-30", 2021, "Swindon Town", "Barrow", 2, 1]
        ["2022-04-30", 2021, "Tranmere Rovers", "Oldham Athletic", 2, 0]
        ["2022-05-02", 2021, "Port Vale", "Newport County", 1, 2]
        ["2022-05-02", 2021, "Salford City", "Mansfield Town", 2, 2]
        ["2022-05-07", 2021, "Barrow", "Northampton Town", 1, 3]
        ["2022-05-07", 2021, "Bradford City", "Carlisle United", 2, 0]
        """;
    
    private const string TsvData = "1\tAlice\t10.5\n2\tBob\t20.3\n3\tCharlie\t30.7";
    
    [TestCase("CSV", CsvDataNoHeader, true, true, TestName = "CSV with columns, compressed")]
    [TestCase("CSV", CsvDataNoHeader, true, false, TestName = "CSV with columns, uncompressed")]
    [TestCase("CSV", CsvDataNoHeader, false, true, TestName = "CSV without columns")]
    [TestCase("CSV", CsvDataWithHeader, false, true, TestName = "CSV with header auto-detection")]
    [TestCase("CSVWithNames", CsvDataWithHeader, false, true, TestName = "CSVWithNames without columns")]
    [TestCase("CSVWithNames", CsvDataWithHeader, true, true, TestName = "CSVWithNames with columns")]
    [TestCase("TSV", TsvData, true, true, TestName = "TSV")]
    public async Task ShouldInsertCsv(string format, string data, bool useColumns, bool useCompression)
    {
        var tableName = $"test.raw_csv_{format}_{useColumns}_{useCompression}";
        await connection.ExecuteStatementAsync($"DROP TABLE IF EXISTS {tableName}");
        await connection.ExecuteStatementAsync($"""
            CREATE TABLE {tableName} (
                id UInt64,
                name String,
                value Float32
            ) ENGINE Memory
            """);
        
        var columns = useColumns ? new[] { "id", "name", "value" } : null;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(data));
        using var response = await connection.InsertRawStreamAsync(
            table: tableName,
            stream: stream,
            format: format,
            columns: columns,
            useCompression: useCompression);

        Assert.That(response.IsSuccessStatusCode, Is.True);

        var count = await connection.ExecuteScalarAsync($"SELECT count() FROM {tableName}");
        Assert.That(count, Is.EqualTo(3));

        var sum = await connection.ExecuteScalarAsync($"SELECT sum(value) FROM {tableName}");
        Assert.That((double)sum, Is.EqualTo(61.5).Within(0.1));
    }

    [Test]
    public async Task ShouldInsertJsonCompactEachRow()
    {
        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.raw_json_compact");
        await connection.ExecuteStatementAsync("""
            CREATE TABLE test.raw_json_compact (
                date Date,
                season UInt16,
                home_team String,
                away_team String,
                home_score UInt8,
                away_score UInt8
            ) ENGINE Memory
            """);

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(JsonCompactEachRowData));
        using var response = await connection.InsertRawStreamAsync(
            table: "test.raw_json_compact",
            stream: stream,
            format: "JSONCompactEachRow",
            columns: new[] { "date", "season", "home_team", "away_team", "home_score", "away_score" });

        Assert.That(response.IsSuccessStatusCode, Is.True);

        var count = await connection.ExecuteScalarAsync("SELECT count() FROM test.raw_json_compact");
        Assert.That(count, Is.EqualTo(7));

        // Verify specific data
        var bradfordAwayScore = await connection.ExecuteScalarAsync(
            "SELECT away_score FROM test.raw_json_compact WHERE home_team = 'Sutton United'");
        Assert.That(bradfordAwayScore, Is.EqualTo(4));
    }

    [Test]
    public async Task ShouldInsertWithQueryId()
    {
        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.raw_query_id");
        await connection.ExecuteStatementAsync("""
            CREATE TABLE test.raw_query_id (
                id UInt64,
                name String,
                value Float32
            ) ENGINE Memory
            """);

        var queryId = "test-raw-stream-insert-" + System.Guid.NewGuid().ToString("N");

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(CsvDataNoHeader));
        using var response = await connection.InsertRawStreamAsync(
            table: "test.raw_query_id",
            stream: stream,
            format: "CSV",
            columns: new[] { "id", "name", "value" },
            queryId: queryId);

        Assert.That(response.IsSuccessStatusCode, Is.True);

        // Verify query ID from response header
        var returnedQueryId = ClickHouseConnection.ExtractQueryId(response);
        Assert.That(returnedQueryId, Is.EqualTo(queryId));
    }

    [Test]
    public async Task ShouldInsertPartialColumns()
    {
        await connection.ExecuteStatementAsync("DROP TABLE IF EXISTS test.raw_partial_columns");
        await connection.ExecuteStatementAsync("""
            CREATE TABLE test.raw_partial_columns (
                id UInt64,
                name String,
                value Float32 DEFAULT 0.0,
                created DateTime DEFAULT now()
            ) ENGINE Memory
            """);

        // Only insert id and name, let value and created use defaults
        var partialCsv = """
            1,"Alice"
            2,"Bob"
            """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(partialCsv));
        using var response = await connection.InsertRawStreamAsync(
            table: "test.raw_partial_columns",
            stream: stream,
            format: "CSV",
            columns: new[] { "id", "name" });

        Assert.That(response.IsSuccessStatusCode, Is.True);

        var count = await connection.ExecuteScalarAsync("SELECT count() FROM test.raw_partial_columns");
        Assert.That(count, Is.EqualTo(2));

        // Verify defaults were applied
        var valueSum = await connection.ExecuteScalarAsync("SELECT sum(value) FROM test.raw_partial_columns");
        Assert.That((double)valueSum, Is.EqualTo(0.0));
    }
}
