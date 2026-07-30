using System.Collections.Concurrent;
using System.Net;
using System.Numerics;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Tests;
using LinqToDB;
using LinqToDB.Async;
using LinqToDB.Data;
using LinqToDB.DataProvider.ClickHouse;

namespace ClickHouse.Driver.IntegrationTests;

public class Tests
{
    sealed class Linq2DbTestTable
    {
        public int Id { get; set; }

        // test provider-specific type mapping
        public ClickHouseDecimal Decimal { get; set; }

        // test custom data reader Get* methods:

        // ClickHouseDataReader::GetIPAddress(int)
        public IPAddress? IPAddress { get; set; }
        // ClickHouseDataReader::GetSByte(int)
        public sbyte SByte { get; set; }
        // ClickHouseDataReader::GetUInt16(int)
        public ushort UInt16 { get; set; }
        // ClickHouseDataReader::GetUInt32(int)
        public uint UInt32 { get; set; }
        // ClickHouseDataReader::GetUInt64(int)
        public ulong UInt64 { get; set; }
        // ClickHouseDataReader::GetBigInteger(int)
        public BigInteger BigInteger { get; set; }

        public static Linq2DbTestTable[] TestData =
        [
            new Linq2DbTestTable()
        {
            Id = 1,
            Decimal = new ClickHouseDecimal(12.3M),
            IPAddress = IPAddress.Parse("1::"),
            SByte = -123,
            UInt16 = ushort.MaxValue,
            UInt32 = uint.MaxValue,
            UInt64 = ulong.MaxValue,
            BigInteger = BigInteger.Parse("567846734868672348679623672346")
        }
        ];
    }

    // Tables created by the tests, dropped on teardown in case a test fails before dropping its own.
    private readonly ConcurrentQueue<string> createdTables = new();

    [OneTimeTearDown]
    public async Task DropCreatedTables()
    {
        using var client = TestUtilities.GetTestClickHouseClient();

        while (createdTables.TryDequeue(out var table))
        {
            try
            {
                await client.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
            }
            catch (Exception e)
            {
                await TestContext.Progress.WriteLineAsync($"Failed to drop test table {table}: {e.Message}");
            }
        }
    }

    [Test]
    public async Task Linq2DbBulkCopy()
    {
        var connectionString = TestUtilities.GetConnectionStringBuilder().ConnectionString;

        // Covers: ClickHouseConnection::..ctor(string)
        await using var db = new DataConnection(new DataOptions().UseClickHouse(connectionString, ClickHouseProvider.ClickHouseDriver));

        // linq2db takes the table name from the mapping class, which cannot carry a per-run name, so
        // the name is overridden explicitly here. For ClickHouse, linq2db builds "database.table" from
        // the database name component, so the database has to be passed separately from the name.
        var tableName = TestUtilities.CreateTableName(database: null);
        createdTables.Enqueue($"{TestUtilities.TestDatabase}.{tableName}");

        // cannot use temp table as we need to test WithoutSession option, incompatible with session tables
        var tb = await db.CreateTableAsync<Linq2DbTestTable>(tableName: tableName, databaseName: TestUtilities.TestDatabase);

        var options = new BulkCopyOptions()
        {
            // Covers: ClickHouseBulkCopy::BatchSize
            MaxBatchSize = 10,
            // Covers: ClickHouseBulkCopy::MaxDegreeOfParallelism
            MaxDegreeOfParallelism = 1,
            // Covers:
            // ClickHouseConnectionStringBuilder::.ctor(string)
            // ClickHouseConnectionStringBuilder::UseSession
            // ClickHouseConnectionStringBuilder::ToString
            WithoutSession = true
        };

        // Covers:
        // ClickHouseBulkCopy::.ctor(ClickHouseConnection)
        // ClickHouseBulkCopy::Dispose()
        // ClickHouseBulkCopy::DestinationTableName
        // ClickHouseBulkCopy::RowsWritten
        // ClickHouseBulkCopy::InitAsync
        // ClickHouseBulkCopy::ColumnNames
        await tb.BulkCopyAsync(options, Linq2DbTestTable.TestData);

        db.InlineParameters = true;
        // Covers:
        // ClickHouseDecimal::ToString(IFormatProvider)
        var record = await tb.Where(r => r.Decimal == new ClickHouseDecimal(12.3M)).SingleAsync();

        // optional assert could be added

        await tb.DropAsync();

        Assert.Pass();
    }
}
