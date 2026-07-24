using System;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Utility;
using NUnit.Framework;
using System.Linq;

namespace ClickHouse.Driver.Tests.ADO;

[Category("Cloud")]
public class SqlSchemaTests : AbstractConnectionTestFixture
{
    [Test]
    public async Task ShouldGetReaderColumnSchema()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT 1 as num, 'a' as str");
        var schema = reader.GetColumnSchema();
        Assert.That(schema.Count, Is.EqualTo(2));
        Assert.Multiple(() =>
        {
            Assert.That(schema[0].ColumnName, Is.EqualTo("num"));
            Assert.That(schema[1].ColumnName, Is.EqualTo("str"));
        });
    }

    [Test]
    public async Task ShouldGetReaderSchemaTable()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT 1 as num, 'a' as str");
        var schema = reader.GetSchemaTable();
        Assert.That(schema.Rows.Count, Is.EqualTo(2));
    }

    [TestCase(0)]
    [TestCase(3)]
    [TestCase(9)]
    public async Task GetSchemaTable_DateTime64Column_ReportsScaleAsNumericScale(int scale)
    {
        using var reader = await connection.ExecuteReaderAsync($"SELECT toDateTime64(0, {scale}) AS t");
        var row = reader.GetSchemaTable().Rows[0];
        Assert.Multiple(() =>
        {
            Assert.That(row["NumericScale"], Is.EqualTo(scale));
            // DateTime64 carries a fractional scale but no decimal-style precision
            Assert.That(row["NumericPrecision"], Is.EqualTo(DBNull.Value));
        });
    }

    [Test]
    public async Task GetSchemaTable_NullableDateTime64Column_ReportsScaleAsNumericScale()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT CAST(0 AS Nullable(DateTime64(3))) AS t");
        var row = reader.GetSchemaTable().Rows[0];
        Assert.Multiple(() =>
        {
            // The scale is reported through the Nullable(...) wrapper, matching how DataType unwraps it
            Assert.That(row["NumericScale"], Is.EqualTo(3));
            Assert.That(row["AllowDBNull"], Is.True);
        });
    }

    [Test]
    public async Task GetColumnSchema_DateTime64Column_ReportsScaleAsNumericScale()
    {
        // GetColumnSchema() is the BCL DbColumn view built from GetSchemaTable(), so it must
        // surface the same fractional scale.
        using var reader = await connection.ExecuteReaderAsync("SELECT toDateTime64(0, 3) AS t");
        var column = reader.GetColumnSchema()[0];
        Assert.That(column.NumericScale, Is.EqualTo(3));
    }

    [Test]
    [RequiredFeature(Feature.Time)]
    public async Task GetSchemaTable_Time64Column_ReportsScaleAsNumericScale()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT CAST('12:30:45.123456' AS Time64(6)) AS t");
        var row = reader.GetSchemaTable().Rows[0];
        Assert.That(row["NumericScale"], Is.EqualTo(6));
    }

    [Test]
    public async Task GetSchemaTable_DateTimeColumn_DoesNotReportNumericScale()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT toDateTime(0) AS t");
        var row = reader.GetSchemaTable().Rows[0];
        Assert.Multiple(() =>
        {
            Assert.That(row["NumericScale"], Is.EqualTo(DBNull.Value));
            Assert.That(row["NumericPrecision"], Is.EqualTo(DBNull.Value));
        });
    }

    [Test]
    public async Task GetSchemaTable_DecimalColumn_ReportsPrecisionAndScale()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT CAST(1.23 AS Decimal(18, 4)) AS d");
        var row = reader.GetSchemaTable().Rows[0];
        Assert.Multiple(() =>
        {
            Assert.That(row["NumericPrecision"], Is.EqualTo(18));
            Assert.That(row["NumericScale"], Is.EqualTo(4));
            Assert.That(row["ColumnSize"], Is.EqualTo(8));
        });
    }

    [Test]
    public async Task GetSchemaTable_NullableDecimalColumn_ReportsPrecisionAndScale()
    {
        using var reader = await connection.ExecuteReaderAsync("SELECT CAST(1.23 AS Nullable(Decimal(18, 4))) AS d");
        var row = reader.GetSchemaTable().Rows[0];
        Assert.Multiple(() =>
        {
            Assert.That(row["NumericPrecision"], Is.EqualTo(18));
            Assert.That(row["NumericScale"], Is.EqualTo(4));
        });
    }

    [Test]
    public void ShouldGetSchemaTableAsDataTable()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT name, total_rows from system.tables";
        using var reader = command.ExecuteReader();
        var table = new DataTable();
        try
        {
            table.Load(reader);
        }
        catch
        {

        }
        var errors = table.GetErrors().Select(e => e.RowError).ToList();
        Assert.That(errors, Is.Empty);
    }
}
