using System;
using System.Threading;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// Covers untyped-row transposition, write-type selection, and diagnostics.
/// </summary>
[TestFixture]
public class UntypedRowColumnsTests
{
    [Test]
    public void Gather_ValuesInTheColumnsCanonicalType_WritesThemUnchanged()
    {
        Block schema = SchemaOf(Target("id", "Int32"), Target("name", "String"));
        object[][] rows = { new object[] { 1, "a" }, new object[] { 2, "b" } };

        using PocoInsertSource<object[]> source = GatherAll(schema, rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(source.Columns[0], Is.InstanceOf<IColumn<int>>());
            Assert.That(source.Columns[1], Is.InstanceOf<IColumn<string>>());
            Assert.That(source.Columns[0].GetValue(1), Is.EqualTo(2));
            Assert.That(source.Columns[1].GetValue(1), Is.EqualTo("b"));
        });
    }

    [Test]
    public void Gather_ValuesInAConvenienceType_WritesThemInThatTypeRatherThanTheWireCount()
    {
        // Preserve the CLR spelling chosen by the values.
        Block schema = SchemaOf(Target("raw", "DateTime('UTC')"), Target("calendar", "DateTime('UTC')"));
        object[][] rows = { new object[] { 1_700_000_000u, DateTime.UnixEpoch } };

        using PocoInsertSource<object[]> source = GatherAll(schema, rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(source.Columns[0], Is.InstanceOf<IColumn<uint>>());
            Assert.That(source.Columns[1], Is.InstanceOf<IColumn<DateTime>>());
        });
    }

    [Test]
    public void CreateSource_LeadingNulls_TakeTheTypeFromTheFirstRowWithAValue()
    {
        Block schema = SchemaOf(Target("value", "Nullable(DateTime('UTC'))"));
        object[][] rows = { new object[] { null }, new object[] { DateTime.UnixEpoch } };

        using PocoInsertSource<object[]> source = GatherAll(schema, rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(source.Columns[0], Is.InstanceOf<IColumn<DateTime?>>());
            Assert.That(source.Columns[0].GetValue(0), Is.Null);
            Assert.That(source.Columns[0].GetValue(1), Is.EqualTo(DateTime.UnixEpoch));
        });
    }

    [Test]
    public void CreateSource_ValueInALaterBlock_StillChoosesTheWriteTypeFromIt()
    {
        // The write type is chosen once for the whole insert, so it has to look past the first block.
        Block schema = SchemaOf(Target("value", "Nullable(DateTime('UTC'))"));
        object[][] rows = { new object[] { null }, new object[] { null }, new object[] { DateTime.UnixEpoch } };
        using var buffer = PocoRowBuffer<object[]>.Create(rows, "rows", blockRows: 2, CancellationToken.None);

        using PocoInsertSource<object[]> source = UntypedRowColumns.CreateSource(schema, buffer, blockRows: 2);

        Assert.That(source.Columns[0], Is.InstanceOf<IColumn<DateTime?>>());
    }

    [Test]
    public void CreateSource_ColumnOfOnlyNulls_FallsBackToTheTargetsCanonicalType()
    {
        // With no non-null value, use the target's preferred write type.
        Block schema = SchemaOf(Target("value", "Nullable(Int32)"));
        object[][] rows = { new object[] { null }, new object[] { null } };

        using PocoInsertSource<object[]> source = GatherAll(schema, rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(source.Columns[0], Is.InstanceOf<IColumn<int?>>());
            Assert.That(source.Columns[0].GetValue(0), Is.Null);
        });
    }

    [Test]
    public void Gather_NullInANonNullableColumn_ThrowsNamingTheRowAndTheColumn()
    {
        Block schema = SchemaOf(Target("value", "Int32"));
        object[][] rows = { new object[] { 1 }, new object[] { null } };

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => GatherAll(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("row 1").And.Contain("value"));
    }

    [Test]
    public void Gather_ASecondBlock_NamesTheRowByItsNumberInTheInsertNotInTheBlock()
    {
        Block schema = SchemaOf(Target("value", "Int32"));
        object[][] rows = { new object[] { 1 }, new object[] { 2 }, new object[] { null } };
        using var buffer = PocoRowBuffer<object[]>.Create(rows, "rows", blockRows: 2, CancellationToken.None);
        using PocoInsertSource<object[]> source = UntypedRowColumns.CreateSource(schema, buffer, blockRows: 2);

        source.Gather(0, 2);
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => source.Gather(2, 1));

        Assert.That(error.Message, Does.Contain("row 2"));
    }

    [Test]
    public void Gather_ALaterColumnFailing_ThrowsNamingIt()
    {
        // A later failure also exercises cleanup of earlier columns.
        Block schema = SchemaOf(Target("ok", "Int32"), Target("bad", "Int32"));
        object[][] rows = { new object[] { 1, null } };

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => GatherAll(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("bad"));
    }

    [Test]
    public void Gather_MixedTypesInOneColumn_ThrowsNamingTheRow()
    {
        // Report the row that conflicts with the selected CLR type.
        Block schema = SchemaOf(Target("value", "Int32"));
        object[][] rows = { new object[] { 1 }, new object[] { "two" } };

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => GatherAll(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("row 1").And.Contain("System.String"));
    }

    [Test]
    public void Gather_ADynamicTarget_TakesEveryValueThroughItsObjectSurface()
    {
        // Dynamic's object surface permits mixed CLR types.
        Block schema = SchemaOf(Target("value", "Dynamic"));
        object[][] rows = { new object[] { 1 }, new object[] { "two" } };

        using PocoInsertSource<object[]> source = GatherAll(schema, rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(source.Columns[0], Is.InstanceOf<IColumn<object>>());
            Assert.That(source.Columns[0].GetValue(1), Is.EqualTo("two"));
        });
    }

    [Test]
    public void CreateSource_ValueOfATypeTheColumnDoesNotAccept_ThrowsNamingWhatItAccepts()
    {
        Block schema = SchemaOf(Target("value", "Int32"));
        object[][] rows = { new object[] { "not a number" } };

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => GatherAll(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("System.String").And.Contain("System.Int32"));
    }

    [Test]
    public void Gather_RowOfTheWrongLength_ThrowsNamingTheRowAndTheTargets()
    {
        Block schema = SchemaOf(Target("id", "Int32"), Target("name", "String"));
        object[][] rows = { new object[] { 1, "a" }, new object[] { 2 } };

        ArgumentException error = Assert.Throws<ArgumentException>(() => GatherAll(schema, rows, rows.Length));

        Assert.Multiple(() =>
        {
            Assert.That(error.Message, Does.Contain("Row 1").And.Contain("id, name"));
            Assert.That(error.ParamName, Is.EqualTo("rows"));
        });
    }

    [Test]
    public void CreateSource_TargetWhoseWriterNeedsItsOwnColumnShape_SaysToUseTheColumnarApi()
    {
        Block schema = SchemaOf(Target("value", "Nested(a UInt8, b String)"));
        object[][] rows = { new object[] { Array.Empty<object[]>() } };

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => GatherAll(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("columnar API"));
    }

    [Test]
    public void CreateSource_ZeroRows_HoldsAnEmptyColumnPerTarget()
    {
        Block schema = SchemaOf(Target("id", "Int32"), Target("name", "String"));

        using PocoInsertSource<object[]> source = GatherAll(schema, Array.Empty<object[]>(), rowCount: 0);

        Assert.Multiple(() =>
        {
            Assert.That(source.Columns, Has.Count.EqualTo(2));
            Assert.That(source.Columns[0].RowCount, Is.EqualTo(0));
            Assert.That(source.Columns[1].RowCount, Is.EqualTo(0));
        });
    }

    [Test]
    public void Gather_FewerRowsThanTheBuffer_ReadsOnlyTheRowsAsked()
    {
        // Ignore unused entries in the pooled row buffer.
        Block schema = SchemaOf(Target("value", "Int32"));
        object[][] rows = { new object[] { 1 }, new object[] { 2 }, null };

        using PocoInsertSource<object[]> source = GatherAll(schema, rows, rowCount: 2);

        Assert.That(source.Columns[0].RowCount, Is.EqualTo(2));
    }

    /// <summary>
    /// Gathers the rows as a single block. The source is returned rather than its columns, because it owns the
    /// buffers the columns read from.
    /// </summary>
    private static PocoInsertSource<object[]> GatherAll(Block schema, object[][] rows, int rowCount)
    {
        // The rows are an array, so the buffer borrows them and holds nothing past the gather.
        using var buffer = PocoRowBuffer<object[]>.Create(rows, "rows", rowCount, CancellationToken.None);
        PocoInsertSource<object[]> source = UntypedRowColumns.CreateSource(schema, buffer, rowCount);
        try
        {
            if (rowCount > 0)
            {
                source.Gather(0, rowCount);
            }

            return source;
        }
        catch
        {
            source.Dispose();
            throw;
        }
    }

    private static IColumn Target(string name, string typeName) => new ArrayColumn<object>(name, typeName, Array.Empty<object>());

    private static Block SchemaOf(params IColumn[] columns)
        => new(string.Empty, BlockInfo.Default, rowCount: 0, columns, ColumnCodecRegistry.Default, new ResolveContext { ServerTimezone = "UTC" });
}
