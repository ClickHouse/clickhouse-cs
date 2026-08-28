using System;
using System.Collections.Generic;
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
    public void Build_ValuesInTheColumnsCanonicalType_WritesThemUnchanged()
    {
        Block schema = SchemaOf(Target("id", "Int32"), Target("name", "String"));
        object[][] rows = { new object[] { 1, "a" }, new object[] { 2, "b" } };

        IReadOnlyList<IColumn> columns = UntypedRowColumns.Build(schema, rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(columns[0], Is.InstanceOf<IColumn<int>>());
            Assert.That(columns[1], Is.InstanceOf<IColumn<string>>());
            Assert.That(columns[0].GetValue(1), Is.EqualTo(2));
            Assert.That(columns[1].GetValue(1), Is.EqualTo("b"));
        });
    }

    [Test]
    public void Build_ValuesInAConvenienceType_WritesThemInThatTypeRatherThanTheWireCount()
    {
        // Preserve the CLR spelling chosen by the values.
        Block schema = SchemaOf(Target("raw", "DateTime('UTC')"), Target("calendar", "DateTime('UTC')"));
        object[][] rows = { new object[] { 1_700_000_000u, DateTime.UnixEpoch } };

        IReadOnlyList<IColumn> columns = UntypedRowColumns.Build(schema, rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(columns[0], Is.InstanceOf<IColumn<uint>>());
            Assert.That(columns[1], Is.InstanceOf<IColumn<DateTime>>());
        });
    }

    [Test]
    public void Build_LeadingNulls_TakeTheTypeFromTheFirstRowWithAValue()
    {
        Block schema = SchemaOf(Target("value", "Nullable(DateTime('UTC'))"));
        object[][] rows = { new object[] { null }, new object[] { DateTime.UnixEpoch } };

        IReadOnlyList<IColumn> columns = UntypedRowColumns.Build(schema, rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(columns[0], Is.InstanceOf<IColumn<DateTime?>>());
            Assert.That(columns[0].GetValue(0), Is.Null);
            Assert.That(columns[0].GetValue(1), Is.EqualTo(DateTime.UnixEpoch));
        });
    }

    [Test]
    public void Build_ColumnOfOnlyNulls_FallsBackToTheTargetsCanonicalType()
    {
        // With no non-null value, use the target's preferred write type.
        Block schema = SchemaOf(Target("value", "Nullable(Int32)"));
        object[][] rows = { new object[] { null }, new object[] { null } };

        IReadOnlyList<IColumn> columns = UntypedRowColumns.Build(schema, rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(columns[0], Is.InstanceOf<IColumn<int?>>());
            Assert.That(columns[0].GetValue(0), Is.Null);
        });
    }

    [Test]
    public void Build_NullInANonNullableColumn_ThrowsNamingTheRowAndTheColumn()
    {
        Block schema = SchemaOf(Target("value", "Int32"));
        object[][] rows = { new object[] { 1 }, new object[] { null } };

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => UntypedRowColumns.Build(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("row 1").And.Contain("value"));
    }

    [Test]
    public void Build_ALaterColumnFailing_ThrowsNamingIt()
    {
        // A later failure also exercises cleanup of earlier columns.
        Block schema = SchemaOf(Target("ok", "Int32"), Target("bad", "Int32"));
        object[][] rows = { new object[] { 1, null } };

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => UntypedRowColumns.Build(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("bad"));
    }

    [Test]
    public void Build_MixedTypesInOneColumn_ThrowsNamingTheRow()
    {
        // Report the row that conflicts with the selected CLR type.
        Block schema = SchemaOf(Target("value", "Int32"));
        object[][] rows = { new object[] { 1 }, new object[] { "two" } };

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => UntypedRowColumns.Build(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("row 1").And.Contain("System.String"));
    }

    [Test]
    public void Build_ADynamicTarget_TakesEveryValueThroughItsObjectSurface()
    {
        // Dynamic's object surface permits mixed CLR types.
        Block schema = SchemaOf(Target("value", "Dynamic"));
        object[][] rows = { new object[] { 1 }, new object[] { "two" } };

        IReadOnlyList<IColumn> columns = UntypedRowColumns.Build(schema, rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(columns[0], Is.InstanceOf<IColumn<object>>());
            Assert.That(columns[0].GetValue(1), Is.EqualTo("two"));
        });
    }

    [Test]
    public void Build_ValueOfATypeTheColumnDoesNotAccept_ThrowsNamingWhatItAccepts()
    {
        Block schema = SchemaOf(Target("value", "Int32"));
        object[][] rows = { new object[] { "not a number" } };

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => UntypedRowColumns.Build(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("System.String").And.Contain("System.Int32"));
    }

    [Test]
    public void Build_RowOfTheWrongLength_ThrowsNamingTheRowAndTheTargets()
    {
        Block schema = SchemaOf(Target("id", "Int32"), Target("name", "String"));
        object[][] rows = { new object[] { 1, "a" }, new object[] { 2 } };

        ArgumentException error = Assert.Throws<ArgumentException>(() => UntypedRowColumns.Build(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("Row 1").And.Contain("id, name"));
    }

    [Test]
    public void Build_TargetWhoseWriterNeedsItsOwnColumnShape_SaysToUseTheColumnarApi()
    {
        Block schema = SchemaOf(Target("value", "Nested(a UInt8, b String)"));
        object[][] rows = { new object[] { Array.Empty<object[]>() } };

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => UntypedRowColumns.Build(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("columnar API"));
    }

    [Test]
    public void Build_ZeroRows_BuildsAnEmptyColumnPerTarget()
    {
        Block schema = SchemaOf(Target("id", "Int32"), Target("name", "String"));

        IReadOnlyList<IColumn> columns = UntypedRowColumns.Build(schema, Array.Empty<object[]>(), rowCount: 0);

        Assert.Multiple(() =>
        {
            Assert.That(columns, Has.Count.EqualTo(2));
            Assert.That(columns[0].RowCount, Is.EqualTo(0));
            Assert.That(columns[1].RowCount, Is.EqualTo(0));
        });
    }

    [Test]
    public void Build_FewerRowsThanTheBuffer_ReadsOnlyTheRowsAsked()
    {
        // Ignore unused entries in the pooled row buffer.
        Block schema = SchemaOf(Target("value", "Int32"));
        object[][] rows = { new object[] { 1 }, new object[] { 2 }, null };

        IReadOnlyList<IColumn> columns = UntypedRowColumns.Build(schema, rows, rowCount: 2);

        Assert.That(columns[0].RowCount, Is.EqualTo(2));
    }

    private static IColumn Target(string name, string typeName) => new ArrayColumn<object>(name, typeName, Array.Empty<object>());

    private static Block SchemaOf(params IColumn[] columns)
        => new(string.Empty, BlockInfo.Default, rowCount: 0, columns, ColumnCodecRegistry.Default, new ResolveContext { ServerTimezone = "UTC" });
}
