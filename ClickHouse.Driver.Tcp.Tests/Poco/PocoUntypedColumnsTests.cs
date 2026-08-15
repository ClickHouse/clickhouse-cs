using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The untyped <c>object[]</c> insert's transposition. Round-trip coverage is in the integration suite; what is here
/// is the choice a round trip cannot see — which CLR type each column was written in, which the values themselves
/// decide — and the failures, which name a row and a column the caller counted.
///
/// <para>
/// The sample blocks are empty columns stamped with the type under test, which is what the server's own sample block
/// is: only the name and the type string are read, the codec coming from the latter.
/// </para>
/// </summary>
[TestFixture]
public class PocoUntypedColumnsTests
{
    [Test]
    public void Build_ValuesInTheColumnsCanonicalType_WritesThemUnchanged()
    {
        Block schema = SchemaOf(Target("id", "Int32"), Target("name", "String"));
        object[][] rows = { new object[] { 1, "a" }, new object[] { 2, "b" } };

        IReadOnlyList<IColumn> columns = PocoUntypedColumns.Build(schema, rows, rows.Length);

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
        // What lets rows written by hand and rows from an untyped read both work: a DateTime column takes either
        // spelling, and the one the values are in is the one the codec is handed.
        Block schema = SchemaOf(Target("raw", "DateTime('UTC')"), Target("calendar", "DateTime('UTC')"));
        object[][] rows = { new object[] { 1_700_000_000u, DateTime.UnixEpoch } };

        IReadOnlyList<IColumn> columns = PocoUntypedColumns.Build(schema, rows, rows.Length);

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

        IReadOnlyList<IColumn> columns = PocoUntypedColumns.Build(schema, rows, rows.Length);

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
        // Nothing names a type, so the target's own leading write type is the only answer available — and the right
        // one, since a Nullable target takes the nulls whichever spelling carries them.
        Block schema = SchemaOf(Target("value", "Nullable(Int32)"));
        object[][] rows = { new object[] { null }, new object[] { null } };

        IReadOnlyList<IColumn> columns = PocoUntypedColumns.Build(schema, rows, rows.Length);

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

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => PocoUntypedColumns.Build(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("row 1").And.Contain("value"));
    }

    [Test]
    public void Build_ALaterColumnFailing_ThrowsNamingIt()
    {
        // Also the path that releases what the earlier columns rented — each is filled into its own buffer, and only
        // the column that wraps one returns it. The release itself is not observable; this pins the failure.
        Block schema = SchemaOf(Target("ok", "Int32"), Target("bad", "Int32"));
        object[][] rows = { new object[] { 1, null } };

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => PocoUntypedColumns.Build(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("bad"));
    }

    [Test]
    public void Build_MixedTypesInOneColumn_ThrowsNamingTheRow()
    {
        // The type is settled by the first row that has a value, so a later row in another type would otherwise fail
        // as a bare unboxing error with no row to point at.
        Block schema = SchemaOf(Target("value", "Int32"));
        object[][] rows = { new object[] { 1 }, new object[] { "two" } };

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => PocoUntypedColumns.Build(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("row 1").And.Contain("System.String"));
    }

    [Test]
    public void Build_ADynamicTarget_TakesEveryValueThroughItsObjectSurface()
    {
        // Dynamic and Variant are written from a column of object, so the type sniffing has nothing to reject and a
        // column may legitimately hold values of several types.
        Block schema = SchemaOf(Target("value", "Dynamic"));
        object[][] rows = { new object[] { 1 }, new object[] { "two" } };

        IReadOnlyList<IColumn> columns = PocoUntypedColumns.Build(schema, rows, rows.Length);

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

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => PocoUntypedColumns.Build(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("System.String").And.Contain("System.Int32"));
    }

    [Test]
    public void Build_RowOfTheWrongLength_ThrowsNamingTheRowAndTheTargets()
    {
        Block schema = SchemaOf(Target("id", "Int32"), Target("name", "String"));
        object[][] rows = { new object[] { 1, "a" }, new object[] { 2 } };

        ArgumentException error = Assert.Throws<ArgumentException>(() => PocoUntypedColumns.Build(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("Row 1").And.Contain("id, name"));
    }

    [Test]
    public void Build_TargetWhoseWriterNeedsItsOwnColumnShape_SaysToUseTheColumnarApi()
    {
        Block schema = SchemaOf(Target("value", "Nested(a UInt8, b String)"));
        object[][] rows = { new object[] { Array.Empty<object[]>() } };

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => PocoUntypedColumns.Build(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("columnar API"));
    }

    [Test]
    public void Build_ZeroRows_BuildsAnEmptyColumnPerTarget()
    {
        Block schema = SchemaOf(Target("id", "Int32"), Target("name", "String"));

        IReadOnlyList<IColumn> columns = PocoUntypedColumns.Build(schema, Array.Empty<object[]>(), rowCount: 0);

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
        // The rows arrive in a pooled array that is usually longer than the insert, and the entries past the count
        // are whatever the pool last held.
        Block schema = SchemaOf(Target("value", "Int32"));
        object[][] rows = { new object[] { 1 }, new object[] { 2 }, null };

        IReadOnlyList<IColumn> columns = PocoUntypedColumns.Build(schema, rows, rowCount: 2);

        Assert.That(columns[0].RowCount, Is.EqualTo(2));
    }

    private static IColumn Target(string name, string typeName) => new ArrayColumn<object>(name, typeName, Array.Empty<object>());

    private static Block SchemaOf(params IColumn[] columns)
        => new(string.Empty, BlockInfo.Default, rowCount: 0, columns, ColumnCodecRegistry.Default, new ResolveContext { ServerTimezone = "UTC" });
}
