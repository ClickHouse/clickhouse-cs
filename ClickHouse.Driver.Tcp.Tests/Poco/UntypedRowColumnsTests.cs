using System;
using System.Collections.Generic;
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

    /// <summary>
    /// The CLR casts an array of an integer type to an array of the other sign, and an array of an enum to an array of
    /// an integer type other than its underlying type. The cast gives each element another meaning, so a later value of
    /// such a type is refused with its row, as the derivation refuses it as the first value.
    /// </summary>
    /// <param name="type">The target type.</param>
    /// <param name="first">The first value, which chooses the write type.</param>
    /// <param name="later">A later value that the CLR casts to the write type.</param>
    [TestCaseSource(nameof(ReinterpretedLaterValues))]
    public void Gather_LaterArrayThatTheCastGivesAnotherMeaning_ThrowsNamingTheRow(string type, object first, object later)
    {
        Block schema = SchemaOf(Target("value", type));
        object[][] rows = { new[] { first }, new[] { later } };

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => GatherAll(schema, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("row 1 holds a " + later.GetType()));
    }

    /// <summary>A later value of another sign in a later block is refused with its row in the insert.</summary>
    [Test]
    public void Gather_UInt32ArrayInALaterBlockThanTheInt32Array_ThrowsNamingTheRow()
    {
        Block schema = SchemaOf(Target("value", "Array(Int32)"));
        object[][] rows = { new object[] { new[] { 7 } }, new object[] { new[] { 3_000_000_000u } } };
        using var buffer = PocoRowBuffer<object[]>.Create(rows, "rows", blockRows: 1, CancellationToken.None);
        using PocoInsertSource<object[]> source = UntypedRowColumns.CreateSource(schema, buffer, blockRows: 1);

        source.Gather(0, 1);
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => source.Gather(1, 1));

        Assert.That(
            error.Message,
            Is.EqualTo("Column 0 ('value', Array(Int32)) is written as System.Int32[], but row 1 holds a System.UInt32[]. Every value of one column must have the same CLR type."));
    }

    /// <summary>
    /// An array of an enum and an array of its underlying type keep the integer value of each element, so they share a
    /// column in either order.
    /// </summary>
    [Test]
    public void Gather_EnumArrayAndArrayOfItsUnderlyingType_ShareAColumn()
    {
        Block schema = SchemaOf(Target("a", "Array(Int32)"), Target("b", "Array(Int32)"));
        object[][] rows = { new object[] { new[] { 7 }, new[] { IntLevel.B } }, new object[] { new[] { IntLevel.B }, new[] { 7 } } };

        using PocoInsertSource<object[]> source = GatherAll(schema, rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(FirstElement(source.Columns[0], 0), Is.EqualTo(7));
            Assert.That(FirstElement(source.Columns[0], 1), Is.EqualTo(2));
            Assert.That(FirstElement(source.Columns[1], 0), Is.EqualTo(2));
            Assert.That(FirstElement(source.Columns[1], 1), Is.EqualTo(7));
        });
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

    private static IEnumerable<TestCaseData> ReinterpretedLaterValues()
    {
        yield return new TestCaseData("Array(Int32)", new[] { 7 }, new[] { 3_000_000_000u }).SetName("{m}(UInt32ArrayAfterInt32Array)");
        yield return new TestCaseData("Array(UInt8)", new byte[] { 7 }, new sbyte[] { -1 }).SetName("{m}(SByteArrayAfterByteArray)");
        yield return new TestCaseData("Array(Array(Int32))", new[] { new[] { 7 } }, new[] { new[] { 3_000_000_000u } }).SetName("{m}(JaggedUInt32AfterJaggedInt32)");
        yield return new TestCaseData("Array(Int32)", new[] { 7 }, new[] { UIntLevel.A }).SetName("{m}(UIntEnumArrayAfterInt32Array)");
        yield return new TestCaseData("Array(Int32)", new[] { IntLevel.B }, new[] { 3_000_000_000u }).SetName("{m}(UInt32ArrayAfterIntEnumArray)");
    }

    // The first element of the array at the row, as a number.
    private static long FirstElement(IColumn column, int row) => Convert.ToInt64(((Array)column.GetValue(row)).GetValue(0));

    private static IColumn Target(string name, string typeName) => new ArrayColumn<object>(name, typeName, Array.Empty<object>());

    private static Block SchemaOf(params IColumn[] columns)
        => new(string.Empty, BlockInfo.Default, rowCount: 0, columns, ColumnCodecRegistry.Default, new ResolveContext { ServerTimezone = "UTC" });

    internal enum IntLevel
    {
        A = 1,
        B = 2,
    }

    internal enum UIntLevel : uint
    {
        A = 1,
    }
}
