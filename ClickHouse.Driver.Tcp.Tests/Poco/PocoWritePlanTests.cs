using System;
using System.Collections.Generic;
using System.Threading;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// Covers POCO write-plan mapping, CLR write-type selection, and cache keys.
/// </summary>
[TestFixture]
public class PocoWritePlanTests
{
    [Test]
    public void Build_TargetColumnMatchingNoProperty_Throws()
    {
        // Every target named by the INSERT must be filled.
        Block schema = SchemaOf(Target("value", "Int32"), Target("extra", "String"));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Plan<Row<int>>(schema));

        Assert.That(error.Message, Does.Contain("extra").And.Contain("INSERT INTO t (a, b) VALUES"));
    }

    [Test]
    public void Build_PropertyWithNoPublicGetter_Throws()
    {
        Block schema = SchemaOf(Target("Value", "Int32"));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Plan<SetterOnlyPoco>(schema));

        Assert.That(error.Message, Does.Contain("no public getter"));
    }

    [Test]
    public void Build_TwoTargetColumnsReachingOneProperty_Throws()
    {
        // Both target names resolve to the same property.
        Block schema = SchemaOf(Target("user_id", "Int32"), Target("userId", "Int32"));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Plan<UserRow>(schema));

        Assert.That(error.Message, Does.Contain("user_id").And.Contain("userId").And.Contain("UserId"));
    }

    [Test]
    public void Build_PropertyTypeTheColumnCannotBeWrittenFrom_ThrowsNamingWhatItAccepts()
    {
        Block schema = SchemaOf(Target("value", "Int32"));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Plan<Row<Guid>>(schema));

        Assert.That(error.Message, Does.Contain("System.Guid").And.Contain("System.Int32"));
    }

    [Test]
    public void Build_WideningPropertyType_IsDeclined()
    {
        // Preserve read/write symmetry by declining numeric widening.
        Block schema = SchemaOf(Target("value", "Int64"));

        Assert.Throws<InvalidOperationException>(() => Plan<Row<int>>(schema));
    }

    [Test]
    public void Build_TargetWhoseWriterNeedsItsOwnColumnShape_SaysToUseTheColumnarApi()
    {
        // Nested requires a specialized column shape that a property cannot provide.
        Block schema = SchemaOf(Target("value", "Nested(a UInt8, b String)"));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Plan<Row<object[][]>>(schema));

        Assert.That(error.Message, Does.Contain("columnar API"));
    }

    [Test]
    public void Gather_CalendarProperty_WritesThroughTheCalendarTypeNotTheWireCount()
    {
        // Leave DateTime conversion to the codec.
        Block schema = SchemaOf(Target("value", "DateTime('UTC')"));
        var rows = new[] { new Row<DateTime> { Value = DateTime.UnixEpoch } };

        using PocoInsertSource<Row<DateTime>> source = GatherAll(Plan<Row<DateTime>>(schema), rows, rows.Length);

        Assert.That(source.Columns[0], Is.InstanceOf<IColumn<DateTime>>());
    }

    [Test]
    public void Gather_NonNullablePropertyIntoANullableColumn_LiftsToTheNullableWriteType()
    {
        Block schema = SchemaOf(Target("value", "Nullable(Int32)"));
        var rows = new[] { new Row<int> { Value = 42 } };

        using PocoInsertSource<Row<int>> source = GatherAll(Plan<Row<int>>(schema), rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(source.Columns[0], Is.InstanceOf<IColumn<int?>>());
            Assert.That(source.Columns[0].GetValue(0), Is.EqualTo(42));
        });
    }

    [Test]
    public void Gather_NullableCalendarProperty_WritesThroughTheLiftedCalendarType()
    {
        // Verify that nullable codecs expose their lifted calendar spelling.
        Block schema = SchemaOf(Target("value", "Nullable(DateTime('UTC'))"));
        var rows = new[] { new Row<DateTime?> { Value = DateTime.UnixEpoch }, new Row<DateTime?> { Value = null } };

        using PocoInsertSource<Row<DateTime?>> source = GatherAll(Plan<Row<DateTime?>>(schema), rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(source.Columns[0], Is.InstanceOf<IColumn<DateTime?>>());
            Assert.That(source.Columns[0].GetValue(0), Is.EqualTo(DateTime.UnixEpoch));
            Assert.That(source.Columns[0].GetValue(1), Is.Null);
        });
    }

    [Test]
    public void Build_TypeThatCannotBeMaterialized_StillBuildsAWritePlan()
    {
        // A write plan does not need to construct the row type.
        Block schema = SchemaOf(Target("Value", "Int32"));
        PocoTypeDescriptor<ConstructorOnlyPoco> descriptor = PocoTypeDescriptor<ConstructorOnlyPoco>.Build();
        var rows = new[] { new ConstructorOnlyPoco(7) };

        Assert.That(descriptor.CanActivate, Is.False, "the type a query could not materialize");

        using PocoInsertSource<ConstructorOnlyPoco> source =
            GatherAll(PocoWritePlan<ConstructorOnlyPoco>.Build(descriptor, schema), rows, rows.Length);

        Assert.That(source.Columns[0].GetValue(0), Is.EqualTo(7));
    }

    [Test]
    public void Gather_NullablePropertyIntoANullableColumn_CarriesTheNullThrough()
    {
        Block schema = SchemaOf(Target("value", "Nullable(Int32)"));
        var rows = new[] { new Row<int?> { Value = null }, new Row<int?> { Value = 7 } };

        using PocoInsertSource<Row<int?>> source = GatherAll(Plan<Row<int?>>(schema), rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(source.Columns[0].GetValue(0), Is.Null);
            Assert.That(source.Columns[0].GetValue(1), Is.EqualTo(7));
        });
    }

    [Test]
    public void Gather_NullPropertyIntoANonNullableColumn_ThrowsNamingTheRow()
    {
        // Nullability is checked per row because non-null values remain valid.
        Block schema = SchemaOf(Target("value", "Int32"));
        var rows = new[] { new Row<int?> { Value = 1 }, new Row<int?> { Value = null } };
        PocoWritePlan<Row<int?>> plan = Plan<Row<int?>>(schema);

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => GatherAll(plan, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("row 1").And.Contain("value").And.Contain("Value"));
    }

    [Test]
    public void Gather_ASecondBlock_NamesTheRowByItsNumberInTheInsertNotInTheBlock()
    {
        // The row a value failed at must be the caller's row number, whichever block held it.
        Block schema = SchemaOf(Target("value", "Int32"));
        var rows = new[] { new Row<int?> { Value = 1 }, new Row<int?> { Value = 2 }, new Row<int?> { Value = null } };
        using var buffer = PocoRowBuffer<Row<int?>>.Create(rows, "rows", blockRows: 2, CancellationToken.None);
        using PocoInsertSource<Row<int?>> source = Plan<Row<int?>>(schema).CreateSource(buffer, blockRows: 2);

        source.Gather(0, 2);
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => source.Gather(2, 1));

        Assert.That(error.Message, Does.Contain("row 2"));
    }

    [Test]
    public void Gather_ALaterColumnFailing_ThrowsNamingIt()
    {
        // A later failure also exercises cleanup of earlier columns.
        Block schema = SchemaOf(Target("Ok", "Int32"), Target("Bad", "Int32"));
        var rows = new[] { new TwoValues { Ok = 1, Bad = null } };
        PocoWritePlan<TwoValues> plan = Plan<TwoValues>(schema);

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => GatherAll(plan, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("Bad"));
    }

    [Test]
    public void Gather_NullablePropertyIntoAColumnWrittenFromObject_CarriesTheNullThrough()
    {
        // Dynamic's object surface can carry null.
        Block schema = SchemaOf(Target("value", "Dynamic"));
        var rows = new[] { new Row<int?> { Value = null }, new Row<int?> { Value = 7 } };

        using PocoInsertSource<Row<int?>> source = GatherAll(Plan<Row<int?>>(schema), rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(source.Columns[0], Is.InstanceOf<IColumn<object>>());
            Assert.That(source.Columns[0].GetValue(0), Is.Null);
            Assert.That(source.Columns[0].GetValue(1), Is.EqualTo(7));
        });
    }

    [Test]
    public void Gather_NullStringPropertyIntoANonNullableColumn_ThrowsNamingTheRow()
    {
        // Reject null references before the block is written.
        Block schema = SchemaOf(Target("value", "String"));
        var rows = new[] { new Row<string> { Value = "a" }, new Row<string> { Value = null } };
        PocoWritePlan<Row<string>> plan = Plan<Row<string>>(schema);

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => GatherAll(plan, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("row 1").And.Contain("value").And.Contain("Value"));
    }

    [Test]
    public void Gather_NullArrayPropertyIntoANonNullableColumn_ThrowsNamingTheRow()
    {
        // The same rule reaches every reference-typed write type, not just string.
        Block schema = SchemaOf(Target("value", "Array(Int32)"));
        var rows = new[] { new Row<int[]> { Value = new[] { 1 } }, new Row<int[]> { Value = null } };
        PocoWritePlan<Row<int[]>> plan = Plan<Row<int[]>>(schema);

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => GatherAll(plan, rows, rows.Length));

        Assert.That(error.Message, Does.Contain("row 1"));
    }

    [Test]
    public void Gather_NullReferencePropertyIntoANullableColumn_CarriesTheNullThrough()
    {
        // A nullable target accepts the reference null directly.
        Block schema = SchemaOf(Target("value", "Nullable(String)"));
        var rows = new[] { new Row<string> { Value = "a" }, new Row<string> { Value = null } };

        using PocoInsertSource<Row<string>> source = GatherAll(Plan<Row<string>>(schema), rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(source.Columns[0].GetValue(0), Is.EqualTo("a"));
            Assert.That(source.Columns[0].GetValue(1), Is.Null);
        });
    }

    [Test]
    public void Gather_EnumProperty_WritesTheOrdinal()
    {
        Block schema = SchemaOf(Target("value", "Enum8('low' = -1, 'high' = 127)"));
        var rows = new[] { new Row<Level> { Value = Level.High } };

        using PocoInsertSource<Row<Level>> source = GatherAll(Plan<Row<Level>>(schema), rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(source.Columns[0], Is.InstanceOf<IColumn<sbyte>>());
            Assert.That(source.Columns[0].GetValue(0), Is.EqualTo((sbyte)127));
        });
    }

    [Test]
    public void Gather_PropertyMatchingNoTargetColumn_IsNotInserted()
    {
        // Properties not named by the INSERT are ignored.
        Block schema = SchemaOf(Target("Id", "Int32"));
        var rows = new[] { new IdName { Id = 3, Name = "not inserted" } };

        using PocoInsertSource<IdName> source = GatherAll(Plan<IdName>(schema), rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(source.Columns, Has.Count.EqualTo(1));
            Assert.That(source.Columns[0].Name, Is.EqualTo("Id"));
            Assert.That(source.Columns[0].GetValue(0), Is.EqualTo(3));
        });
    }

    [Test]
    public void Gather_SeveralTargets_AreFilledInSchemaOrderWithTheTargetsNamesAndTypes()
    {
        // Output columns follow the sample block's order, names, and types.
        Block schema = SchemaOf(Target("Name", "String"), Target("Id", "Int32"));
        var rows = new[] { new IdName { Id = 1, Name = "a" }, new IdName { Id = 2, Name = "b" } };

        using PocoInsertSource<IdName> source = GatherAll(Plan<IdName>(schema), rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(source.Columns[0].Name, Is.EqualTo("Name"));
            Assert.That(source.Columns[0].TypeName, Is.EqualTo("String"));
            Assert.That(source.Columns[0].RowCount, Is.EqualTo(2));
            Assert.That(source.Columns[1].Name, Is.EqualTo("Id"));
            Assert.That(source.Columns[1].TypeName, Is.EqualTo("Int32"));
            Assert.That(source.Columns[1].GetValue(1), Is.EqualTo(2));
        });
    }

    [Test]
    public void Gather_FewerRowsThanTheBuffer_ExposesOnlyTheRowsAsked()
    {
        // Use the block's row count, not the pooled buffer length.
        Block schema = SchemaOf(Target("value", "Int32"));
        var rows = new[] { new Row<int> { Value = 1 }, new Row<int> { Value = 2 }, new Row<int> { Value = 3 } };

        using PocoInsertSource<Row<int>> source = GatherAll(Plan<Row<int>>(schema), rows, rowCount: 2);

        Assert.That(source.Columns[0].RowCount, Is.EqualTo(2));
    }

    [Test]
    public void CreateSource_BeforeTheFirstGather_HoldsColumnsWithNoRows()
    {
        // The insert plan is built from these columns before any row is converted.
        Block schema = SchemaOf(Target("value", "Int32"));
        var rows = new[] { new Row<int> { Value = 1 } };
        using var buffer = PocoRowBuffer<Row<int>>.Create(rows, "rows", blockRows: 1, CancellationToken.None);

        using PocoInsertSource<Row<int>> source = Plan<Row<int>>(schema).CreateSource(buffer, blockRows: 1);

        Assert.Multiple(() =>
        {
            Assert.That(source.Columns, Has.Count.EqualTo(1));
            Assert.That(source.Columns[0].RowCount, Is.Zero);
            Assert.That(source.Columns[0], Is.InstanceOf<IColumn<int>>());
        });
    }

    [Test]
    public void Gather_EachBlockInTurn_ReusesTheSameColumnObjects()
    {
        // The write plan holds these columns, so their identity has to survive every block.
        Block schema = SchemaOf(Target("value", "Int32"));
        var rows = new[] { new Row<int> { Value = 1 }, new Row<int> { Value = 2 }, new Row<int> { Value = 3 } };
        using var buffer = PocoRowBuffer<Row<int>>.Create(rows, "rows", blockRows: 2, CancellationToken.None);
        using PocoInsertSource<Row<int>> source = Plan<Row<int>>(schema).CreateSource(buffer, blockRows: 2);

        source.Gather(0, 2);
        IColumn first = source.Columns[0];
        object firstBlock = first.GetValue(1);

        source.Gather(2, 1);

        Assert.Multiple(() =>
        {
            Assert.That(firstBlock, Is.EqualTo(2));
            Assert.That(source.Columns[0], Is.SameAs(first));
            Assert.That(source.Columns[0].RowCount, Is.EqualTo(1), "the column holds the current block only");
            Assert.That(source.Columns[0].GetValue(0), Is.EqualTo(3));
        });
    }

    [Test]
    public void Gather_ABlockLongerThanTheBuffer_ThrowsRatherThanWriteAWrongSizedBlock()
    {
        Block schema = SchemaOf(Target("value", "Int32"));
        var rows = new[] { new Row<int> { Value = 1 }, new Row<int> { Value = 2 } };
        using var buffer = PocoRowBuffer<Row<int>>.Create(rows, "rows", blockRows: 1, CancellationToken.None);
        using PocoInsertSource<Row<int>> source = Plan<Row<int>>(schema).CreateSource(buffer, blockRows: 1);

        Assert.Throws<ArgumentOutOfRangeException>(() => source.Gather(0, 2));
    }

    [Test]
    public void WritePlanFor_SameSchema_ReturnsTheCachedPlan()
    {
        var registry = new PocoTypeRegistry();

        PocoWritePlan<Row<int>> first = registry.WritePlanFor<Row<int>>(SchemaOf(Target("value", "Int32")));
        PocoWritePlan<Row<int>> second = registry.WritePlanFor<Row<int>>(SchemaOf(Target("value", "Int32")));

        Assert.That(second, Is.SameAs(first));
    }

    [Test]
    public void WritePlanFor_SameColumnNameDifferentType_CompilesItsOwnPlan()
    {
        // The exact target type is part of the plan key.
        var registry = new PocoTypeRegistry();

        PocoWritePlan<Row<int>> ints = registry.WritePlanFor<Row<int>>(SchemaOf(Target("value", "Int32")));
        PocoWritePlan<Row<int>> nullables = registry.WritePlanFor<Row<int>>(SchemaOf(Target("value", "Nullable(Int32)")));

        var rows = new[] { new Row<int> { Value = 1 } };
        using PocoInsertSource<Row<int>> intSource = GatherAll(ints, rows, rows.Length);
        using PocoInsertSource<Row<int>> nullableSource = GatherAll(nullables, rows, rows.Length);
        Assert.Multiple(() =>
        {
            Assert.That(nullables, Is.Not.SameAs(ints));
            Assert.That(intSource.Columns[0], Is.InstanceOf<IColumn<int>>());
            Assert.That(nullableSource.Columns[0], Is.InstanceOf<IColumn<int?>>());
        });
    }

    [Test]
    public void WritePlanFor_ColumnNameHoldingTheKeySeparators_DoesNotCollideWithAnotherSchema()
    {
        // Arbitrary column names must not collide with cache-key separators.
        var registry = new PocoTypeRegistry();
        Block spelled = SchemaOf(Target("Id", "Int32"), Target("Name\tInt32\nScore", "Int32"));
        Block three = SchemaOf(Target("Id", "Int32"), Target("Name", "Int32"), Target("Score", "Int32"));

        PocoWritePlan<SeparatorColumns> spelledPlan = registry.WritePlanFor<SeparatorColumns>(spelled);
        PocoWritePlan<SeparatorColumns> threePlan = registry.WritePlanFor<SeparatorColumns>(three);

        var rows = new[] { new SeparatorColumns { Id = 1, Spelled = 9, Name = 2, Score = 3 } };
        using PocoInsertSource<SeparatorColumns> spelledSource = GatherAll(spelledPlan, rows, rows.Length);
        using PocoInsertSource<SeparatorColumns> threeSource = GatherAll(threePlan, rows, rows.Length);
        Assert.Multiple(() =>
        {
            Assert.That(threePlan, Is.Not.SameAs(spelledPlan));
            Assert.That(Names(spelledSource.Columns), Is.EqualTo(new[] { "Id", "Name\tInt32\nScore" }));
            Assert.That(Names(threeSource.Columns), Is.EqualTo(new[] { "Id", "Name", "Score" }));
        });
    }

    [Test]
    public void WritePlanFor_SameSchemaDifferentSessionTimezone_CompilesItsOwnPlan()
    {
        // Timezone-less DateTime plans depend on the session timezone.
        var registry = new PocoTypeRegistry();
        var utc = new ResolveContext { ServerTimezone = "UTC" };
        var kolkata = new ResolveContext { ServerTimezone = "Asia/Kolkata" };

        PocoWritePlan<Row<DateTime>> first = registry.WritePlanFor<Row<DateTime>>(SchemaOf(utc, Target("value", "DateTime")));
        PocoWritePlan<Row<DateTime>> second = registry.WritePlanFor<Row<DateTime>>(SchemaOf(kolkata, Target("value", "DateTime")));

        Assert.That(second, Is.Not.SameAs(first));
    }

    [Test]
    public void WritePlanFor_BuildFailure_IsNotCached()
    {
        var registry = new PocoTypeRegistry();
        Block schema = SchemaOf(Target("value", "Int32"));

        Assert.Throws<InvalidOperationException>(() => registry.WritePlanFor<Row<Guid>>(schema));
        Assert.Throws<InvalidOperationException>(() => registry.WritePlanFor<Row<Guid>>(schema), "the failure must be reported to every caller, not only the first");
    }

    private static PocoWritePlan<T> Plan<T>(Block schema)
        where T : class
        => PocoWritePlan<T>.Build(PocoTypeDescriptor<T>.Build(), schema);

    /// <summary>
    /// Gathers the rows as a single block. The source is returned rather than its columns, because it owns the
    /// buffers the columns read from.
    /// </summary>
    private static PocoInsertSource<T> GatherAll<T>(PocoWritePlan<T> plan, T[] rows, int rowCount)
        where T : class
    {
        // The rows are an array, so the buffer borrows them and holds nothing past the gather.
        using var buffer = PocoRowBuffer<T>.Create(rows, "rows", rowCount, CancellationToken.None);
        PocoInsertSource<T> source = plan.CreateSource(buffer, rowCount);
        try
        {
            source.Gather(0, rowCount);
            return source;
        }
        catch
        {
            source.Dispose();
            throw;
        }
    }

    private static string[] Names(IReadOnlyList<IColumn> columns)
    {
        var names = new string[columns.Count];
        for (int i = 0; i < names.Length; i++)
        {
            names[i] = columns[i].Name;
        }

        return names;
    }

    /// <summary>Creates an empty sample-block column.</summary>
    private static IColumn Target(string name, string typeName) => new ArrayColumn<object>(name, typeName, Array.Empty<object>());

    private static Block SchemaOf(params IColumn[] columns) => SchemaOf(new ResolveContext { ServerTimezone = "UTC" }, columns);

    private static Block SchemaOf(ResolveContext context, params IColumn[] columns)
        => new(string.Empty, BlockInfo.Default, rowCount: 0, columns, ColumnCodecRegistry.Default, context);

    private enum Level : sbyte
    {
        Low = -1,
        High = 127,
    }

    private sealed class IdName
    {
        public int Id { get; set; }

        public string Name { get; set; }
    }

    /// <summary>Maps both cache-key collision shapes used by the test.</summary>
    private sealed class SeparatorColumns
    {
        public int Id { get; set; }

        [ClickHouseTcpColumn(Name = "Name\tInt32\nScore")]
        public int Spelled { get; set; }

        public int Name { get; set; }

        public int Score { get; set; }
    }

    private sealed class UserRow
    {
        public int UserId { get; set; }
    }

    private sealed class TwoValues
    {
        public int Ok { get; set; }

        public int? Bad { get; set; }
    }

    private sealed class ConstructorOnlyPoco
    {
        public ConstructorOnlyPoco(int value) => Value = value;

        public int Value { get; }
    }

    private sealed class SetterOnlyPoco
    {
        private int written;

        public int Value
        {
            set => written = value;
        }

        public int Read() => written;
    }
}
