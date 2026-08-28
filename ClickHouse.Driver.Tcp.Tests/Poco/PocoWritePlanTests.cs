using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The compiled write plan: which CLR type a target column is written in, which (property type, column type) pairs
/// are accepted, and which are refused. Per-type value coverage lives in the integration suite, driven off the
/// round-trip corpus; what is here is what a server round trip cannot reach — the refusals, the write type the plan
/// picked (a round trip sees only the values, not the spelling they went out in), and the plan cache key.
///
/// <para>
/// A plan reads only a target column's name and <see cref="IColumn.TypeName"/> — the codec comes from the type
/// string — so the sample blocks here are empty columns stamped with the type under test, which is what the
/// server's own sample block is.
/// </para>
/// </summary>
[TestFixture]
public class PocoWritePlanTests
{
    [Test]
    public void Build_TargetColumnMatchingNoProperty_Throws()
    {
        // Unlike a read, where an unmapped column is simply not read, every target column has to be filled: the
        // server expects a value for each column of the statement's list.
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
        // Both names reach UserId through the matcher's looser tiers, and one property cannot decide which column it
        // means — the mirror of the read side's refusal.
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
        // D6c, in the write direction: an Int32 property does not fill an Int64 column, because a POCO that inserts
        // has to be a POCO that reads back, and the read side declines the same pair.
        Block schema = SchemaOf(Target("value", "Int64"));

        Assert.Throws<InvalidOperationException>(() => Plan<Row<int>>(schema));
    }

    [Test]
    public void Build_TargetWhoseWriterNeedsItsOwnColumnShape_SaysToUseTheColumnarApi()
    {
        // Nested writes from flat field columns behind shared offsets, a shape no property can hold. Naming the CLR
        // types it "accepts" would send the caller round a loop they cannot win, so the message names the way out.
        Block schema = SchemaOf(Target("value", "Nested(a UInt8, b String)"));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Plan<Row<object[][]>>(schema));

        Assert.That(error.Message, Does.Contain("columnar API"));
    }

    [Test]
    public void BuildColumns_CalendarProperty_WritesThroughTheCalendarTypeNotTheWireCount()
    {
        // The choice a round trip cannot observe: the plan hands the codec a DateTime column and lets it do the
        // epoch arithmetic, rather than converting to the canonical uint itself.
        Block schema = SchemaOf(Target("value", "DateTime('UTC')"));
        var rows = new[] { new Row<DateTime> { Value = DateTime.UnixEpoch } };

        IReadOnlyList<IColumn> columns = Plan<Row<DateTime>>(schema).BuildColumns(rows, rows.Length);

        Assert.That(columns[0], Is.InstanceOf<IColumn<DateTime>>());
    }

    [Test]
    public void BuildColumns_NonNullablePropertyIntoANullableColumn_LiftsToTheNullableWriteType()
    {
        Block schema = SchemaOf(Target("value", "Nullable(Int32)"));
        var rows = new[] { new Row<int> { Value = 42 } };

        IReadOnlyList<IColumn> columns = Plan<Row<int>>(schema).BuildColumns(rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(columns[0], Is.InstanceOf<IColumn<int?>>());
            Assert.That(columns[0].GetValue(0), Is.EqualTo(42));
        });
    }

    [Test]
    public void BuildColumns_NullableCalendarProperty_WritesThroughTheLiftedCalendarType()
    {
        // A Nullable(DateTime) column accepts DateTime? only because the codec lifts its inner's write types to its
        // own nullable surface. Which of the three lifted spellings was chosen is invisible to a round trip.
        Block schema = SchemaOf(Target("value", "Nullable(DateTime('UTC'))"));
        var rows = new[] { new Row<DateTime?> { Value = DateTime.UnixEpoch }, new Row<DateTime?> { Value = null } };

        IReadOnlyList<IColumn> columns = Plan<Row<DateTime?>>(schema).BuildColumns(rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(columns[0], Is.InstanceOf<IColumn<DateTime?>>());
            Assert.That(columns[0].GetValue(0), Is.EqualTo(DateTime.UnixEpoch));
            Assert.That(columns[0].GetValue(1), Is.Null);
        });
    }

    [Test]
    public void Build_TypeThatCannotBeMaterialized_StillBuildsAWritePlan()
    {
        // The read plan asks the descriptor for its activator before anything else, so an immutable POCO fails
        // there. An insert never constructs a T, so the write plan must not ask — that is what makes such a type
        // insert-only rather than unusable.
        Block schema = SchemaOf(Target("Value", "Int32"));
        PocoTypeDescriptor<ConstructorOnlyPoco> descriptor = PocoTypeDescriptor<ConstructorOnlyPoco>.Build();
        var rows = new[] { new ConstructorOnlyPoco(7) };

        Assert.That(descriptor.CanActivate, Is.False, "the type a query could not materialize");

        IReadOnlyList<IColumn> columns = PocoWritePlan<ConstructorOnlyPoco>.Build(descriptor, schema).BuildColumns(rows, rows.Length);

        Assert.That(columns[0].GetValue(0), Is.EqualTo(7));
    }

    [Test]
    public void BuildColumns_NullablePropertyIntoANullableColumn_CarriesTheNullThrough()
    {
        Block schema = SchemaOf(Target("value", "Nullable(Int32)"));
        var rows = new[] { new Row<int?> { Value = null }, new Row<int?> { Value = 7 } };

        IReadOnlyList<IColumn> columns = Plan<Row<int?>>(schema).BuildColumns(rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(columns[0].GetValue(0), Is.Null);
            Assert.That(columns[0].GetValue(1), Is.EqualTo(7));
        });
    }

    [Test]
    public void BuildColumns_NullPropertyIntoANonNullableColumn_ThrowsNamingTheRow()
    {
        // D6a in the write direction: the shape is legal — a nullable property holding no nulls writes fine — so the
        // check is per row rather than a build-time refusal.
        Block schema = SchemaOf(Target("value", "Int32"));
        var rows = new[] { new Row<int?> { Value = 1 }, new Row<int?> { Value = null } };
        PocoWritePlan<Row<int?>> plan = Plan<Row<int?>>(schema);

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => plan.BuildColumns(rows, rows.Length));

        Assert.That(error.Message, Does.Contain("row 1").And.Contain("value").And.Contain("Value"));
    }

    [Test]
    public void BuildColumns_ALaterColumnFailing_ThrowsNamingIt()
    {
        // Also the path that releases what the earlier columns rented — each is gathered into its own buffer, and
        // only the column that wraps one returns it. The release itself is not observable; this pins the failure.
        Block schema = SchemaOf(Target("Ok", "Int32"), Target("Bad", "Int32"));
        var rows = new[] { new TwoValues { Ok = 1, Bad = null } };
        PocoWritePlan<TwoValues> plan = Plan<TwoValues>(schema);

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => plan.BuildColumns(rows, rows.Length));

        Assert.That(error.Message, Does.Contain("Bad"));
    }

    [Test]
    public void BuildColumns_NullablePropertyIntoAColumnWrittenFromObject_CarriesTheNullThrough()
    {
        // Dynamic and Variant are written from a column of object, which holds a null perfectly well — so the
        // "nowhere to put a null" refusal is about a bare value type, not about the write type being unwrapped.
        Block schema = SchemaOf(Target("value", "Dynamic"));
        var rows = new[] { new Row<int?> { Value = null }, new Row<int?> { Value = 7 } };

        IReadOnlyList<IColumn> columns = Plan<Row<int?>>(schema).BuildColumns(rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(columns[0], Is.InstanceOf<IColumn<object>>());
            Assert.That(columns[0].GetValue(0), Is.Null);
            Assert.That(columns[0].GetValue(1), Is.EqualTo(7));
        });
    }

    [Test]
    public void BuildColumns_NullStringPropertyIntoANonNullableColumn_ThrowsNamingTheRow()
    {
        // A reference-typed property is null whenever it was not set, which is the commonest way a row arrives
        // incomplete. Unguarded, that null reaches the codec, which faults part-way through writing the block and
        // takes the connection with it — where a value-typed null fails cleanly before anything is sent.
        Block schema = SchemaOf(Target("value", "String"));
        var rows = new[] { new Row<string> { Value = "a" }, new Row<string> { Value = null } };
        PocoWritePlan<Row<string>> plan = Plan<Row<string>>(schema);

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => plan.BuildColumns(rows, rows.Length));

        Assert.That(error.Message, Does.Contain("row 1").And.Contain("value").And.Contain("Value"));
    }

    [Test]
    public void BuildColumns_NullArrayPropertyIntoANonNullableColumn_ThrowsNamingTheRow()
    {
        // The same rule reaches every reference-typed write type, not just string.
        Block schema = SchemaOf(Target("value", "Array(Int32)"));
        var rows = new[] { new Row<int[]> { Value = new[] { 1 } }, new Row<int[]> { Value = null } };
        PocoWritePlan<Row<int[]>> plan = Plan<Row<int[]>>(schema);

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => plan.BuildColumns(rows, rows.Length));

        Assert.That(error.Message, Does.Contain("row 1"));
    }

    [Test]
    public void BuildColumns_NullReferencePropertyIntoANullableColumn_CarriesTheNullThrough()
    {
        // The other side of the same rule: a target with a NULL of its own takes the null, and no test is compiled
        // into the loop at all.
        Block schema = SchemaOf(Target("value", "Nullable(String)"));
        var rows = new[] { new Row<string> { Value = "a" }, new Row<string> { Value = null } };

        IReadOnlyList<IColumn> columns = Plan<Row<string>>(schema).BuildColumns(rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(columns[0].GetValue(0), Is.EqualTo("a"));
            Assert.That(columns[0].GetValue(1), Is.Null);
        });
    }

    [Test]
    public void BuildColumns_EnumProperty_WritesTheOrdinal()
    {
        Block schema = SchemaOf(Target("value", "Enum8('low' = -1, 'high' = 127)"));
        var rows = new[] { new Row<Level> { Value = Level.High } };

        IReadOnlyList<IColumn> columns = Plan<Row<Level>>(schema).BuildColumns(rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(columns[0], Is.InstanceOf<IColumn<sbyte>>());
            Assert.That(columns[0].GetValue(0), Is.EqualTo((sbyte)127));
        });
    }

    [Test]
    public void BuildColumns_PropertyMatchingNoTargetColumn_IsNotInserted()
    {
        // What lets one POCO fill part of a table: the statement's column list decides, and the properties it does
        // not name are left out rather than being an error.
        Block schema = SchemaOf(Target("Id", "Int32"));
        var rows = new[] { new IdName { Id = 3, Name = "not inserted" } };

        IReadOnlyList<IColumn> columns = Plan<IdName>(schema).BuildColumns(rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(columns, Has.Count.EqualTo(1));
            Assert.That(columns[0].Name, Is.EqualTo("Id"));
            Assert.That(columns[0].GetValue(0), Is.EqualTo(3));
        });
    }

    [Test]
    public void BuildColumns_SeveralTargets_AreBuiltInSchemaOrderWithTheTargetsNamesAndTypes()
    {
        // The insert aligns by name against the sample block, and the target's own type string is what the values
        // are serialized as — so both have to come from the schema rather than from the property.
        Block schema = SchemaOf(Target("Name", "String"), Target("Id", "Int32"));
        var rows = new[] { new IdName { Id = 1, Name = "a" }, new IdName { Id = 2, Name = "b" } };

        IReadOnlyList<IColumn> columns = Plan<IdName>(schema).BuildColumns(rows, rows.Length);

        Assert.Multiple(() =>
        {
            Assert.That(columns[0].Name, Is.EqualTo("Name"));
            Assert.That(columns[0].TypeName, Is.EqualTo("String"));
            Assert.That(columns[0].RowCount, Is.EqualTo(2));
            Assert.That(columns[1].Name, Is.EqualTo("Id"));
            Assert.That(columns[1].TypeName, Is.EqualTo("Int32"));
            Assert.That(columns[1].GetValue(1), Is.EqualTo(2));
        });
    }

    [Test]
    public void BuildColumns_FewerRowsThanTheBuffer_ExposesOnlyTheRowsAsked()
    {
        // The rows arrive in a pooled array that is usually longer than the insert, so the count decides the column's
        // length, not the array's.
        Block schema = SchemaOf(Target("value", "Int32"));
        var rows = new[] { new Row<int> { Value = 1 }, new Row<int> { Value = 2 }, new Row<int> { Value = 3 } };

        IReadOnlyList<IColumn> columns = Plan<Row<int>>(schema).BuildColumns(rows, rowCount: 2);

        Assert.That(columns[0].RowCount, Is.EqualTo(2));
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
        // Keying on anything less exact than the server's own type string would hand one target's plan to another,
        // which then writes the wrong CLR type into it.
        var registry = new PocoTypeRegistry();

        PocoWritePlan<Row<int>> ints = registry.WritePlanFor<Row<int>>(SchemaOf(Target("value", "Int32")));
        PocoWritePlan<Row<int>> nullables = registry.WritePlanFor<Row<int>>(SchemaOf(Target("value", "Nullable(Int32)")));

        var rows = new[] { new Row<int> { Value = 1 } };
        Assert.Multiple(() =>
        {
            Assert.That(nullables, Is.Not.SameAs(ints));
            Assert.That(ints.BuildColumns(rows, 1)[0], Is.InstanceOf<IColumn<int>>());
            Assert.That(nullables.BuildColumns(rows, 1)[0], Is.InstanceOf<IColumn<int?>>());
        });
    }

    [Test]
    public void WritePlanFor_ColumnNameHoldingTheKeySeparators_DoesNotCollideWithAnotherSchema()
    {
        // A column name is arbitrary text, so a key joined on separators alone lets one target shape spell another.
        // The collision would be silent: one plan's columns handed to a differently shaped target.
        var registry = new PocoTypeRegistry();
        Block spelled = SchemaOf(Target("Id", "Int32"), Target("Name\tInt32\nScore", "Int32"));
        Block three = SchemaOf(Target("Id", "Int32"), Target("Name", "Int32"), Target("Score", "Int32"));

        PocoWritePlan<SeparatorColumns> spelledPlan = registry.WritePlanFor<SeparatorColumns>(spelled);
        PocoWritePlan<SeparatorColumns> threePlan = registry.WritePlanFor<SeparatorColumns>(three);

        var rows = new[] { new SeparatorColumns { Id = 1, Spelled = 9, Name = 2, Score = 3 } };
        Assert.Multiple(() =>
        {
            Assert.That(threePlan, Is.Not.SameAs(spelledPlan));
            Assert.That(Names(spelledPlan.BuildColumns(rows, 1)), Is.EqualTo(new[] { "Id", "Name\tInt32\nScore" }));
            Assert.That(Names(threePlan.BuildColumns(rows, 1)), Is.EqualTo(new[] { "Id", "Name", "Score" }));
        });
    }

    [Test]
    public void WritePlanFor_SameSchemaDifferentSessionTimezone_CompilesItsOwnPlan()
    {
        // A bare DateTime target interprets an Unspecified property in the session timezone. The type string does
        // not carry that context, so the cache key must: a plan compiled for UTC cannot serve Kolkata's wall clock.
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

    private static string[] Names(IReadOnlyList<IColumn> columns)
    {
        var names = new string[columns.Count];
        for (int i = 0; i < names.Length; i++)
        {
            names[i] = columns[i].Name;
        }

        return names;
    }

    /// <summary>
    /// One target column of a sample block. Empty and untyped on purpose: the server's sample block carries no rows,
    /// and a write plan reads only the name and the type string, resolving the codec from the latter.
    /// </summary>
    /// <param name="name">The target column's name.</param>
    /// <param name="typeName">The target column's ClickHouse type.</param>
    /// <returns>The column.</returns>
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

    /// <summary>
    /// Carries a property for the separator-spelling column as well as for the three plain ones, so both target
    /// shapes build — a target column with no property is a refusal, which would hide the key collision the test is
    /// about.
    /// </summary>
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
