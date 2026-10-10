using System;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// Unit coverage for POCO read-plan validation, tier selection and parity, cache keys, and synthetic column shapes.
/// The plans use the scatter tier that the runtime chooses; <see cref="PocoReadPlanFillTests"/> runs the same tests
/// through the tier of a runtime without dynamic code.
/// </summary>
[TestFixture]
[System.Diagnostics.CodeAnalysis.SuppressMessage("Structure", "NUnit1034:Base TestFixtures should be abstract", Justification = "The fixture runs its tests in the tier that the runtime chooses, and PocoReadPlanFillTests runs them again in the Fill tier.")]
public class PocoReadPlanTests
{
    /// <summary>The scatter tier of the plans that a test does not build with a tier of its own, or null to choose one.</summary>
    private protected virtual PocoScatterTier? Tier => null;

    [Test]
    public void Materialize_PropertyMatchingTheColumn_FillsEveryRow()
    {
        Block block = BlockOf(3, Ints("value", 1, -2, int.MaxValue));

        Row<int>[] rows = Materialize<Row<int>>(block);

        Assert.That(Values(rows), Is.EqualTo(new[] { 1, -2, int.MaxValue }));
    }

    [Test]
    public void Materialize_ColumnMatchingNoProperty_LeavesItUnread()
    {
        // The extra column is not merely ignored: nothing reads it, so a column type the POCO cannot model does not
        // stop the columns it can from being read.
        Block block = BlockOf(2, Ints("Id", 7, 8), new ArrayColumn<object>("Extra", "Variant(Int32, String)", new object[] { 1, "x" }));

        IdName[] rows = Materialize<IdName>(block);

        Assert.That(Array.ConvertAll(rows, row => row.Id), Is.EqualTo(new[] { 7, 8 }));
    }

    [Test]
    public void Materialize_PropertyMatchingNoColumn_LeavesItAtItsDefault()
    {
        Block block = BlockOf(2, Ints("Id", 7, 8));

        IdName[] rows = Materialize<IdName>(block);

        Assert.Multiple(() =>
        {
            Assert.That(Array.ConvertAll(rows, row => row.Id), Is.EqualTo(new[] { 7, 8 }));
            Assert.That(Array.ConvertAll(rows, row => row.Name), Is.EqualTo(new string[] { null, null }));
        });
    }

    [Test]
    public void Materialize_UnderscoredColumnName_ReachesThePropertyThroughTheMatcher()
    {
        Block block = BlockOf(1, Ints("user_id", 42));

        UserRow[] rows = Materialize<UserRow>(block);

        Assert.That(rows[0].UserId, Is.EqualTo(42));
    }

    [Test]
    public void Materialize_WindowStartingPastTheFirstRow_ReadsThatWindowIntoTheFirstRows()
    {
        // The window's start has to rebase the column read while leaving the destination at 0, and a start of 0
        // would not show that: a scatter ignoring the parameter passes. Both tiers source a value differently, so
        // proving one says nothing about the other: each fixture of these tests reads in its own tier.
        Block block = BlockOf(5, Ints("value", 10, 11, 12, 13, 14));
        PocoReadPlan<Row<int>> plan = PocoReadPlan<Row<int>>.Build(PocoTypeDescriptor<Row<int>>.Build(), block, Tier);
        var rows = new Row<int>[2];

        plan.Materialize(block, rows, start: 2, count: 2, rowOffset: 2);

        Assert.That(Values(rows), Is.EqualTo(new[] { 12, 13 }));
    }

    [Test]
    public void Materialize_WholeBlockInWindows_ProducesTheSameRowsAsOnePass()
    {
        // What the client's windowed loop does, against the single pass it replaced. A window that does not divide
        // the block evenly is the interesting case: the last one is short.
        Block block = BlockOf(7, Ints("value", 0, 1, 2, 3, 4, 5, 6));
        PocoReadPlan<Row<int>> plan = PocoReadPlan<Row<int>>.Build(PocoTypeDescriptor<Row<int>>.Build(), block, Tier);
        var windowed = new Row<int>[7];
        var window = new Row<int>[3];

        for (int start = 0; start < block.RowCount; start += window.Length)
        {
            int count = Math.Min(window.Length, block.RowCount - start);
            plan.Materialize(block, window, start, count, start);
            Array.Copy(window, 0, windowed, start, count);
        }

        Assert.That(Values(windowed), Is.EqualTo(Values(Materialize<Row<int>>(block))));
    }

    [Test]
    public void Materialize_NullInALaterWindow_NamesTheRowOfTheResultRatherThanOfTheWindow()
    {
        // The row a caller counts is rowOffset plus the offset within the window, so a window that starts part-way
        // into a block of a later block must still name the absolute row. Off-by-one here is invisible in the
        // first window of the first block, where every candidate expression agrees.
        Block block = BlockOf(5, Decoded(new ArrayColumn<int?>("value", "Nullable(Int32)", new int?[] { 1, 2, 3, null, 5 })));
        PocoReadPlan<Row<int>> plan = PocoReadPlan<Row<int>>.Build(PocoTypeDescriptor<Row<int>>.Build(), block, Tier);
        var rows = new Row<int>[2];

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => plan.Materialize(block, rows, start: 2, count: 2, rowOffset: 1002));

        Assert.That(error.Message, Does.Contain("row 1003").And.Contain("Row`1.Value"));
    }

    [Test]
    public void Materialize_ZeroRows_ConstructsNothing()
    {
        Block block = BlockOf(0, Ints("value"));

        Row<int>[] rows = Materialize<Row<int>>(block);

        Assert.That(rows, Is.Empty);
    }

    [Test]
    public void Build_TypeWithoutAParameterlessConstructor_ThrowsNamingTheReason()
    {
        Block block = BlockOf(1, Ints("Value", 1));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Materialize<ConstructorOnlyPoco>(block));

        Assert.That(error.Message, Does.Contain("ConstructorOnlyPoco").And.Contain("no public parameterless constructor"));
    }

    [Test]
    public void Build_ColumnMappedToAGetterOnlyProperty_ThrowsNamingTheProperty()
    {
        Block block = BlockOf(1, Ints("Value", 1));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Materialize<GetterOnlyPoco>(block));

        Assert.That(error.Message, Does.Contain("'Value'").And.Contain("GetterOnlyPoco.Value").And.Contain("no setter"));
    }

    [Test]
    public void Build_ColumnMappedToAnInitOnlyProperty_ThrowsNamingTheSetter()
    {
        Block block = BlockOf(1, Ints("Value", 1));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Materialize<InitOnlyPoco>(block));

        Assert.That(error.Message, Does.Contain("InitOnlyPoco.Value").And.Contain("init-only"));
    }

    [Test]
    public void Build_TwoColumnsReachingOneProperty_ThrowsNamingBoth()
    {
        // Neither name matches exactly, so both land on UserId through the looser tiers; whichever scattered last
        // would win silently.
        Block block = BlockOf(1, Ints("user_id", 1), Ints("userid", 2));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Materialize<UserRow>(block));

        Assert.That(error.Message, Does.Contain("'user_id'").And.Contain("'userid'").And.Contain("UserRow.UserId"));
    }

    [Test]
    public void Build_NoColumnReachingAnyProperty_ThrowsRatherThanReturningDefaults()
    {
        Block block = BlockOf(1, Ints("something_else", 1));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Materialize<IdName>(block));

        Assert.That(error.Message, Does.Contain("something_else").And.Contain("Id").And.Contain("Name"));
    }

    [Test]
    public void Build_WiderNumericProperty_ThrowsRatherThanWidening()
    {
        // D6c: an Int32 column does not fill a long property, however lossless the conversion would be.
        Block block = BlockOf(1, Ints("value", 1));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Materialize<Row<long>>(block));

        Assert.That(error.Message, Does.Contain("Int32").And.Contain("System.Int64"));
    }

    [Test]
    public void Build_WiderNumericPropertyAcrossTheNullableCombinations_ThrowsRatherThanWidening()
    {
        // Each nullable combination is its own arm of the resolution, and only the plain one is covered above. Left
        // untested, replacing any of these three checks with a bare cast would start widening int into long silently.
        Block nullableColumn = BlockOf(1, new ArrayColumn<int?>("value", "Nullable(Int32)", new int?[] { 1 }));
        Block plainColumn = BlockOf(1, Ints("value", 1));

        Assert.Multiple(() =>
        {
            Assert.Throws<InvalidOperationException>(() => Materialize<Row<long>>(nullableColumn), "Nullable(Int32) -> long");
            Assert.Throws<InvalidOperationException>(() => Materialize<Row<long?>>(plainColumn), "Int32 -> long?");
            Assert.Throws<InvalidOperationException>(() => Materialize<Row<long?>>(nullableColumn), "Nullable(Int32) -> long?");
        });
    }

    [Test]
    public void Build_UntypedNullColumn_ThrowsTellingTheCallerToTypeItInTheQuery()
    {
        // `SELECT NULL AS Name` yields Nullable(Nothing), which reads only as object — so the usual "give the property
        // one of these types" advice is useless: the property is fine, the column has no type.
        Block block = BlockOf(1, new ArrayColumn<object>("value", "Nullable(Nothing)", new object[] { null }));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Materialize<Row<string>>(block));

        Assert.That(error.Message, Does.Contain("Nullable(Nothing)").And.Contain("untyped NULL").And.Contain("CAST(NULL AS Nullable(String))"));
    }

    [Test]
    public void Build_EnumLabelSpellingATypeName_StillGetsTheOrdinaryAdvice()
    {
        // An enum label is arbitrary text that rides inside the type string, so a column can be perfectly well typed
        // and still mention Nothing. Diagnosing it as an untyped NULL would send the caller after the wrong thing.
        Block block = BlockOf(1, PrimitiveColumn<sbyte>.FromValues("value", "Enum8('Nothing' = 1)", new sbyte[] { 1 }));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Materialize<Row<Guid>>(block));

        Assert.That(error.Message, Does.Not.Contain("untyped NULL").And.Contain("Give the property one of those types"));
    }

    [Test]
    public void Materialize_NullInALaterBlock_NamesTheRowOfTheResultNotOfTheBlock()
    {
        // The scatter's counter restarts per block, so without the offset a NULL in the second block of a result
        // reports as row 1 — pointing the caller at a row that is not the one that failed.
        Block block = BlockOf(2, Decoded(new ArrayColumn<int?>("value", "Nullable(Int32)", new int?[] { 1, null })));
        PocoReadPlan<Row<int>> plan = PocoReadPlan<Row<int>>.Build(PocoTypeDescriptor<Row<int>>.Build(), block, Tier);

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => plan.Materialize(block, new Row<int>[2], rowOffset: 65_536));

        Assert.That(error.Message, Does.Contain("row 65537 of the result"));
    }

    [Test]
    public void Build_BlockWithNoColumns_ThrowsSayingSo()
    {
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Materialize<IdName>(BlockOf(1)));

        Assert.That(error.Message, Does.Contain("no columns"));
    }

    [Test]
    public void Build_CompositePropertyLiftingItsChildsReading_FillsTheLiftedElements()
    {
        Block block = BlockOf(1, Decoded(new ArrayColumn<uint[]>("value", "Array(DateTime('UTC'))", new[] { new uint[] { 0, 60 } })));

        Row<DateTime[]>[] rows = Materialize<Row<DateTime[]>>(block);

        Assert.That(rows[0].Value, Is.EqualTo(new[]
        {
            DateTimeOffset.FromUnixTimeSeconds(0).UtcDateTime,
            DateTimeOffset.FromUnixTimeSeconds(60).UtcDateTime,
        }));
    }

    [Test]
    public void Build_CompositePropertyNeedingAnUnofferedElementReading_ThrowsNamingWhatTheColumnReadsAs()
    {
        Block block = BlockOf(1, new ArrayColumn<uint[]>("value", "Array(DateTime)", new[] { new uint[] { 1, 2 } }));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Materialize<Row<Guid[]>>(block));

        Assert.That(error.Message, Does.Contain("Array(DateTime)").And.Contain("System.UInt32[]").And.Contain("System.Guid[]"));
    }

    [Test]
    public void Materialize_NullableColumnIntoANullableProperty_KeepsTheNulls()
    {
        Block block = BlockOf(3, Decoded(new ArrayColumn<int?>("value", "Nullable(Int32)", new int?[] { 1, null, -3 })));

        Row<int?>[] rows = Materialize<Row<int?>>(block);

        Assert.That(Values(rows), Is.EqualTo(new int?[] { 1, null, -3 }));
    }

    [Test]
    public void Materialize_NullableColumnWithNoNullsIntoANonNullableProperty_FillsEveryRow()
    {
        Block block = BlockOf(2, Decoded(new ArrayColumn<int?>("value", "Nullable(Int32)", new int?[] { 1, -3 })));

        Row<int>[] rows = Materialize<Row<int>>(block);

        Assert.That(Values(rows), Is.EqualTo(new[] { 1, -3 }));
    }

    [Test]
    public void Materialize_NullReachingANonNullableProperty_ThrowsNamingTheRow()
    {
        // D6a: assigning default would make a NULL indistinguishable from a stored zero.
        Block block = BlockOf(3, Decoded(new ArrayColumn<int?>("value", "Nullable(Int32)", new int?[] { 1, null, 3 })));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Materialize<Row<int>>(block));

        Assert.That(error.Message, Does.Contain("'value'").And.Contain("Nullable(Int32)").And.Contain("row 1").And.Contain("Row`1.Value"));
    }

    [Test]
    public void Materialize_NonNullableColumnIntoANullableProperty_Lifts()
    {
        Block block = BlockOf(2, Ints("value", 1, -2));

        Row<int?>[] rows = Materialize<Row<int?>>(block);

        Assert.That(Values(rows), Is.EqualTo(new int?[] { 1, -2 }));
    }

    [Test]
    public void Materialize_NullablePropertyOverAProjectedColumn_ProjectsThenLifts()
    {
        // Both halves at once: the codec's own conversion, then the lift a nullable property needs. Neither the
        // corpus nor the plain lift case reaches this pair.
        Block block = BlockOf(1, PrimitiveColumn<uint>.FromValues("value", "DateTime('UTC')", new uint[] { 1_700_000_000 }));

        Row<DateTime?>[] rows = Materialize<Row<DateTime?>>(block);

        Assert.That(rows[0].Value, Is.EqualTo(DateTime.UnixEpoch.AddSeconds(1_700_000_000)));
    }

    [Test]
    public void Materialize_NullableEnumPropertyOverANonNullableColumn_CastsThenLifts()
    {
        Block block = BlockOf(2, PrimitiveColumn<sbyte>.FromValues("value", "Enum8('low' = -1, 'high' = 127)", new sbyte[] { -1, 127 }));

        Row<Level?>[] rows = Materialize<Row<Level?>>(block);

        Assert.That(Values(rows), Is.EqualTo(new Level?[] { Level.Low, Level.High }));
    }

    [Test]
    public void Materialize_ObjectProperty_BoxesWhateverTheColumnSurfaces()
    {
        Block block = BlockOf(2, Ints("value", 1, -2));

        Row<object>[] rows = Materialize<Row<object>>(block);

        Assert.That(Values(rows), Is.EqualTo(new object[] { 1, -2 }));
    }

    [Test]
    public void Materialize_EnumProperty_CastsTheOrdinal()
    {
        // D6b: the cast is blind, so an ordinal the CLR enum does not name arrives as that ordinal rather than
        // being rejected. 5 is not a Level member.
        Block block = BlockOf(3, PrimitiveColumn<sbyte>.FromValues("value", "Enum8('low' = -1, 'high' = 127)", new sbyte[] { -1, 127, 5 }));

        Row<Level>[] rows = Materialize<Row<Level>>(block);

        Assert.That(Values(rows), Is.EqualTo(new[] { Level.Low, Level.High, (Level)5 }));
    }

    [Test]
    public void Materialize_NullableEnumProperty_KeepsTheNullsAndCastsTheRest()
    {
        Block block = BlockOf(3, Decoded(new ArrayColumn<sbyte?>("value", "Nullable(Enum8('low' = -1, 'high' = 127))", new sbyte?[] { -1, null, 127 })));

        Row<Level?>[] rows = Materialize<Row<Level?>>(block);

        Assert.That(Values(rows), Is.EqualTo(new Level?[] { Level.Low, null, Level.High }));
    }

    [Test]
    public void Materialize_EnumPropertyOverANullableColumnWithANull_ThrowsNamingTheRow()
    {
        Block block = BlockOf(2, Decoded(new ArrayColumn<sbyte?>("value", "Nullable(Enum8('low' = -1))", new sbyte?[] { -1, null })));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Materialize<Row<Level>>(block));

        Assert.That(error.Message, Does.Contain("row 1"));
    }

    [Test]
    public void Materialize_DateTimePropertyOverADateTimeColumn_ProjectsThroughTheCodec()
    {
        // The column's canonical CLR type is the raw epoch second count, so this is the codec's projection being
        // inlined into the scatter — the single most common POCO shape there is.
        Block block = BlockOf(1, PrimitiveColumn<uint>.FromValues("value", "DateTime('UTC')", new uint[] { 1_700_000_000 }));

        Row<DateTime>[] rows = Materialize<Row<DateTime>>(block);

        Assert.That(rows[0].Value, Is.EqualTo(DateTime.UnixEpoch.AddSeconds(1_700_000_000)));
    }

    [Test]
    public void Materialize_RawPropertyOverADateTimeColumn_StillReadsTheWireValue()
    {
        Block block = BlockOf(1, PrimitiveColumn<uint>.FromValues("value", "DateTime('UTC')", new uint[] { 1_700_000_000 }));

        Row<uint>[] rows = Materialize<Row<uint>>(block);

        Assert.That(rows[0].Value, Is.EqualTo(1_700_000_000u));
    }

    [Test]
    public void Build_ColumnNotSurfacingItsElementType_ReportsTheCodecMismatch()
    {
        // No shipped codec produces such a column, but both tiers cast to IColumn<T>, so one would fail that cast
        // inside compiled code on the first block. Reported at plan build, naming the column and the codec.
        Block block = BlockOf(2, new UntypedColumn("value", "Int32", 1, -2));

        var error = Assert.Throws<InvalidOperationException>(() => Materialize<Row<int>>(block));

        Assert.That(error.Message, Does.Contain("value").And.Contain("IColumn<System.Int32>"));
    }

    [Test]
    public void Materialize_ColumnWithoutTheDecodedShapeOfItsType_ThrowsNamingTheColumnAndTheShape()
    {
        // The converter tree of a Nullable type reads the null map and the inner column that its codec decodes. A
        // column that a test builds can have the type name without that shape; a block from the server cannot.
        Block block = BlockOf(2, new ArrayColumn<int?>("value", "Nullable(Int32)", new int?[] { 1, null }));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Materialize<Row<int?>>(block));

        Assert.That(error.Message, Does.Contain("Column 'value' (Nullable(Int32))").And.Contain("INullableColumn"));
    }

    [Test]
    public void ReadPlanFor_SameHeader_ReturnsTheCachedPlan()
    {
        var registry = new PocoTypeRegistry();

        PocoReadPlan<Row<int>> first = registry.ReadPlanFor<Row<int>>(BlockOf(1, Ints("value", 1)), Tier);
        PocoReadPlan<Row<int>> second = registry.ReadPlanFor<Row<int>>(BlockOf(1, Ints("value", 2)), Tier);

        Assert.That(second, Is.SameAs(first));
    }

    [Test]
    public void ReadPlanFor_SameColumnNameDifferentType_CompilesItsOwnPlan()
    {
        // The regression the HTTP client's box-free path hit: keying on anything less exact than the server's own
        // type string lets one shape's plan be handed to another, which then casts the wrong column type.
        var registry = new PocoTypeRegistry();
        Block ints = BlockOf(1, Ints("value", 42));
        Block strings = BlockOf(1, Decoded(new ArrayColumn<string>("value", "String", new[] { "x" })));

        PocoReadPlan<Row<object>> intPlan = registry.ReadPlanFor<Row<object>>(ints, Tier);
        PocoReadPlan<Row<object>> stringPlan = registry.ReadPlanFor<Row<object>>(strings, Tier);

        var intRows = new Row<object>[1];
        var stringRows = new Row<object>[1];
        intPlan.Materialize(ints, intRows, rowOffset: 0);
        stringPlan.Materialize(strings, stringRows, rowOffset: 0);

        Assert.Multiple(() =>
        {
            Assert.That(stringPlan, Is.Not.SameAs(intPlan));
            Assert.That(intRows[0].Value, Is.EqualTo(42));
            Assert.That(stringRows[0].Value, Is.EqualTo("x"));
        });
    }

    [Test]
    public void ReadPlanFor_ColumnNameHoldingTheKeySeparators_DoesNotCollideWithAnotherHeader()
    {
        // A column name is arbitrary text — `SELECT 1 AS "a<tab>b"` is a legal alias — so a key joined on separators
        // alone lets one header spell another. The collision is silent: the wrong plan scatters the columns it was
        // built for, leaving the rest of the block unread, or indexes past it.
        var registry = new PocoTypeRegistry();
        Block spelled = BlockOf(1, Ints("Id", 10), Ints("Name\tInt32\nScore", 20));
        Block three = BlockOf(1, Ints("Id", 10), Ints("Name", 20), Ints("Score", 30));

        var spelledRows = new ThreeColumns[1];
        var threeRows = new ThreeColumns[1];
        registry.ReadPlanFor<ThreeColumns>(spelled, Tier).Materialize(spelled, spelledRows, rowOffset: 0);
        registry.ReadPlanFor<ThreeColumns>(three, Tier).Materialize(three, threeRows, rowOffset: 0);

        Assert.Multiple(() =>
        {
            Assert.That(spelledRows[0].Score, Is.EqualTo(0), "the two-column header has no Score column");
            Assert.That(threeRows[0].Id, Is.EqualTo(10));
            Assert.That(threeRows[0].Name, Is.EqualTo(20));
            Assert.That(threeRows[0].Score, Is.EqualTo(30));
        });
    }

    [Test]
    public void ReadPlanFor_SameHeaderDifferentSessionTimezone_CompilesItsOwnPlan()
    {
        // A DateTime column whose type string names no timezone is presented in the session timezone, which the
        // header cannot show — so the header alone is not enough to key a plan on.
        var registry = new PocoTypeRegistry();
        Block utc = BlockOf(new ResolveContext { ServerTimezone = "UTC" }, 1, PrimitiveColumn<uint>.FromValues("value", "DateTime", new uint[] { 1_700_000_000 }));
        Block kolkata = BlockOf(new ResolveContext { ServerTimezone = "Asia/Kolkata" }, 1, PrimitiveColumn<uint>.FromValues("value", "DateTime", new uint[] { 1_700_000_000 }));

        var utcRows = new Row<DateTime>[1];
        var kolkataRows = new Row<DateTime>[1];
        registry.ReadPlanFor<Row<DateTime>>(utc, Tier).Materialize(utc, utcRows, rowOffset: 0);
        registry.ReadPlanFor<Row<DateTime>>(kolkata, Tier).Materialize(kolkata, kolkataRows, rowOffset: 0);

        Assert.That(kolkataRows[0].Value - utcRows[0].Value, Is.EqualTo(new TimeSpan(5, 30, 0)));
    }

    [Test]
    public void ReadPlanFor_BuildFailure_IsNotCached()
    {
        var registry = new PocoTypeRegistry();
        Block block = BlockOf(1, Ints("value", 1));

        Assert.Throws<InvalidOperationException>(() => registry.ReadPlanFor<Row<long>>(block, Tier));
        Assert.Throws<InvalidOperationException>(() => registry.ReadPlanFor<Row<long>>(block, Tier), "the failure must be reported to every caller, not only the first");
    }

    [Test]
    public void MatchesHeader_ADifferentHeader_IsRejectedSoTheCacheIsConsulted()
    {
        Block block = BlockOf(1, Ints("value", 1));
        PocoReadPlan<Row<int>> plan = PocoReadPlan<Row<int>>.Build(PocoTypeDescriptor<Row<int>>.Build(), block, Tier);

        Assert.Multiple(() =>
        {
            Assert.That(plan.MatchesHeader(BlockOf(1, Ints("value", 2))), Is.True, "same shape");
            Assert.That(plan.MatchesHeader(BlockOf(1, Ints("value", 1), Ints("other", 1))), Is.False, "column count");
            Assert.That(plan.MatchesHeader(BlockOf(1, Ints("renamed", 1))), Is.False, "column name");
            Assert.That(plan.MatchesHeader(BlockOf(1, PrimitiveColumn<long>.FromValues("value", "Int64", new long[] { 1 }))), Is.False, "column type");
            Assert.That(
                plan.MatchesHeader(BlockOf(new ResolveContext { ServerTimezone = "Asia/Kolkata" }, 1, Ints("value", 1))),
                Is.False,
                "session timezone");
            Assert.That(plan.MatchesHeader(BlockOf(default, 1, Ints("value", 1))), Is.False, "no session timezone at all");
        });
    }

    internal static Block MixedBlock() => BlockOf(
        2,
        Ints("Id", 1, 2),
        Decoded(new ArrayColumn<string>("Name", "String", new[] { "a", "b" })),
        PrimitiveColumn<uint>.FromValues("Stamp", "DateTime('UTC')", new uint[] { 1_700_000_000, 0 }),
        Decoded(new ArrayColumn<double?>("Score", "Nullable(Float64)", new double?[] { 1.5, null })),
        Decoded(new ArrayColumn<string[]>("Tags", "Array(String)", new[] { new[] { "x", "y" }, Array.Empty<string>() })),
        PrimitiveColumn<sbyte>.FromValues("Level", "Enum8('low' = -1, 'high' = 127)", new sbyte[] { -1, 127 }));

    // The column that the codec of its type decodes from the bytes that an insert writes for source: the column that a
    // block from the server holds. A converter reads the decoded shape of the column type (for example the null map of a
    // Nullable column, or the bytes of a String column), which a column that a test builds does not have.
    internal static IColumn Decoded(IColumn source)
    {
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(source.TypeName, new ResolveContext { ServerTimezone = "UTC" });
        byte[] bytes = CodecTestHarness.WriteAsync(w => codec.WriteFull(w, source)).GetAwaiter().GetResult();
        using ClickHouseBinaryReader reader = CodecTestHarness.ReaderOver(bytes);
        if (source.RowCount > 0)
        {
            codec.ReadStatePrefixAsync(reader, CodecTestHarness.None).AsTask().GetAwaiter().GetResult();
        }

        return codec.ReadColumnAsync(reader, source.Name, source.TypeName, source.RowCount, CodecTestHarness.None).AsTask().GetAwaiter().GetResult();
    }

    internal static Block BlockOf(int rowCount, params IColumn[] columns)
        => BlockOf(new ResolveContext { ServerTimezone = "UTC" }, rowCount, columns);

    private static Block BlockOf(ResolveContext context, int rowCount, params IColumn[] columns)
        => new(string.Empty, BlockInfo.Default, rowCount, columns, ColumnCodecRegistry.Default, context);

    internal static IColumn Ints(string name, params int[] values) => PrimitiveColumn<int>.FromValues(name, "Int32", values);

    private T[] Materialize<T>(Block block)
        where T : class
        => Materialize<T>(block, Tier);

    internal static T[] Materialize<T>(Block block, PocoScatterTier? tier)
        where T : class
    {
        PocoReadPlan<T> plan = PocoReadPlan<T>.Build(PocoTypeDescriptor<T>.Build(), block, tier);
        var rows = new T[block.RowCount];
        plan.Materialize(block, rows, rowOffset: 0);
        return rows;
    }

    private static TValue[] Values<TValue>(Row<TValue>[] rows) => Array.ConvertAll(rows, row => row.Value);

    /// <summary>A column that surfaces its values only boxed, which no tier can source through.</summary>
    private sealed class UntypedColumn : IColumn
    {
        private readonly object[] values;

        public UntypedColumn(string name, string typeName, params object[] values)
        {
            Name = name;
            TypeName = typeName;
            this.values = values;
        }

        public string Name { get; }

        public string TypeName { get; }

        public int RowCount => values.Length;

        public object GetValue(int row) => values[row];

        public void Dispose()
        {
        }
    }

    internal enum Level : sbyte
    {
        Low = -1,
        High = 127,
    }

    private sealed class IdName
    {
        public int Id { get; set; }

        public string Name { get; set; }
    }

    private sealed class UserRow
    {
        public int UserId { get; set; }
    }

    private sealed class ThreeColumns
    {
        public int Id { get; set; }

        public int Name { get; set; }

        public int Score { get; set; }
    }

    internal sealed class MixedRow
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public DateTime Stamp { get; set; }

        public double? Score { get; set; }

        public string[] Tags { get; set; }

        public Level Level { get; set; }
    }

    private sealed class GetterOnlyPoco
    {
        public int Value => 5;
    }

    private sealed class InitOnlyPoco
    {
        public int Value { get; init; }
    }

    private sealed class ConstructorOnlyPoco
    {
        public ConstructorOnlyPoco(int value) => Value = value;

        public int Value { get; set; }
    }
}
