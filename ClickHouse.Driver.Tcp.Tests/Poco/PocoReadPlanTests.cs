using System;
using System.Runtime.CompilerServices;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The compiled read plan: which tier a column is scattered through, which (column type, property type) pairs are
/// accepted, and which are refused. Per-type value coverage lives in the integration suite, driven off the
/// round-trip corpus; what is here is what a server round trip cannot reach — the refusals, the tier choice and the
/// parity between tiers, the plan cache key, and the shapes a real result cannot produce.
///
/// <para>
/// A plan only needs a column's <see cref="IColumn.TypeName"/> and its <see cref="IColumn{T}"/> shape, so the
/// blocks here are built from plain columns stamped with the type string under test — the same shape a decoded
/// block presents.
/// </para>
/// </summary>
[TestFixture]
public class PocoReadPlanTests
{
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
        Block block = BlockOf(2, new ArrayColumn<int?>("value", "Nullable(Int32)", new int?[] { 1, null }));
        PocoReadPlan<Row<int>> plan = PocoReadPlan<Row<int>>.Build(PocoTypeDescriptor<Row<int>>.Build(), block, forcedTier: null);

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
        // A container lifts its child's readings, so Array(DateTime) — which decodes as uint[] — fills a DateTime[]
        // property, the shape a caller would reasonably expect. The timezone is named so the reading is deterministic.
        Block block = BlockOf(1, new ArrayColumn<uint[]>("value", "Array(DateTime('UTC'))", new[] { new uint[] { 0, 60 } }));

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
        // Lifting reaches only the readings the child actually offers, and Guid is not one of a DateTime's. The
        // message still has to name what the column does read as.
        Block block = BlockOf(1, new ArrayColumn<uint[]>("value", "Array(DateTime)", new[] { new uint[] { 1, 2 } }));

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => Materialize<Row<Guid[]>>(block));

        Assert.That(error.Message, Does.Contain("Array(DateTime)").And.Contain("System.UInt32[]").And.Contain("System.Guid[]"));
    }

    [Test]
    public void Materialize_NullableColumnIntoANullableProperty_KeepsTheNulls()
    {
        Block block = BlockOf(3, new ArrayColumn<int?>("value", "Nullable(Int32)", new int?[] { 1, null, -3 }));

        Row<int?>[] rows = Materialize<Row<int?>>(block);

        Assert.That(Values(rows), Is.EqualTo(new int?[] { 1, null, -3 }));
    }

    [Test]
    public void Materialize_NullableColumnWithNoNullsIntoANonNullableProperty_FillsEveryRow()
    {
        Block block = BlockOf(2, new ArrayColumn<int?>("value", "Nullable(Int32)", new int?[] { 1, -3 }));

        Row<int>[] rows = Materialize<Row<int>>(block);

        Assert.That(Values(rows), Is.EqualTo(new[] { 1, -3 }));
    }

    [Test]
    public void Materialize_NullReachingANonNullableProperty_ThrowsNamingTheRow()
    {
        // D6a: assigning default would make a NULL indistinguishable from a stored zero.
        Block block = BlockOf(3, new ArrayColumn<int?>("value", "Nullable(Int32)", new int?[] { 1, null, 3 }));

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
        Block block = BlockOf(3, new ArrayColumn<sbyte?>("value", "Nullable(Enum8('low' = -1, 'high' = 127))", new sbyte?[] { -1, null, 127 }));

        Row<Level?>[] rows = Materialize<Row<Level?>>(block);

        Assert.That(Values(rows), Is.EqualTo(new Level?[] { Level.Low, null, Level.High }));
    }

    [Test]
    public void Materialize_EnumPropertyOverANullableColumnWithANull_ThrowsNamingTheRow()
    {
        Block block = BlockOf(2, new ArrayColumn<sbyte?>("value", "Nullable(Enum8('low' = -1))", new sbyte?[] { -1, null }));

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

    // The tiers are cases of one loop rather than [TestCase]s, because the enum is internal and a public test
    // method cannot take it as a parameter. Iterating the enum also covers a tier added later for free.
    [Test]
    public void Materialize_EveryTier_ProducesTheSameRows()
    {
        // The tiers differ only in how one value is sourced, so they must agree — including on the
        // conversions, which is why the block mixes a raw type, a projected one, a nullable and a composite. This
        // doubles as the proof that the span-free tier a runtime without dynamic code falls back to is equivalent.
        Assert.Multiple(() =>
        {
            foreach (PocoScatterTier tier in Enum.GetValues<PocoScatterTier>())
            {
                MixedRow[] rows = Materialize<MixedRow>(MixedBlock(), tier);

                Assert.That(Array.ConvertAll(rows, row => row.Id), Is.EqualTo(new[] { 1, 2 }), $"{tier}: Id");
                Assert.That(Array.ConvertAll(rows, row => row.Name), Is.EqualTo(new[] { "a", "b" }), $"{tier}: Name");
                Assert.That(Array.ConvertAll(rows, row => row.Stamp), Is.EqualTo(new[] { DateTime.UnixEpoch.AddSeconds(1_700_000_000), DateTime.UnixEpoch }), $"{tier}: Stamp");
                Assert.That(Array.ConvertAll(rows, row => row.Score), Is.EqualTo(new double?[] { 1.5, null }), $"{tier}: Score");
                Assert.That(Array.ConvertAll(rows, row => row.Tags), Is.EqualTo(new[] { new[] { "x", "y" }, Array.Empty<string>() }), $"{tier}: Tags");
                Assert.That(Array.ConvertAll(rows, row => row.Level), Is.EqualTo(new[] { Level.Low, Level.High }), $"{tier}: Level");
            }
        });
    }

    [Test]
    public void Create_IndexerTier_AlsoRunsUnderTheExpressionInterpreter()
    {
        // Why the indexer tier exists at all: a runtime without dynamic code interprets the tree instead of
        // compiling it, and an interpreted tree cannot hold a ReadOnlySpan<T>. A test host that has dynamic code
        // never takes that path, so the interpreter is asked for explicitly here — otherwise the fallback ships
        // untested and only fails on NativeAOT.
        IColumn column = Ints("value", 1, -2);
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve("Int32", new ResolveContext());
        PocoMember member = PocoTypeDescriptor<Row<int>>.Build().Members[0];
        PocoColumnScatter<Row<int>> scatter = PocoColumnScatterFactory.Create<Row<int>>(column, codec, member, PocoScatterTier.Indexer, preferInterpretation: true);
        var rows = new[] { new Row<int>(), new Row<int>() };

        scatter(column, rows, rows.Length, rowOffset: 0);

        Assert.That(Values(rows), Is.EqualTo(new[] { 1, -2 }));
    }

    [Test]
    public void SelectTier_NoForcedTier_PrefersTheSpanTierWhereverTreesCompile()
    {
        // The interpreter cannot hold a ReadOnlySpan<T>, so the span tier is only offered where a tree becomes IL.
        PocoScatterTier expected = RuntimeFeature.IsDynamicCodeCompiled ? PocoScatterTier.Span : PocoScatterTier.Indexer;

        Assert.That(PocoColumnScatterFactory.SelectTier(null), Is.EqualTo(expected));
    }

    [Test]
    public void SelectTier_ForcedTier_IsHonored()
    {
        Assert.Multiple(() =>
        {
            foreach (PocoScatterTier tier in Enum.GetValues<PocoScatterTier>())
            {
                Assert.That(PocoColumnScatterFactory.SelectTier(tier), Is.EqualTo(tier), tier.ToString());
            }
        });
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
    public void ReadPlanFor_SameHeader_ReturnsTheCachedPlan()
    {
        var registry = new PocoTypeRegistry();

        PocoReadPlan<Row<int>> first = registry.ReadPlanFor<Row<int>>(BlockOf(1, Ints("value", 1)), forcedTier: null);
        PocoReadPlan<Row<int>> second = registry.ReadPlanFor<Row<int>>(BlockOf(1, Ints("value", 2)), forcedTier: null);

        Assert.That(second, Is.SameAs(first));
    }

    [Test]
    public void ReadPlanFor_SameColumnNameDifferentType_CompilesItsOwnPlan()
    {
        // The regression the HTTP client's box-free path hit: keying on anything less exact than the server's own
        // type string lets one shape's plan be handed to another, which then casts the wrong column type.
        var registry = new PocoTypeRegistry();
        Block ints = BlockOf(1, Ints("value", 42));
        Block strings = BlockOf(1, new ArrayColumn<string>("value", "String", new[] { "x" }));

        PocoReadPlan<Row<object>> intPlan = registry.ReadPlanFor<Row<object>>(ints, forcedTier: null);
        PocoReadPlan<Row<object>> stringPlan = registry.ReadPlanFor<Row<object>>(strings, forcedTier: null);

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
        registry.ReadPlanFor<ThreeColumns>(spelled, forcedTier: null).Materialize(spelled, spelledRows, rowOffset: 0);
        registry.ReadPlanFor<ThreeColumns>(three, forcedTier: null).Materialize(three, threeRows, rowOffset: 0);

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
        registry.ReadPlanFor<Row<DateTime>>(utc, forcedTier: null).Materialize(utc, utcRows, rowOffset: 0);
        registry.ReadPlanFor<Row<DateTime>>(kolkata, forcedTier: null).Materialize(kolkata, kolkataRows, rowOffset: 0);

        Assert.That(kolkataRows[0].Value - utcRows[0].Value, Is.EqualTo(new TimeSpan(5, 30, 0)));
    }

    [Test]
    public void ReadPlanFor_DifferentForcedTiers_CompileTheirOwnPlans()
    {
        var registry = new PocoTypeRegistry();
        Block block = BlockOf(1, Ints("value", 1));

        PocoReadPlan<Row<int>> span = registry.ReadPlanFor<Row<int>>(block, PocoScatterTier.Span);
        PocoReadPlan<Row<int>> indexer = registry.ReadPlanFor<Row<int>>(block, PocoScatterTier.Indexer);

        Assert.That(indexer, Is.Not.SameAs(span));
    }

    [Test]
    public void ReadPlanFor_BuildFailure_IsNotCached()
    {
        var registry = new PocoTypeRegistry();
        Block block = BlockOf(1, Ints("value", 1));

        Assert.Throws<InvalidOperationException>(() => registry.ReadPlanFor<Row<long>>(block, forcedTier: null));
        Assert.Throws<InvalidOperationException>(() => registry.ReadPlanFor<Row<long>>(block, forcedTier: null), "the failure must be reported to every caller, not only the first");
    }

    [Test]
    public void MatchesHeader_ADifferentHeader_IsRejectedSoTheCacheIsConsulted()
    {
        Block block = BlockOf(1, Ints("value", 1));
        PocoReadPlan<Row<int>> plan = PocoReadPlan<Row<int>>.Build(PocoTypeDescriptor<Row<int>>.Build(), block, forcedTier: null);

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

    private static Block MixedBlock() => BlockOf(
        2,
        Ints("Id", 1, 2),
        new ArrayColumn<string>("Name", "String", new[] { "a", "b" }),
        PrimitiveColumn<uint>.FromValues("Stamp", "DateTime('UTC')", new uint[] { 1_700_000_000, 0 }),
        new ArrayColumn<double?>("Score", "Nullable(Float64)", new double?[] { 1.5, null }),
        new ArrayColumn<string[]>("Tags", "Array(String)", new[] { new[] { "x", "y" }, Array.Empty<string>() }),
        PrimitiveColumn<sbyte>.FromValues("Level", "Enum8('low' = -1, 'high' = 127)", new sbyte[] { -1, 127 }));

    private static Block BlockOf(int rowCount, params IColumn[] columns)
        => BlockOf(new ResolveContext { ServerTimezone = "UTC" }, rowCount, columns);

    private static Block BlockOf(ResolveContext context, int rowCount, params IColumn[] columns)
        => new(string.Empty, BlockInfo.Default, rowCount, columns, ColumnCodecRegistry.Default, context);

    private static IColumn Ints(string name, params int[] values) => PrimitiveColumn<int>.FromValues(name, "Int32", values);

    private static T[] Materialize<T>(Block block, PocoScatterTier? tier = null)
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

    private sealed class MixedRow
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
