using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using ClickHouse.Driver.Tcp.Poco;

namespace ClickHouse.Driver.Tcp.Tests.Poco;

/// <summary>
/// The per-type half of the POCO mapping: which properties are discovered, what column name each matches on, how a
/// column name finds its property through the matcher's tiers, and which failures are refused at build time.
///
/// <para>
/// All of this is unreachable from a server round trip — it is decided before a single column type is known — so it
/// belongs here rather than in the integration suite.
/// </para>
/// </summary>
[TestFixture]
public class PocoTypeDescriptorTests
{
    [Test]
    public void Build_PublicInstanceProperties_MapsEachToItsOwnName()
    {
        PocoTypeDescriptor<SimplePoco> descriptor = PocoTypeDescriptor<SimplePoco>.Build();

        Assert.That(ColumnNames(descriptor), Is.EquivalentTo(new[] { "Id", "Name" }));
        Assert.That(descriptor.PocoType, Is.EqualTo(typeof(SimplePoco)));
    }

    [Test]
    public void Build_NotMappedProperty_LeavesItOutOfTheMemberSet()
    {
        PocoTypeDescriptor<PartlyMappedPoco> descriptor = PocoTypeDescriptor<PartlyMappedPoco>.Build();

        Assert.That(ColumnNames(descriptor), Is.EquivalentTo(new[] { "Kept" }));
    }

    [Test]
    public void Build_ColumnAttributeWithAName_MatchesOnThatNameInsteadOfThePropertyName()
    {
        PocoTypeDescriptor<RenamedPoco> descriptor = PocoTypeDescriptor<RenamedPoco>.Build();

        Assert.That(ColumnNames(descriptor), Is.EquivalentTo(new[] { "event_time" }));
        Assert.That(descriptor.Members[0].MemberName, Is.EqualTo("Timestamp"));
    }

    [Test]
    public void Build_ColumnAttributeWithoutAName_FallsBackToThePropertyName()
    {
        // The attribute may be present for a future property, so a null Name is not an error.
        PocoTypeDescriptor<AttributeWithoutNamePoco> descriptor = PocoTypeDescriptor<AttributeWithoutNamePoco>.Build();

        Assert.That(ColumnNames(descriptor), Is.EquivalentTo(new[] { "Value" }));
    }

    [Test]
    public void Build_BlankColumnAttributeName_ThrowsNamingTheProperty()
    {
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => PocoTypeDescriptor<BlankColumnNamePoco>.Build());

        Assert.That(error.Message, Does.Contain("BlankColumnNamePoco.Value"));
        Assert.That(error.Message, Does.Contain("empty or whitespace"));
    }

    [Test]
    public void Build_Indexer_IsNotMapped()
    {
        PocoTypeDescriptor<IndexerPoco> descriptor = PocoTypeDescriptor<IndexerPoco>.Build();

        Assert.That(ColumnNames(descriptor), Is.EquivalentTo(new[] { "Value" }));
    }

    [Test]
    public void Build_StaticProperty_IsNotMappedWhileTheInstanceOneIs()
    {
        PocoTypeDescriptor<StaticAndInstancePoco> descriptor = PocoTypeDescriptor<StaticAndInstancePoco>.Build();

        Assert.That(ColumnNames(descriptor), Is.EquivalentTo(new[] { "Instance" }));
    }

    [Test]
    public void Build_InheritedProperty_IsMappedAlongsideTheDeclaredOne()
    {
        PocoTypeDescriptor<InheritedPoco> descriptor = PocoTypeDescriptor<InheritedPoco>.Build();

        Assert.That(ColumnNames(descriptor), Is.EquivalentTo(new[] { "BaseValue", "DerivedValue" }));
    }

    [Test]
    public void Build_ShadowedProperty_KeepsOnlyTheMostDerivedDeclaration()
    {
        // Reflection reports a `new` property once per declaring type. Left alone, the two would look like two
        // properties competing for one column and fail the exact-duplicate check.
        PocoTypeDescriptor<ShadowingPoco> descriptor = PocoTypeDescriptor<ShadowingPoco>.Build();

        Assert.That(descriptor.Members.Count, Is.EqualTo(1));
        Assert.That(descriptor.Members[0].MemberType, Is.EqualTo(typeof(long)), "the derived declaration should win");
    }

    [Test]
    public void Build_Interface_MapsThePropertiesItInheritsFromItsBaseInterfaces()
    {
        // GetProperties walks a class's base types but stops dead on an interface, which has no base type. Missing
        // this would silently drop BaseValue and insert a server default into its column.
        PocoTypeDescriptor<IDerivedShape> descriptor = PocoTypeDescriptor<IDerivedShape>.Build();

        Assert.That(ColumnNames(descriptor), Is.EquivalentTo(new[] { "BaseValue", "DerivedValue" }));
    }

    [Test]
    public void Build_InterfaceRedeclaringAnInheritedProperty_KeepsTheDerivedDeclaration()
    {
        PocoTypeDescriptor<IShadowingShape> descriptor = PocoTypeDescriptor<IShadowingShape>.Build();

        Assert.That(descriptor.Members.Count, Is.EqualTo(1));
        Assert.That(descriptor.Members[0].MemberType, Is.EqualTo(typeof(long)));
    }

    [Test]
    public void Build_InterfaceInheritingOneNameFromTwoUnrelatedInterfaces_ThrowsRatherThanPickingOne()
    {
        // C# needs a cast to read such a property, so there is no declaration to prefer. Only interfaces can get
        // here — a class hierarchy is one chain.
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => PocoTypeDescriptor<IAmbiguousShape>.Build());

        Assert.That(error.Message, Does.Contain("ILeftShape"));
        Assert.That(error.Message, Does.Contain("IRightShape"));
        Assert.That(error.Message, Does.Contain("neither inherits the other"));
    }

    [Test]
    public void Build_NotMappedOnAVirtualBaseProperty_ExcludesTheOverride()
    {
        // GetCustomAttribute<T> walks base property definitions, so the attribute carries to the override. Pinned
        // because PropertyInfo.GetCustomAttributes(type, inherit: true) does not do the same, and swapping the two
        // APIs would silently change this.
        PocoTypeDescriptor<OverridingPoco> descriptor = PocoTypeDescriptor<OverridingPoco>.Build();

        Assert.That(ColumnNames(descriptor), Does.Not.Contain("Excluded"));
    }

    [Test]
    public void Build_ColumnAttributeOnAVirtualBaseProperty_RenamesTheOverride()
    {
        Assert.That(Member<OverridingPoco>("renamed_column").MemberName, Is.EqualTo("Renamed"));
    }

    [Test]
    public void KeepMostDerived_BaseDeclarationComingFirst_StillKeepsTheDerivedOne()
    {
        // Reflection does not contract the order it reports a `new`-shadowed property in, so the pass must not
        // depend on the derived declaration arriving first — which is the only order Build can observe here.
        var baseFirst = new List<PropertyInfo>
        {
            DeclaredProperty(typeof(ShadowedBase), "Value"),
            DeclaredProperty(typeof(ShadowingPoco), "Value"),
        };

        List<PropertyInfo> kept = PocoTypeDescriptor.KeepMostDerived(baseFirst);

        Assert.That(kept.Count, Is.EqualTo(1));
        Assert.That(kept[0].DeclaringType, Is.EqualTo(typeof(ShadowingPoco)));
    }

    [Test]
    public void Build_TwoPropertiesMappedToOneColumn_ThrowsNamingBoth()
    {
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => PocoTypeDescriptor<DuplicateColumnPoco>.Build());

        // Asserted as the whole phrase: 'Value' is also the column name, so a bare substring check would pass
        // without the first property ever being named.
        Assert.That(error.Message, Does.Contain("both 'Value' and 'Other'"));
    }

    [Test]
    public void Build_TypeWithNoProperties_ThrowsSayingThereAreNone()
    {
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => PocoTypeDescriptor<EmptyPoco>.Build());

        Assert.That(error.Message, Does.Contain("no public instance properties"));
    }

    [Test]
    public void Build_TypeWhoseOnlyPropertyIsAnIndexer_ThrowsSayingIndexersAreNeverMapped()
    {
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => PocoTypeDescriptor<IndexerOnlyPoco>.Build());

        Assert.That(error.Message, Does.Contain("indexers"));
    }

    [Test]
    public void Build_EveryPropertyNotMapped_ThrowsSayingTheyAreAllExcluded()
    {
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => PocoTypeDescriptor<AllNotMappedPoco>.Build());

        Assert.That(error.Message, Does.Contain("every mappable property (2)"));
        Assert.That(error.Message, Does.Contain("[ClickHouseTcpNotMapped]"));
    }

    [Test]
    public void Build_EveryPropertyNotMappedAlongsideAnIndexer_CountsOnlyTheMappableOnes()
    {
        // The count is over what could have been mapped, so the indexer must not inflate it.
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => PocoTypeDescriptor<NotMappedWithIndexerPoco>.Build());

        Assert.That(error.Message, Does.Contain("every mappable property (1)"));
    }

    [Test]
    public void Build_ReadWriteProperty_IsUsableInBothDirections()
    {
        PocoMember member = Member<SimplePoco>("Id");

        Assert.That(member.CanGet, Is.True);
        Assert.That(member.CanSet, Is.True);
    }

    [Test]
    public void Build_GetterOnlyProperty_IsGettableButNotSettable()
    {
        PocoMember member = Member<AccessorsPoco>("GetterOnly");

        Assert.That(member.CanGet, Is.True);
        Assert.That(member.CanSet, Is.False);
        Assert.That(member.DescribeWhyNotSettable(), Is.EqualTo("it has no setter"));
    }

    [Test]
    public void Build_InitOnlyProperty_IsNotSettableBecauseAssigningItWouldDefeatTheImmutability()
    {
        PocoMember member = Member<AccessorsPoco>("InitOnly");

        Assert.That(member.CanGet, Is.True);
        Assert.That(member.CanSet, Is.False);
        Assert.That(member.DescribeWhyNotSettable(), Is.EqualTo("its setter is init-only"));
    }

    [Test]
    public void Build_PrivateSetter_IsNotSettable()
    {
        PocoMember member = Member<AccessorsPoco>("PrivateSetter");

        Assert.That(member.CanGet, Is.True);
        Assert.That(member.CanSet, Is.False);
        Assert.That(member.DescribeWhyNotSettable(), Is.EqualTo("its setter is not public"));
    }

    [Test]
    public void Build_SetterOnlyProperty_IsSettableButNotGettable()
    {
        PocoMember member = Member<AccessorsPoco>("SetterOnly");

        Assert.That(member.CanGet, Is.False);
        Assert.That(member.CanSet, Is.True);
    }

    [Test]
    public void Build_PrivateGetter_IsNotGettableSoItCannotBeAnInsertSource()
    {
        // The insert-side mirror of the private-setter case: reflection reports a getter, but not a public one.
        PocoMember member = Member<AccessorsPoco>("PrivateGetter");

        Assert.That(member.CanGet, Is.False);
        Assert.That(member.CanSet, Is.True);
    }

    [TestCase("Required", false, null)]
    [TestCase("Optional", true, typeof(int))]
    [TestCase("Text", true, null)]
    public void Build_MemberType_RecordsWhetherNullFitsAndWhatTheNullableUnderlyingTypeIs(
        string columnName, bool canAssignNull, Type nullableUnderlying)
    {
        PocoMember member = Member<NullabilityPoco>(columnName);

        Assert.That(member.CanAssignNull, Is.EqualTo(canAssignNull));
        Assert.That(member.NullableUnderlyingType, Is.EqualTo(nullableUnderlying));
    }

    [Test]
    public void Activator_ClassWithAParameterlessConstructor_CreatesDistinctInstances()
    {
        PocoTypeDescriptor<SimplePoco> descriptor = PocoTypeDescriptor<SimplePoco>.Build();

        Assert.That(descriptor.CanActivate, Is.True);
        SimplePoco first = descriptor.Activator();
        SimplePoco second = descriptor.Activator();
        Assert.That(first, Is.Not.Null);
        Assert.That(second, Is.Not.SameAs(first));
    }

    [Test]
    public void Build_ImmutableTypeWithNoParameterlessConstructor_StillBuildsSoItCanBeInserted()
    {
        // The descriptor is shared with the insert path, which never constructs a T. Refusing the type here would
        // make an immutable POCO uninsertable for a constructor only the query path needs.
        PocoTypeDescriptor<ImmutablePoco> descriptor = PocoTypeDescriptor<ImmutablePoco>.Build();

        Assert.That(ColumnNames(descriptor), Is.EquivalentTo(new[] { "Id" }));
        Assert.That(descriptor.CanActivate, Is.False);

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(() => _ = descriptor.Activator);
        Assert.That(error.Message, Does.Contain("no public parameterless constructor"));
    }

    [Test]
    public void Activator_AbstractClass_IsBlockedForBeingAbstractRatherThanForLackingAConstructor()
    {
        // The declared public constructor makes GetConstructor succeed, so only the IsAbstract check catches this;
        // otherwise it would compile and fail when the delegate first ran.
        PocoTypeDescriptor<AbstractPocoWithConstructor> descriptor = PocoTypeDescriptor<AbstractPocoWithConstructor>.Build();

        Assert.That(descriptor.CanActivate, Is.False);
        Assert.That(
            Assert.Throws<InvalidOperationException>(() => _ = descriptor.Activator).Message,
            Does.Contain("it is abstract"));
    }

    [Test]
    public void Activator_Interface_IsBlockedAndSaysSo()
    {
        PocoTypeDescriptor<IPocoShape> descriptor = PocoTypeDescriptor<IPocoShape>.Build();

        Assert.That(descriptor.CanActivate, Is.False);
        Assert.That(
            Assert.Throws<InvalidOperationException>(() => _ = descriptor.Activator).Message,
            Does.Contain("it is an interface"));
    }

    [Test]
    public void TryMatchColumn_ExactName_MatchesThatMember()
    {
        Assert.That(Match<SimplePoco>("Name").MemberName, Is.EqualTo("Name"));
    }

    [Test]
    public void TryMatchColumn_NameDifferingOnlyInCase_MatchesThatMember()
    {
        Assert.That(Match<SimplePoco>("name").MemberName, Is.EqualTo("Name"));
    }

    [Test]
    public void TryMatchColumn_SnakeCaseColumn_MatchesThePascalCaseProperty()
    {
        // The reason most POCOs need no attribute at all.
        Assert.That(Match<SnakeCasePoco>("user_id").MemberName, Is.EqualTo("UserId"));
    }

    [Test]
    public void TryMatchColumn_SnakeCaseColumnInAnotherCase_StillMatchesThePascalCaseProperty()
    {
        // Both looser tiers at once: the underscores go, and what is left is compared case-insensitively.
        Assert.That(Match<SnakeCasePoco>("USER_ID").MemberName, Is.EqualTo("UserId"));
    }

    [Test]
    public void TryMatchColumn_UnknownColumn_ReturnsFalseSoTheColumnCanBeSkipped()
    {
        PocoTypeDescriptor<SimplePoco> descriptor = PocoTypeDescriptor<SimplePoco>.Build();

        Assert.That(descriptor.TryMatchColumn("absent", out PocoMember member), Is.False);
        Assert.That(member, Is.Null);
    }

    [Test]
    public void TryMatchColumn_ColumnMatchingOneMemberExactlyAndAnotherByCase_PrefersTheExactMatch()
    {
        Assert.That(Match<CaseCollidingPoco>("Value").MemberName, Is.EqualTo("Value"));
        Assert.That(Match<CaseCollidingPoco>("value").MemberName, Is.EqualTo("Lowercase"));
    }

    [Test]
    public void TryMatchColumn_ColumnMatchingOneMemberExactlyWhileTheUnderscoreTierIsAmbiguous_PrefersTheExactMatch()
    {
        // The tightest tier holds every member, so an exact name wins even when the loosest tier would collide
        // three ways.
        Assert.That(Match<ExactBeatsUnderscorePoco>("userid").MemberName, Is.EqualTo("Compact"));
    }

    [Test]
    public void TryMatchColumn_ColumnMatchingSeveralMembersOnlyByCase_ThrowsNamingThem()
    {
        PocoTypeDescriptor<CaseCollidingPoco> descriptor = PocoTypeDescriptor<CaseCollidingPoco>.Build();

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => descriptor.TryMatchColumn("VALUE", out _));

        Assert.That(error.Message, Does.Contain("VALUE"));
        Assert.That(error.Message, Does.Contain("Value"));
        Assert.That(error.Message, Does.Contain("Lowercase"));
        Assert.That(error.Message, Does.Contain("[ClickHouseTcpColumn(Name = \"VALUE\")]"));
    }

    [Test]
    public void TryMatchColumn_ColumnMatchingSeveralMembersOnlyOnceUnderscoresAreIgnored_Throws()
    {
        PocoTypeDescriptor<UnderscoreCollidingPoco> descriptor = PocoTypeDescriptor<UnderscoreCollidingPoco>.Build();

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => descriptor.TryMatchColumn("userid", out _));

        Assert.That(error.Message, Does.Contain("UserId"));
        Assert.That(error.Message, Does.Contain("Alternate"));
    }

    [Test]
    public void TryMatchColumn_ColumnResolvableByCaseWhileTheUnderscoreTierIsAmbiguous_NeverReachesTheAmbiguity()
    {
        // What earns the case tier its place: 'USER_ID' resolves there, even though ignoring underscores as well
        // would have made it ambiguous.
        Assert.That(Match<UnderscoreCollidingPoco>("USER_ID").MemberName, Is.EqualTo("UserId"));
    }

    [Test]
    public void TryMatchColumn_ExplicitColumnName_StillMatchesThroughTheLooserTiers()
    {
        // One rule for every column name, whether it came from the attribute or the property.
        Assert.That(Match<RenamedPoco>("EVENT_TIME").MemberName, Is.EqualTo("Timestamp"));
    }

    [Test]
    public void TryMatchColumn_UnsettableMember_StillMatchesBecauseMatchingIsDirectionAgnostic()
    {
        // Whether the member can be written is the query plan's business; the descriptor serves both directions.
        Assert.That(Match<ImmutablePoco>("id").MemberName, Is.EqualTo("Id"));
    }

    private static PropertyInfo DeclaredProperty(Type declaringType, string name)
        => declaringType.GetProperty(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);

    private static PocoMember Match<T>(string columnName)
        where T : class
    {
        Assert.That(PocoTypeDescriptor<T>.Build().TryMatchColumn(columnName, out PocoMember member), Is.True);
        return member;
    }

    private static PocoMember Member<T>(string columnName)
        where T : class
        => PocoTypeDescriptor<T>.Build().Members.Single(member => member.ColumnName == columnName);

    private static IEnumerable<string> ColumnNames<T>(PocoTypeDescriptor<T> descriptor)
        where T : class
        => descriptor.Members.Select(member => member.ColumnName);

    private class SimplePoco
    {
        public long Id { get; set; }

        public string Name { get; set; }
    }

    private class PartlyMappedPoco
    {
        public int Kept { get; set; }

        [ClickHouseTcpNotMapped]
        public int Dropped { get; set; }
    }

    private class RenamedPoco
    {
        [ClickHouseTcpColumn(Name = "event_time")]
        public DateTime Timestamp { get; set; }
    }

    private class AttributeWithoutNamePoco
    {
        [ClickHouseTcpColumn]
        public int Value { get; set; }
    }

    private class BlankColumnNamePoco
    {
        [ClickHouseTcpColumn(Name = "  ")]
        public int Value { get; set; }
    }

    private class IndexerPoco
    {
        public int Value { get; set; }

        public int this[int index] => index;
    }

    private class IndexerOnlyPoco
    {
        public int this[int index] => index;
    }

    private class StaticAndInstancePoco
    {
        public static int Static { get; set; }

        public int Instance { get; set; }
    }

    private class EmptyPoco
    {
    }

    private class AllNotMappedPoco
    {
        [ClickHouseTcpNotMapped]
        public int First { get; set; }

        [ClickHouseTcpNotMapped]
        public int Second { get; set; }
    }

    private class NotMappedWithIndexerPoco
    {
        [ClickHouseTcpNotMapped]
        public int Dropped { get; set; }

        public int this[int index] => index;
    }

    private class InheritedBase
    {
        public int BaseValue { get; set; }
    }

    private class InheritedPoco : InheritedBase
    {
        public int DerivedValue { get; set; }
    }

    private class ShadowedBase
    {
        public int Value { get; set; }
    }

    private class ShadowingPoco : ShadowedBase
    {
        public new long Value { get; set; }
    }

    private class DuplicateColumnPoco
    {
        public int Value { get; set; }

        [ClickHouseTcpColumn(Name = "Value")]
        public int Other { get; set; }
    }

    private class AccessorsPoco
    {
        public int GetterOnly => 1;

        public int InitOnly { get; init; }

        public int PrivateSetter { get; private set; }

        public int PrivateGetter { private get; set; }

        public int SetterOnly
        {
            set => _ = value;
        }
    }

    private class NullabilityPoco
    {
        public int Required { get; set; }

        public int? Optional { get; set; }

        public string Text { get; set; }
    }

    private class ImmutablePoco
    {
        public ImmutablePoco(long id) => Id = id;

        public long Id { get; }
    }

    // The public parameterless constructor is the point: reflection finds it, so only the IsAbstract check stops
    // the activator from compiling and then failing on first use.
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1012:Abstract types should not have public constructors", Justification = "The public constructor is what the test exercises.")]
    private abstract class AbstractPocoWithConstructor
    {
        public AbstractPocoWithConstructor()
        {
        }

        public int Value { get; set; }
    }

    private interface IPocoShape
    {
        int Value { get; set; }
    }

    private interface IBaseShape
    {
        int BaseValue { get; set; }
    }

    private interface IDerivedShape : IBaseShape
    {
        int DerivedValue { get; set; }
    }

    private interface IShadowedShape
    {
        int Value { get; set; }
    }

    private interface IShadowingShape : IShadowedShape
    {
        new long Value { get; set; }
    }

    private interface ILeftShape
    {
        int Value { get; set; }
    }

    private interface IRightShape
    {
        int Value { get; set; }
    }

    private interface IAmbiguousShape : ILeftShape, IRightShape
    {
    }

    private abstract class OverriddenBase
    {
        [ClickHouseTcpNotMapped]
        public virtual int Excluded { get; set; }

        [ClickHouseTcpColumn(Name = "renamed_column")]
        public virtual int Renamed { get; set; }

        public virtual int Plain { get; set; }
    }

    private class OverridingPoco : OverriddenBase
    {
        public override int Excluded { get; set; }

        public override int Renamed { get; set; }

        public override int Plain { get; set; }
    }

    private class SnakeCasePoco
    {
        public int UserId { get; set; }
    }

    private class CaseCollidingPoco
    {
        public int Value { get; set; }

        [ClickHouseTcpColumn(Name = "value")]
        public int Lowercase { get; set; }
    }

    // Three column names that all collapse to 'userid' once underscores go, one of which is that name exactly.
    private class ExactBeatsUnderscorePoco
    {
        [ClickHouseTcpColumn(Name = "userid")]
        public int Compact { get; set; }

        [ClickHouseTcpColumn(Name = "user_id")]
        public int Snake { get; set; }

        [ClickHouseTcpColumn(Name = "us_erid")]
        public int Odd { get; set; }
    }

    // Column names that differ only in where their underscores fall: distinct at the case tier, indistinguishable
    // once underscores are ignored.
    private class UnderscoreCollidingPoco
    {
        [ClickHouseTcpColumn(Name = "user_id")]
        public int UserId { get; set; }

        [ClickHouseTcpColumn(Name = "us_erid")]
        public int Alternate { get; set; }
    }
}
