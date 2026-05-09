using System;
using System.Linq;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Numerics;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Copy;

[TestFixture]
public class PocoEdgeCaseTests
{
    private ClickHouseClient client;

    [SetUp]
    public void SetUp()
    {
        client = new ClickHouseClient(new ClickHouseClientSettings());
    }

    [TearDown]
    public void TearDown()
    {
        client?.Dispose();
    }

    // ----- Records -----
    // Positional records get a primary constructor with parameters and no parameterless ctor.
    // Insert-only registration succeeds (it doesn't construct), but read registration must fail.
    public record PositionalRecord(int Id, string Name);

    // Record class with parameterless ctor but only init-only properties — read registration fails
    // because no property has a public non-init setter; insert-only registration succeeds.
    public record class RecordWithInitProperties
    {
        public int Id { get; init; }
        public string Name { get; init; }
    }

    // Record class with parameterless ctor and a public non-init setter on at least one property.
    // The init-only property is silently ignored; the get/set property is mapped.
    public record class RecordWithMixedAccessors
    {
        public int Id { get; init; }
        public string Name { get; set; }
    }

    // ----- Standalone init-only -----
    private class InitOnlyOnly
    {
        public int Id { get; init; }
        public string Name { get; init; }
    }

    // ----- Private setter -----
    private class PrivateSetterPoco
    {
        public int Id { get; private set; }
        public string Name { get; set; }
    }

    private class AllPrivateSettersPoco
    {
        public int Id { get; private set; }
        public string Name { get; private set; }
    }

    // ----- Read-only auto-property -----
    private class ReadOnlyOnlyPoco
    {
        public int Id { get; }
        public string Name { get; }
    }

    private class MixedReadOnlyPoco
    {
        public int Id { get; }
        public string Name { get; set; }
    }

    // ----- Inheritance -----
    private class BasePoco
    {
        public int InheritedId { get; set; }
    }

    private class DerivedPoco : BasePoco
    {
        public string OwnName { get; set; }
    }

    // ----- Abstract class -----
    private abstract class AbstractPoco
    {
        public int Id { get; set; }
    }

    // ----- Field-only / static-only -----
    private class FieldsOnlyPoco
    {
        public int Id;
        public string Name;
    }

    private class StaticPropertyOnlyPoco
    {
        public static int Counter { get; set; }
    }

    // ----- Write-only property -----
    // The write-only property must be ignored; the regular property must still register.
    private class WriteOnlyPropertyPoco
    {
        public int Id { get; set; }

        private string backingName;
        public string Name { set => backingName = value; }
    }

#if NET7_0_OR_GREATER
    // `required` does not change accessor visibility, only the call-site enforcement. The setter
    // is still public and non-init, so the property is mapped. Reflection-based construction via
    // `Expression.New(ctor)` does not enforce required-member initialization, so we can construct
    // the instance and then assign properties at materialization time.
    private class RequiredMembersPoco
    {
        public required int Id { get; set; }
        public required string Name { get; set; }
    }
#endif

    [Test]
    public void RegisterBinaryInsertType_PositionalRecord_RegistersSuccessfully()
    {
        // Insert-only registration does not construct instances, so positional records (which have
        // no public parameterless constructor) are accepted on the insert side.
        Assert.DoesNotThrow(() => client.RegisterBinaryInsertType<PositionalRecord>());
    }

    [Test]
    public void RegisterPocoReadType_PositionalRecord_ThrowsInvalidOperation()
    {
        // Read registration requires a public parameterless ctor — positional records lack one.
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoReadType<PositionalRecord>());

        Assert.That(ex.Message, Does.Contain("parameterless constructor"));
    }

    [Test]
    public void RegisterBinaryInsertType_RecordWithInitProperties_RegistersSuccessfully()
    {
        // The insert path only requires public getters; init-only properties are read just fine.
        Assert.DoesNotThrow(() => client.RegisterBinaryInsertType<RecordWithInitProperties>());
    }

    [Test]
    public void RegisterPocoReadType_RecordWithInitProperties_ThrowsBecauseNoMappedProperties()
    {
        // All properties are init-only, so no public non-init setter exists for the read path.
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoReadType<RecordWithInitProperties>());

        Assert.That(ex.Message, Does.Contain("RecordWithInitProperties"));
        Assert.That(ex.Message, Does.Contain("no public properties"));
    }

    [Test]
    public void RegisterPocoType_RecordWithMixedAccessors_RegistersOnlyNonInitProperty()
    {
        // The init-only property is silently ignored; the get/set property satisfies the
        // "at least one mapped property" rule.
        Assert.DoesNotThrow(() => client.RegisterPocoType<RecordWithMixedAccessors>());
    }

    [Test]
    public void RegisterPocoReadType_AllInitOnlyProperties_ThrowsBecauseNoMappedProperties()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoReadType<InitOnlyOnly>());

        Assert.That(ex.Message, Does.Contain("InitOnlyOnly"));
    }

    [Test]
    public void RegisterBinaryInsertType_AllInitOnlyProperties_RegistersSuccessfully()
    {
        // Insert-only doesn't care about setter accessibility; all properties have public getters.
        Assert.DoesNotThrow(() => client.RegisterBinaryInsertType<InitOnlyOnly>());
    }

    [Test]
    public void RegisterPocoReadType_PrivateSetter_IgnoresPropertyButRegistersOthers()
    {
        // Id has a private setter and is excluded from the read path; Name is mapped.
        Assert.DoesNotThrow(() => client.RegisterPocoReadType<PrivateSetterPoco>());
    }

    [Test]
    public void RegisterPocoReadType_AllPrivateSetters_ThrowsBecauseNoMappedProperties()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoReadType<AllPrivateSettersPoco>());

        Assert.That(ex.Message, Does.Contain("AllPrivateSettersPoco"));
    }

    [Test]
    public void RegisterBinaryInsertType_AllPrivateSetters_RegistersSuccessfully()
    {
        // The insert path doesn't care about setter accessibility; public getters suffice.
        Assert.DoesNotThrow(() => client.RegisterBinaryInsertType<AllPrivateSettersPoco>());
    }

    [Test]
    public void RegisterPocoReadType_AllReadOnlyProperties_ThrowsBecauseNoMappedProperties()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoReadType<ReadOnlyOnlyPoco>());

        Assert.That(ex.Message, Does.Contain("ReadOnlyOnlyPoco"));
    }

    [Test]
    public void RegisterBinaryInsertType_AllReadOnlyProperties_RegistersSuccessfully()
    {
        // Read-only auto-properties have public getters and are valid for insert-only registration.
        Assert.DoesNotThrow(() => client.RegisterBinaryInsertType<ReadOnlyOnlyPoco>());
    }

    [Test]
    public void RegisterPocoType_MixedReadOnlyAndReadWrite_RegistersOnlyReadWrite()
    {
        // Read-only Id is silently ignored; read/write Name is mapped.
        Assert.DoesNotThrow(() => client.RegisterPocoType<MixedReadOnlyPoco>());
    }

    [Test]
    public void RegisterPocoType_DerivedType_IncludesInheritedProperties()
    {
        // The base property must be visible alongside the derived property.
        Assert.DoesNotThrow(() => client.RegisterPocoType<DerivedPoco>());
    }

    [Test]
    public void RegisterPocoReadType_AbstractType_ThrowsInvalidOperation()
    {
        // Abstract classes have no public parameterless constructor — read registration fails.
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoReadType<AbstractPoco>());

        Assert.That(ex.Message, Does.Contain("parameterless constructor"));
    }

    [Test]
    public void RegisterBinaryInsertType_AbstractType_RegistersSuccessfully()
    {
        // Insert-only registration doesn't construct instances. Users would still need to supply
        // concrete-derived instances at insert time; that's fine — registration itself is permissive.
        Assert.DoesNotThrow(() => client.RegisterBinaryInsertType<AbstractPoco>());
    }

    [Test]
    public void RegisterPocoType_FieldsOnly_ThrowsBecauseNoMappedProperties()
    {
        // Public fields are not mapped — only properties are.
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<FieldsOnlyPoco>());

        Assert.That(ex.Message, Does.Contain("FieldsOnlyPoco"));
    }

    [Test]
    public void RegisterPocoType_StaticPropertiesOnly_ThrowsBecauseNoMappedProperties()
    {
        // Static properties don't satisfy `BindingFlags.Instance` and are not mapped.
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<StaticPropertyOnlyPoco>());

        Assert.That(ex.Message, Does.Contain("StaticPropertyOnlyPoco"));
    }

    [Test]
    public void RegisterPocoType_WriteOnlyProperty_IgnoresPropertyButRegistersOthers()
    {
        // The write-only property has no public getter and is excluded; Id is mapped.
        Assert.DoesNotThrow(() => client.RegisterPocoType<WriteOnlyPropertyPoco>());
    }

#if NET7_0_OR_GREATER
    [Test]
    public void RegisterPocoType_RequiredMembers_RegistersSuccessfully()
    {
        // `required` properties have public non-init setters and are mappable. The constraint is
        // `where T : class` (no `new()`), so required-member types are accepted; the runtime
        // construction goes through Expression.New, which emits raw `newobj` IL and does not
        // enforce required-member initialization.
        Assert.DoesNotThrow(() => client.RegisterPocoType<RequiredMembersPoco>());
    }
#endif
}

[TestFixture]
public class PocoReadEdgeCaseTests : AbstractConnectionTestFixture
{
    public class SimplePoco
    {
        public ulong Id { get; set; }
        public string Value { get; set; }
    }

    public class SecondPoco
    {
        public ulong Id { get; set; }
        public string Other { get; set; }
    }

    public class ObjectPropertyPoco
    {
        public ulong Id { get; set; }
        public object Value { get; set; }
    }

    public class WidenedIntPoco
    {
        // Column will be Int16; property is Int32 — strict v1 disallows widening.
        public int Id { get; set; }
    }

    public class DerivedReadPoco : ReadBasePoco
    {
        public string OwnValue { get; set; }
    }

    public class ReadBasePoco
    {
        public ulong Id { get; set; }
    }

    [Test]
    public async Task GetRecord_NumericWideningInt16ToInt32_ThrowsInvalidOperation()
    {
        client.RegisterPocoType<WidenedIntPoco>();

        // Column is Int16; property is Int32 — v1 makes no widening conversions.
        using var reader = await client.ExecuteReaderAsync("SELECT toInt16(7) AS Id");
        Assert.That(reader.Read(), Is.True);

        var ex = Assert.Throws<InvalidOperationException>(() => reader.GetRecord<WidenedIntPoco>());
        Assert.That(ex.Message, Does.Contain("Id"));
        Assert.That(ex.Message, Does.Contain("System.Int32"));
        Assert.That(ex.Message, Does.Contain("System.Int16"));
    }

    [Test]
    public async Task GetRecord_StringIntoObjectProperty_AssignsValue()
    {
        // `object` is assignable from any reference type — should succeed without conversion.
        client.RegisterPocoType<ObjectPropertyPoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, 'hello' AS Value");
        Assert.That(reader.Read(), Is.True);

        var poco = reader.GetRecord<ObjectPropertyPoco>();
        Assert.That(poco.Id, Is.EqualTo(1UL));
        Assert.That(poco.Value, Is.EqualTo("hello"));
    }

    [Test]
    public async Task GetRecord_DerivedTypeInheritedProperty_MapsCorrectly()
    {
        client.RegisterPocoType<DerivedReadPoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(42) AS Id, 'derived' AS OwnValue");
        Assert.That(reader.Read(), Is.True);

        var poco = reader.GetRecord<DerivedReadPoco>();
        Assert.That(poco.Id, Is.EqualTo(42UL));
        Assert.That(poco.OwnValue, Is.EqualTo("derived"));
    }

    [Test]
    public async Task GetRecord_MultipleTypesOnSameReader_MaterializeIndependently()
    {
        // Two POCO types should both work against the same reader (binding plan caches per type).
        client.RegisterPocoType<SimplePoco>();
        client.RegisterPocoType<SecondPoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, 'a' AS Value, 'b' AS Other");
        Assert.That(reader.Read(), Is.True);

        var first = reader.GetRecord<SimplePoco>();
        var second = reader.GetRecord<SecondPoco>();

        Assert.That(first.Id, Is.EqualTo(1UL));
        Assert.That(first.Value, Is.EqualTo("a"));
        Assert.That(second.Id, Is.EqualTo(1UL));
        Assert.That(second.Other, Is.EqualTo("b"));
    }

    [Test]
    public async Task GetRecord_BeforeRead_ThrowsInvalidOperation()
    {
        client.RegisterPocoType<SimplePoco>();

        using var reader = await client.ExecuteReaderAsync("SELECT toUInt64(1) AS Id, 'a' AS Value");

        // No Read() yet — there is no current row. GetRecord must surface that precondition
        // rather than silently materializing a default-filled instance.
        var ex = Assert.Throws<InvalidOperationException>(() => reader.GetRecord<SimplePoco>());
        Assert.That(ex.Message, Does.Contain("Read()"));
    }

    [Test]
    public async Task GetRecord_AfterEndOfStream_ThrowsInvalidOperation()
    {
        client.RegisterPocoType<SimplePoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, 'a' AS Value");

        Assert.That(reader.Read(), Is.True);
        // Drain the stream — the next Read() returns false.
        Assert.That(reader.Read(), Is.False);

        var ex = Assert.Throws<InvalidOperationException>(() => reader.GetRecord<SimplePoco>());
        Assert.That(ex.Message, Does.Contain("Read()"));
    }

    [Test]
    public async Task GetRecord_RequiredMembers_MaterializesInstance()
    {
#if NET7_0_OR_GREATER
        client.RegisterPocoType<EndToEndRequiredPoco>();

        using var reader = await client.ExecuteReaderAsync(
            "SELECT toInt32(7) AS Id, 'alice' AS Name");

        Assert.That(reader.Read(), Is.True);
        var poco = reader.GetRecord<EndToEndRequiredPoco>();

        Assert.That(poco.Id, Is.EqualTo(7));
        Assert.That(poco.Name, Is.EqualTo("alice"));
#else
        Assert.Ignore("`required` members require .NET 7+");
#endif
    }

#if NET7_0_OR_GREATER
    public class EndToEndRequiredPoco
    {
        public required int Id { get; set; }
        public required string Name { get; set; }
    }
#endif

    [Test]
    public async Task GetRecord_DecimalWithUseCustomDecimalsFalse_AssignsValue()
    {
        // With UseCustomDecimals=false, Decimal columns are returned as System.Decimal directly,
        // so a System.Decimal property is assignable. This complements the v1 "no implicit conversion"
        // rule: customers who want decimal interoperability today must opt out of ClickHouseDecimal.
        using var customClient = TestUtilities.GetTestClickHouseClient(customDecimals: false);
        customClient.RegisterPocoType<PocoReadTests.DecimalPoco>();

        using var reader = await customClient.ExecuteReaderAsync(
            "SELECT toUInt64(1) AS Id, toDecimal128('123.45', 2) AS Amount");
        Assert.That(reader.Read(), Is.True);

        var poco = reader.GetRecord<PocoReadTests.DecimalPoco>();
        Assert.That(poco.Id, Is.EqualTo(1UL));
        Assert.That(poco.Amount, Is.EqualTo(123.45m));
    }

    [Test]
    public async Task QueryAsync_UnregisteredType_ThrowsOnFirstYield()
    {
        // Enumeration starts the query; the first MoveNextAsync calls GetRecord<T> which throws
        // because the type is not registered. The exception surfaces from the foreach.
        var enumerator = client.QueryAsync<UnregisteredQueryPoco>("SELECT toUInt64(1) AS Id").GetAsyncEnumerator();
        try
        {
            Assert.ThrowsAsync<InvalidOperationException>(async () => await enumerator.MoveNextAsync());
        }
        finally
        {
            await enumerator.DisposeAsync();
        }
    }

    private class UnregisteredQueryPoco
    {
        public ulong Id { get; set; }
    }
}
