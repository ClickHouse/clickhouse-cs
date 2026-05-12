using System;
using ClickHouse.Driver.ADO;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Copy;

[TestFixture]
public class PocoRegistrationTests
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

    private class EmptyPoco
    {
    }

    private class AllNotMappedPoco
    {
        [ClickHouseNotMapped]
        public int Id { get; set; }
    }

    private class PocoWithDuplicateColumnNames
    {
        [ClickHouseColumn(Name = "shared")]
        public int A { get; set; }

        [ClickHouseColumn(Name = "shared")]
        public int B { get; set; }
    }

    private class PocoWithWhitespaceColumnName
    {
        [ClickHouseColumn(Name = "   ")]
        public int Id { get; set; }
    }

    private class PocoWithoutParameterlessConstructor
    {
        public PocoWithoutParameterlessConstructor(int id)
        {
            Id = id;
        }

        public int Id { get; set; }
    }

    [Test]
    public void RegisterPocoType_NoMappedProperties_ThrowsInvalidOperation()
    {
        var emptyEx = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<EmptyPoco>());
        Assert.That(emptyEx.Message, Does.Contain("EmptyPoco"));

        var notMappedEx = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<AllNotMappedPoco>());
        Assert.That(notMappedEx.Message, Does.Contain("AllNotMappedPoco"));
    }

    [Test]
    public void RegisterPocoType_DuplicateColumnNames_ThrowsInvalidOperation()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<PocoWithDuplicateColumnNames>());

        Assert.That(ex.Message, Does.Contain("shared"));
    }

    [Test]
    public void RegisterPocoType_WhitespaceColumnName_ThrowsInvalidOperation()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<PocoWithWhitespaceColumnName>());

        Assert.That(ex.Message, Does.Contain("empty or whitespace"));
    }

    [Test]
    public void RegisterPocoType_TypeWithoutParameterlessConstructor_ThrowsInvalidOperation()
    {
        // The convenience method registers both insert and read; the read leg fails on the missing ctor.
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<PocoWithoutParameterlessConstructor>());

        Assert.That(ex.Message, Does.Contain("parameterless constructor"));
    }

    // ----- POCO shapes used by both sides ----------------------------------------------------

    private record PositionalRecord(int Id, string Name);

    private record class RecordWithInitProperties
    {
        public int Id { get; init; }
        public string Name { get; init; }
    }

    private class InitOnlyOnly
    {
        public int Id { get; init; }
        public string Name { get; init; }
    }

    private class AllPrivateSettersPoco
    {
        public int Id { get; private set; }
        public string Name { get; private set; }
    }

    private class ReadOnlyOnlyPoco
    {
        public int Id { get; }
        public string Name { get; }
    }

    private abstract class AbstractPoco
    {
        public int Id { get; set; }
    }

    // Abstract class with an explicit public parameterless ctor. The ctor itself is callable
    // from derived classes via base(), so GetConstructor(Type.EmptyTypes) returns it; without
    // the IsAbstract guard, the read registration would succeed and only fail on first MapTo.
    private abstract class AbstractPocoWithPublicCtor
    {
        public AbstractPocoWithPublicCtor() { }
        public int Id { get; set; }
    }

    private interface IPocoInterface
    {
        int Id { get; set; }
    }

    private class FieldsOnlyPoco
    {
        public int Id;
        public string Name;
    }

    private class StaticPropertyOnlyPoco
    {
        public static int Counter { get; set; }
    }

    private class WriteOnlyPropertyPoco
    {
        public int Id { get; set; }

        private string backingName;
        public string Name { set => backingName = value; }
    }

    // ----- Read side: RegisterPocoType<T> rejects shapes it cannot materialize ---------------

    [Test]
    public void RegisterPocoType_PositionalRecord_ThrowsInvalidOperation()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<PositionalRecord>());

        Assert.That(ex.Message, Does.Contain("parameterless constructor"));
    }

    [Test]
    public void RegisterPocoType_RecordWithInitProperties_ThrowsBecauseNoMappedProperties()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<RecordWithInitProperties>());

        Assert.That(ex.Message, Does.Contain("RecordWithInitProperties"));
        Assert.That(ex.Message, Does.Contain("no public properties"));
    }

    [Test]
    public void RegisterPocoType_AllInitOnlyProperties_ThrowsBecauseNoMappedProperties()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<InitOnlyOnly>());

        Assert.That(ex.Message, Does.Contain("InitOnlyOnly"));
    }

    [Test]
    public void RegisterPocoType_AllPrivateSetters_ThrowsBecauseNoMappedProperties()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<AllPrivateSettersPoco>());

        Assert.That(ex.Message, Does.Contain("AllPrivateSettersPoco"));
    }

    [Test]
    public void RegisterPocoType_AllReadOnlyProperties_ThrowsBecauseNoMappedProperties()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<ReadOnlyOnlyPoco>());

        Assert.That(ex.Message, Does.Contain("ReadOnlyOnlyPoco"));
    }

    [Test]
    public void RegisterPocoType_AbstractType_ThrowsInvalidOperation()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<AbstractPoco>());

        Assert.That(ex.Message, Does.Contain("abstract"));
    }

    [Test]
    public void RegisterPocoType_AbstractTypeWithPublicParameterlessCtor_ThrowsInvalidOperation()
    {
        // The ctor lookup alone would not catch this — abstract types can declare a public
        // parameterless ctor that is only callable via base() from derived classes. The
        // IsAbstract guard must reject the type at registration time so the failure is not
        // deferred to the first MapTo<T> call.
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<AbstractPocoWithPublicCtor>());

        Assert.That(ex.Message, Does.Contain("abstract"));
    }

    [Test]
    public void RegisterPocoType_Interface_ThrowsInvalidOperation()
    {
        // Interfaces satisfy the `where T : class` constraint but cannot be instantiated.
        // The IsAbstract check covers them (Type.IsAbstract is true for interfaces).
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<IPocoInterface>());

        Assert.That(ex.Message, Does.Contain("abstract"));
    }

    [Test]
    public void RegisterPocoType_FieldsOnly_ThrowsBecauseNoMappedProperties()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<FieldsOnlyPoco>());

        Assert.That(ex.Message, Does.Contain("FieldsOnlyPoco"));
    }

    [Test]
    public void RegisterPocoType_StaticPropertiesOnly_ThrowsBecauseNoMappedProperties()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<StaticPropertyOnlyPoco>());

        Assert.That(ex.Message, Does.Contain("StaticPropertyOnlyPoco"));
    }

    [Test]
    public void RegisterPocoType_WriteOnlyProperty_IgnoresPropertyButRegistersOthers()
    {
        // Write-only property has no public getter and is excluded from the insert side; the
        // regular Id property satisfies both sides, so registration succeeds.
        Assert.DoesNotThrow(() => client.RegisterPocoType<WriteOnlyPropertyPoco>());
    }

    // ----- Insert side: RegisterBinaryInsertType<T> accepts shapes that read rejects --------
    // The insert path requires only a public getter on at least one property, so shapes that
    // the read side rejects (no parameterless ctor, all-init-only, all-private-setter,
    // all-readonly, abstract) are still acceptable for insert-only registration.

    [Test]
    public void RegisterBinaryInsertType_PositionalRecord_RegistersSuccessfully()
    {
        Assert.DoesNotThrow(() => client.RegisterBinaryInsertType<PositionalRecord>());
    }

    [Test]
    public void RegisterBinaryInsertType_RecordWithInitProperties_RegistersSuccessfully()
    {
        Assert.DoesNotThrow(() => client.RegisterBinaryInsertType<RecordWithInitProperties>());
    }

    [Test]
    public void RegisterBinaryInsertType_AllInitOnlyProperties_RegistersSuccessfully()
    {
        Assert.DoesNotThrow(() => client.RegisterBinaryInsertType<InitOnlyOnly>());
    }

    [Test]
    public void RegisterBinaryInsertType_AllPrivateSetters_RegistersSuccessfully()
    {
        Assert.DoesNotThrow(() => client.RegisterBinaryInsertType<AllPrivateSettersPoco>());
    }

    [Test]
    public void RegisterBinaryInsertType_AllReadOnlyProperties_RegistersSuccessfully()
    {
        Assert.DoesNotThrow(() => client.RegisterBinaryInsertType<ReadOnlyOnlyPoco>());
    }

    [Test]
    public void RegisterBinaryInsertType_AbstractType_RegistersSuccessfully()
    {
        Assert.DoesNotThrow(() => client.RegisterBinaryInsertType<AbstractPoco>());
    }
}
