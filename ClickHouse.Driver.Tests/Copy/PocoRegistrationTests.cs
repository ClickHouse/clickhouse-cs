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

    private class PocoWithNotMapped
    {
        public int Id { get; set; }

        [ClickHouseNotMapped]
        public string IgnoreMe { get; set; }
    }

    private class PocoWithIndexer
    {
        public int Id { get; set; }

        public string this[int i] => null;
    }

    private class PocoWithoutParameterlessConstructor
    {
        public PocoWithoutParameterlessConstructor(int id)
        {
            Id = id;
        }

        public int Id { get; set; }
    }

    // Public setter (read-mappable) + private getter (not insert-mappable). Used to exercise
    // the case where read validation passes but insert validation throws "no mapped properties".
    private class PrivateGetterOnlyPoco
    {
        public int Id { private get; set; }
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
    public void RegisterPocoType_NotMappedProperty_ExcludesPropertyFromBothMappings()
    {
        client.RegisterPocoType<PocoWithNotMapped>();

        var insertMapping = client.PocoRegistry.GetInsertMapping<PocoWithNotMapped>();
        Assert.That(insertMapping, Is.Not.Null);
        Assert.That(
            insertMapping.Properties.Length, Is.EqualTo(1),
            "[ClickHouseNotMapped] property must not appear in the insert mapping.");
        Assert.That(insertMapping.Properties[0].PropertyName, Is.EqualTo("Id"));

        var readMapping = client.PocoRegistry.GetReadMapping<PocoWithNotMapped>();
        Assert.That(readMapping, Is.Not.Null);
        Assert.That(
            readMapping.Properties.Length, Is.EqualTo(1),
            "[ClickHouseNotMapped] property must not appear in the read mapping.");
        Assert.That(readMapping.Properties[0].PropertyName, Is.EqualTo("Id"));
        Assert.That(
            readMapping.ColumnNameToPropertyIndex.ContainsKey("IgnoreMe"), Is.False,
            "[ClickHouseNotMapped] column name must not be in the read lookup.");
    }

    [Test]
    public void RegisterPocoType_IndexerProperty_ExcludesIndexerFromBothMappings()
    {
        client.RegisterPocoType<PocoWithIndexer>();

        var insertMapping = client.PocoRegistry.GetInsertMapping<PocoWithIndexer>();
        Assert.That(insertMapping, Is.Not.Null);
        Assert.That(
            insertMapping.Properties.Length, Is.EqualTo(1),
            "Indexer must not be counted as a mapped property on the insert side.");
        Assert.That(insertMapping.Properties[0].PropertyName, Is.EqualTo("Id"));

        var readMapping = client.PocoRegistry.GetReadMapping<PocoWithIndexer>();
        Assert.That(readMapping, Is.Not.Null);
        Assert.That(
            readMapping.Properties.Length, Is.EqualTo(1),
            "Indexer must not be counted as a mapped property on the read side.");
        Assert.That(readMapping.Properties[0].PropertyName, Is.EqualTo("Id"));
    }

    [Test]
    public void RegisterBinaryInsertType_TypeWithoutParameterlessConstructor_RegistersInsertOnly()
    {
        // Insert-only registration does not need to construct instances, so the absence of a
        // public parameterless constructor must not block registration. Pin both that the
        // insert mapping is produced and that nothing is registered on the read side.
        client.RegisterBinaryInsertType<PocoWithoutParameterlessConstructor>();

        var insertMapping = client.PocoRegistry.GetInsertMapping<PocoWithoutParameterlessConstructor>();
        Assert.That(insertMapping, Is.Not.Null);
        Assert.That(insertMapping.Properties.Length, Is.EqualTo(1));
        Assert.That(insertMapping.Properties[0].PropertyName, Is.EqualTo("Id"));
        Assert.That(insertMapping.Getters.Length, Is.EqualTo(1));

        Assert.That(
            client.PocoRegistry.GetReadMapping<PocoWithoutParameterlessConstructor>(),
            Is.Null,
            "RegisterBinaryInsertType<T> must not register the type for read.");
    }

    [Test]
    public void RegisterPocoType_TypeWithoutParameterlessConstructor_ThrowsInvalidOperation()
    {
        // The convenience method registers both insert and read; the read leg fails on the missing ctor.
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<PocoWithoutParameterlessConstructor>());

        Assert.That(ex.Message, Does.Contain("parameterless constructor"));
    }

    [Test]
    public void RegisterPocoType_FailedReadRegistration_LeavesInsertUnregistered()
    {
        // PocoWithoutParameterlessConstructor passes insert validation (no ctor needed) but
        // fails read validation. The convenience method must be atomic: a thrown exception
        // must leave neither side registered.
        Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<PocoWithoutParameterlessConstructor>());

        Assert.That(
            client.PocoRegistry.GetInsertMapping<PocoWithoutParameterlessConstructor>(),
            Is.Null,
            "Failed RegisterPocoType<T> must not leave an insert mapping behind.");
        Assert.That(
            client.PocoRegistry.GetReadMapping<PocoWithoutParameterlessConstructor>(),
            Is.Null,
            "Failed RegisterPocoType<T> must not leave a read mapping behind.");
    }

    [Test]
    public void RegisterPocoType_FailedInsertRegistration_LeavesReadUnregistered()
    {
        // PrivateGetterOnlyPoco passes read validation (public non-init setter is present) but
        // fails insert validation (the only property's getter is private, so insert finds zero
        // mappable properties). The convenience method must build both mappings up front so
        // the failed insert validation cannot leave a tentative read commit behind.
        Assert.Throws<InvalidOperationException>(() =>
            client.RegisterPocoType<PrivateGetterOnlyPoco>());

        Assert.That(
            client.PocoRegistry.GetInsertMapping<PrivateGetterOnlyPoco>(),
            Is.Null,
            "Failed RegisterPocoType<T> must not leave an insert mapping behind.");
        Assert.That(
            client.PocoRegistry.GetReadMapping<PrivateGetterOnlyPoco>(),
            Is.Null,
            "Failed RegisterPocoType<T> must not leave a read mapping behind.");
    }
}
