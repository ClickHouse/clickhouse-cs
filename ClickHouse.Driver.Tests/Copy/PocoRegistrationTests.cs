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
}
