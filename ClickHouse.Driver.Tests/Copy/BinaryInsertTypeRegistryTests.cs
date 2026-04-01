using System;
using System.Collections.Generic;
using System.IO;
using ClickHouse.Driver.ADO;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Copy;

[TestFixture]
public class BinaryInsertTypeRegistryTests
{
    private ClickHouseClient client;

    private class SimplePoco
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public double Score { get; set; }
    }

    private class PocoWithTypeAttribute
    {
        [ClickHouseColumn(Type = "UInt64")]
        public long Id { get; set; }

        [ClickHouseColumn(Type = "Nullable(String)")]
        public string Name { get; set; }
    }

    private class PocoWithEmptyColumnName
    {
        [ClickHouseColumn(Name = "")]
        public int Id { get; set; }
    }

    private class PocoWithWhitespaceColumnName
    {
        [ClickHouseColumn(Name = "   ")]
        public int Id { get; set; }
    }

    private class PocoWithEmptyTypeName
    {
        [ClickHouseColumn(Type = "")]
        public int Id { get; set; }
    }

    private class PocoWithWhitespaceTypeName
    {
        [ClickHouseColumn(Type = "   ")]
        public int Id { get; set; }
    }

    private class PocoWithDuplicateColumnNames
    {
        [ClickHouseColumn(Name = "shared_col")]
        public int First { get; set; }

        [ClickHouseColumn(Name = "shared_col")]
        public string Second { get; set; }
    }

    private class EmptyPoco
    {
    }

    private class AllNotMappedPoco
    {
        [ClickHouseNotMapped]
        public int Id { get; set; }

        [ClickHouseNotMapped]
        public string Name { get; set; }
    }

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

    [Test]
    public void RegisterBinaryInsertType_WithEmptyColumnName_ShouldThrow()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterBinaryInsertType<PocoWithEmptyColumnName>());

        Assert.That(ex.Message, Does.Contain("empty or whitespace"));
    }

    [Test]
    public void RegisterBinaryInsertType_WithWhitespaceColumnName_ShouldThrow()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterBinaryInsertType<PocoWithWhitespaceColumnName>());

        Assert.That(ex.Message, Does.Contain("empty or whitespace"));
    }

    [Test]
    public void RegisterBinaryInsertType_WithEmptyTypeName_ShouldThrow()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterBinaryInsertType<PocoWithEmptyTypeName>());

        Assert.That(ex.Message, Does.Contain("empty or whitespace"));
        Assert.That(ex.Message, Does.Contain("ClickHouse type"));
    }

    [Test]
    public void RegisterBinaryInsertType_WithWhitespaceTypeName_ShouldThrow()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterBinaryInsertType<PocoWithWhitespaceTypeName>());

        Assert.That(ex.Message, Does.Contain("empty or whitespace"));
        Assert.That(ex.Message, Does.Contain("ClickHouse type"));
    }

    [Test]
    public void RegisterBinaryInsertType_WithDuplicateColumnNames_ShouldThrow()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterBinaryInsertType<PocoWithDuplicateColumnNames>());

        Assert.That(ex.Message, Does.Contain("shared_col"));
    }


    [Test]
    public void RegisterBinaryInsertType_WithNoPublicProperties_ShouldThrow()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterBinaryInsertType<EmptyPoco>());

        Assert.That(ex.Message, Does.Contain("no public readable properties"));
    }

    [Test]
    public void RegisterBinaryInsertType_WithAllPropertiesNotMapped_ShouldThrow()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            client.RegisterBinaryInsertType<AllNotMappedPoco>());

        Assert.That(ex.Message, Does.Contain("no public readable properties"));
    }
}
