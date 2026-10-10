using System;
using System.Threading;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Tests.Differential;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// The exact texts of the refusals that suggest CLR types (<see cref="ConverterDerivation.SuggestedTypes"/>): the
/// columnar read, the POCO read, the POCO write, the untyped write and the columnar insert.
/// </summary>
[TestFixture]
public class RefusalMessageTests
{
    private static readonly ResolveContext Context = DifferentialEngine.Context;

    [TestCase("UInt32", "Column 'value' has type 'UInt32', whose values cannot be read as System.Int64. It reads as: System.UInt32.")]
    [TestCase("FixedString(4)", "Column 'value' has type 'FixedString(4)', whose values cannot be read as System.Int64. It reads as: System.Byte[], System.String.")]
    public void ReadAs_TypeWithNoSuchReading_NamesTheSuggestedTypes(string type, string expected)
    {
        using Block block = ReadRulesTests.DecodeSample(type);

        var thrown = Assert.Throws<InvalidCastException>(() => block.ReadAs<long>(0));

        Assert.That(thrown.Message, Is.EqualTo(expected));
    }

    [Test]
    public void PocoRead_PropertyTheColumnCannotBeReadAs_NamesTheSuggestedTypes()
    {
        using Block block = ReadRulesTests.DecodeSample("LowCardinality(Nullable(DateTime('UTC')))");

        var thrown = Assert.Throws<InvalidOperationException>(() => PocoReadPlan<Row<Guid>>.Build(PocoTypeDescriptor<Row<Guid>>.Build(), block, forcedTier: null));

        Assert.That(thrown.Message, Is.EqualTo(
            "Column 'value' (LowCardinality(Nullable(DateTime('UTC')))) maps to property 'Row`1.Value' of type System.Guid, which it cannot be read as. " +
            "It reads as System.Nullable`1[System.UInt32] or System.Nullable`1[System.DateTimeOffset] or System.Nullable`1[System.DateTime]. " +
            "Give the property one of those types, exclude it with [ClickHouseTcpNotMapped], or read the column through the block-level API."));
    }

    [TestCase(
        "FixedString(4)",
        "Column 'value' (FixedString(4)) is filled from property 'Row`1.Value' of type System.Int32, which it cannot be written from. " +
        "It accepts System.Byte[] or System.String, and an Array, Map or Tuple type also accepts rows of the types that its element types accept. " +
        "Give the property one of those types, or insert that column through the columnar API.")]
    [TestCase(
        "LowCardinality(String)",
        "Column 'value' (LowCardinality(String)) is filled from property 'Row`1.Value' of type System.Int32, which it cannot be written from. " +
        "It accepts System.String or System.Byte[], and an Array, Map or Tuple type also accepts rows of the types that its element types accept. " +
        "Give the property one of those types, or insert that column through the columnar API.")]
    [TestCase(
        "Nested(a Int32)",
        "Column 'value' (Nested(a Int32)) is filled from property 'Row`1.Value' of type System.Int32, which it cannot be written from. " +
        "No property type can fill a 'Nested(a Int32)' column: insert it through the columnar API, which can build the column shape it needs.")]
    public void PocoWrite_PropertyTheColumnCannotBeWrittenFrom_NamesTheSuggestedTypes(string type, string expected)
    {
        var thrown = Assert.Throws<InvalidOperationException>(() => PocoWritePlan<Row<int>>.Build(PocoTypeDescriptor<Row<int>>.Build(), RowWriteArms.Schema(type, Context)));

        Assert.That(thrown.Message, Is.EqualTo(expected));
    }

    [Test]
    public void UntypedWrite_ValuesTheColumnCannotBeWrittenFrom_NamesTheSuggestedTypes()
    {
        object[][] rows = { new object[] { new[] { Guid.Empty } } };
        using var buffer = PocoRowBuffer<object[]>.Create(rows, "rows", rows.Length, CancellationToken.None);

        var thrown = Assert.Throws<InvalidOperationException>(() => UntypedRowColumns.CreateSource(RowWriteArms.Schema("Array(DateTime('UTC'))", Context), buffer, rows.Length));

        Assert.That(thrown.Message, Is.EqualTo(
            "Column 0 ('value', Array(DateTime('UTC'))) was given values of type System.Guid[], which it cannot be written from. " +
            "It accepts System.UInt32[], and an Array, Map or Tuple type also accepts values of the types that its element types accept."));
    }

    [TestCase(
        "FixedString(4)",
        "Column 'value' (FixedString(4)) was given a column of element type System.Int32, which it cannot be written from. " +
        "It accepts System.Byte[] or System.String, and an Array, Map or Tuple type also accepts a column whose elements are of the types that its element types accept.")]
    [TestCase(
        "Nested(a Int32)",
        "Column 'value' (Nested(a Int32)) was given a column of element type System.Int32, which it cannot be written from. " +
        "No column built from a CLR element type can fill a 'Nested(a Int32)' column; re-insert one read back from a query of the same type.")]
    public void InsertAsync_ColumnTheTypeCannotBeWrittenFrom_NamesTheSuggestedTypes(string type, string expected)
    {
        using var values = new ArrayColumn<int>("value", type, new[] { 1 });
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(type, Context);
        Assert.That(InsertColumnWrite.For(codec, values, type, Context, ConverterDerivation.Default), Is.Null, "the insert plan refuses the column");

        string message = ClickHouseTcpConnection.DescribeUnwritableColumn(
            new InsertColumn("value", type, codec, values),
            ConverterDerivation.Default.SuggestedTypes(type, Context, ConversionDirection.Write));

        Assert.That(message, Is.EqualTo(expected));
    }
}
