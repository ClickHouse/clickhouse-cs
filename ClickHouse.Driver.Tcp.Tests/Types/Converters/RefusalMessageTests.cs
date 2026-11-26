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

    // A boxed value is never a nullable value type, so the message names the value type that a NULL-holding column takes.
    [Test]
    public void UntypedWrite_ValuesANullableColumnCannotBeWrittenFrom_NamesTheValueType()
    {
        object[][] rows = { new object[] { "a" } };
        using var buffer = PocoRowBuffer<object[]>.Create(rows, "rows", rows.Length, CancellationToken.None);

        var thrown = Assert.Throws<InvalidOperationException>(() => UntypedRowColumns.CreateSource(RowWriteArms.Schema("Nullable(Int32)", Context), buffer, rows.Length));

        Assert.That(thrown.Message, Is.EqualTo(
            "Column 0 ('value', Nullable(Int32)) was given values of type System.String, which it cannot be written from. " +
            "It accepts System.Int32, and an Array, Map or Tuple type also accepts values of the types that its element types accept."));
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
            new InsertColumn("value", type, codec, values, write: null),
            ConverterDerivation.Default.SuggestedTypes(type, Context, ConversionDirection.Write));

        Assert.That(message, Is.EqualTo(expected));
    }

    /// <summary>
    /// A column that a query read as another type, whose part the insert cannot write: the message names the type that the
    /// column was read as, then the reason of the part.
    /// </summary>
    [Test]
    public void InsertAsync_ReadColumnWithAPartThatCannotBeWritten_NamesTheReadTypeAndTheReasonOfThePart()
    {
        const string source = "Tuple(String, Int32)";
        const string target = "Tuple(String, Int64)";
        using Block block = ReadRulesTests.DecodeSample(source);
        IColumn values = block[0];
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(target, Context);
        InsertColumnWrite write = InsertColumnWrite.For(codec, values, target, Context, ConverterDerivation.Default, out string refusal);

        string message = ClickHouseTcpConnection.DescribeUnconvertedColumn(new InsertColumn("value", target, codec, values, write: null), refusal);

        Assert.Multiple(() =>
        {
            Assert.That(write, Is.Null, "the insert plan refuses the column");
            Assert.That(message, Is.EqualTo(
                "Column 'value' (Tuple(String, Int64)) was given a column that a query read as Tuple(String, Int32), and the insert writes such a column part by part. " +
                "'Int64' cannot be written from System.Int32. It is written from: System.Int64."));
        });
    }

    /// <summary>
    /// A column that a query read as a <c>DateTime64</c>, into a <c>Time64</c>: the message names the read type, says that
    /// the values are instants and the target holds durations, and names the two ways to insert the values.
    /// </summary>
    [Test]
    public void InsertAsync_ReadInstantIntoADuration_NamesTheTwoKindsOfCountAndWhatToDo()
    {
        const string source = "Array(DateTime64(3))";
        const string target = "Array(Time64(6))";
        using Block block = ReadRulesTests.DecodeSample(source);
        IColumn values = block[0];
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(target, Context);
        InsertColumnWrite write = InsertColumnWrite.For(codec, values, target, Context, ConverterDerivation.Default, out string refusal);

        string message = ClickHouseTcpConnection.DescribeUnconvertedColumn(new InsertColumn("value", target, codec, values, write: null), refusal);

        Assert.Multiple(() =>
        {
            Assert.That(write, Is.Null, "the insert plan refuses the column");
            Assert.That(message, Is.EqualTo(
                "Column 'value' (Array(Time64(6))) was given a column that a query read as Array(DateTime64(3)), and the insert writes such a column part by part. " +
                "A value of DateTime64(3) is an instant and a value of Time64(6) is a duration, so a column read as DateTime64(3) is not written into Time64(6). " +
                "Convert the values in the query, or insert the counts as a column of System.Int64 values."));
        });
    }

    /// <summary>
    /// A <c>Variant</c> that a query read, into a <c>Variant</c> that does not have one of its alternatives: the message
    /// names the alternative and the cast that converts its values.
    /// </summary>
    [Test]
    public void InsertAsync_ReadVariantIntoAVariantWithoutOneOfItsAlternatives_NamesTheAlternativeAndTheCast()
    {
        const string source = "Variant(Int32, String)";
        const string target = "Variant(Int64, String)";
        using Block block = ReadRulesTests.DecodeSample(source);
        IColumn values = block[0];
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(target, Context);
        InsertColumnWrite write = InsertColumnWrite.For(codec, values, target, Context, ConverterDerivation.Default, out string refusal);

        string message = ClickHouseTcpConnection.DescribeUnconvertedColumn(new InsertColumn("value", target, codec, values, write: null), refusal);

        Assert.Multiple(() =>
        {
            Assert.That(write, Is.Null, "the insert plan refuses the column");
            Assert.That(message, Is.EqualTo(
                "Column 'value' (Variant(Int64, String)) was given a column that a query read as Variant(Int32, String), and the insert writes such a column part by part. " +
                "A column read as Variant(Int32, String) is written into Variant(Int64, String) only when each of its alternatives is an alternative of Variant(Int64, String), " +
                "and Variant(Int64, String) has no alternative Int32. A CAST of the column to Variant(Int64, String) in the query converts those values."));
        });
    }

    /// <summary>
    /// A <c>String</c> that a query read, into a <c>Dynamic</c>: the message names the part, says why its text is not
    /// written, and names the two ways out.
    /// </summary>
    [Test]
    public void InsertAsync_ReadStringIntoADynamic_NamesThePartAndTheWaysOut()
    {
        const string target = "Dynamic";
        using Block block = ReadRulesTests.DecodeSample("String");
        IColumn values = block[0];
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(target, Context);
        InsertColumnWrite write = InsertColumnWrite.For(codec, values, target, Context, ConverterDerivation.Default, out string refusal);

        string message = ClickHouseTcpConnection.DescribeUnconvertedColumn(new InsertColumn("value", target, codec, values, write: null), refusal);

        Assert.Multiple(() =>
        {
            Assert.That(write, Is.Null, "the insert plan refuses the column");
            Assert.That(message, Is.EqualTo(
                "Column 'value' (Dynamic) was given a column that a query read as String, and the insert writes such a column part by part. " +
                "A part read as String is written into Dynamic only through the text of its String values, but the insert writes a String that a query read only with its bytes. " +
                "Convert the values with a CAST to Dynamic in the query, or insert a column of string values that you build from them with ClickHouseTcpColumn.Create."));
        });
    }
}
