using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// Compares each read pair of the leaf table with the current read (<see cref="ColumnReadProjections.ReadAs{T}"/>),
/// through <see cref="BoundReader{T}.Fill"/> and through a compiled <see cref="ColumnReader.Emit"/>, over the whole
/// column and over a window that starts after row 0.
/// </summary>
[TestFixture]
public class LeafReaderTests
{
    private static readonly ConverterDerivation Derivation = ConverterDerivation.Default;

    public static IEnumerable<TestCaseData> ReadPairs()
    {
        foreach (string type in LeafSamples.Types)
        {
            IColumnCodec codec = ConverterHarness.Codec(type);
            foreach (Type clrType in LeafTableTests.LeafOf(type).ReadTypes(codec))
            {
                yield return new TestCaseData(type, clrType).SetArgDisplayNames(type, clrType.Name);
            }
        }
    }

    [TestCaseSource(nameof(ReadPairs))]
    public Task Read_LeafPair_GivesTheCurrentValuesThroughFillAndEmit(string type, Type clrType)
        => (Task)ConverterHarness.InvokeGeneric(typeof(LeafReaderTests), nameof(AssertReadsLikeTheCurrentPathAsync), new[] { clrType }, type);

    [TestCaseSource(nameof(ReadPairs))]
    public Task Read_ZeroRows_GivesNoValues(string type, Type clrType)
        => (Task)ConverterHarness.InvokeGeneric(typeof(LeafReaderTests), nameof(AssertReadsNoRowsAsync), new[] { clrType }, type);

    /// <summary>
    /// A value that the CLR type cannot hold fails the same way in all three paths: the same exception and message.
    /// </summary>
    [TestCase("Time", typeof(TimeOnly), new object[] { -1 })]
    [TestCase("Time", typeof(TimeOnly), new object[] { 86_400 })]
    [TestCase("Time64(3)", typeof(TimeOnly), new object[] { -1L })]
    [TestCase("Time64(3)", typeof(TimeOnly), new object[] { 86_400_000L })]
    [TestCase("DateTime64(0, 'UTC')", typeof(DateTimeOffset), new object[] { long.MaxValue })]
    [TestCase("DateTime64(0, 'UTC')", typeof(DateTime), new object[] { long.MinValue })]
    [TestCase("DateTime('Fixed/UTC+00:00:30')", typeof(DateTimeOffset), new object[] { 0u })]
    [TestCase("DateTime64(3, 'Fixed/UTC-00:00:07')", typeof(DateTime), new object[] { 0L })]
    public Task Read_ValueOutOfRangeForTheClrType_FailsAsTheCurrentReadDoes(string type, Type clrType, object[] stored)
    {
        Type storedType = stored[0].GetType();
        Array values = Array.CreateInstance(storedType, stored.Length);
        Array.Copy(stored, values, stored.Length);
        var source = (IColumn)Activator.CreateInstance(typeof(ArrayColumn<>).MakeGenericType(storedType), "c", type, values);
        return (Task)ConverterHarness.InvokeGeneric(typeof(LeafReaderTests), nameof(AssertFailsLikeTheCurrentPathAsync), new[] { clrType }, type, source);
    }

    /// <summary>
    /// A timezone that the platform cannot represent fails only a calendar reading: the raw count still reads, as on
    /// the current path.
    /// </summary>
    [Test]
    public async Task Read_UnrepresentableTimezoneAsTheStoredCount_Succeeds()
    {
        const string type = "DateTime('Fixed/UTC+00:00:30')";
        using IColumn column = await ConverterHarness.DecodeAsync(type, new ArrayColumn<uint>("c", type, new uint[] { 1, 2 }));
        ColumnReader<uint> reader = Derivation.Reader<uint>(type, ConverterHarness.Context);

        Assert.That(ConverterHarness.ReadFill(reader, column, 0, 2), Is.EqualTo(new uint[] { 1, 2 }));
    }

    /// <summary>
    /// A column that a caller built can carry a <c>FixedString</c> type name without the decoded storage. The current
    /// read gives its text through the indexer, and so does the leaf.
    /// </summary>
    [Test]
    public void Read_FixedStringAsTextFromACallerBuiltColumn_ReadsThroughTheIndexer()
    {
        var column = new ArrayColumn<byte[]>("c", "FixedString(2)", new[] { "ab"u8.ToArray(), new byte[] { 0x63, 0 } });
        ColumnReader<string> reader = Derivation.Reader<string>("FixedString(2)", ConverterHarness.Context);
        string[] expected = ConverterHarness.ReadOld<string>(column, 0, 2);

        Assert.Multiple(() =>
        {
            ConverterHarness.AssertSameValues(expected, ConverterHarness.ReadFill(reader, column, 0, 2), "Fill");
            ConverterHarness.AssertSameValues(expected, ConverterHarness.ReadEmit(reader, column, 0, 2), "Emit");
        });
    }

    /// <summary>
    /// A column without the storage that a leaf reads is refused with a message that names the column and the
    /// missing surface, in both paths.
    /// </summary>
    [Test]
    public void Bind_ColumnWithoutTheStringStorage_ThrowsANamedMessage()
    {
        var column = new ArrayColumn<string>("text", "String", new[] { "a" });
        ColumnReader<byte[]> reader = Derivation.Reader<byte[]>("String", ConverterHarness.Context);

        var bind = Assert.Throws<InvalidOperationException>(() => reader.Bind(column));
        var emit = Assert.Throws<InvalidOperationException>(() => ConverterHarness.ReadEmit(reader, column, 0, 1));
        Assert.Multiple(() =>
        {
            Assert.That(bind.Message, Does.Contain("Column 'text' (String)").And.Contain("IStringColumn"));
            Assert.That(emit.Message, Is.EqualTo(bind.Message));
        });
    }

    /// <summary>The message names a generic surface as C# writes it.</summary>
    [Test]
    public void Bind_ColumnWithoutTheStoredValues_NamesTheGenericSurface()
    {
        var column = new ArrayColumn<string>("when", "DateTime", new[] { "a" });
        ColumnReader<DateTimeOffset> reader = Derivation.Reader<DateTimeOffset>("DateTime", ConverterHarness.Context);

        var thrown = Assert.Throws<InvalidOperationException>(() => reader.Bind(column));
        Assert.That(thrown.Message, Does.Contain("Column 'when' (DateTime)").And.Contain("does not expose IColumn<UInt32>,"));
    }

    [Test]
    public async Task Fill_RowsPastTheEnd_Throws()
    {
        using IColumn column = await ConverterHarness.DecodeAsync("Int32", new ArrayColumn<int>("c", "Int32", new[] { 1, 2, 3 }));
        BoundReader<int> bound = Derivation.Reader<int>("Int32", ConverterHarness.Context).Bind(column);

        Assert.Throws<ArgumentOutOfRangeException>(() => bound.Fill(2, new int[2]));
    }

    private static async Task AssertReadsLikeTheCurrentPathAsync<T>(string type)
    {
        using IColumn column = await LeafSamples.DecodedAsync(type, typeof(T));
        ColumnReader<T> reader = Derivation.Reader<T>(type, ConverterHarness.Context);
        int rows = column.RowCount;
        Assert.That(rows, Is.GreaterThanOrEqualTo(3), "a sample needs a window that starts after row 0 and ends before the last row");

        T[] all = ConverterHarness.ReadOld<T>(column, 0, rows);
        T[] window = ConverterHarness.ReadOld<T>(column, 1, rows - 2);
        Assert.Multiple(() =>
        {
            ConverterHarness.AssertSameValues(all, ConverterHarness.ReadFill(reader, column, 0, rows), "Fill");
            ConverterHarness.AssertSameValues(all, ConverterHarness.ReadEmit(reader, column, 0, rows), "Emit");
            ConverterHarness.AssertSameValues(window, ConverterHarness.ReadFill(reader, column, 1, rows - 2), "Fill from row 1");
            ConverterHarness.AssertSameValues(window, ConverterHarness.ReadEmit(reader, column, 1, rows - 2), "Emit from row 1");
        });
    }

    private static async Task AssertReadsNoRowsAsync<T>(string type)
    {
        using IColumn column = type == "Nothing"
            ? await ConverterHarness.DecodeNothingAsync(0)
            : await ConverterHarness.DecodeAsync(type, EmptySource(type));
        ColumnReader<T> reader = Derivation.Reader<T>(type, ConverterHarness.Context);

        Assert.Multiple(() =>
        {
            Assert.That(ConverterHarness.ReadFill(reader, column, 0, 0), Is.Empty);
            Assert.That(ConverterHarness.ReadEmit(reader, column, 0, 0), Is.Empty);
        });
    }

    private static async Task AssertFailsLikeTheCurrentPathAsync<T>(string type, IColumn source)
    {
        using IColumn column = await ConverterHarness.DecodeAsync(type, source);
        ColumnReader<T> reader = Derivation.Reader<T>(type, ConverterHarness.Context);
        Exception expected = ConverterHarness.Catch(() => ConverterHarness.ReadOld<T>(column, 0, column.RowCount));

        Assert.Multiple(() =>
        {
            ConverterHarness.AssertSameFailure(expected, ConverterHarness.Catch(() => ConverterHarness.ReadFill(reader, column, 0, column.RowCount)), "Fill");
            ConverterHarness.AssertSameFailure(expected, ConverterHarness.Catch(() => ConverterHarness.ReadEmit(reader, column, 0, column.RowCount)), "Emit");
        });
    }

    private static IColumn EmptySource(string type)
    {
        Type elementType = ConverterHarness.Codec(type).ElementType;
        return (IColumn)Activator.CreateInstance(typeof(ArrayColumn<>).MakeGenericType(elementType), "c", type, Array.CreateInstance(elementType, 0));
    }
}
