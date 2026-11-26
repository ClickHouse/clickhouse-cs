using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// The read tests of the leaves that the differential tests (<see cref="LeafConverterRegistration"/>) do not run: the
/// pairs that no differential case reaches, compared here with the old read
/// (<see cref="Differential.LegacyColumnarRead"/>) through <see cref="BoundReader{T}.Fill"/> and a compiled
/// <see cref="ColumnReader.Emit"/>; zero rows; columns that a caller built; the surface messages.
/// </summary>
[TestFixture]
public class LeafReaderTests
{
    private static readonly ConverterDerivation Derivation = ConverterDerivation.Default;

    public static IEnumerable<TestCaseData> ReadPairs() => Pairs(onlyNotInTheCaseList: false);

    public static IEnumerable<TestCaseData> ReadPairsNotInTheCaseList() => Pairs(onlyNotInTheCaseList: true);

    [TestCaseSource(nameof(ReadPairsNotInTheCaseList))]
    public Task Read_LeafPairNotInTheCaseList_GivesTheCurrentValuesThroughFillAndEmit(string type, Type clrType)
        => (Task)ConverterHarness.InvokeGeneric(typeof(LeafReaderTests), nameof(AssertReadsLikeTheCurrentPathAsync), new[] { clrType }, type);

    [TestCaseSource(nameof(ReadPairs))]
    public Task Read_ZeroRows_GivesNoValues(string type, Type clrType)
        => (Task)ConverterHarness.InvokeGeneric(typeof(LeafReaderTests), nameof(AssertReadsNoRowsAsync), new[] { clrType }, type);

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
    /// A null row of a <c>FixedString</c> column that a caller built has no text. The text read throws
    /// <see cref="ArgumentNullException"/> with the column and the row, through <c>Fill</c> from row 0, <c>Fill</c>
    /// from a later row, and a compiled <c>Emit</c>.
    /// </summary>
    [TestCase("Fill", 0, 4)]
    [TestCase("Fill", 2, 2)]
    [TestCase("Emit", 0, 4)]
    public void Read_FixedStringAsTextFromACallerBuiltColumnWithANullRow_ThrowsArgumentNullException(string path, int start, int count)
    {
        ColumnReader<string> reader = Derivation.Reader<string>("FixedString(3)", ConverterHarness.Context);

        var thrown = Assert.Throws<ArgumentNullException>(() => ReadPath(path, reader, CallerBuiltWithANullRow(), start, count));
        Assert.That(thrown.Message, Does.Contain("column 'c'").And.Contain("row 2"));
    }

    /// <summary>
    /// A null row of a <c>FixedString</c> column that a caller built reads as a null array, through <c>Fill</c> from
    /// row 0, <c>Fill</c> from a later row, and a compiled <c>Emit</c>.
    /// </summary>
    [TestCase("Fill", 0, 4)]
    [TestCase("Fill", 2, 2)]
    [TestCase("Emit", 0, 4)]
    public void Read_FixedStringAsBytesFromACallerBuiltColumnWithANullRow_GivesNull(string path, int start, int count)
    {
        ColumnReader<byte[]> reader = Derivation.Reader<byte[]>("FixedString(3)", ConverterHarness.Context);

        byte[][] values = ReadPath(path, reader, CallerBuiltWithANullRow(), start, count);

        Assert.Multiple(() =>
        {
            Assert.That(values[2 - start], Is.Null);
            Assert.That(values[3 - start], Is.EqualTo("abc"u8.ToArray()));
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

    private static IEnumerable<TestCaseData> Pairs(bool onlyNotInTheCaseList)
    {
        foreach (string type in LeafSamples.Types)
        {
            IColumnCodec codec = ConverterHarness.Codec(type);
            Leaf leaf = LeafTableTests.LeafOf(type);
            foreach (Type clrType in leaf.ReadTypes(codec))
            {
                if (!onlyNotInTheCaseList || LeafConverterRegistrationTests.NotInTheCaseList.Contains(new LeafPairKey(leaf.Name, clrType, ConversionDirection.Read)))
                {
                    yield return new TestCaseData(type, clrType).SetArgDisplayNames(type, clrType.Name);
                }
            }
        }
    }

    private static ArrayColumn<byte[]> CallerBuiltWithANullRow()
    {
        byte[] value = "abc"u8.ToArray();
        return new ArrayColumn<byte[]>("c", "FixedString(3)", new[] { value, value, null, value });
    }

    private static T[] ReadPath<T>(string path, ColumnReader<T> reader, IColumn column, int start, int count) => path == "Fill"
        ? ConverterHarness.ReadFill(reader, column, start, count)
        : ConverterHarness.ReadEmit(reader, column, start, count);

    private static IColumn EmptySource(string type)
    {
        Type elementType = ConverterHarness.Codec(type).ElementType;
        return (IColumn)Activator.CreateInstance(typeof(ArrayColumn<>).MakeGenericType(elementType), "c", type, Array.CreateInstance(elementType, 0));
    }
}
