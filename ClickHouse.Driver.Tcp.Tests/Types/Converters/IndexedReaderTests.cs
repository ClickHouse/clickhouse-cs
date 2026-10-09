using System;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// The reader of the types that read only as their canonical type (<see cref="IndexedReader{T}"/>): it reads the
/// column through its indexer, so a read of some rows does not make the values of every row into the column's cache,
/// as <see cref="IColumn{T}.Values"/> does for these columns.
/// </summary>
[TestFixture]
public class IndexedReaderTests
{
    [TestCase("Variant(String, UInt64)")]
    [TestCase("Dynamic")]
    public void Read_ColumnWhoseValuesTheReaderMustNotRead_ReadsEachRowThroughTheIndexer(string type)
    {
        ColumnReader<object> reader = ConverterDerivation.Default.Reader<object>(type, ConverterHarness.Context);
        var column = new IndexedOnlyColumn(type, "a", 7ul, null, "b");

        Assert.Multiple(() =>
        {
            Assert.That(reader, Is.InstanceOf<IndexedReader<object>>());
            Assert.That(ConverterHarness.ReadFill(reader, column, 1, 3), Is.EqualTo(new object[] { 7ul, null, "b" }), "Fill");
            Assert.That(ConverterHarness.ReadEmit(reader, column, 1, 3), Is.EqualTo(new object[] { 7ul, null, "b" }), "Emit");
        });
    }

    [TestCase(-1, 1)]
    [TestCase(3, 2)]
    [TestCase(5, 1)]
    public void Fill_RowsOutsideTheColumn_ThrowsNamingTheRows(int start, int count)
    {
        var column = new IndexedOnlyColumn("Dynamic", "a", "b", "c", "d");

        ArgumentOutOfRangeException error = Assert.Throws<ArgumentOutOfRangeException>(
            () => IndexedReader<object>.Instance.Bind(column).Fill(start, new object[count]));

        Assert.That(error.Message, Does.Contain($"Rows [{start}, {start + count})").And.Contain("4 row(s) of column 'c'"));
    }

    /// <summary>A column of the canonical type <see cref="object"/> whose <see cref="Values"/> throws.</summary>
    private sealed class IndexedOnlyColumn : IColumn<object>
    {
        private readonly object[] values;

        public IndexedOnlyColumn(string typeName, params object[] values)
        {
            TypeName = typeName;
            this.values = values;
        }

        public string Name => "c";

        public string TypeName { get; }

        public int RowCount => values.Length;

        public ReadOnlySpan<object> Values => throw new InvalidOperationException("The reader reads Values, which makes every value of the column.");

        public object this[int row] => values[row];

        public object GetValue(int row) => values[row];

        public void Dispose()
        {
        }
    }
}
