using System;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types.Converters;

/// <summary>
/// The converted entries that a LowCardinality reader keeps for each column (<see cref="DictionaryEntryCache{TEntry}"/>):
/// the windows of a POCO read run the setup of <see cref="ColumnReader.Emit"/> for each window, and bind the readers
/// again, and still convert the dictionary of a column once.
/// </summary>
[TestFixture]
public class DictionaryEntryCacheTests
{
    private static readonly string[] Text = { "x", "y", "x", "z", "y" };

    [Test]
    public async Task Emit_SetupForEachWindowOfOneColumn_ConvertsTheDictionaryOnce()
    {
        using IColumn column = await LowCardinalityAsync();
        var leaf = new CountingReader<string>(ConverterDerivation.Default.Reader<string>("String", ConverterHarness.Context));
        var reader = new DictionaryReader<string>(leaf, DictionaryOrder.FirstEntry);

        string[] values = ReadInWindows(column, (start, count) => ConverterHarness.ReadEmit(reader, column, start, count));

        Assert.Multiple(() =>
        {
            Assert.That(values, Is.EqualTo(Text));
            Assert.That(leaf.Fills, Is.EqualTo(1), "the bulk conversion of the dictionary, for all the windows");
        });
    }

    [Test]
    public async Task Fill_BindForEachWindowOfOneColumn_ConvertsTheDictionaryOnce()
    {
        using IColumn column = await LowCardinalityAsync();
        var leaf = new CountingReader<string>(ConverterDerivation.Default.Reader<string>("String", ConverterHarness.Context));
        var reader = new DictionaryReader<string>(leaf, DictionaryOrder.FirstEntry);

        string[] values = ReadInWindows(column, (start, count) => ConverterHarness.ReadFill(reader, column, start, count));

        Assert.Multiple(() =>
        {
            Assert.That(values, Is.EqualTo(Text));
            Assert.That(leaf.Fills, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Emit_ArrayOfLowCardinalityInWindows_ConvertsTheDictionaryOnce()
    {
        // The setup of an Array reader binds its element reader again for each window.
        const string type = "Array(LowCardinality(String))";
        using IColumn column = await ConverterHarness.DecodeAsync(
            type,
            new ArrayColumn<string[]>("c", type, new[] { new[] { "x", "y" }, Array.Empty<string>(), new[] { "y" }, new[] { "x", "z" }, new[] { "z" } }));
        var leaf = new CountingReader<string>(ConverterDerivation.Default.Reader<string>("String", ConverterHarness.Context));
        var reader = new ArrayReader<string>(new DictionaryReader<string>(leaf, DictionaryOrder.FirstEntry));

        string[][] values = ReadInWindows(column, (start, count) => ConverterHarness.ReadEmit(reader, column, start, count));

        Assert.Multiple(() =>
        {
            Assert.That(values, Is.EqualTo(new[] { new[] { "x", "y" }, Array.Empty<string>(), new[] { "y" }, new[] { "x", "z" }, new[] { "z" } }));
            Assert.That(leaf.Fills, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Emit_LiftingDictionaryInWindows_ConvertsTheDictionaryOnce()
    {
        const string type = "LowCardinality(Nullable(Int32))";
        using IColumn column = await ConverterHarness.DecodeAsync(type, new ArrayColumn<int?>("c", type, new int?[] { 1, null, 2, 1, null }));
        var leaf = new CountingReader<int>(ConverterDerivation.Default.Reader<int>("Int32", ConverterHarness.Context));
        var reader = new LiftingDictionaryReader<int>(leaf, DictionaryOrder.FirstEntry);

        int?[] values = ReadInWindows(column, (start, count) => ConverterHarness.ReadEmit(reader, column, start, count));

        Assert.Multiple(() =>
        {
            Assert.That(values, Is.EqualTo(new int?[] { 1, null, 2, 1, null }));
            Assert.That(leaf.Fills, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task For_TwoColumns_ConvertsEachColumnOnce()
    {
        using IColumn first = await LowCardinalityAsync();
        using IColumn second = await ConverterHarness.DecodeAsync("LowCardinality(String)", new ArrayColumn<string>("c", "LowCardinality(String)", new[] { "q" }));
        var leaf = new CountingReader<string>(ConverterDerivation.Default.Reader<string>("String", ConverterHarness.Context));
        var reader = new DictionaryReader<string>(leaf, DictionaryOrder.FirstEntry);

        string[] firstValues = ConverterHarness.ReadEmit(reader, first, 0, first.RowCount);
        string[] secondValues = ConverterHarness.ReadEmit(reader, second, 0, second.RowCount);
        ConverterHarness.ReadEmit(reader, first, 0, first.RowCount);

        Assert.Multiple(() =>
        {
            Assert.That(firstValues, Is.EqualTo(Text));
            Assert.That(secondValues, Is.EqualTo(new[] { "q" }));
            Assert.That(leaf.Fills, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task For_ManyThreadsAtOnce_GetOneSetOfEntries()
    {
        using IColumn column = await LowCardinalityAsync();
        var lowCardinality = (ILowCardinalityColumn)column;
        ColumnReader<string> leaf = ConverterDerivation.Default.Reader<string>("String", ConverterHarness.Context);
        var cache = new DictionaryEntryCache<string>(c => DictionaryEntries<string>.Convert(leaf, c, DictionaryOrder.FirstEntry, static values => values));
        const int threads = 8;
        using var start = new Barrier(threads);

        DictionaryEntries<string>[] entries = await Task.WhenAll(Enumerable.Range(0, threads).Select(_ => Task.Run(() =>
        {
            start.SignalAndWait();
            return cache.For(lowCardinality);
        })));

        Assert.That(entries.Distinct().Count(), Is.EqualTo(1));
    }

    [Test]
    public void For_ColumnThatNothingElseHolds_IsNotKeptAlive()
    {
        ColumnReader<string> leaf = ConverterDerivation.Default.Reader<string>("String", ConverterHarness.Context);
        var cache = new DictionaryEntryCache<string>(c => DictionaryEntries<string>.Convert(leaf, c, DictionaryOrder.FirstEntry, static values => values));

        WeakReference column = ConvertOneColumn(cache);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Assert.That(column.IsAlive, Is.False);
    }

    // Reads all the rows in windows of two rows, as a POCO read reads a block in windows.
    private static T[] ReadInWindows<T>(IColumn column, Func<int, int, T[]> read)
    {
        var values = new T[column.RowCount];
        for (int start = 0; start < column.RowCount; start += 2)
        {
            int count = Math.Min(2, column.RowCount - start);
            read(start, count).CopyTo(values, start);
        }

        return values;
    }

    private static Task<IColumn> LowCardinalityAsync()
        => ConverterHarness.DecodeAsync("LowCardinality(String)", new ArrayColumn<string>("c", "LowCardinality(String)", Text));

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference ConvertOneColumn(DictionaryEntryCache<string> cache)
    {
        IColumn column = ConverterHarness.DecodeAsync("LowCardinality(String)", new ArrayColumn<string>("c", "LowCardinality(String)", Text)).GetAwaiter().GetResult();
        cache.For((ILowCardinalityColumn)column);
        return new WeakReference(column);
    }

    /// <summary>A reader that counts the bulk reads of the columns that it is bound to. It has no expression.</summary>
    internal sealed class CountingReader<T> : ColumnReader<T>
    {
        private readonly ColumnReader<T> inner;
        private int fills;

        public CountingReader(ColumnReader<T> inner) => this.inner = inner;

        public int Fills => Volatile.Read(ref fills);

        public override BoundReader<T> Bind(IColumn column) => new Bound(inner.Bind(column), this);

        public override Expression Emit(Expression column, ParameterExpression row, EmitScope scope)
            => throw new NotSupportedException("The dictionary reader converts its entries with the bulk read of the child.");

        private sealed class Bound : BoundReader<T>
        {
            private readonly BoundReader<T> inner;
            private readonly CountingReader<T> owner;

            public Bound(BoundReader<T> inner, CountingReader<T> owner)
            {
                this.inner = inner;
                this.owner = owner;
            }

            public override void Fill(int start, Span<T> destination)
            {
                Interlocked.Increment(ref owner.fills);
                inner.Fill(start, destination);
            }
        }
    }
}
