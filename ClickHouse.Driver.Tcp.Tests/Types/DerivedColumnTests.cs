using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Tests.Types.Converters;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Converters;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// The view of <see cref="Block.ReadAs{T}(string)"/> (decision D1): it converts the whole column once, on the first
/// access, into a pooled array that the block gives back when it is disposed; one view for each column and type of a
/// block; safe when threads access it for the first time together.
/// </summary>
[TestFixture]
public class DerivedColumnTests
{
    private const string NullFailure =
        "Column 'value' (Nullable(Int32)) has NULL at row 1, and the target type System.Int32 cannot hold NULL. " +
        "Read the column as a nullable type, or remove the NULL values in the query.";

    [Test]
    public void Values_FirstAccess_FillsTheWholeColumnOnceIntoOneRentedArray()
    {
        var pool = new CountingPool<int>();
        var reader = new CountingReader<int>(row => row * 10);
        var view = new DerivedColumn<int>(Source(4), reader, pool);

        int[] values = view.Values.ToArray();
        int third = view[2];
        int length = view.Values.Length;

        Assert.Multiple(() =>
        {
            Assert.That(values, Is.EqualTo(new[] { 0, 10, 20, 30 }));
            Assert.That(third, Is.EqualTo(20));
            Assert.That(length, Is.EqualTo(4), "The view gives the rows of the column, not the length of the rented array.");
            Assert.That(reader.Fills, Is.EqualTo(new[] { (0, 4) }));
            Assert.That(pool.Rented, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task Values_ManyThreadsAtTheFirstAccess_FillOnce()
    {
        var pool = new CountingPool<int>();
        var reader = new CountingReader<int>(row => row, delay: TimeSpan.FromMilliseconds(50));
        var view = new DerivedColumn<int>(Source(1_000), reader, pool);
        using var start = new Barrier(8);

        int[][] results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            start.SignalAndWait();
            return view.Values.ToArray();
        })));

        Assert.Multiple(() =>
        {
            Assert.That(reader.Fills, Has.Count.EqualTo(1));
            Assert.That(pool.Rented, Has.Count.EqualTo(1));
            Assert.That(results, Has.All.EqualTo(Enumerable.Range(0, 1_000)));
        });
    }

    [Test]
    public void Release_AfterTheFirstAccess_ReturnsTheArrayAndALaterAccessThrows()
    {
        var pool = new CountingPool<int>();
        var view = new DerivedColumn<int>(Source(3), new CountingReader<int>(row => row), pool);
        _ = view.Values;

        view.Release();

        Assert.Multiple(() =>
        {
            Assert.That(pool.Returned, Is.EqualTo(pool.Rented));
            Assert.That(() => view.Values.Length, Throws.TypeOf<ObjectDisposedException>().With.Message.Contains("Column 'c' was read as System.Int32 from a block that is disposed."));
            Assert.That(() => view[0], Throws.TypeOf<ObjectDisposedException>());
        });
    }

    [Test]
    public void Release_BeforeTheFirstAccess_RentsNothingAndALaterAccessThrows()
    {
        var pool = new CountingPool<int>();
        var reader = new CountingReader<int>(row => row);
        var view = new DerivedColumn<int>(Source(3), reader, pool);

        view.Release();

        Assert.Multiple(() =>
        {
            Assert.That(() => view.Values.Length, Throws.TypeOf<ObjectDisposedException>());
            Assert.That(pool.Rented, Is.Empty);
            Assert.That(reader.Fills, Is.Empty);
        });
    }

    [Test]
    public void Values_ReaderFails_ReturnsTheArrayAndFailsAgainOnTheNextAccess()
    {
        var pool = new CountingPool<int>();
        var reader = new CountingReader<int>(row => row == 1 ? throw new InvalidOperationException("row 1 fails") : row);
        var view = new DerivedColumn<int>(Source(3), reader, pool);

        Exception first = ConverterHarness.Catch(() => _ = view.Values.Length);
        Exception second = ConverterHarness.Catch(() => _ = view[0]);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.TypeOf<InvalidOperationException>().With.Message.EqualTo("row 1 fails"));
            Assert.That(second, Is.TypeOf<InvalidOperationException>().With.Message.EqualTo("row 1 fails"));
            Assert.That(reader.Fills, Has.Count.EqualTo(2));
            Assert.That(pool.Returned, Is.EqualTo(pool.Rented));
        });
    }

    [Test]
    public void Values_ReaderFindsANull_ThrowsTheMessageThatNamesTheColumnTheTypeAndTheRow()
    {
        var pool = new CountingPool<int>();
        var view = new DerivedColumn<int>(
            new ArrayColumn<int?>("value", "Nullable(Int32)", new int?[] { 1, null, 3 }),
            new CountingReader<int>(row => row == 1 ? throw NullValueException.At(1) : row),
            pool);

        Exception thrown = ConverterHarness.Catch(() => _ = view.Values.Length);

        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.TypeOf<InvalidOperationException>().With.Message.EqualTo(NullFailure));
            Assert.That(thrown?.InnerException, Is.TypeOf<NullValueException>());
            Assert.That(pool.Returned, Is.EqualTo(pool.Rented));
        });
    }

    [Test]
    public void Indexer_RowOutsideTheColumn_ThrowsIndexOutOfRange()
    {
        var view = new DerivedColumn<int>(Source(2), new CountingReader<int>(row => row), new CountingPool<int>());

        Assert.Multiple(() =>
        {
            Assert.That(() => view[2], Throws.TypeOf<IndexOutOfRangeException>());
            Assert.That(() => view[-1], Throws.TypeOf<IndexOutOfRangeException>());
        });
    }

    [Test]
    public void Values_ZeroRows_RentsNothing()
    {
        var pool = new CountingPool<int>();
        var view = new DerivedColumn<int>(Source(0), new CountingReader<int>(row => row), pool);

        Assert.Multiple(() =>
        {
            Assert.That(view.Values.Length, Is.Zero);
            Assert.That(pool.Rented, Is.Empty);
        });
    }

    [Test]
    public void ReadAs_SameColumnAndType_GivesTheSameView()
    {
        using Block block = ReadRulesTests.DecodeSample("DateTime('UTC')");

        IColumn<DateTimeOffset> byIndex = block.ReadAs<DateTimeOffset>(0);
        IColumn<DateTimeOffset> byName = block.ReadAs<DateTimeOffset>("value");
        IColumn<DateTime> other = block.ReadAs<DateTime>(0);
        IColumn<uint> canonical = block.ReadAs<uint>(0);

        Assert.Multiple(() =>
        {
            Assert.That(byName, Is.SameAs(byIndex));
            Assert.That(byIndex, Is.InstanceOf<DerivedColumn<DateTimeOffset>>());
            Assert.That(other, Is.Not.SameAs(byIndex));
            Assert.That(canonical, Is.SameAs(block[0]), "A column that already reads as the type is the column itself.");
        });
    }

    [Test]
    public async Task ReadAs_ManyThreadsOnOneBlock_GiveOneView()
    {
        using Block block = ReadRulesTests.DecodeSample("Nullable(DateTime('UTC'))");
        using var start = new Barrier(8);

        IColumn<DateTimeOffset?>[] views = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            start.SignalAndWait();
            IColumn<DateTimeOffset?> view = block.ReadAs<DateTimeOffset?>(0);
            _ = view.Values.Length;
            return view;
        })));

        Assert.That(views, Has.All.SameAs(views[0]));
    }

    [Test]
    public void Dispose_BlockWithViews_ReleasesEveryView()
    {
        Block block = ReadRulesTests.DecodeSample("DateTime('UTC')");
        IColumn<DateTimeOffset> offsets = block.ReadAs<DateTimeOffset>(0);
        IColumn<DateTime> instants = block.ReadAs<DateTime>(0);
        _ = offsets.Values.Length;
        _ = instants.Values.Length;

        block.Dispose();
        block.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(() => offsets.Values.Length, Throws.TypeOf<ObjectDisposedException>());
            Assert.That(() => instants[0], Throws.TypeOf<ObjectDisposedException>());
        });
    }

    [Test]
    public void ReadAs_AfterTheBlockIsDisposed_GivesAViewThatThrowsOnAccess()
    {
        Block block = ReadRulesTests.DecodeSample("DateTime('UTC')");
        block.Dispose();

        IColumn<DateTimeOffset> view = block.ReadAs<DateTimeOffset>(0);

        Assert.That(() => view.Values.Length, Throws.TypeOf<ObjectDisposedException>());
    }

    [Test]
    public void ReadAs_NullableColumnAsAValueType_FailsAtTheFirstNullOnEveryAccess()
    {
        using Block block = ReadRulesTests.DecodeSample("Nullable(Int32)");

        IColumn<int> view = block.ReadAs<int>(0);

        Assert.Multiple(() =>
        {
            Assert.That(() => view.Values.Length, Throws.InvalidOperationException.With.Message.EqualTo(NullFailure));
            Assert.That(() => view[0], Throws.InvalidOperationException.With.Message.EqualTo(NullFailure), "The first access converts the whole column.");
        });
    }

    [Test]
    public void ReadAs_EnumObjectAndArrayCastTargets_ReadAsPocoMappingReads()
    {
        using Block integers = ReadRulesTests.DecodeSample("Int8");
        using Block text = ReadRulesTests.DecodeSample("String");

        Assert.Multiple(() =>
        {
            Assert.That(integers.ReadAs<ReadRulesTests.SByteEnum>(0).Values.ToArray(), Is.EqualTo(new sbyte[] { -128, -1, 0, 127, 5 }.Select(v => (ReadRulesTests.SByteEnum)v)));
            Assert.That(text.ReadAs<object>(0).Values.ToArray(), Is.EqualTo(new object[] { string.Empty, "a", "héllo✓", "a\0b", "zz" }));
            Assert.That(integers.ReadAs<sbyte?>(0)[3], Is.EqualTo((sbyte)127));
        });
    }

    [TestCase("Int8", typeof(ReadRulesTests.SByteEnum), true)]
    [TestCase("Nullable(Int8)", typeof(ReadRulesTests.SByteEnum), true)]
    [TestCase("String", typeof(object), true)]
    [TestCase("Nullable(Int32)", typeof(int), true)]
    [TestCase("UInt64", typeof(ulong?), true)]
    [TestCase("Array(UInt32)", typeof(int[]), true)]
    [TestCase("String", typeof(int), false)]
    [TestCase("Tuple(String, UInt8)", typeof((byte[], byte)?), false)]
    public void CanRead_ReadingOfTheReadRules_IsWhatReadAsDoes(string type, Type target, bool reads)
        => Assert.That(ClickHouseTcpTypes.CanRead(type, target), Is.EqualTo(reads));

    private static IColumn Source(int rows) => new ArrayColumn<int>("c", "Int32", new int[rows]);

    /// <summary>A pool that records each array that it rents and each array that comes back.</summary>
    private sealed class CountingPool<T> : ArrayPool<T>
    {
        private readonly object gate = new();

        public List<T[]> Rented { get; } = new();

        public List<T[]> Returned { get; } = new();

        // Longer than asked, as a pool can give.
        public override T[] Rent(int minimumLength)
        {
            var array = new T[minimumLength + 3];
            lock (gate)
            {
                Rented.Add(array);
            }

            return array;
        }

        public override void Return(T[] array, bool clearArray = false)
        {
            lock (gate)
            {
                Returned.Add(array);
            }
        }
    }

    /// <summary>A bound reader that records each bulk read, and gives the value of a function of the row.</summary>
    private sealed class CountingReader<T> : BoundReader<T>
    {
        private readonly Func<int, T> value;
        private readonly TimeSpan delay;
        private readonly object gate = new();

        public CountingReader(Func<int, T> value, TimeSpan delay = default)
        {
            this.value = value;
            this.delay = delay;
        }

        public List<(int Start, int Length)> Fills { get; } = new();

        public override void Fill(int start, Span<T> destination)
        {
            lock (gate)
            {
                Fills.Add((start, destination.Length));
            }

            if (delay > TimeSpan.Zero)
            {
                Thread.Sleep(delay);
            }

            for (int i = 0; i < destination.Length; i++)
            {
                destination[i] = value(start + i);
            }
        }
    }
}
