using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// The array casts of the read and write rules against a server. An enum array reads from, and is written as, an array
/// of its underlying type, because each element keeps its integer value. An array cast between integer types of the
/// other sign gives each element another meaning, so every tier refuses it before it reads or writes a value, and the
/// refusal names the CLR type that the column reads as or is written from.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ArrayElementCastIntegrationTests
{
    private const string EnumArray = "SELECT CAST(if(number = 0, ['a', 'b', 'a'], []), 'Array(Enum8(\\'a\\' = 1, \\'b\\' = 2))') AS value FROM system.numbers LIMIT 2";

    private const string UInt32Array = "SELECT CAST([3000000000, 7], 'Array(UInt32)') AS value";

    private static readonly CancellationToken None = CancellationToken.None;

    internal enum Level : sbyte
    {
        A = 1,
        B = 2,
    }

    /// <summary>
    /// <c>Array(Enum8(...))</c> reads as an array of an enum whose underlying type is <see cref="sbyte"/>, through POCO
    /// mapping and through <c>ReadAs</c>: the column reads as <see cref="T:sbyte[]"/>, and the cast keeps each ordinal.
    /// </summary>
    [Test]
    public async Task QueryAsync_ArrayOfEnum8IntoAnEnumArray_ReadsTheOrdinalsInEveryTier()
    {
        await using var client = TcpServerFixture.CreateClient();

        List<LevelsRow> rows = await client.QueryAsync<LevelsRow>(EnumArray, cancellationToken: None).ToListAsync();
        var readAs = new List<Level[]>();
        await foreach (Block block in client.StreamAsync(EnumArray, cancellationToken: None))
        {
            IColumn<Level[]> column = block.ReadAs<Level[]>(0);
            for (int row = 0; row < column.RowCount; row++)
            {
                readAs.Add(column[row]);
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(rows.Select(row => Ordinals(row.Value)), Is.EqualTo(new[] { new sbyte[] { 1, 2, 1 }, Array.Empty<sbyte>() }));
            Assert.That(rows[0].Value[1] == Level.B, Is.True);
            Assert.That(readAs.Select(Ordinals), Is.EqualTo(new[] { new sbyte[] { 1, 2, 1 }, Array.Empty<sbyte>() }));
            Assert.That(ClickHouseTcpTypes.CanRead("Array(Enum8('a' = 1, 'b' = 2))", typeof(Level[])), Is.True);
        });
    }

    /// <summary>
    /// <c>Array(UInt32)</c> does not read as <see cref="T:int[]"/>: the cast would read 3000000000 as -1294967296. POCO
    /// mapping and <c>ReadAs</c> refuse it before they read a row, and both messages name <see cref="T:uint[]"/>.
    /// <c>CanRead</c> says false.
    /// </summary>
    [Test]
    public async Task QueryAsync_ArrayOfUInt32AsAnInt32Array_IsRefusedInEveryTier()
    {
        await using var client = TcpServerFixture.CreateClient();

        Exception poco = Assert.CatchAsync(async () => await client.QueryAsync<CountsRow>(UInt32Array, cancellationToken: None).ToListAsync());
        Exception readAs = null;
        await foreach (Block block in client.StreamAsync(UInt32Array, cancellationToken: None))
        {
            readAs = Assert.Catch(() => block.ReadAs<int[]>(0));
        }

        Assert.Multiple(() =>
        {
            Assert.That(poco, Is.TypeOf<InvalidOperationException>());
            Assert.That(
                poco.Message,
                Is.EqualTo(
                    "Column 'value' (Array(UInt32)) maps to property 'CountsRow.Value' of type System.Int32[], which it cannot be read as. It reads as System.UInt32[]. " +
                    "Give the property one of those types, exclude it with [ClickHouseTcpNotMapped], or read the column through the block-level API."));
            Assert.That(readAs, Is.TypeOf<InvalidCastException>());
            Assert.That(readAs.Message, Is.EqualTo("Column 'value' has type 'Array(UInt32)', whose values cannot be read as System.Int32[]. It reads as: System.UInt32[]."));
            Assert.That(ClickHouseTcpTypes.CanRead("Array(UInt32)", typeof(int[])), Is.False);
        });
    }

    /// <summary>
    /// A <see cref="T:uint[]"/> is not written into <c>Array(Int32)</c>: the cast would store 3000000000 as -1294967296.
    /// The columnar insert (also of an array that <c>CreateArray</c> builds over a column of <see cref="uint"/>), the POCO
    /// insert and the untyped insert refuse it before they send a block, each message names <see cref="T:int[]"/>, and the
    /// table stays empty. <c>CanWrite</c> says false.
    /// </summary>
    [Test]
    public async Task InsertAsync_UInt32ArrayIntoArrayOfInt32_IsRefusedInEveryTier()
    {
        await using var client = TcpServerFixture.CreateClient();
        string table = $"tcp_array_cast_test_{Guid.NewGuid():N}";
        try
        {
            await client.ExecuteAsync($"CREATE TABLE {table} (id UInt32, value Array(Int32)) ENGINE = Memory", cancellationToken: None);
            string insert = $"INSERT INTO {table} (id, value) VALUES";
            uint[][] values = { new[] { 3_000_000_000u }, new[] { 7u } };

            Exception columnar = Assert.CatchAsync(async () => await client.InsertAsync(
                insert,
                new IColumn[] { ClickHouseTcpColumn.Create("id", new uint[] { 0, 1 }), ClickHouseTcpColumn.Create("value", values) },
                cancellationToken: None));
            Exception dense = Assert.CatchAsync(async () => await client.InsertAsync(
                insert,
                new IColumn[]
                {
                    ClickHouseTcpColumn.Create("id", new uint[] { 0, 1 }),
                    ClickHouseTcpColumn.CreateArray("value", ClickHouseTcpColumn.Create("value", new[] { 3_000_000_000u, 7u }), new[] { 0, 1, 2 }),
                },
                cancellationToken: None));
            Exception poco = Assert.CatchAsync(async () => await client.InsertRowsAsync(
                insert,
                values.Select((value, i) => new IdCountsRow { Id = (uint)i, Value = value }).ToArray(),
                cancellationToken: None));
            Exception untyped = Assert.CatchAsync(async () => await client.InsertRowsAsync(
                insert,
                values.Select((value, i) => new object[] { (uint)i, value }).ToArray(),
                cancellationToken: None));
            var count = new List<string>();
            await foreach (Block block in client.StreamAsync($"SELECT toString(count()) FROM {table}", cancellationToken: None))
            {
                count.Add((string)block[0].GetValue(0));
            }

            Assert.Multiple(() =>
            {
                Assert.That(columnar, Is.TypeOf<ArgumentException>());
                Assert.That(
                    columnar.Message,
                    Is.EqualTo(
                        "Column 'value' (Array(Int32)) was given a column of element type System.UInt32[], which it cannot be written from. It accepts System.Int32[], " +
                        "and an Array, Map or Tuple type also accepts a column whose elements are of the types that its element types accept. (Parameter 'columns')"));
                Assert.That(dense, Is.TypeOf<ArgumentException>());
                Assert.That(dense.Message, Is.EqualTo(columnar.Message));
                Assert.That(poco, Is.TypeOf<InvalidOperationException>());
                Assert.That(
                    poco.Message,
                    Is.EqualTo(
                        "Column 'value' (Array(Int32)) is filled from property 'IdCountsRow.Value' of type System.UInt32[], which it cannot be written from. It accepts System.Int32[], " +
                        "and an Array, Map or Tuple type also accepts rows of the types that its element types accept. Give the property one of those types, or insert that column through the columnar API."));
                Assert.That(untyped, Is.TypeOf<InvalidOperationException>());
                Assert.That(
                    untyped.Message,
                    Is.EqualTo(
                        "Column 1 ('value', Array(Int32)) was given values of type System.UInt32[], which it cannot be written from. It accepts System.Int32[], " +
                        "and an Array, Map or Tuple type also accepts values of the types that its element types accept."));
                Assert.That(count, Is.EqualTo(new[] { "0" }));
                Assert.That(ClickHouseTcpTypes.CanWrite("Array(Int32)", typeof(uint[])), Is.False);
            });
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {table}", cancellationToken: None);
        }
    }

    // The ordinals of an enum array, which compare as numbers.
    private static sbyte[] Ordinals(Level[] values) => values.Select(value => (sbyte)value).ToArray();

    internal sealed class LevelsRow
    {
        public Level[] Value { get; set; }
    }

    internal sealed class CountsRow
    {
        public int[] Value { get; set; }
    }

    internal sealed class IdCountsRow
    {
        public uint Id { get; set; }

        public uint[] Value { get; set; }
    }
}
