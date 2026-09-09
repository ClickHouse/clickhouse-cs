using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Integration;

/// <summary>
/// Covers the public columnar surface of the composite column types against a real server. Every concrete column
/// class is internal, so these interfaces — obtained by pattern-matching an <see cref="IColumn"/> — are the only way
/// a consumer reaches the wire layout underneath a composite. These tests assert the shape a real decoded block
/// presents: that the view is reachable at all, that its spans are sliced to the column's row count rather than to a
/// pooled buffer's length, and that the raw columnar data it exposes agrees with the materialized rows. Where the
/// same data doubles as a zero-copy <em>write</em> source, the invariant deciding whether it may be re-emitted
/// verbatim is pinned here too, alongside the layout rules it depends on.
///
/// <para>
/// A yielded <see cref="Block"/> is borrowed — valid only for its iteration — so every test reads or copies what it
/// needs inside the <c>await foreach</c>, never retaining the block or a span taken from it.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class ColumnarReadSurfaceIntegrationTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    [Test]
    public async Task QueryAsync_NullableColumn_ExposesInnerAndNullMapThroughINullableColumn()
    {
        // A Nullable(T) column's wire layout is a dense inner column (a decoded value at *every* row, placeholder
        // included where the row is null) plus the per-row null-map that says which rows are really null. The
        // materialized IColumn<int?> surface folds those two together and discards the distinction, so reaching the
        // pair is the whole point of INullableColumn<T>.
        //
        // Note the type argument is the *inner* type: a Nullable(Int32) column is an INullableColumn<int>, not an
        // INullableColumn<int?>, even though its IColumn<T> surface is IColumn<int?>.
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        bool matchedInner = false;
        bool matchedNullable = false;
        byte[] nullMap = null;
        int nullMapLength = 0;
        int rowCount = 0;
        int innerRowCount = 0;
        int?[] materialized = null;
        var innerAtNonNullRows = new int[3];

        await foreach (Block block in connection.QueryAsync(
            "SELECT CAST(number = 1 ? NULL : toInt32(number * 10 - 10), 'Nullable(Int32)') FROM system.numbers LIMIT 4",
            cancellationToken: None))
        {
            IColumn column = block[0];
            matchedInner = column is INullableColumn<int>;
            matchedNullable = column is INullableColumn<int?>;

            var nullable = (INullableColumn<int>)column;
            nullMap = nullable.NullMap.ToArray();
            nullMapLength = nullable.NullMap.Length;
            rowCount = nullable.RowCount;
            innerRowCount = nullable.Inner.RowCount;
            materialized = ((IColumn<int?>)column).Values.ToArray();

            // Read the non-null rows straight out of the inner column, skipping the nulls via the map — the
            // allocation-free access pattern the interface exists to enable.
            int next = 0;
            for (int row = 0; row < nullable.RowCount; row++)
            {
                if (nullable.NullMap[row] == 0)
                {
                    innerAtNonNullRows[next++] = nullable.Inner[row];
                }
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(matchedInner, Is.True, "the view is parameterized by the inner type");
            Assert.That(matchedNullable, Is.False, "and not by the nullable type");
            Assert.That(rowCount, Is.EqualTo(4));
            Assert.That(nullMap, Is.EqualTo(new byte[] { 0, 1, 0, 0 }), "one entry per row, non-zero marking null");
            Assert.That(nullMapLength, Is.EqualTo(rowCount), "the map is sliced to the row count, not the pooled buffer length");
            Assert.That(innerRowCount, Is.EqualTo(rowCount), "the inner column is dense: one decoded value per row");
            Assert.That(materialized, Is.EqualTo(new int?[] { -10, null, 10, 20 }));
            Assert.That(innerAtNonNullRows, Is.EqualTo(new[] { -10, 10, 20 }), "the null-marked row is skipped, the rest read from the inner column");
        });
    }

    [Test]
    public async Task QueryAsync_NullableReferenceColumn_ExposesInnerAndNullMapThroughINullableColumn()
    {
        // Nullable(String) decodes to a separate reference-typed column class with its own copy of the pair, whose
        // IColumn<T> surface is IColumn<string> (a reference is already nullable) rather than IColumn<T?>. Its view
        // is still parameterized by the inner type, so the pattern-match spelling stays uniform with the value case.
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        bool matched = false;
        byte[] nullMap = null;
        int nullMapLength = 0;
        int rowCount = 0;
        int innerRowCount = 0;
        string[] materialized = null;

        await foreach (Block block in connection.QueryAsync(
            "SELECT CAST(number = 1 ? NULL : concat('v', toString(number)), 'Nullable(String)') FROM system.numbers LIMIT 3",
            cancellationToken: None))
        {
            IColumn column = block[0];
            matched = column is INullableColumn<string>;

            var nullable = (INullableColumn<string>)column;
            nullMap = nullable.NullMap.ToArray();
            nullMapLength = nullable.NullMap.Length;
            rowCount = nullable.RowCount;
            innerRowCount = nullable.Inner.RowCount;
            materialized = ((IColumn<string>)column).Values.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(matched, Is.True);
            Assert.That(rowCount, Is.EqualTo(3));
            Assert.That(nullMap, Is.EqualTo(new byte[] { 0, 1, 0 }));
            Assert.That(nullMapLength, Is.EqualTo(rowCount));
            Assert.That(innerRowCount, Is.EqualTo(rowCount));
            Assert.That(materialized, Is.EqualTo(new[] { "v0", null, "v2" }));
        });
    }

    [Test]
    public async Task QueryAsync_ArrayColumn_ExposesFlatElementsAndOffsetsThroughIArrayColumn()
    {
        // An Array(T) column's wire layout is every row's elements concatenated into one flat run plus the per-row
        // offsets that delimit them. The materialized IColumn<T[]> surface allocates a fresh array per row;
        // IArrayColumn<TElement> is the allocation-free alternative, so the test walks the rows through the spans
        // and checks they reconstruct what the materialized surface produced.
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        bool matched = false;
        int rowCount = 0;
        int[] offsets = null;
        int[] innerValues = null;
        int innerRowCount = 0;
        var slices = new List<int[]>();
        int[][] materialized = null;

        // Rows: [], [0], [0, 1], [0, 1, 2] — an empty leading row makes a zero-length slice part of the check.
        await foreach (Block block in connection.QueryAsync(
            "SELECT CAST(range(number), 'Array(Int32)') FROM system.numbers LIMIT 4",
            cancellationToken: None))
        {
            IColumn column = block[0];
            matched = column is IArrayColumn<int>;

            var array = (IArrayColumn<int>)column;
            rowCount = array.RowCount;
            offsets = array.Offsets.ToArray();
            innerValues = array.InnerValues.ToArray();
            innerRowCount = array.Inner.RowCount;
            materialized = ((IColumn<int[]>)column).Values.ToArray();

            for (int row = 0; row < array.RowCount; row++)
            {
                slices.Add(array.InnerValues.Slice(array.Offsets[row], array.Offsets[row + 1] - array.Offsets[row]).ToArray());
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(matched, Is.True);
            Assert.That(rowCount, Is.EqualTo(4));
            Assert.That(offsets, Is.EqualTo(new[] { 0, 0, 1, 3, 6 }), "one more entry than rows; [0] is 0");
            Assert.That(offsets.Length, Is.EqualTo(rowCount + 1), "sliced to the row count, not the pooled buffer length");
            Assert.That(innerValues, Is.EqualTo(new[] { 0, 0, 1, 0, 1, 2 }), "every row's elements end-to-end");
            Assert.That(innerRowCount, Is.EqualTo(6), "the inner column is flat: one entry per element, not per row");
            Assert.That(slices, Is.EqualTo(materialized), "the zero-copy slices reconstruct the materialized rows");
        });
    }

    [Test]
    public async Task QueryAsync_NestedArrayColumn_ExposesInnerAsAnotherArrayColumnForRecursion()
    {
        // Why IArrayColumn exposes Inner as a column and not just InnerValues as a span: when the element type is
        // itself composite, the span's element type is the *materialized* form (here int[]), so reading it defeats
        // the point. Inner hands back the flat inner column instead, which pattern-matches to the element type's own
        // view — so a nested composite can be walked to the bottom without materializing an intermediate level.
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        bool outerMatched = false;
        bool innerMatched = false;
        int[] outerOffsets = null;
        int[] innerOffsets = null;
        int[] leafValues = null;
        int[][][] materialized = null;

        // Rows: [[0], [0, 1]] and [[1], [1, 2]].
        await foreach (Block block in connection.QueryAsync(
            "SELECT [[toInt32(number)], [toInt32(number), toInt32(number + 1)]] FROM system.numbers LIMIT 2",
            cancellationToken: None))
        {
            IColumn column = block[0];
            outerMatched = column is IArrayColumn<int[]>;

            var outer = (IArrayColumn<int[]>)column;
            outerOffsets = outer.Offsets.ToArray();
            materialized = ((IColumn<int[][]>)column).Values.ToArray();

            innerMatched = outer.Inner is IArrayColumn<int>;
            var inner = (IArrayColumn<int>)outer.Inner;
            innerOffsets = inner.Offsets.ToArray();
            leafValues = inner.InnerValues.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(outerMatched, Is.True, "the outer view's element type is the materialized inner array");
            Assert.That(innerMatched, Is.True, "and Inner re-enters the columnar surface one level down");
            Assert.That(outerOffsets, Is.EqualTo(new[] { 0, 2, 4 }), "two inner arrays per outer row");
            Assert.That(innerOffsets, Is.EqualTo(new[] { 0, 1, 3, 4, 6 }), "four inner arrays, of lengths 1, 2, 1, 2");
            Assert.That(leafValues, Is.EqualTo(new[] { 0, 0, 1, 1, 1, 2 }), "the leaf run, reached without materializing a level");
            Assert.That(materialized, Is.EqualTo(new[] { new[] { new[] { 0 }, new[] { 0, 1 } }, new[] { new[] { 1 }, new[] { 1, 2 } } }));
        });
    }

    [Test]
    public async Task QueryAsync_NamedTupleColumn_ExposesPerElementChildColumnsThroughITupleColumn()
    {
        // A Tuple(...) is stored as its N element columns side by side, each as tall as the tuple. Reading one
        // element through the materialized ValueTuple surface forces every other element to be decoded and boxed
        // into the tuple too; ITupleColumn.Children hands back the element columns directly, so a caller that wants
        // one field pays for one field. FieldNames carries the declared names, which the wire layout does not.
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        bool matched = false;
        int childCount = 0;
        string[] fieldNames = null;
        int[] childRowCounts = null;
        int[] firstElement = null;
        string[] secondElement = null;
        (int, string)[] materialized = null;

        await foreach (Block block in connection.QueryAsync(
            "SELECT CAST((toInt32(number), concat('n', toString(number))), 'Tuple(a Int32, b String)') FROM system.numbers LIMIT 3",
            cancellationToken: None))
        {
            IColumn column = block[0];
            matched = column is ITupleColumn;

            var tuple = (ITupleColumn)column;
            childCount = tuple.Children.Count;
            fieldNames = tuple.FieldNames?.ToArray();
            childRowCounts = new[] { tuple.Children[0].RowCount, tuple.Children[1].RowCount };
            firstElement = ((IColumn<int>)tuple.Children[0]).Values.ToArray();
            secondElement = ((IColumn<string>)tuple.Children[1]).Values.ToArray();
            materialized = ((IColumn<(int, string)>)column).Values.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(matched, Is.True);
            Assert.That(childCount, Is.EqualTo(2), "one child per tuple element, in declaration order");
            Assert.That(fieldNames, Is.EqualTo(new[] { "a", "b" }), "the declared names, aligned with Children");
            Assert.That(childRowCounts, Is.EqualTo(new[] { 3, 3 }), "every child is exactly as tall as the tuple");
            Assert.That(firstElement, Is.EqualTo(new[] { 0, 1, 2 }), "one element read without decoding the other");
            Assert.That(secondElement, Is.EqualTo(new[] { "n0", "n1", "n2" }));
            Assert.That(materialized, Is.EqualTo(new[] { (0, "n0"), (1, "n1"), (2, "n2") }));
        });
    }

    [Test]
    public async Task QueryAsync_UnnamedTupleWithCompositeElement_OmitsFieldNamesAndAllowsChildRecursion()
    {
        // Two things the named case cannot show: an unnamed tuple carries no names at all (FieldNames is null, not
        // a list of nulls), and a child that is itself a composite pattern-matches to its own columnar view — so a
        // Tuple(Array(Int32), ...) can be walked into without materializing the tuple or the array rows.
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        bool hasFieldNames = true;
        bool childIsArray = false;
        int[] childOffsets = null;
        int[] childInnerValues = null;

        // Rows: ([], 'r0'), ([0], 'r1'), ([0, 1], 'r2').
        await foreach (Block block in connection.QueryAsync(
            "SELECT tuple(CAST(range(number), 'Array(Int32)'), concat('r', toString(number))) FROM system.numbers LIMIT 3",
            cancellationToken: None))
        {
            var tuple = (ITupleColumn)block[0];
            hasFieldNames = tuple.FieldNames is not null;

            childIsArray = tuple.Children[0] is IArrayColumn<int>;
            var child = (IArrayColumn<int>)tuple.Children[0];
            childOffsets = child.Offsets.ToArray();
            childInnerValues = child.InnerValues.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(hasFieldNames, Is.False, "an unnamed tuple reports no names rather than a list of nulls");
            Assert.That(childIsArray, Is.True, "a composite child re-enters the columnar surface");
            Assert.That(childOffsets, Is.EqualTo(new[] { 0, 0, 1, 3 }), "the child array's own per-row offsets");
            Assert.That(childInnerValues, Is.EqualTo(new[] { 0, 0, 1 }));
        });
    }

    [Test]
    public async Task QueryAsync_MapColumn_ExposesFlatKeyAndValueColumnsThroughIMapColumn()
    {
        // A Map(K, V) is byte-identical to Array(Tuple(K, V)) on the wire: per-row offsets over two flat, aligned
        // runs. The materialized surface builds a KeyValuePair[] per row; IMapColumn hands back the two columns, so
        // a caller wanting only the keys — a common case — never builds a pair at all.
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        bool matched = false;
        int rowCount = 0;
        int[] offsets = null;
        string[] flatKeys = null;
        int[] flatValues = null;
        int keyColumnRowCount = 0;
        var keysPerRow = new List<string[]>();
        KeyValuePair<string, int>[][] materialized = null;

        // Rows: {}, {'k0': 0}, {'k0': 0, 'k1': 100} — an empty leading row keeps a zero-length range in play.
        await foreach (Block block in connection.QueryAsync(
            """
            SELECT CAST(
                arrayMap(i -> (concat('k', toString(i)), toInt32(i * 100)), range(number)),
                'Map(String, Int32)')
            FROM system.numbers LIMIT 3
            """,
            cancellationToken: None))
        {
            IColumn column = block[0];
            matched = column is IMapColumn<string, int>;

            var map = (IMapColumn<string, int>)column;
            rowCount = map.RowCount;
            offsets = map.Offsets.ToArray();
            flatKeys = map.KeyColumn.Values.ToArray();
            flatValues = map.ValueColumn.Values.ToArray();
            keyColumnRowCount = map.KeyColumn.RowCount;
            materialized = ((IColumn<KeyValuePair<string, int>[]>)column).Values.ToArray();

            // Take just the keys of each row, without materializing the values or the pairs.
            for (int row = 0; row < map.RowCount; row++)
            {
                int start = map.Offsets[row];
                keysPerRow.Add(map.KeyColumn.Values.Slice(start, map.Offsets[row + 1] - start).ToArray());
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(matched, Is.True);
            Assert.That(rowCount, Is.EqualTo(3));
            Assert.That(offsets, Is.EqualTo(new[] { 0, 0, 1, 3 }), "one more entry than rows");
            Assert.That(offsets.Length, Is.EqualTo(rowCount + 1), "sliced to the row count, not the pooled buffer length");
            Assert.That(flatKeys, Is.EqualTo(new[] { "k0", "k0", "k1" }), "every row's keys end-to-end");
            Assert.That(flatValues, Is.EqualTo(new[] { 0, 0, 100 }), "and the values, aligned entry-for-entry");
            Assert.That(keyColumnRowCount, Is.EqualTo(3), "the key column is flat: one entry per pair, not per row");
            Assert.That(keysPerRow, Is.EqualTo(new[] { Array.Empty<string>(), new[] { "k0" }, new[] { "k0", "k1" } }));
            Assert.That(materialized.Select(r => r.Length), Is.EqualTo(new[] { 0, 1, 2 }));
            Assert.That(materialized[2], Is.EqualTo(new[] { new KeyValuePair<string, int>("k0", 0), new KeyValuePair<string, int>("k1", 100) }));
        });
    }

    [Test]
    public async Task QueryAsync_MapColumnWithDuplicateKeys_PreservesWireOrderAndDuplicates()
    {
        // The flat columns are the wire's own bytes, so they carry duplicate keys and entry order intact — the
        // property a Dictionary-shaped view would destroy. ClickHouse itself permits a literal map with a repeated
        // key, so this is reachable data, not a hypothetical.
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        string[] flatKeys = null;
        int[] flatValues = null;

        await foreach (Block block in connection.QueryAsync(
            "SELECT CAST([('dup', 1), ('dup', 2), ('other', 3)], 'Map(String, Int32)')",
            cancellationToken: None))
        {
            var map = (IMapColumn<string, int>)block[0];
            flatKeys = map.KeyColumn.Values.ToArray();
            flatValues = map.ValueColumn.Values.ToArray();
        }

        Assert.Multiple(() =>
        {
            Assert.That(flatKeys, Is.EqualTo(new[] { "dup", "dup", "other" }), "the duplicate survives");
            Assert.That(flatValues, Is.EqualTo(new[] { 1, 2, 3 }), "in wire order, so both entries stay addressable");
        });
    }

    [Test]
    public async Task QueryAsync_NestedColumn_ExposesPerFieldColumnsAndSharedOffsetsThroughINestedColumn()
    {
        // Nested is the case where the columnar view is the *primary* access path rather than an optimization: a
        // Nested can carry any number of fields, so there is no generic per-row value type for it and the
        // IColumn<T> surface has to degrade to object[][] — a boxed object[] per record, per row. INestedColumn is
        // how a consumer reads it typed. One offsets array is shared by every field, since within a row all fields
        // have the same element count.
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        bool matched = false;
        int rowCount = 0;
        int fieldCount = 0;
        string[] fieldNames = null;
        int[] offsets = null;
        byte[] fieldA = null;
        string[] fieldB = null;
        bool byNameMatchesByIndex = false;
        int[] fieldRowCounts = null;
        var recordsOfLastRow = new List<(byte A, string B)>();

        // Rows: [], [(0, 'f0')], [(0, 'f0'), (1, 'f1')].
        await foreach (Block block in connection.QueryAsync(
            """
            SELECT CAST(
                arrayMap(i -> (toUInt8(i), concat('f', toString(i))), range(number)),
                'Nested(a UInt8, b String)')
            FROM system.numbers LIMIT 3
            """,
            cancellationToken: None))
        {
            IColumn column = block[0];
            matched = column is INestedColumn;

            var nested = (INestedColumn)column;
            rowCount = nested.RowCount;
            fieldCount = nested.FieldCount;
            fieldNames = nested.FieldNames.ToArray();
            offsets = nested.Offsets.ToArray();
            fieldA = ((IColumn<byte>)nested.GetField(0)).Values.ToArray();
            fieldB = ((IColumn<string>)nested.GetField(1)).Values.ToArray();
            byNameMatchesByIndex = ReferenceEquals(nested.GetField("b"), nested.GetField(1));
            fieldRowCounts = new[] { nested.GetField(0).RowCount, nested.GetField(1).RowCount };

            // Reassemble the last row's records by walking the shared offsets across both field columns — the
            // typed, allocation-free equivalent of what the object[][] surface would box.
            int last = nested.RowCount - 1;
            var a = (IColumn<byte>)nested.GetField(0);
            var b = (IColumn<string>)nested.GetField(1);
            for (int i = nested.Offsets[last]; i < nested.Offsets[last + 1]; i++)
            {
                recordsOfLastRow.Add((a[i], b[i]));
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(matched, Is.True);
            Assert.That(rowCount, Is.EqualTo(3));
            Assert.That(fieldCount, Is.EqualTo(2));
            Assert.That(fieldNames, Is.EqualTo(new[] { "a", "b" }));
            Assert.That(offsets, Is.EqualTo(new[] { 0, 0, 1, 3 }), "one shared offsets array, one more entry than rows");
            Assert.That(offsets.Length, Is.EqualTo(rowCount + 1), "sliced to the row count, not the pooled buffer length");
            Assert.That(fieldA, Is.EqualTo(new byte[] { 0, 0, 1 }), "field-major: every row's 'a' elements end-to-end");
            Assert.That(fieldB, Is.EqualTo(new[] { "f0", "f0", "f1" }), "and every row's 'b', aligned with 'a'");
            Assert.That(fieldRowCounts, Is.EqualTo(new[] { 3, 3 }), "every field holds the same total element count");
            Assert.That(byNameMatchesByIndex, Is.True, "name lookup resolves to the same column as the index");
            Assert.That(recordsOfLastRow, Is.EqualTo(new[] { ((byte)0, "f0"), ((byte)1, "f1") }));
        });
    }

    [Test]
    public async Task QueryAsync_LowCardinalityColumn_ExposesDictionaryAndKeysThroughILowCardinalityColumn()
    {
        // LowCardinality is the type with the widest gap between the two surfaces: the materialized one resolves
        // every row to its dictionary entry, so N rows over a K-entry dictionary produce N values. Reading the
        // dictionary and keys instead means touching each distinct value once — the whole reason the encoding exists.
        // Here 12 rows collapse onto 3 distinct values.
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        bool matched = false;
        int rowCount = 0;
        int keyCount = 0;
        int dictionarySize = 0;
        int reservedSlots = 0;
        string[] dictionary = null;
        string[] materialized = null;
        var resolvedThroughKeys = new List<string>();

        await foreach (Block block in connection.QueryAsync(
            "SELECT CAST(concat('v', toString(number % 3)), 'LowCardinality(String)') FROM system.numbers LIMIT 12",
            cancellationToken: None))
        {
            IColumn column = block[0];
            matched = column is ILowCardinalityColumn<string>;

            var lc = (ILowCardinalityColumn<string>)column;
            rowCount = lc.RowCount;
            keyCount = lc.Keys.Length;
            dictionarySize = lc.Dictionary.RowCount;
            reservedSlots = lc.ReservedSlotCount;
            dictionary = lc.Dictionary.Values.ToArray();
            materialized = ((IColumn<string>)column).Values.ToArray();

            for (int row = 0; row < lc.RowCount; row++)
            {
                resolvedThroughKeys.Add(lc.Dictionary[lc.Keys[row]]);
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(matched, Is.True);
            Assert.That(rowCount, Is.EqualTo(12));
            Assert.That(keyCount, Is.EqualTo(rowCount), "one key per row, sliced to the row count rather than the pooled buffer length");
            Assert.That(dictionarySize, Is.EqualTo(4), "three distinct values plus the reserved default slot at [0]");
            Assert.That(reservedSlots, Is.EqualTo(1), "a non-nullable inner reserves exactly one slot, so key 0 is a value and not NULL");
            Assert.That(dictionary[0], Is.Empty, "and that slot holds the type default");
            Assert.That(dictionary.Skip(1), Is.EquivalentTo(new[] { "v0", "v1", "v2" }));
            Assert.That(resolvedThroughKeys, Is.EqualTo(materialized), "Dictionary[Keys[i]] reproduces the materialized rows");
        });
    }

    [Test]
    public async Task QueryAsync_LowCardinalityNullableColumn_ReservesTwoDictionarySlotsAndMarksNullWithKeyZero()
    {
        // LowCardinality(Nullable(T)) decodes to a different column class whose dictionary reserves *two* leading
        // slots — [0] NULL, [1] the default — so real values start at [2] and a key of 0 means NULL. That shift is
        // the one thing a consumer reading Keys directly has to know, and it is invisible from the materialized
        // surface. Note the view is still spelled with the bare inner type, matching the non-nullable case.
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        bool matched = false;
        int dictionarySize = 0;
        int reservedSlots = 0;
        int[] keys = null;
        string[] dictionary = null;
        string[] materialized = null;
        var nullRowsFromKeys = new List<int>();

        await foreach (Block block in connection.QueryAsync(
            """
            SELECT CAST(number = 1 ? NULL : concat('v', toString(number % 2)), 'LowCardinality(Nullable(String))')
            FROM system.numbers LIMIT 4
            """,
            cancellationToken: None))
        {
            IColumn column = block[0];
            matched = column is ILowCardinalityColumn<string>;

            var lc = (ILowCardinalityColumn<string>)column;
            dictionarySize = lc.Dictionary.RowCount;
            reservedSlots = lc.ReservedSlotCount;
            keys = lc.Keys.ToArray();
            dictionary = lc.Dictionary.Values.ToArray();
            materialized = ((IColumn<string>)column).Values.ToArray();

            // Identify the null rows from the keys alone, the way a consumer should: ReservedSlotCount says whether
            // slot 0 is a NULL marker, so nothing here parses the type string.
            for (int row = 0; row < lc.RowCount; row++)
            {
                if (lc.ReservedSlotCount == 2 && lc.Keys[row] == 0)
                {
                    nullRowsFromKeys.Add(row);
                }
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(matched, Is.True, "spelled with the bare inner type, as for the non-nullable column");
            Assert.That(materialized, Is.EqualTo(new[] { "v0", null, "v0", "v1" }));
            Assert.That(reservedSlots, Is.EqualTo(2), "a nullable inner reserves two slots — this is what makes key 0 mean NULL");
            Assert.That(dictionarySize, Is.EqualTo(4), "two reserved slots plus the two distinct values");
            Assert.That(keys[1], Is.Zero, "the null row's key is 0 — the reserved NULL slot");
            Assert.That(keys.Where((_, i) => i != 1), Is.All.GreaterThanOrEqualTo(reservedSlots), "real values start past the reserve");
            Assert.That(dictionary.Skip(reservedSlots), Is.EquivalentTo(new[] { "v0", "v1" }));
            Assert.That(nullRowsFromKeys, Is.EqualTo(new[] { 1 }), "the keys alone identify the null rows, with no type-string parsing");
        });
    }

    [Test]
    public async Task InsertAsync_NonNullableLowCardinalityColumnIntoNullableTarget_RebuildsRatherThanReemitting()
    {
        // The reserved-slot difference the two tests above describe is not just a reading hazard: it decides whether
        // a decoded column may be written back verbatim. Both low-cardinality shapes expose the dictionary/keys pair
        // through ILowCardinalityColumn<T>, but only the nullable ones additionally claim IDenseLowCardinality<T> —
        // the internal marker meaning "a nullable codec may re-emit this pair as-is". The non-nullable column must
        // not claim it, because its dictionary reserves one leading slot where a nullable dictionary reserves two, so
        // a verbatim re-emit shifts every key by one and makes the reader decode slot 0 as NULL.
        //
        // Conflating the two interfaces corrupts data silently, and nothing else covers it, so it is pinned here
        // rather than in the insert fixture — this is where the slot semantics are documented.
        await using var connection = await TcpServerFixture.ConnectAsync(None);
        string table = $"tcp_lc_dense_exclusion_{Guid.NewGuid():N}";
        try
        {
            await ExecuteAsync(connection, $"CREATE TABLE {table} (value LowCardinality(Nullable(String))) ENGINE = Memory");

            // Row 0's key is 0 deliberately. To a non-nullable dictionary slot 0 is the type default — an ordinary
            // value for a row to reference — while to a nullable dictionary it is the NULL marker. A row keyed at 0
            // is therefore the only row that can distinguish the two paths: rows keyed above the reserve survive a
            // verbatim re-emit unharmed, so a case without a slot-0 row passes whether or not the bug is present.
            var dictionary = new ArrayColumn<string>("value", "String", new[] { string.Empty, "alpha", "beta" });
            using var nonNullableDense = new LowCardinalityColumn<string>(
                "value",
                "LowCardinality(String)",
                dictionary,
                new[] { 0, 1, 2, 1 },
                rowCount: 4,
                pooledKeys: false);

            Assert.That(nonNullableDense, Is.Not.InstanceOf<IDenseLowCardinality<string>>(), "the non-nullable column must not claim dense write eligibility");
            Assert.That(nonNullableDense, Is.InstanceOf<ILowCardinalityColumn<string>>(), "though it still exposes the pair for reading");

            await connection.InsertAsync($"INSERT INTO {table} (value) VALUES", new IColumn[] { nonNullableDense }, cancellationToken: None);

            var readBack = new List<string>();
            await foreach (Block block in connection.QueryAsync($"SELECT value FROM {table}", cancellationToken: None))
            {
                readBack.AddRange(((IColumn<string>)block[0]).Values.ToArray());
            }

            Assert.That(readBack, Is.EqualTo(new[] { string.Empty, "alpha", "beta", "alpha" }), "the slot-0 row stays the empty string rather than turning into NULL");
        }
        finally
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Test]
    public async Task QueryAsync_VariantColumn_ExposesDiscriminatorsAndPerTypeChildColumnsThroughIVariantColumn()
    {
        // Variant is the composite with no useful materialized element type: its IColumn<T> surface is
        // IColumn<object>, so every row read through it is boxed. The columnar view is the only typed way in —
        // dispatch on the discriminator, then read the selected type's child column, which is typed. Each child holds
        // only its own rows, contiguously, so the per-row position within a child is not the row index; LocalIndices
        // carries that mapping, precomputed by one walk of the discriminators.
        var settings = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["allow_experimental_variant_type"] = "1",
            ["allow_suspicious_variant_types"] = "1",
        };
        await using var connection = await TcpServerFixture.ConnectAsync(None);

        bool matched = false;
        int typeCount = 0;
        int rowCount = 0;
        byte[] discriminators = null;
        int discriminatorLength = 0;
        int[] localIndices = null;
        string[] stringChild = null;
        ulong[] intChild = null;
        var typedReads = new List<object>();
        var materialized = new List<object>();

        // Rows: 100, 'a', NULL, 400, 'b' — interleaved so neither child's rows are contiguous by row index.
        await foreach (Block block in connection.QueryAsync(
            """
            SELECT CAST(multiIf(number = 1, CAST('a', 'Variant(String, UInt64)'),
                                number = 2, CAST(NULL, 'Variant(String, UInt64)'),
                                number = 4, CAST('b', 'Variant(String, UInt64)'),
                                CAST(toUInt64((number + 1) * 100), 'Variant(String, UInt64)')), 'Variant(String, UInt64)')
            FROM system.numbers LIMIT 5
            """,
            settings: settings,
            cancellationToken: None))
        {
            IColumn column = block[0];
            matched = column is IVariantColumn;

            var variant = (IVariantColumn)column;
            typeCount = variant.TypeCount;
            rowCount = variant.RowCount;
            discriminators = variant.Discriminators.ToArray();
            discriminatorLength = variant.Discriminators.Length;
            localIndices = variant.LocalIndices.ToArray();
            stringChild = ((IColumn<string>)variant.GetTypeColumn(0)).Values.ToArray();
            intChild = ((IColumn<ulong>)variant.GetTypeColumn(1)).Values.ToArray();

            // The typed dispatch the interface exists for: no boxing except what we do here to compare.
            for (int row = 0; row < variant.RowCount; row++)
            {
                byte d = variant.Discriminators[row];
                if (d == IVariantColumn.NullDiscriminator)
                {
                    typedReads.Add(null);
                }
                else if (d == 0)
                {
                    typedReads.Add(((IColumn<string>)variant.GetTypeColumn(0))[variant.LocalIndices[row]]);
                }
                else
                {
                    typedReads.Add(((IColumn<ulong>)variant.GetTypeColumn(1))[variant.LocalIndices[row]]);
                }

                materialized.Add(column.GetValue(row));
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(matched, Is.True);
            Assert.That(typeCount, Is.EqualTo(2));
            Assert.That(rowCount, Is.EqualTo(5));
            Assert.That(discriminatorLength, Is.EqualTo(rowCount), "sliced to the row count, not the pooled buffer length");
            Assert.That(discriminators, Is.EqualTo(new byte[] { 1, 0, IVariantColumn.NullDiscriminator, 1, 0 }), "0 = String, 1 = UInt64, 255 = NULL");
            Assert.That(localIndices, Is.EqualTo(new[] { 0, 0, -1, 1, 1 }), "per-type running position; a NULL row addresses no child, so -1");
            Assert.That(stringChild, Is.EqualTo(new[] { "a", "b" }), "each child holds only its own rows, contiguously");
            Assert.That(intChild, Is.EqualTo(new ulong[] { 100, 400 }));
            Assert.That(typedReads, Is.EqualTo(materialized), "the typed columnar dispatch agrees with the boxed surface");
            Assert.That(materialized, Is.EqualTo(new object[] { 100UL, "a", null, 400UL, "b" }));
        });
    }

    private static async Task ExecuteAsync(ClickHouseTcpConnection connection, string sql)
    {
        await foreach (Block block in connection.QueryAsync(sql, cancellationToken: None))
        {
            _ = block;
        }
    }
}
