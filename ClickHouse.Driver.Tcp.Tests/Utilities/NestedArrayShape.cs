using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// A ladder of <c>Array(Array(...))</c> columns of increasing nesting depth, shared by the round-trip cases and the
/// block-splitting integration tests so one set of row literals serves both.
///
/// <para>
/// Depth is the number of <c>Array</c> levels. Every level of every shape carries an empty row somewhere and rows of
/// differing lengths, so each level's offsets stream holds both an equal consecutive pair (an empty row) and an
/// uneven run. That matters most on the write path: the ergonomic (jagged) write gives a nested <c>Array</c> inner a
/// <c>ConcatColumn</c> flattening view rather than driving it per row, so depth <c>d</c> stacks <c>d - 1</c> of those
/// views, each one resolving flat element indices back through the level below it.
/// </para>
/// </summary>
public sealed class NestedArrayShape
{
    private readonly Func<string, IColumn> build;

    private NestedArrayShape(int depth, string clickHouseType, int rowCount, Func<string, IColumn> build)
    {
        Depth = depth;
        ClickHouseType = clickHouseType;
        RowCount = rowCount;
        this.build = build;
    }

    /// <summary>The number of <c>Array</c> levels the column nests.</summary>
    public int Depth { get; }

    /// <summary>The full <c>Array(...)</c> type for the target column.</summary>
    public string ClickHouseType { get; }

    /// <summary>The number of rows the shape's column holds.</summary>
    public int RowCount { get; }

    /// <summary>Builds the ergonomic (jagged) column for this shape, stamped with <paramref name="columnName"/>.</summary>
    /// <param name="columnName">The column name, which must match the target table column.</param>
    /// <returns>The column to insert.</returns>
    internal IColumn BuildColumn(string columnName) => build(columnName);

    public override string ToString() => $"{ClickHouseType} [{RowCount} rows]";

    /// <summary>The shapes, for use as an NUnit <c>TestCaseSource</c>.</summary>
    public static IEnumerable<NestedArrayShape> Shapes()
    {
        // The UInt8 ladder, depth 2 to 7: a fixed-width leaf, so the innermost write is the bulk blit reached through
        // the whole ConcatColumn stack. Depth 7 stacks six of those views, and the type-parsing corpus goes to 10.
        yield return Shape<byte[]>(2, "Array(UInt8)", Depth2Rows);
        yield return Shape<byte[][]>(3, "Array(Array(UInt8))", Depth3Rows);
        yield return Shape<byte[][][]>(4, "Array(Array(Array(UInt8)))", Depth4Rows);
        yield return Shape<byte[][][][]>(5, "Array(Array(Array(Array(UInt8))))", Depth5Rows);
        yield return Shape<byte[][][][][]>(6, "Array(Array(Array(Array(Array(UInt8)))))", Depth6Rows);
        yield return Shape<byte[][][][][][]>(7, "Array(Array(Array(Array(Array(Array(UInt8))))))", Depth7Rows);

        // Two other leaf kinds under the same skeleton, at depth 3 — enough to put more than one Array level above
        // the leaf, which is all that distinguishes them. Deeper adds another ConcatColumn but no new branch, so the
        // ladder above carries the depth and these carry the leaf.
        //
        // String: a variable-width leaf, so the innermost write is a per-element length prefix rather than a blit.
        yield return Shape<string[][]>(3, "Array(Array(String))", StringDepth3Rows);

        // Nullable(UInt32): a *sectioned* leaf. Its null-map has to be emitted once spanning every element of the
        // whole flattened run, so a level that drove its inner per row instead of handing over one column would
        // interleave the map into the values and corrupt the stream.
        yield return Shape<uint?[][]>(3, "Array(Array(Nullable(UInt32)))", NullableDepth3Rows);
    }

    // Rows for the depth-2 shape. Row 1 is an empty outer row, row 2 holds a single empty leaf row, and row 3 puts an
    // empty leaf row between two non-empty ones — so both levels see an empty and an uneven run.
    private static readonly byte[][][] Depth2Rows =
    {
        new[] { new byte[] { 1, 2 }, new byte[] { 3 } },
        Array.Empty<byte[]>(),
        new[] { Array.Empty<byte>() },
        new[] { new byte[] { 4 }, Array.Empty<byte>(), new byte[] { 5, 6, 7 } },
        new[] { new byte[] { 8 } },
    };

    // Each deeper shape wraps the one above it and adds an empty at the new level, keeping the same five-row skeleton:
    // a populated row, an empty outer row, a row holding one empty child, a second populated row, and a short row.
    private static readonly byte[][][][] Depth3Rows =
    {
        new[] { Depth2Rows[0], new[] { Array.Empty<byte>() } },
        Array.Empty<byte[][]>(),
        new[] { Array.Empty<byte[]>() },
        new[] { Depth2Rows[3], Array.Empty<byte[]>(), Depth2Rows[4] },
        new[] { new[] { new byte[] { 9 } } },
    };

    private static readonly byte[][][][][] Depth4Rows =
    {
        new[] { Depth3Rows[0], Array.Empty<byte[][]>() },
        Array.Empty<byte[][][]>(),
        new[] { Array.Empty<byte[][]>() },
        new[] { Depth3Rows[3], Depth3Rows[4] },
        new[] { new[] { new[] { Array.Empty<byte>() } } },
    };

    private static readonly byte[][][][][][] Depth5Rows =
    {
        new[] { Depth4Rows[0] },
        Array.Empty<byte[][][][]>(),
        new[] { Array.Empty<byte[][][]>() },
        new[] { Depth4Rows[3], Depth4Rows[4] },
        new[] { new[] { new[] { new[] { Array.Empty<byte>() } } } },
    };

    private static readonly byte[][][][][][][] Depth6Rows =
    {
        new[] { Depth5Rows[0] },
        Array.Empty<byte[][][][][]>(),
        new[] { Array.Empty<byte[][][][]>() },
        new[] { Depth5Rows[3], Depth5Rows[4] },
        new[] { new[] { new[] { new[] { new[] { Array.Empty<byte>() } } } } },
    };

    private static readonly byte[][][][][][][][] Depth7Rows =
    {
        new[] { Depth6Rows[0] },
        Array.Empty<byte[][][][][][]>(),
        new[] { Array.Empty<byte[][][][][]>() },
        new[] { Depth6Rows[3], Depth6Rows[4] },
        new[] { new[] { new[] { new[] { new[] { new[] { Array.Empty<byte>() } } } } } },
    };

    // The depth-2 skeleton over a variable-width leaf. The empty string is a distinct value from an empty row, so
    // both appear.
    private static readonly string[][][] StringDepth2Rows =
    {
        new[] { new[] { "a", "bb" }, new[] { string.Empty } },
        Array.Empty<string[]>(),
        new[] { Array.Empty<string>() },
        new[] { new[] { "héllo✓" }, Array.Empty<string>(), new[] { "c", "d", "e" } },
        new[] { new[] { "f" } },
    };

    private static readonly string[][][][] StringDepth3Rows =
    {
        new[] { StringDepth2Rows[0], new[] { Array.Empty<string>() } },
        Array.Empty<string[][]>(),
        new[] { Array.Empty<string[]>() },
        new[] { StringDepth2Rows[3], Array.Empty<string[]>(), StringDepth2Rows[4] },
        new[] { new[] { new[] { "g" } } },
    };

    // The depth-2 skeleton over the sectioned leaf. A null sits next to a non-null in the same leaf row, and one leaf
    // row is all nulls, so the null-map is neither all-set nor all-clear over the flattened run.
    private static readonly uint?[][][] NullableDepth2Rows =
    {
        new[] { new uint?[] { 1, null }, new uint?[] { 0 } },
        Array.Empty<uint?[]>(),
        new[] { Array.Empty<uint?>() },
        new[] { new uint?[] { null, null }, Array.Empty<uint?>(), new uint?[] { 2, uint.MaxValue, null } },
        new[] { new uint?[] { null } },
    };

    private static readonly uint?[][][][] NullableDepth3Rows =
    {
        new[] { NullableDepth2Rows[0], new[] { Array.Empty<uint?>() } },
        Array.Empty<uint?[][]>(),
        new[] { Array.Empty<uint?[]>() },
        new[] { NullableDepth2Rows[3], Array.Empty<uint?[]>(), NullableDepth2Rows[4] },
        new[] { new[] { new uint?[] { 3, null } } },
    };

    // Wraps the rows in the ergonomic jagged column the callers insert. TElement is the inner codec's element type,
    // so a row is TElement[] and the declared type is Array(innerType).
    private static NestedArrayShape Shape<TElement>(int depth, string innerType, TElement[][] rows)
    {
        string type = $"Array({innerType})";
        return new NestedArrayShape(depth, type, rows.Length, name => new ArrayColumn<TElement[]>(name, type, rows));
    }
}
