using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// Rows of a case that a read or a write covers: <see cref="Start"/> and <see cref="Count"/> in the column, and
/// <see cref="CaseRow"/>, the first row of the case that they are. A preceded range is in a column with one more row
/// before the rows of the case.
/// </summary>
internal readonly record struct RowRange(int Start, int Count, bool Preceded, int CaseRow)
{
    /// <summary>Row <paramref name="i"/> of the range, alone.</summary>
    public RowRange Row(int i) => new(Start + i, 1, Preceded, CaseRow + i);

    /// <inheritdoc/>
    public override string ToString() => $"rows [{Start}, {Start + Count}){(Preceded ? " after a preceding row" : string.Empty)}";
}

/// <summary>
/// Runs one case in the tiers of the client. The insert write of the first write input gives the bytes of the column,
/// and each read decodes a new block from them, so a cache that a column fills on its first read is empty for each read.
/// </summary>
/// <remarks>
/// Every read and write runs for all rows and for the tail. The tail of a case of two rows or more is
/// <c>[RowCount / 2, RowCount)</c>. The tail of a case of one row is row 1 of a column with a row before the case's row:
/// for an <c>Array</c> type the row's elements twice (so the child offset is not 0), else a copy. So every read and
/// write also starts above row 0.
/// </remarks>
internal static class DifferentialEngine
{
    private static readonly ConcurrentDictionary<(string Id, bool Preceded), byte[]> Sources = new();

    private static readonly MethodInfo ReadMethod = typeof(DifferentialEngine).GetMethod(nameof(ReadTyped), BindingFlags.NonPublic | BindingFlags.Static);
    private static readonly MethodInfo WriteMethod = typeof(DifferentialEngine).GetMethod(nameof(WriteTyped), BindingFlags.NonPublic | BindingFlags.Static);

    /// <summary>The context that every case resolves its codecs with.</summary>
    internal static ResolveContext Context { get; } = new() { ServerTimezone = "UTC" };

    public static RowRange All(DifferentialCase testCase) => new(0, testCase.RowCount, Preceded: false, CaseRow: 0);

    public static RowRange Tail(DifferentialCase testCase) => testCase.RowCount == 1
        ? new(1, 1, Preceded: true, CaseRow: 0)
        : new(testCase.RowCount / 2, testCase.RowCount - (testCase.RowCount / 2), Preceded: false, CaseRow: testCase.RowCount / 2);

    /// <summary>The insert write (state prefix and body) of all rows of the source column of a case, with a row before the case's row when preceded.</summary>
    public static byte[] SourceBytes(DifferentialCase testCase, bool preceded = false)
        => Sources.GetOrAdd((testCase.Id, preceded), key =>
        {
            RowRange all = preceded ? new(0, testCase.RowCount + 1, true, 0) : All(testCase);
            Outcome source = Write(ClientArms.Write, testCase, testCase.WriteInputs[0], all);
            return source.Kind == OutcomeKind.Bytes ? source.Bytes : throw new InvalidOperationException($"{testCase.Id}: the insert cannot write the source column: {source}.");
        });

    /// <summary>The block of the source rows, decoded from <see cref="SourceBytes"/>, which the caller disposes.</summary>
    public static Block Decode(DifferentialCase testCase, bool preceded)
    {
        int rows = testCase.RowCount + (preceded ? 1 : 0);
        return new Block(string.Empty, BlockInfo.Default, rows, new[] { DecodeColumn(testCase.ColumnType, SourceBytes(testCase, preceded), rows) }, ColumnCodecRegistry.Default, Context);
    }

    /// <summary>The column that a query reads from the bytes of a column write. Fails when bytes are left over.</summary>
    public static IColumn DecodeColumn(string columnType, byte[] bytes, int rows)
    {
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(columnType, Context);
        using var stream = new MemoryStream(bytes);
        using var reader = new ClickHouseBinaryReader(stream);
        codec.ReadStatePrefixAsync(reader, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        IColumn column = codec.ReadColumnAsync(reader, "value", columnType, rows, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        if (reader.BufferedBytes != 0 || stream.Position != stream.Length)
        {
            column.Dispose();
            throw new InvalidDataException($"The decode of {rows} rows of '{columnType}' left {reader.BufferedBytes + stream.Length - stream.Position} of {bytes.Length} bytes.");
        }

        return column;
    }

    /// <summary>Reads the rows of a range of the decoded column as <paramref name="target"/>.</summary>
    public static Outcome Read(ReadArm arm, DifferentialCase testCase, Type target, RowRange range)
        => (Outcome)Invoke(ReadMethod, target, arm, testCase, range);

    /// <summary>Writes the rows of a range of a write input; <see cref="OutcomeKind.Unavailable"/> for a read back that has no values.</summary>
    public static Outcome Write(WriteArm arm, DifferentialCase testCase, WriteInput input, RowRange range)
        => (Outcome)Invoke(WriteMethod, input.ElementType, arm, testCase, input, range);

    /// <summary>The failures of the rows of a range, each read or written alone.</summary>
    public static IEnumerable<Outcome> RowFailures(RowRange range, Func<RowRange, Outcome> run)
        => Enumerable.Range(0, range.Count).Select(i => run(range.Row(i))).Where(o => o.Kind == OutcomeKind.Failed);

    private static object Invoke(MethodInfo method, Type typeArgument, params object[] arguments)
        => RowWriteArms.Invoke(method, typeArgument, arguments);

    private static Outcome ReadTyped<T>(ReadArm arm, DifferentialCase testCase, RowRange range)
    {
        using Block block = Decode(testCase, range.Preceded);
        RowReader<T> reader;
        try
        {
            reader = arm.Bind<T>(block);
        }
        catch (Exception e)
        {
            return Outcome.Refusal(e);
        }

        try
        {
            return Outcome.OfValues(reader(range.Start, range.Count));
        }
        catch (Exception e)
        {
            return Outcome.Failure(e);
        }
    }

    private static Outcome WriteTyped<T>(WriteArm arm, DifferentialCase testCase, WriteInput input, RowRange range)
    {
        Block block = null;
        IColumn column;
        switch (input.Kind)
        {
            case WriteInputKind.Built:
                column = Preceded<T>(testCase, input.Build("value"), range.Preceded);
                break;

            case WriteInputKind.Decoded:
                block = Decode(testCase, range.Preceded);
                column = block[0];
                break;

            default:
                Outcome values = Read(ClientArms.ReadAs, testCase, input.ElementType, All(testCase));
                if (values.Kind != OutcomeKind.Values)
                {
                    return Outcome.Unavailable($"Block.ReadAs gives no values: {values}");
                }

                column = Preceded<T>(testCase, new ArrayColumn<T>("value", testCase.ColumnType, values.ValuesAs<T>()), range.Preceded);
                break;
        }

        try
        {
            SliceWriter writer;
            try
            {
                writer = arm.Bind((IColumn<T>)column, testCase.ColumnType, Context);
            }
            catch (Exception e)
            {
                return Outcome.Refusal(e);
            }

            using var stream = new MemoryStream();
            try
            {
                using (var output = new ClickHouseBinaryWriter(stream))
                {
                    writer(output, range.Start, range.Count);
                    output.FlushAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
                }
            }
            catch (Exception e)
            {
                return Outcome.Failure(e);
            }

            return Outcome.OfBytes(stream.ToArray());
        }
        finally
        {
            if (block is not null)
            {
                block.Dispose();
            }
            else
            {
                column.Dispose();
            }
        }
    }

    // The column with a row before its only row: for an Array type the row's elements twice, else a copy.
    private static IColumn Preceded<T>(DifferentialCase testCase, IColumn column, bool preceded)
    {
        if (!preceded)
        {
            return column;
        }

        if (column is not ArrayColumn<T> { RowCount: 1 } array)
        {
            throw new InvalidOperationException($"{testCase.Id}: no row can be put before the only row of a {column.GetType().Name}. Give the case two rows or more.");
        }

        T row = array[0];
        T preceding = row;
        if (row is Array { Length: > 0 } elements && TypeParser.Parse(testCase.ColumnType).Name == "Array")
        {
            var twice = Array.CreateInstance(elements.GetType().GetElementType(), elements.Length * 2);
            elements.CopyTo(twice, 0);
            elements.CopyTo(twice, elements.Length);
            preceding = (T)(object)twice;
        }

        column.Dispose();
        return new ArrayColumn<T>("value", testCase.ColumnType, new[] { preceding, row });
    }
}
