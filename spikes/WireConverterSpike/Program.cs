// SPIKE for ClickHouse/integrations#801. Not production code.
//
//   dotnet run -c Release -- verify          compare every candidate result with the current client
//   dotnet run -c Release -- bench [filter]  BenchmarkDotNet, e.g. bench '*Read*'
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using ClickHouse.Driver.Tcp;
using ClickHouse.Driver.Tcp.Format;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Protocol;
using ClickHouse.Driver.Tcp.Types;

namespace WireConverterSpike;

#pragma warning disable SA1300, IDE1006 // property names match column names; the spike maps by exact name

public sealed class ArrayRow
{
    public byte[][] arr { get; set; }
}

public sealed class LcRow
{
    public byte[] lc { get; set; }
}

public sealed class NdtRow
{
    public DateTimeOffset? ndt { get; set; }
}

public sealed class WideRow
{
    public byte[][] arr { get; set; }

    public byte[] lc { get; set; }

    public DateTimeOffset? ndt { get; set; }

    public string s { get; set; }

    public DateTimeOffset dt { get; set; }

    public string lcn { get; set; }
}

#pragma warning restore SA1300, IDE1006

public enum ReadShape
{
    ArrayStringAsBytes,
    LowCardinalityStringAsBytes,
    NullableDateTimeAsOffset,
    Wide,
}

public enum WriteShape
{
    LowCardinalityString_HighRepeat,
    LowCardinalityString_LowRepeat,
    LowCardinalityFixedStringBytes_HighRepeat,
    LowCardinalityFixedStringBytes_LowRepeat,
    NullableDateTimeFromOffset,
    ArrayStringFromText,
}

internal static class Data
{
    public static readonly ResolveContext Context = new() { ServerTimezone = "UTC" };

    public static Block Block(ReadShape shape, int rows)
    {
        var columns = new List<IColumn>();
        if (shape is ReadShape.ArrayStringAsBytes or ReadShape.Wide)
        {
            columns.Add(Decode("arr", "Array(String)", new ArrayColumn<string[]>("arr", "Array(String)",
                Enumerable.Range(0, rows).Select(i => Enumerable.Range(0, i % 5).Select(j => $"item-{i}-{j}").ToArray()).ToArray())));
        }

        if (shape is ReadShape.LowCardinalityStringAsBytes or ReadShape.Wide)
        {
            columns.Add(Decode("lc", "LowCardinality(String)", new ArrayColumn<string>("lc", "LowCardinality(String)",
                Enumerable.Range(0, rows).Select(i => $"category-{i % 100}").ToArray())));
        }

        if (shape is ReadShape.NullableDateTimeAsOffset or ReadShape.Wide)
        {
            columns.Add(Decode("ndt", "Nullable(DateTime('UTC'))", new ArrayColumn<DateTimeOffset?>("ndt", "Nullable(DateTime('UTC'))",
                Enumerable.Range(0, rows).Select(i => i % 5 == 0 ? (DateTimeOffset?)null : DateTimeOffset.FromUnixTimeSeconds(1_700_000_000 + i)).ToArray())));
        }

        if (shape is ReadShape.Wide)
        {
            columns.Add(Decode("s", "String", new ArrayColumn<string>("s", "String",
                Enumerable.Range(0, rows).Select(i => $"text-{i}").ToArray())));
            columns.Add(Decode("dt", "DateTime('UTC')", new ArrayColumn<DateTimeOffset>("dt", "DateTime('UTC')",
                Enumerable.Range(0, rows).Select(i => DateTimeOffset.FromUnixTimeSeconds(1_600_000_000 + i)).ToArray())));
            columns.Add(Decode("lcn", "LowCardinality(Nullable(String))", new ArrayColumn<string>("lcn", "LowCardinality(Nullable(String))",
                Enumerable.Range(0, rows).Select(i => i % 10 == 0 ? null : $"tag-{i % 30}").ToArray())));
        }

        return new Block(string.Empty, BlockInfo.Default, rows, columns, ColumnCodecRegistry.Default, Context);
    }

    /// <summary>Writes through the current codec and decodes the bytes back, so the column is a real decoded one.</summary>
    private static IColumn Decode(string name, string type, IColumn ergonomic)
    {
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(type, Context);
        var stream = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(stream))
        {
            codec.WriteFull(writer, ergonomic);
            writer.FlushAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
        }

        stream.Position = 0;
        using var reader = new ClickHouseBinaryReader(stream);
        codec.ReadStatePrefixAsync(reader, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        return codec.ReadColumnAsync(reader, name, type, ergonomic.RowCount, CancellationToken.None).AsTask().GetAwaiter().GetResult();
    }

    public static (string Type, IColumn Column, Func<ClickHouseBinaryWriter, int> Candidate) Write(WriteShape shape, int rows)
    {
        switch (shape)
        {
            case WriteShape.LowCardinalityString_HighRepeat:
            case WriteShape.LowCardinalityString_LowRepeat:
            {
                int distinct = shape == WriteShape.LowCardinalityString_HighRepeat ? 100 : rows;
                string[] values = Enumerable.Range(0, rows).Select(i => $"category-{i % distinct}").ToArray();
                return Pair("LowCardinality(String)", values);
            }

            case WriteShape.LowCardinalityFixedStringBytes_HighRepeat:
            case WriteShape.LowCardinalityFixedStringBytes_LowRepeat:
            {
                int distinct = shape == WriteShape.LowCardinalityFixedStringBytes_HighRepeat ? 100 : rows;
                byte[][] values = Enumerable.Range(0, rows).Select(i => Encoding.ASCII.GetBytes($"key-{i % distinct:D12}")).ToArray();
                return Pair("LowCardinality(FixedString(16))", values);
            }

            case WriteShape.NullableDateTimeFromOffset:
                return Pair("Nullable(DateTime('UTC'))", Enumerable.Range(0, rows)
                    .Select(i => i % 5 == 0 ? (DateTimeOffset?)null : DateTimeOffset.FromUnixTimeSeconds(1_700_000_000 + i)).ToArray());

            default:
                return Pair("Array(String)", Enumerable.Range(0, rows)
                    .Select(i => Enumerable.Range(0, i % 5).Select(j => $"item-{i}-{j}").ToArray()).ToArray());
        }
    }

    private static (string, IColumn, Func<ClickHouseBinaryWriter, int>) Pair<T>(string type, T[] values)
    {
        ColumnWriter<T> candidate = WriteDerivation.Derive<T>(type);
        return (type, new ArrayColumn<T>("c", type, values), writer =>
        {
            candidate.WritePrefix(writer);
            candidate.Write(writer, values);
            return values.Length;
        });
    }

    public static byte[] Capture(Action<ClickHouseBinaryWriter> write)
    {
        var stream = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(stream))
        {
            write(writer);
            writer.FlushAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
        }

        return stream.ToArray();
    }
}

internal static class Verify
{
    public static int Run()
    {
        int failures = 0;
        const int rows = 10_000;

        foreach (ReadShape shape in Enum.GetValues<ReadShape>())
        {
            using Block block = Data.Block(shape, rows);
            for (int c = 0; c < block.ColumnCount; c++)
            {
                IColumn column = block[c];
                Type target = typeof(WideRow).GetProperty(column.Name).PropertyType;
                failures += (int)typeof(Verify).GetMethod(nameof(CompareColumn), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)
                    .MakeGenericMethod(target).Invoke(null, new object[] { block, c })!;
            }
        }

        using (Block wide = Data.Block(ReadShape.Wide, rows))
        {
            WideRow[] current = new WideRow[rows];
            PocoReadPlan<WideRow>.Build(PocoTypeDescriptor<WideRow>.Build(), wide, null).Materialize(wide, current, 0);
            WideRow[] candidate = new WideRow[rows];
            ConverterPocoPlan<WideRow>.Build(wide).Materialize(wide, candidate, 0, rows);
            for (int i = 0; i < rows; i++)
            {
                if (!SameRow(current[i], candidate[i]))
                {
                    Console.WriteLine($"POCO Wide: row {i} differs");
                    failures++;
                    break;
                }
            }
        }

        foreach (WriteShape shape in Enum.GetValues<WriteShape>())
        {
            (string type, IColumn column, var candidate) = Data.Write(shape, rows);
            IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(type, default);
            byte[] expected = Data.Capture(w => codec.WriteFull(w, column));
            byte[] actual = Data.Capture(w => candidate(w));
            bool same = expected.AsSpan().SequenceEqual(actual);
            Console.WriteLine($"write {shape,-45} {(same ? "same bytes" : $"DIFFERENT ({expected.Length} vs {actual.Length} bytes)")}");
            failures += same ? 0 : 1;
        }

        // A write the current client refuses: LowCardinality(FixedString(N)) from text has no key writer.
        string[] text = Enumerable.Range(0, 100).Select(i => $"k{i % 7}").ToArray();
        IColumnCodec lcFixed = ColumnCodecRegistry.Default.Resolve("LowCardinality(FixedString(4))", default);
        Console.WriteLine($"current CanWrite LowCardinality(FixedString(4)) from string: {lcFixed.CanWrite(new ArrayColumn<string>("c", "LowCardinality(FixedString(4))", text))}");
        byte[] fromText = Data.Capture(w =>
        {
            ColumnWriter<string> writer = WriteDerivation.Derive<string>("LowCardinality(FixedString(4))");
            writer.WritePrefix(w);
            writer.Write(w, text);
        });
        byte[] fromBytes = Data.Capture(w => lcFixed.WriteFull(w, new ArrayColumn<byte[]>("c", "LowCardinality(FixedString(4))",
            text.Select(t => { var b = new byte[4]; Encoding.UTF8.GetBytes(t, b); return b; }).ToArray())));
        bool fixedSame = fromText.AsSpan().SequenceEqual(fromBytes);
        Console.WriteLine($"candidate LowCardinality(FixedString(4)) from string == current from padded byte[]: {fixedSame}");
        failures += fixedSame ? 0 : 1;

        Console.WriteLine(failures == 0 ? "VERIFY OK" : $"VERIFY FAILED: {failures}");
        return failures == 0 ? 0 : 1;
    }

    private static int CompareColumn<T>(Block block, int c)
    {
        T[] current = block.ReadAs<T>(c).Values.ToArray();
        var candidate = new T[block.RowCount];
        ReadDerivation.Derive<T>(block[c].TypeName, block.Context).Bind(block[c]).Fill(0, candidate);
        bool same = current.Zip(candidate).All(p => SameValue(p.First, p.Second));
        Console.WriteLine($"read  {block[c].TypeName,-35} as {typeof(T).Name,-16} {(same ? "same values" : "DIFFERENT")}");
        return same ? 0 : 1;
    }

    private static bool SameRow(WideRow a, WideRow b)
        => SameValue(a.arr, b.arr) && SameValue(a.lc, b.lc) && a.ndt == b.ndt && a.s == b.s && a.dt == b.dt && a.lcn == b.lcn;

    private static bool SameValue(object a, object b) => (a, b) switch
    {
        (null, null) => true,
        (byte[] x, byte[] y) => x.AsSpan().SequenceEqual(y),
        (byte[][] x, byte[][] y) => x.Length == y.Length && x.Zip(y).All(p => p.First.AsSpan().SequenceEqual(p.Second)),
        (DateTimeOffset x, DateTimeOffset y) => x == y && x.Offset == y.Offset,
        _ => Equals(a, b),
    };
}

[MemoryDiagnoser]
public class ReadBenchmarks
{
    private Block block;
    private object currentPlan;
    private object candidatePlan;
    private Array[] columnarOut;

    [Params(100_000)]
    public int Rows { get; set; }

    [ParamsAllValues]
    public ReadShape Shape { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        block = Data.Block(Shape, Rows);
        switch (Shape)
        {
            case ReadShape.ArrayStringAsBytes: Plans<ArrayRow>(); break;
            case ReadShape.LowCardinalityStringAsBytes: Plans<LcRow>(); break;
            case ReadShape.NullableDateTimeAsOffset: Plans<NdtRow>(); break;
            default: Plans<WideRow>(); break;
        }
    }

    [GlobalCleanup]
    public void Cleanup() => block.Dispose();

    /// <summary>Current columnar tier: <c>Block.ReadAs&lt;T&gt;(i).Values</c> for every column.</summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Columnar")]
    public int Columnar_Current()
    {
        int n = 0;
        for (int c = 0; c < block.ColumnCount; c++)
        {
            n += ColumnarCurrent(c);
        }

        return n;
    }

    /// <summary>Candidate columnar tier: the derived reader bound and filled into a fresh array.</summary>
    [Benchmark]
    [BenchmarkCategory("Columnar")]
    public int Columnar_Candidate()
    {
        int n = 0;
        for (int c = 0; c < block.ColumnCount; c++)
        {
            n += ColumnarCandidate(c);
        }

        return n;
    }

    [Benchmark]
    [BenchmarkCategory("Poco")]
    public int Poco_Current() => Shape switch
    {
        ReadShape.ArrayStringAsBytes => PocoCurrent<ArrayRow>(),
        ReadShape.LowCardinalityStringAsBytes => PocoCurrent<LcRow>(),
        ReadShape.NullableDateTimeAsOffset => PocoCurrent<NdtRow>(),
        _ => PocoCurrent<WideRow>(),
    };

    [Benchmark]
    [BenchmarkCategory("Poco")]
    public int Poco_Candidate() => Shape switch
    {
        ReadShape.ArrayStringAsBytes => PocoCandidate<ArrayRow>(),
        ReadShape.LowCardinalityStringAsBytes => PocoCandidate<LcRow>(),
        ReadShape.NullableDateTimeAsOffset => PocoCandidate<NdtRow>(),
        _ => PocoCandidate<WideRow>(),
    };

    private void Plans<TRow>()
        where TRow : class, new()
    {
        currentPlan = PocoReadPlan<TRow>.Build(PocoTypeDescriptor<TRow>.Build(), block, null);
        candidatePlan = ConverterPocoPlan<TRow>.Build(block);
    }

    // POCO reads run in windows in the client; one whole-block window is the best case for both.
    private int PocoCurrent<TRow>()
        where TRow : class
    {
        var rows = new TRow[Rows];
        ((PocoReadPlan<TRow>)currentPlan).Materialize(block, rows, 0);
        return rows.Length;
    }

    private int PocoCandidate<TRow>()
        where TRow : class, new()
    {
        var rows = new TRow[Rows];
        ((ConverterPocoPlan<TRow>)candidatePlan).Materialize(block, rows, 0, Rows);
        return rows.Length;
    }

    private int ColumnarCurrent(int c) => block[c].Name switch
    {
        "arr" => block.ReadAs<byte[][]>(c).Values.Length,
        "lc" => block.ReadAs<byte[]>(c).Values.Length,
        "ndt" or "dt" when block[c].TypeName.StartsWith("Nullable") => block.ReadAs<DateTimeOffset?>(c).Values.Length,
        "dt" => block.ReadAs<DateTimeOffset>(c).Values.Length,
        _ => block.ReadAs<string>(c).Values.Length,
    };

    private int ColumnarCandidate(int c) => block[c].Name switch
    {
        "arr" => Fill<byte[][]>(c),
        "lc" => Fill<byte[]>(c),
        "ndt" => Fill<DateTimeOffset?>(c),
        "dt" => Fill<DateTimeOffset>(c),
        _ => Fill<string>(c),
    };

    private int Fill<T>(int c)
    {
        var values = new T[Rows];
        ReadDerivation.Derive<T>(block[c].TypeName, block.Context).Bind(block[c]).Fill(0, values);
        return values.Length;
    }
}

[MemoryDiagnoser]
public class WriteBenchmarks
{
    private IColumnCodec codec;
    private IColumn column;
    private Func<ClickHouseBinaryWriter, int> candidate;

    [Params(100_000)]
    public int Rows { get; set; }

    [ParamsAllValues]
    public WriteShape Shape { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        (string type, column, candidate) = Data.Write(Shape, Rows);
        codec = ColumnCodecRegistry.Default.Resolve(type, default);
    }

    [Benchmark(Baseline = true)]
    public long Current()
    {
        using var writer = new ClickHouseBinaryWriter(Stream.Null);
        codec.WriteFull(writer, column);
        return writer.BytesWritten;
    }

    [Benchmark]
    public long Candidate()
    {
        using var writer = new ClickHouseBinaryWriter(Stream.Null);
        candidate(writer);
        return writer.BytesWritten;
    }
}

public static class Program
{
    public static int Main(string[] args)
    {
        if (args.Length > 0 && args[0] == "verify")
        {
            return Verify.Run();
        }

        if (args.Length > 0 && args[0] == "quick")
        {
            Quick.Run(args.Length > 1 ? int.Parse(args[1]) : 41);
            return 0;
        }

        BenchmarkSwitcher.FromTypes(new[] { typeof(ReadBenchmarks), typeof(WriteBenchmarks) }).Run(args.Skip(1).ToArray());
        return 0;
    }
}

/// <summary>
/// The box this ran on is shared and loaded, so BenchmarkDotNet's means are noise. This runs the two arms of
/// each case alternately and reports the minimum and median, which are robust against background load.
/// </summary>
internal static class Quick
{
    public static void Run(int rounds)
    {
        Console.WriteLine($"{"case",-62} {"cur min",9} {"cand min",9} {"min ratio",9} {"cur med",9} {"cand med",9} {"med ratio",9}");
        foreach (ReadShape shape in Enum.GetValues<ReadShape>())
        {
            var bench = new ReadBenchmarks { Rows = 100_000, Shape = shape };
            bench.Setup();
            Report($"read  columnar {shape}", rounds, () => bench.Columnar_Current(), () => bench.Columnar_Candidate());
            Report($"read  poco     {shape}", rounds, () => bench.Poco_Current(), () => bench.Poco_Candidate());
            bench.Cleanup();
        }

        foreach (WriteShape shape in Enum.GetValues<WriteShape>())
        {
            var bench = new WriteBenchmarks { Rows = 100_000, Shape = shape };
            bench.Setup();
            Report($"write          {shape}", rounds, () => bench.Current(), () => bench.Candidate());
        }
    }

    private static void Report(string name, int rounds, Func<long> current, Func<long> candidate)
    {
        var a = new double[rounds];
        var b = new double[rounds];
        for (int i = 0; i < 5; i++)
        {
            current();
            candidate();
        }

        for (int i = 0; i < rounds; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            long t0 = System.Diagnostics.Stopwatch.GetTimestamp();
            current();
            long t1 = System.Diagnostics.Stopwatch.GetTimestamp();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            long t2 = System.Diagnostics.Stopwatch.GetTimestamp();
            candidate();
            long t3 = System.Diagnostics.Stopwatch.GetTimestamp();
            a[i] = (t1 - t0) * 1000.0 / System.Diagnostics.Stopwatch.Frequency;
            b[i] = (t3 - t2) * 1000.0 / System.Diagnostics.Stopwatch.Frequency;
        }

        Array.Sort(a);
        Array.Sort(b);
        double am = a[rounds / 2], bm = b[rounds / 2];
        Console.WriteLine($"{name,-62} {a[0],9:F2} {b[0],9:F2} {b[0] / a[0],9:F2} {am,9:F2} {bm,9:F2} {bm / am,9:F2}");
    }
}
