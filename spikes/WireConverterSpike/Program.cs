// SPIKE for ClickHouse/integrations#801. Not production code.
//
//   dotnet run -c Release -- verify          compare every candidate result with the current client
//   dotnet run -c Release -- quick [rounds]  interleaved min/median timings (SPIKE_ONLY=Shape,... to filter)
//   dotnet run -c Release -- bench --filter X  BenchmarkDotNet
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

public sealed class SRow
{
    public string s { get; set; }
}

public sealed class DtRow
{
    public DateTimeOffset dt { get; set; }
}

public sealed class LcnRow
{
    public string lcn { get; set; }
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
    StringAsString,
    DateTimeAsOffset,
    LowCardinalityNullableStringAsString,
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

        if (shape is ReadShape.StringAsString)
        {
            columns.Add(Decode("s", "String", new ArrayColumn<string>("s", "String",
                Enumerable.Range(0, rows).Select(i => $"text-{i}").ToArray())));
        }

        if (shape is ReadShape.DateTimeAsOffset)
        {
            columns.Add(Decode("dt", "DateTime('UTC')", new ArrayColumn<DateTimeOffset>("dt", "DateTime('UTC')",
                Enumerable.Range(0, rows).Select(i => DateTimeOffset.FromUnixTimeSeconds(1_600_000_000 + i)).ToArray())));
        }

        if (shape is ReadShape.LowCardinalityNullableStringAsString)
        {
            columns.Add(Decode("lcn", "LowCardinality(Nullable(String))", new ArrayColumn<string>("lcn", "LowCardinality(Nullable(String))",
                Enumerable.Range(0, rows).Select(i => i % 10 == 0 ? null : $"tag-{i % 30}").ToArray())));
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
            WideRow[] fused = new WideRow[rows];
            FusedPocoPlan<WideRow> fusedPlan = FusedPocoPlan<WideRow>.Build(wide);
            for (int start = 0; start < rows; start += 4096)
            {
                // Windows, as the client reads, so the dictionary cache and the start offset are both exercised.
                int n = Math.Min(4096, rows - start);
                var window = new WideRow[n];
                fusedPlan.Materialize(wide, window, start, n);
                Array.Copy(window, 0, fused, start, n);
            }

            int bad = 0;
            for (int i = 0; i < rows; i++)
            {
                bad += SameRow(current[i], candidate[i]) ? 0 : 1;
                bad += SameRow(current[i], fused[i]) ? 0 : 1;
            }

            Console.WriteLine($"poco  Wide, bulk and fused (windows of 4096)    {(bad == 0 ? "same rows" : $"{bad} rows DIFFERENT")}");
            failures += bad == 0 ? 0 : 1;
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

        // Lone surrogates: different strings, same UTF-8 (EF BF BD). The current writer keys on the string and
        // keeps two entries; the candidate interns the canonical bytes and keeps one. Both are valid on the wire.
        string[] surrogates = { "\uD800", "\uDBFF", "a", "\uD800" };
        IColumnCodec lcString = ColumnCodecRegistry.Default.Resolve("LowCardinality(String)", default);
        byte[] currentSurrogates = Data.Capture(w => lcString.WriteFull(w, new ArrayColumn<string>("c", "LowCardinality(String)", surrogates)));
        byte[] candidateSurrogates = Data.Capture(w =>
        {
            ColumnWriter<string> writer = WriteDerivation.Derive<string>("LowCardinality(String)");
            writer.WritePrefix(w);
            writer.Write(w, surrogates);
        });
        Console.WriteLine($"LowCardinality(String) from lone surrogates: dictionary size current {DictionarySize(currentSurrogates)}, candidate {DictionarySize(candidateSurrogates)} (expected 4 and 3)");
        failures += DictionarySize(currentSurrogates) == 4 && DictionarySize(candidateSurrogates) == 3 ? 0 : 1;

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

    // State prefix (8 bytes), flags (8 bytes), then the dictionary size.
    private static long DictionarySize(byte[] wire) => BitConverter.ToInt64(wire, 16);

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

// One invocation per iteration, each on a freshly decoded block: the current client's String and
// LowCardinality columns cache their decoded values, so a reused block measures cache hits.
[MemoryDiagnoser]
[InvocationCount(1, 1)]
[WarmupCount(5)]
[IterationCount(30)]
public class ReadBenchmarks
{
    private Block block;
    private object currentPlan;
    private object candidatePlan;
    private object fusedPlan;
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
            case ReadShape.StringAsString: Plans<SRow>(); break;
            case ReadShape.DateTimeAsOffset: Plans<DtRow>(); break;
            case ReadShape.LowCardinalityNullableStringAsString: Plans<LcnRow>(); break;
            default: Plans<WideRow>(); break;
        }
    }

    [GlobalCleanup]
    public void Cleanup() => block.Dispose();

    /// <summary>
    /// Replaces the block with a freshly decoded one. <c>StringColumn</c> and the LowCardinality columns cache
    /// their decoded values on first access, so a reused block measures cache hits for the current client.
    /// </summary>
    [IterationSetup]
    public void Fresh()
    {
        block.Dispose();
        block = Data.Block(Shape, Rows);
    }

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
        ReadShape.StringAsString => PocoCurrent<SRow>(),
        ReadShape.DateTimeAsOffset => PocoCurrent<DtRow>(),
        ReadShape.LowCardinalityNullableStringAsString => PocoCurrent<LcnRow>(),
        _ => PocoCurrent<WideRow>(),
    };

    [Benchmark]
    [BenchmarkCategory("Poco")]
    public int Poco_Candidate() => Shape switch
    {
        ReadShape.ArrayStringAsBytes => PocoCandidate<ArrayRow>(),
        ReadShape.LowCardinalityStringAsBytes => PocoCandidate<LcRow>(),
        ReadShape.NullableDateTimeAsOffset => PocoCandidate<NdtRow>(),
        ReadShape.StringAsString => PocoCandidate<SRow>(),
        ReadShape.DateTimeAsOffset => PocoCandidate<DtRow>(),
        ReadShape.LowCardinalityNullableStringAsString => PocoCandidate<LcnRow>(),
        _ => PocoCandidate<WideRow>(),
    };

    [Benchmark]
    [BenchmarkCategory("Poco")]
    public int Poco_Fused() => Shape switch
    {
        ReadShape.ArrayStringAsBytes => PocoFused<ArrayRow>(),
        ReadShape.LowCardinalityStringAsBytes => PocoFused<LcRow>(),
        ReadShape.NullableDateTimeAsOffset => PocoFused<NdtRow>(),
        ReadShape.StringAsString => PocoFused<SRow>(),
        ReadShape.DateTimeAsOffset => PocoFused<DtRow>(),
        ReadShape.LowCardinalityNullableStringAsString => PocoFused<LcnRow>(),
        _ => PocoFused<WideRow>(),
    };

    private void Plans<TRow>()
        where TRow : class, new()
    {
        currentPlan = PocoReadPlan<TRow>.Build(PocoTypeDescriptor<TRow>.Build(), block, null);
        candidatePlan = ConverterPocoPlan<TRow>.Build(block);
        fusedPlan = FusedPocoPlan<TRow>.Build(block);
    }

    private int PocoFused<TRow>()
        where TRow : class, new()
    {
        var rows = new TRow[Rows];
        ((FusedPocoPlan<TRow>)fusedPlan).Materialize(block, rows, 0, Rows);
        return rows.Length;
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

        if (args.Length > 0 && args[0] == "issue792")
        {
            return Issue792.Run();
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
        string only = Environment.GetEnvironmentVariable("SPIKE_ONLY");
        foreach (ReadShape shape in Enum.GetValues<ReadShape>())
        {
            if (only is not null && !only.Split(',').Contains(shape.ToString()))
            {
                continue;
            }

            var bench = new ReadBenchmarks { Rows = 100_000, Shape = shape };
            bench.Setup();
            Report($"read  columnar {shape}", rounds, () => bench.Columnar_Current(), () => bench.Columnar_Candidate(), bench.Fresh);
            Report($"read  poco     {shape}", rounds, () => bench.Poco_Current(), () => bench.Poco_Candidate(), bench.Fresh);
            Report($"read  poco fused {shape}", rounds, () => bench.Poco_Current(), () => bench.Poco_Fused(), bench.Fresh);
            bench.Cleanup();
        }

        foreach (WriteShape shape in Enum.GetValues<WriteShape>())
        {
            if (only is not null && !only.Split(',').Contains(shape.ToString()))
            {
                continue;
            }

            var bench = new WriteBenchmarks { Rows = 100_000, Shape = shape };
            bench.Setup();
            Report($"write          {shape}", rounds, () => bench.Current(), () => bench.Candidate());
        }
    }

    private static void Report(string name, int rounds, Func<long> current, Func<long> candidate, Action fresh = null)
    {
        fresh ??= static () => { };
        var a = new double[rounds];
        var b = new double[rounds];
        for (int i = 0; i < 5; i++)
        {
            fresh();
            current();
            fresh();
            candidate();
        }

        for (int i = 0; i < rounds; i++)
        {
            fresh();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            long t0 = System.Diagnostics.Stopwatch.GetTimestamp();
            current();
            long t1 = System.Diagnostics.Stopwatch.GetTimestamp();
            fresh();
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

/// <summary>
/// ClickHouse/integrations#792: writing byte[] into LowCardinality(String) and the composites around it.
/// </summary>
internal static class Issue792
{
    private static readonly byte[][] Valid = { Encoding.UTF8.GetBytes("a"), Encoding.UTF8.GetBytes("bc"), Encoding.UTF8.GetBytes("a"), Array.Empty<byte>() };

    // 0xFF is not valid UTF-8, so no string can carry these bytes.
    private static readonly byte[][] Invalid = { new byte[] { 0x41, 0xFF }, new byte[] { 0xFE }, new byte[] { 0x41, 0xFF } };

    public static int Run()
    {
        int failures = 0;
        foreach (string type in new[] { "LowCardinality(String)", "LowCardinality(Nullable(String))", "Array(LowCardinality(String))" })
        {
            Type element = type.StartsWith("Array") ? typeof(byte[][]) : typeof(byte[]);
            Console.WriteLine($"current CanWrite {type,-36} from {element.Name,-9}: {ClickHouseTcpTypes.CanWrite(type, element)}");
        }

        // Valid UTF-8: the candidate's bytes must equal the current client's write of the same text.
        failures += SameAsText("LowCardinality(String)", Valid, Valid.Select(Text).ToArray());
        byte[][] withNull = Valid.Append(null).ToArray();
        failures += SameAsText("LowCardinality(Nullable(String))", withNull, withNull.Select(b => b is null ? null : Text(b)).ToArray());
        byte[][][] nested = { Valid, Array.Empty<byte[]>(), Valid[..2] };
        failures += SameAsText("Array(LowCardinality(String))", nested, nested.Select(r => r.Select(Text).ToArray()).ToArray());

        // Invalid UTF-8: decode the candidate's bytes with the current codec and read them back as byte[].
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve("LowCardinality(String)", default);
        byte[] wire = Data.Capture(w =>
        {
            ColumnWriter<byte[]> writer = WriteDerivation.Derive<byte[]>("LowCardinality(String)");
            writer.WritePrefix(w);
            writer.Write(w, Invalid);
        });
        using var reader = new ClickHouseBinaryReader(new MemoryStream(wire));
        codec.ReadStatePrefixAsync(reader, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        using IColumn decoded = codec.ReadColumnAsync(reader, "c", "LowCardinality(String)", Invalid.Length, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        byte[][] back = new byte[Invalid.Length][];
        ReadDerivation.Derive<byte[]>("LowCardinality(String)", default).Bind(decoded).Fill(0, back);
        bool same = back.Zip(Invalid).All(p => p.First.AsSpan().SequenceEqual(p.Second));
        int entries = ((ILowCardinalityColumn)decoded).Dictionary.RowCount;
        Console.WriteLine($"candidate LowCardinality(String) from invalid UTF-8 byte[]: round trip {(same ? "same bytes" : "DIFFERENT")}, {entries} dictionary entries (1 reserved + 2 distinct)");
        failures += same && entries == 3 ? 0 : 1;

        Console.WriteLine(failures == 0 ? "ISSUE792 OK" : $"ISSUE792 FAILED: {failures}");
        return failures == 0 ? 0 : 1;
    }

    private static string Text(byte[] bytes) => Encoding.UTF8.GetString(bytes);

    private static int SameAsText<TBytes, TText>(string type, TBytes[] bytes, TText[] text)
    {
        IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(type, default);
        byte[] expected = Data.Capture(w => codec.WriteFull(w, new ArrayColumn<TText>("c", type, text)));
        byte[] actual = Data.Capture(w =>
        {
            ColumnWriter<TBytes> writer = WriteDerivation.Derive<TBytes>(type);
            writer.WritePrefix(w);
            writer.Write(w, bytes);
        });

        // If the current client accepts byte[] (after a fix), its bytes must match too.
        string current = "refused";
        IColumn byteColumn = new ArrayColumn<TBytes>("c", type, bytes);
        if (codec.CanWrite(byteColumn))
        {
            byte[] currentBytes = Data.Capture(w => codec.WriteFull(w, byteColumn));
            current = currentBytes.AsSpan().SequenceEqual(expected) ? "same bytes as text" : "DIFFERENT from text";
        }

        bool same = expected.AsSpan().SequenceEqual(actual);
        Console.WriteLine($"{type,-36} from byte[]: candidate {(same ? "same bytes as text" : "DIFFERENT")}; current {current}");
        return same && !current.StartsWith("DIFFERENT") ? 0 : 1;
    }
}
