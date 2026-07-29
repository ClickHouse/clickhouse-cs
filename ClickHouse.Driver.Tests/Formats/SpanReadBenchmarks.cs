using System;
using System.Diagnostics;
using System.IO;
using ClickHouse.Driver.Formats;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Formats;

/// <summary>
/// Measures per-scalar read cost through the real reader stream chain
/// (<see cref="PooledReadBufferStream"/> → optional <see cref="ExceptionTagAwareStream"/> →
/// <see cref="PeekableStreamWrapper"/> → <see cref="System.IO.BinaryReader"/>) with a
/// <see cref="MemoryStream"/> at the bottom, so there is no network or server noise.
///
/// The read loops are written out per type rather than behind a delegate: a
/// <c>Func&lt;..., object&gt;</c> would box every value and the boxing would dominate the measurement.
///
/// <c>[Explicit]</c> — never runs in CI.
/// </summary>
[TestFixture]
[Explicit("Benchmark, not an assertion. Run manually.")]
public class SpanReadBenchmarks
{
    private const int Reps = 5;
    private const int PayloadBytes = 32 * 1024 * 1024;

    [Test]
    public void BenchmarkScalarReads()
    {
        Console.WriteLine($"{PayloadBytes / (1024 * 1024)} MiB payload per case, min of {Reps}");
        Console.WriteLine($"{"scalar",-8} {"exceptionTag",13} {"ns/value",10} {"MB/s",9}");
        Console.WriteLine(new string('-', 45));

        foreach (var tagged in new[] { false, true })
        {
            Run("Int16", sizeof(short), tagged);
            Run("Int32", sizeof(int), tagged);
            Run("Int64", sizeof(long), tagged);
            Run("Double", sizeof(double), tagged);
        }
    }

    private static void Run(string name, int width, bool tagged)
    {
        var payload = new byte[PayloadBytes];
        for (var i = 0; i < payload.Length; i++)
            payload[i] = (byte)((i * 31 + 7) & 0xFF);
        var count = payload.Length / width;

        // Warm up this exact chain + loop shape before timing.
        ReadAll(name, payload, tagged, Math.Min(count, 200_000));

        var best = double.MaxValue;
        for (var rep = 0; rep < Reps; rep++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            var sw = Stopwatch.StartNew();
            ReadAll(name, payload, tagged, count);
            sw.Stop();
            best = Math.Min(best, sw.Elapsed.TotalMilliseconds);
        }

        Console.WriteLine($"{name,-8} {(tagged ? "yes" : "no"),13} " +
                          $"{best * 1_000_000.0 / count,10:F2} " +
                          $"{payload.Length / (best / 1000.0) / (1024 * 1024),9:F0}");
    }

    private static void ReadAll(string name, byte[] payload, bool tagged, int count)
    {
        using var source = new MemoryStream(payload, writable: false);
        using var buffered = new PooledReadBufferStream(source, 64 * 1024, leaveOpen: true);

        Stream forReader = buffered;
        ExceptionTagAwareStream tagStream = null;
        if (tagged)
        {
            tagStream = new ExceptionTagAwareStream(buffered, "BENCHTOKEN");
            forReader = tagStream;
        }

        try
        {
            using var reader = new ExtendedBinaryReader(forReader);

            // Accumulate so the JIT cannot elide the reads.
            long sink = 0;
            switch (name)
            {
                case "Int16":
                    for (var i = 0; i < count; i++) sink += reader.ReadInt16();
                    break;
                case "Int32":
                    for (var i = 0; i < count; i++) sink += reader.ReadInt32();
                    break;
                case "Int64":
                    for (var i = 0; i < count; i++) sink += reader.ReadInt64();
                    break;
                case "Double":
                    for (var i = 0; i < count; i++) sink += (long)reader.ReadDouble();
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(name), name, "unknown scalar");
            }

            if (sink == long.MinValue)
                Console.WriteLine("unreachable");
        }
        finally
        {
            tagStream?.Dispose();
        }
    }
}
