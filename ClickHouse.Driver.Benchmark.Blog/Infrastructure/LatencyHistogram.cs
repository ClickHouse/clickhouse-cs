using System;
using System.Numerics;

namespace ClickHouse.Driver.Benchmark.Blog.Infrastructure;

/// <summary>
/// Fixed-size, allocation-free latency histogram. Log-linear buckets over microseconds.
/// </summary>
/// <remarks>
/// <para>
/// The overhead-bound family reports p50/p95/p99, and the tail is the whole point — a mean would hide
/// exactly the connection-pool and GC-pause effects the family exists to expose. But recording a
/// timestamp per query into a growable list would allocate megabytes <i>inside</i> the measured
/// region and corrupt the allocation numbers next to it.
/// </para>
/// <para>
/// So: one pre-allocated <c>int[]</c> per worker, sized at construction, never resized, never
/// allocating on <see cref="Record"/>. Precision is ~1/16 of a binary decade (about 6%), which is far
/// finer than the run-to-run variance of anything measured here.
/// </para>
/// </remarks>
internal sealed class LatencyHistogram
{
    /// <summary>Sub-buckets per binary decade. 16 gives ~6% relative precision.</summary>
    private const int SubBucketBits = 4;
    private const int SubBuckets = 1 << SubBucketBits;

    /// <summary>Decades covered: 1 µs to ~2^40 µs (~12 days). Nothing here will exceed it.</summary>
    private const int Decades = 41;

    private readonly int[] counts = new int[Decades * SubBuckets];

    public long Count { get; private set; }

    public long TotalMicroseconds { get; private set; }

    public long MaxMicroseconds { get; private set; }

    /// <summary>Records one observation. Allocation-free and branch-light; safe on the hot path.</summary>
    public void Record(long microseconds)
    {
        if (microseconds < 0)
            microseconds = 0;

        Count++;
        TotalMicroseconds += microseconds;
        if (microseconds > MaxMicroseconds)
            MaxMicroseconds = microseconds;

        counts[IndexOf(microseconds)]++;
    }

    /// <summary>Folds another histogram in, so per-worker histograms combine without locking.</summary>
    public void Add(LatencyHistogram other)
    {
        for (var i = 0; i < counts.Length; i++)
            counts[i] += other.counts[i];

        Count += other.Count;
        TotalMicroseconds += other.TotalMicroseconds;
        MaxMicroseconds = Math.Max(MaxMicroseconds, other.MaxMicroseconds);
    }

    public void Reset()
    {
        Array.Clear(counts);
        Count = 0;
        TotalMicroseconds = 0;
        MaxMicroseconds = 0;
    }

    public double MeanMicroseconds => Count == 0 ? 0d : (double)TotalMicroseconds / Count;

    /// <summary>
    /// Nearest-rank percentile, returning the upper bound of the containing bucket — i.e. it never
    /// understates. <paramref name="quantile"/> is in [0, 1].
    /// </summary>
    public double Percentile(double quantile)
    {
        if (Count == 0)
            return 0d;

        var target = (long)Math.Ceiling(quantile * Count);
        if (target < 1)
            target = 1;

        long seen = 0;
        for (var i = 0; i < counts.Length; i++)
        {
            seen += counts[i];
            if (seen >= target)
                return UpperBoundOf(i);
        }

        return MaxMicroseconds;
    }

    /// <summary>Records mean/p50/p95/p99/max plus the observation count into the side-channel CSV.</summary>
    public void RecordTo(string benchmark, string args, string prefix)
    {
        SideMetrics.Record(benchmark, args, prefix + "_count", Count);
        SideMetrics.Record(benchmark, args, prefix + "_mean_us", MeanMicroseconds);
        SideMetrics.Record(benchmark, args, prefix + "_p50_us", Percentile(0.50));
        SideMetrics.Record(benchmark, args, prefix + "_p95_us", Percentile(0.95));
        SideMetrics.Record(benchmark, args, prefix + "_p99_us", Percentile(0.99));
        SideMetrics.Record(benchmark, args, prefix + "_max_us", MaxMicroseconds);
    }

    private static int IndexOf(long value)
    {
        if (value < SubBuckets)
            return (int)value;

        var decade = 63 - BitOperations.LeadingZeroCount((ulong)value);
        var shift = decade - SubBucketBits;
        var sub = (int)((value >> shift) & (SubBuckets - 1));
        var index = ((decade - SubBucketBits + 1) * SubBuckets) + sub;

        return Math.Min(index, (Decades * SubBuckets) - 1);
    }

    /// <summary>Inclusive upper bound of a bucket, in microseconds.</summary>
    private static double UpperBoundOf(int index)
    {
        if (index < SubBuckets)
            return index;

        var block = (index / SubBuckets) - 1;
        var sub = index % SubBuckets;

        // Bucket [block, sub] covers ((SubBuckets + sub) << block) .. ((SubBuckets + sub + 1) << block) - 1
        return (double)(((long)SubBuckets + sub + 1) << block) - 1;
    }
}
