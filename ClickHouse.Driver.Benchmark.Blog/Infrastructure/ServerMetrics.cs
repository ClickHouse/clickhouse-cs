using System;
using System.Globalization;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Benchmark.Blog.Infrastructure;

/// <summary>
/// What one query (or one batch of them) cost the <i>server</i>, read back from
/// <c>system.query_log</c>.
/// </summary>
/// <param name="Queries">Matched <c>QueryFinish</c> rows. Zero means the lookup found nothing.</param>
/// <param name="CpuMicroseconds">
/// <c>ProfileEvents['OSCPUVirtualTimeMicroseconds']</c> — the metric the codec matrix is built on. It
/// is transport-independent, so it cannot be confounded by network conditions, and on a managed
/// service it is what the user literally pays for.
/// </param>
/// <param name="CpuWaitMicroseconds">
/// <c>ProfileEvents['OSCPUWaitMicroseconds']</c> — time threads were runnable but not scheduled.
/// Not a cost metric: a <b>validity</b> metric. If this is a large fraction of
/// <see cref="CpuMicroseconds"/> the server was contended and the timing is not publishable.
/// </param>
/// <param name="NetworkSendBytes">
/// <c>ProfileEvents['NetworkSendBytes']</c> — bytes the server actually wrote to the socket, i.e.
/// post-compression, plus HTTP framing. An independent check on the client-side wire count.
/// </param>
/// <param name="NetworkReceiveBytes"><c>ProfileEvents['NetworkReceiveBytes']</c>; the insert direction.</param>
/// <param name="ReadRows">Rows the server read.</param>
/// <param name="ReadBytes">Uncompressed bytes the server read from storage.</param>
/// <param name="ResultBytes">Uncompressed size of the result before any HTTP compression.</param>
/// <param name="MemoryUsage">Peak server memory for the query.</param>
/// <param name="DurationMilliseconds">Server-side duration, excluding client and network time.</param>
internal readonly record struct ServerCost(
    long Queries,
    long CpuMicroseconds,
    long CpuWaitMicroseconds,
    long NetworkSendBytes,
    long NetworkReceiveBytes,
    long ReadRows,
    long ReadBytes,
    long ResultBytes,
    long MemoryUsage,
    long DurationMilliseconds)
{
    /// <summary>
    /// Compression ratio the server achieved on the response, or 0 when nothing was sent. Derived
    /// rather than measured, so it can never disagree with the two byte counts it comes from.
    /// </summary>
    public double ResponseCompressionRatio =>
        NetworkSendBytes > 0 ? (double)ResultBytes / NetworkSendBytes : 0d;

    /// <summary>
    /// Scheduling pressure as a fraction of CPU time. Above roughly 0.25 the box was busy with
    /// something else and any wall-clock number from the same run should be thrown away.
    /// </summary>
    public double CpuWaitRatio =>
        CpuMicroseconds > 0 ? (double)CpuWaitMicroseconds / CpuMicroseconds : 0d;
}

/// <summary>
/// Reads per-query server cost out of <c>system.query_log</c> without racing the server.
/// </summary>
/// <remarks>
/// <para>
/// This is the benchmark-side counterpart of <c>ClickHouse.Driver.Tests/Utilities/QueryLog.cs</c>,
/// which cannot be reused here: it lives in the test assembly and reports failures through NUnit's
/// <c>Assert.Fail</c>.
/// </para>
/// <para>
/// <b>The flush race is real and fails quietly.</b> <c>SYSTEM FLUSH LOGS</c> only flushes what the
/// server has already queued, and a query's <c>QueryFinish</c> record is queued independently of its
/// HTTP response reaching the client. A single flush issued right after the query can therefore miss
/// it — and because this helper sums over matched rows, a miss shows up as a <i>smaller number</i>,
/// not as a missing one. Always check <see cref="ServerCost.Queries"/> against the number of queries
/// the arm actually issued; <see cref="MeasureAsync"/> does that for you.
/// </para>
/// <para>
/// Identify queries by <c>query_id</c>, never by a marker in the query text: a text match would also
/// match this helper's own lookups.
/// </para>
/// </remarks>
internal static class ServerMetrics
{
    /// <summary>Flush-and-read attempts before giving up.</summary>
    public const int MaxAttempts = 5;

    private static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// Sums the server cost of every query whose id is <paramref name="queryIdPrefix"/> or starts with
    /// <c>"{queryIdPrefix}-"</c>.
    /// </summary>
    /// <remarks>
    /// The suffix form is not optional. <c>InsertBinaryAsync</c> splits a batch across requests and
    /// gives each one <c>{queryId}-N</c>, so an exact-match lookup silently reports the cost of no
    /// batches at all for a multi-batch insert.
    /// </remarks>
    /// <param name="client">Client used for the flush and the lookup.</param>
    /// <param name="queryIdPrefix">Base query id handed to <c>QueryOptions.QueryId</c>.</param>
    /// <param name="expectedQueries">
    /// How many <c>QueryFinish</c> rows the caller issued. Retrying stops once at least this many are
    /// visible. Pass the real count — it is the only defence against summing a partial flush.
    /// </param>
    public static async Task<ServerCost> ReadAsync(ClickHouseClient client, string queryIdPrefix, long expectedQueries = 1)
    {
        var escaped = queryIdPrefix.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("'", "\\'", StringComparison.Ordinal);

        var sql = $@"
SELECT count()                                                  AS queries,
       sum(ProfileEvents['OSCPUVirtualTimeMicroseconds'])       AS cpu_us,
       sum(ProfileEvents['OSCPUWaitMicroseconds'])              AS cpu_wait_us,
       sum(ProfileEvents['NetworkSendBytes'])                   AS net_send,
       sum(ProfileEvents['NetworkReceiveBytes'])                AS net_recv,
       sum(read_rows)                                           AS read_rows,
       sum(read_bytes)                                          AS read_bytes,
       sum(result_bytes)                                        AS result_bytes,
       max(memory_usage)                                        AS memory_usage,
       sum(query_duration_ms)                                   AS duration_ms
FROM system.query_log
WHERE type = 'QueryFinish'
  AND (query_id = '{escaped}' OR startsWith(query_id, '{escaped}-'))";

        var cost = default(ServerCost);

        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            await client.ExecuteNonQueryAsync("SYSTEM FLUSH LOGS");

            using var reader = await client.ExecuteReaderAsync(sql);
            if (reader.Read())
            {
                cost = new ServerCost(
                    Queries: ToLong(reader.GetValue(0)),
                    CpuMicroseconds: ToLong(reader.GetValue(1)),
                    CpuWaitMicroseconds: ToLong(reader.GetValue(2)),
                    NetworkSendBytes: ToLong(reader.GetValue(3)),
                    NetworkReceiveBytes: ToLong(reader.GetValue(4)),
                    ReadRows: ToLong(reader.GetValue(5)),
                    ReadBytes: ToLong(reader.GetValue(6)),
                    ResultBytes: ToLong(reader.GetValue(7)),
                    MemoryUsage: ToLong(reader.GetValue(8)),
                    DurationMilliseconds: ToLong(reader.GetValue(9)));
            }

            if (cost.Queries >= expectedQueries)
                return cost;

            if (attempt < MaxAttempts)
                await Task.Delay(RetryDelay);
        }

        return cost;
    }

    /// <summary>
    /// <see cref="ReadAsync"/> plus the check that makes the number trustworthy: throws when fewer
    /// rows became visible than the caller issued, rather than returning a quietly-too-small sum.
    /// </summary>
    public static async Task<ServerCost> MeasureAsync(ClickHouseClient client, string queryIdPrefix, long expectedQueries = 1)
    {
        var cost = await ReadAsync(client, queryIdPrefix, expectedQueries);

        if (cost.Queries < expectedQueries)
        {
            throw new InvalidOperationException(
                $"system.query_log showed {cost.Queries} of {expectedQueries} expected QueryFinish rows for " +
                $"query id '{queryIdPrefix}' after {MaxAttempts} flushes. Server cost would be understated, " +
                $"so it is not reported. Raise ServerMetrics.MaxAttempts, or check that query_log is enabled " +
                $"(log_queries=1) on this endpoint.");
        }

        return cost;
    }

    /// <summary>A fresh, collision-proof base query id carrying a readable arm label.</summary>
    /// <remarks>
    /// The label makes an interrupted run diagnosable straight from <c>system.query_log</c>, which is
    /// worth more than it costs given these runs last minutes.
    /// </remarks>
    public static string NewQueryId(string label) =>
        $"blogbench-{Sanitize(label)}-{Guid.NewGuid():N}";

    private static string Sanitize(string label)
    {
        Span<char> buffer = stackalloc char[Math.Min(label.Length, 48)];
        for (var i = 0; i < buffer.Length; i++)
            buffer[i] = char.IsLetterOrDigit(label[i]) ? char.ToLowerInvariant(label[i]) : '_';

        return new string(buffer);
    }

    private static long ToLong(object value) =>
        value is null or DBNull ? 0L : Convert.ToInt64(value, CultureInfo.InvariantCulture);
}
