using System.Diagnostics;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// <see cref="ClickHouseTcpQueryCallbacks"/>: the metadata the server interleaves into a response —
/// <see cref="ClickHouseTcpQueryCallbacks.OnProgress"/> while the query is still running,
/// <see cref="ClickHouseTcpQueryCallbacks.OnProfileInfo"/> once with the execution summary, and
/// <see cref="ClickHouseTcpQueryCallbacks.OnProfileEvents"/> with the server's own performance counters.
///
/// <para>
/// This is what the native protocol has that HTTP does not. HTTP reports the same numbers in a trailing header,
/// after the response; here they arrive as packets between the data blocks, so a long query can drive a progress
/// bar while it runs.
/// </para>
///
/// <para>
/// <b>The contract matters more than the numbers.</b> A callback runs synchronously on the thread draining the
/// response, in packet order, so anything slow in one stalls the read. A callback that throws propagates out of
/// the operation and terminates the connection — this example does not demonstrate that, because there is nothing
/// to see: the result is simply gone. Keep them to counters and a log line, and never let one throw.
/// </para>
/// </summary>
public static class TcpProgressAndStatistics
{
    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();

        await ProgressArrivesDuringTheQuery(client);
        IncrementsNotTotals();
        await ProfileInfoOnce(client);
        await ProfileEvents(client);
        WhatElseIsThere();
    }

    private static async Task ProgressArrivesDuringTheQuery(ClickHouseTcpClient client)
    {
        Console.WriteLine("1. OnProgress arrives while the query runs\n");

        // The record of what happened, in the order it happened. Appending from the callback is safe without a
        // lock precisely because callbacks run on the thread draining the response — the same thread this loop
        // body runs on.
        var timeline = new List<string>();
        var clock = Stopwatch.StartNew();
        ClickHouseTcpProgress total = default;
        int packets = 0;
        int rowsSoFar = 0;
        int beforeTheLastRow = 0;

        var options = new ClickHouseTcpQueryOptions
        {
            Callbacks = new ClickHouseTcpQueryCallbacks
            {
                OnProgress = progress =>
                {
                    packets++;
                    total += progress;
                    if (rowsSoFar < 8)
                    {
                        beforeTheLastRow++;
                    }

                    timeline.Add($"progress  +{progress.Rows} rows  at {clock.ElapsedMilliseconds,4} ms");
                },
            },

            // interactive_delay is how often the server reports progress, in microseconds. The default is 100 ms;
            // 30 ms makes the interleaving obvious in an example short enough to run in CI.
            Settings = new Dictionary<string, string>
            {
                ["interactive_delay"] = "30000",
                ["max_block_size"] = "1",
            },
        };

        int rows = 0;
        await foreach (object[] row in client.QueryAsync(
            "SELECT number, sleepEachRow(0.04) FROM numbers(8)", options))
        {
            rows++;
            rowsSoFar = rows;
            timeline.Add($"row {rows}                    at {clock.ElapsedMilliseconds,4} ms");
        }

        Console.WriteLine("   8 rows, each taking the server 40 ms, one row per block:\n");
        foreach (string line in timeline)
        {
            Console.WriteLine($"     {line}");
        }

        Console.WriteLine();
        Console.WriteLine($"   {packets} progress packets and {rows} rows, interleaved — {beforeTheLastRow} of the packets arrived before the");
        Console.WriteLine("   last row, which is the whole point. On HTTP every one of those numbers arrives after");
        Console.WriteLine($"   the response. Summed: {total.Rows} rows, {total.Bytes} bytes, {total.ElapsedNs / 1_000_000} ms of server-side time.");
    }

    private static void IncrementsNotTotals()
    {
        Console.WriteLine("\n2. Every counter is an increment\n");

        // Two packets, added rather than replaced. Keeping the last one reports the most recent step, not the run.
        var first = new ClickHouseTcpProgress(rows: 100, bytes: 800, totalRows: 1000, wroteRows: 0, wroteBytes: 0, elapsedNs: 5_000_000);
        var next = new ClickHouseTcpProgress(rows: 250, bytes: 2000, totalRows: 500, wroteRows: 0, wroteBytes: 0, elapsedNs: 7_000_000);

        Console.WriteLine($"   packet 1        Rows={first.Rows,4} Bytes={first.Bytes,5} TotalRows={first.TotalRows}");
        Console.WriteLine($"   packet 2        Rows={next.Rows,4} Bytes={next.Bytes,5} TotalRows={next.TotalRows}");
        Console.WriteLine($"   first + next    Rows={(first + next).Rows,4} Bytes={(first + next).Bytes,5} TotalRows={(first + next).TotalRows}");
        Console.WriteLine();
        Console.WriteLine("   TotalRows is an increment too: it is the rise in the server's estimate of the rows");
        Console.WriteLine("   this query has to read, so a progress bar's denominator is the running sum of it and");
        Console.WriteLine("   can grow as the server learns more. Use operator + or ClickHouseTcpProgress.Add.");
        Console.WriteLine();
        Console.WriteLine("   WroteRows and WroteBytes are the insert side of the same packet. On 26.6 an insert");
        Console.WriteLine("   through this client produces no progress packets at all, so they read zero — a large");
        Console.WriteLine("   insert has no progress to report yet.");
    }

    private static async Task ProfileInfoOnce(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n3. OnProfileInfo, once, with totals rather than increments\n");

        ClickHouseTcpProfileInfo info = default;
        int calls = 0;

        var options = new ClickHouseTcpQueryOptions
        {
            Callbacks = new ClickHouseTcpQueryCallbacks
            {
                OnProfileInfo = summary =>
                {
                    info = summary;
                    calls++;
                },
            },
        };

        // A LIMIT, so that AppliedLimit and RowsBeforeLimit have something to say.
        int rows = 0;
        await foreach (object[] row in client.QueryAsync(
            "SELECT number FROM numbers(1000) ORDER BY number DESC LIMIT 5", options))
        {
            rows++;
        }

        Console.WriteLine($"   SELECT number FROM numbers(1000) ORDER BY number DESC LIMIT 5   ({rows} rows read)\n");
        Console.WriteLine($"     called                     {calls} time");
        Console.WriteLine($"     Rows                       {info.Rows}");
        Console.WriteLine($"     Blocks                     {info.Blocks}");
        Console.WriteLine($"     Bytes                      {info.Bytes}");
        Console.WriteLine($"     AppliedLimit               {info.AppliedLimit}");
        Console.WriteLine($"     RowsBeforeLimit            {info.RowsBeforeLimit}");
        Console.WriteLine($"     CalculatedRowsBeforeLimit  {info.CalculatedRowsBeforeLimit}");
        Console.WriteLine();
        Console.WriteLine("   RowsBeforeLimit is what a paging UI wants for its 'of N' — but only when");
        Console.WriteLine("   CalculatedRowsBeforeLimit is true. The server does not always work it out, and the");
        Console.WriteLine("   field is then zero rather than absent, so the flag is the one to read first.");
        Console.WriteLine();
        Console.WriteLine("   Bytes counts the result as the server measured it in memory, not the bytes that");
        Console.WriteLine("   crossed the socket. Tcp_024 measures those.");
    }

    private static async Task ProfileEvents(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n4. OnProfileEvents: the server's own counters, as it goes\n");

        // Two dictionaries, because the block carries two kinds of row. type 1 is an increment to add up; type 2
        // is a gauge reading that replaces the last one.
        var increments = new Dictionary<string, long>(StringComparer.Ordinal);
        var gauges = new Dictionary<string, long>(StringComparer.Ordinal);
        var threadIds = new HashSet<ulong>();
        int blocks = 0;

        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["interactive_delay"] = "30000", ["max_block_size"] = "1" },
            Callbacks = new ClickHouseTcpQueryCallbacks
            {
                OnProfileEvents = block =>
                {
                    blocks++;

                    // The block is borrowed: valid until the callback returns. Names have to be copied out (they
                    // are already strings); spans must not outlive it.
                    IColumn<string> name = block.Column<string>("name");
                    ReadOnlySpan<long> value = block.Column<long>("value").Values;
                    ReadOnlySpan<sbyte> type = block.Column<sbyte>("type").Values;
                    ReadOnlySpan<ulong> thread = block.Column<ulong>("thread_id").Values;

                    for (int row = 0; row < block.RowCount; row++)
                    {
                        threadIds.Add(thread[row]);
                        if (type[row] == 1)
                        {
                            increments.TryGetValue(name[row], out long soFar);
                            increments[name[row]] = soFar + value[row];
                        }
                        else
                        {
                            gauges[name[row]] = value[row];
                        }
                    }
                },
            },
        };

        await foreach (object[] row in client.QueryAsync("SELECT number, sleepEachRow(0.03) FROM numbers(8)", options))
        {
        }

        Console.WriteLine($"   {blocks} blocks of counters arrived during the query, {increments.Count} distinct increments and");
        Console.WriteLine($"   {gauges.Count} gauges. thread_id values seen: {string.Join(", ", threadIds.Order())} — 0 is the query-wide total.\n");

        foreach (string counter in new[] { "SelectedRows", "SelectedBytes", "SleepFunctionMicroseconds", "NetworkSendBytes", "RealTimeMicroseconds" })
        {
            string reading = increments.TryGetValue(counter, out long sum) ? sum.ToString("N0") : "(not reported)";
            Console.WriteLine($"     increment  {counter,-26} {reading,12}");
        }

        foreach (string gauge in gauges.Keys.Order())
        {
            Console.WriteLine($"     gauge      {gauge,-26} {gauges[gauge],12:N0}");
        }

        Console.WriteLine();
        Console.WriteLine("   Every counter in system.events and system.metrics can appear here, so this is the");
        Console.WriteLine("   whole of what the server knows about its own work on this query. Reading `name`");
        Console.WriteLine("   allocates a string per row and the same counter arrives on every packet, so pick the");
        Console.WriteLine("   handful you care about rather than keeping them all.");
    }

    private static void WhatElseIsThere()
    {
        Console.WriteLine("\n5. The rest of the record\n");
        Console.WriteLine("   OnLog          the server's own log lines, when the query sets send_logs_level.");
        Console.WriteLine("                  priority is a Poco severity, so a lower number is more severe.");
        Console.WriteLine("   OnTotals       the WITH TOTALS row, in the query's own result shape.");
        Console.WriteLine("   OnExtremes     two rows, the minimum and the maximum, when the extremes setting is on.");
        Console.WriteLine();
        Console.WriteLine("   All three hand over a borrowed Block on the same contract as StreamAsync: copy out");
        Console.WriteLine("   what must outlive the callback, and retain neither the block nor a span over it.");
        Console.WriteLine();
        Console.WriteLine("   An unset callback costs nothing beyond the discarded result. The packets are decoded");
        Console.WriteLine("   either way, because skipping one would leave the connection misaligned.");
    }
}
