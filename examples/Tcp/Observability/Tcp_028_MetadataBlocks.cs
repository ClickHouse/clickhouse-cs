using ClickHouse.Driver.Tcp;
using Microsoft.Extensions.Logging;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// The three <see cref="Block"/>-shaped callbacks on <see cref="ClickHouseTcpQueryCallbacks"/>:
/// <see cref="ClickHouseTcpQueryCallbacks.OnLog"/> with <c>send_logs_level</c>,
/// <see cref="ClickHouseTcpQueryCallbacks.OnTotals"/> for <c>WITH TOTALS</c>, and
/// <see cref="ClickHouseTcpQueryCallbacks.OnExtremes"/> with the <c>extremes</c> setting. Tcp_021 covers the other
/// three, which hand over structs rather than blocks.
///
/// <para>
/// <b>Every one of these blocks is borrowed.</b> Its columns are views over pooled buffers that are released as
/// soon as the callback returns, so the rule for all three is the same: copy out what you need inside the
/// callback, and keep neither the block, its columns, nor a span over them. Every section below does the copying
/// in the callback and the printing afterwards, which is also what an application does — a callback runs
/// synchronously on the thread draining the response, so the less it does the better.
/// </para>
///
/// <para>
/// The contract otherwise is the one Tcp_021 states: in packet order, on the reading thread, and never allowed to
/// throw — an exception propagates out of the operation and terminates the connection. So keep the callback to
/// copying values out, and do the parsing that can fail somewhere it is allowed to. Even a copy has to be written
/// with that in mind: a named column lookup or a span index throws if the name or the row is not there.
/// </para>
/// </summary>
public static class TcpMetadataBlocks
{
    public static async Task Run()
    {
        WhatTurnsEachOneOn();

        await using var client = ExampleConfig.CreateTcpClient();

        await ServerLogLines(client);
        await BridgingThemIntoALogger(client);
        await HowMuchEachLevelSays(client);
        await TheTotalsRow(client);
        await TheExtremesRows(client);
        await NothingFiresWhenThereIsNothingToSend(client);
    }

    private static void WhatTurnsEachOneOn()
    {
        Console.WriteLine("Three callbacks, and what each one needs before the server sends anything:\n");
        Console.WriteLine("  OnLog        Settings[\"send_logs_level\"] = \"debug\" (or trace) — the default, fatal, is silent");
        Console.WriteLine("  OnTotals     WITH TOTALS in the query, right after GROUP BY");
        Console.WriteLine("  OnExtremes   Settings[\"extremes\"] = \"1\"");
        Console.WriteLine();
        Console.WriteLine("Setting the callback alone gets you nothing, and so does turning the feature on without the");
        Console.WriteLine("callback: the block is decoded either way, to keep the connection aligned, and then dropped.");
    }

    private static async Task ServerLogLines(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n1. OnLog: the server's own log, for this query only\n");

        // The copies. Everything that outlives the callback is in here, and nothing in here points into a block.
        var lines = new List<(sbyte Priority, string Source, string Text, uint EventTime)>();
        int blocks = 0;

        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["send_logs_level"] = "trace" },
            Callbacks = new ClickHouseTcpQueryCallbacks
            {
                OnLog = block =>
                {
                    blocks++;

                    // Named columns, so the order on the wire does not matter. The span is borrowed; the strings
                    // an IColumn<string> hands back are already copies.
                    ReadOnlySpan<sbyte> priority = block.Column<sbyte>("priority").Values;
                    ReadOnlySpan<uint> eventTime = block.Column<uint>("event_time").Values;
                    IColumn<string> source = block.Column<string>("source");
                    IColumn<string> text = block.Column<string>("text");

                    for (int row = 0; row < block.RowCount; row++)
                    {
                        lines.Add((priority[row], source[row], text[row], eventTime[row]));
                    }
                },
            },
        };

        _ = await client.ExecuteScalarAsync("SELECT count() FROM numbers(200000)", options);

        Console.WriteLine($"   {blocks} log blocks, {lines.Count} lines, all of them copied out before the blocks went back:\n");
        foreach ((sbyte priority, string source, string text, uint _) in lines.Take(8))
        {
            Console.WriteLine($"     {priority}  {source,-22} {(text.Length <= 78 ? text : text[..78] + "...")}");
        }

        if (lines.Count > 8)
        {
            Console.WriteLine($"     ... and {lines.Count - 8} more");
        }

        Console.WriteLine();
        Console.WriteLine("   The columns are event_time, event_time_microseconds, host_name, query_id, thread_id,");
        Console.WriteLine("   priority, source and text. event_time is a DateTime column, which on this tier is the");
        Console.WriteLine($"   integer the wire carried — {lines[0].EventTime} whole Unix seconds, so");
        Console.WriteLine($"   DateTimeOffset.FromUnixTimeSeconds gives {DateTimeOffset.FromUnixTimeSeconds(lines[0].EventTime):HH:mm:ss} UTC, with the sub-second part in the");
        Console.WriteLine("   microseconds column beside it.");
        Console.WriteLine();
        Console.WriteLine("   These are the same lines the server writes to its own log, so this is how a client gets");
        Console.WriteLine("   the server's account of one query without access to the server's log file — which is what");
        Console.WriteLine("   makes it useful when a query is slow on someone else's cluster.");
    }

    private static async Task BridgingThemIntoALogger(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n2. priority is a Poco severity, so a lower number is more severe\n");
        Console.WriteLine("     1 fatal   2 critical   3 error   4 warning   5 notice");
        Console.WriteLine("     6 information   7 debug   8 trace   9 test\n");

        var seen = new SortedDictionary<sbyte, (int Count, string Example)>();

        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["send_logs_level"] = "trace" },
            Callbacks = new ClickHouseTcpQueryCallbacks
            {
                OnLog = block =>
                {
                    ReadOnlySpan<sbyte> priority = block.Column<sbyte>("priority").Values;
                    IColumn<string> source = block.Column<string>("source");

                    for (int row = 0; row < block.RowCount; row++)
                    {
                        seen.TryGetValue(priority[row], out (int Count, string Example) soFar);
                        seen[priority[row]] = (soFar.Count + 1, source[row]);
                    }
                },
            },
        };

        _ = await client.ExecuteScalarAsync("SELECT count() FROM numbers(200000)", options);

        Console.WriteLine("   What this query reported, and where each line would go in an ILogger:\n");
        foreach ((sbyte priority, (int count, string example)) in seen)
        {
            Console.WriteLine($"     priority {priority}  {count,2} line(s)  ILogger level {ToLogLevel(priority),-11} e.g. from {example}");
        }

        Console.WriteLine();
        Console.WriteLine("   So filter with <=, and treat anything outside 1..9 as unknown rather than as severe. A");
        Console.WriteLine("   query that runs cleanly says nothing above debug, which is why raising send_logs_level to");
        Console.WriteLine("   warning is a way to be told only about the queries that had a problem.");
        Console.WriteLine();
        Console.WriteLine("   Forwarding these to an ILogger is a few lines and yours to write: the client logs its own");
        Console.WriteLine("   lifecycle only (Tcp_026) and never what the server says.");
    }

    private static async Task HowMuchEachLevelSays(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n3. What each send_logs_level costs\n");

        foreach (string level in new[] { "none", "warning", "information", "debug", "trace" })
        {
            int blocks = 0;
            int rows = 0;

            _ = await client.ExecuteScalarAsync("SELECT count() FROM numbers(200000)", new ClickHouseTcpQueryOptions
            {
                Settings = new Dictionary<string, string> { ["send_logs_level"] = level },
                Callbacks = new ClickHouseTcpQueryCallbacks
                {
                    OnLog = block =>
                    {
                        blocks++;
                        rows += block.RowCount;
                    },
                },
            });

            Console.WriteLine($"     send_logs_level = {level,-12} {blocks} block(s), {rows,2} line(s)");
        }

        Console.WriteLine();
        Console.WriteLine("   The lines are packets on the same connection as the result, so they are not free: text");
        Console.WriteLine("   the server would otherwise only write to its own log crosses the wire. trace on a busy");
        Console.WriteLine("   client is a lot of it. debug on the queries you are investigating is the usable setting,");
        Console.WriteLine("   and it can be set per query rather than on the client (Tcp_020).");
    }

    private static async Task TheTotalsRow(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n4. OnTotals: the WITH TOTALS row, in the query's own shape\n");

        const string sql =
            "SELECT number % 3 AS bucket, count() AS rows, sum(number) AS total " +
            "FROM numbers(30) GROUP BY bucket WITH TOTALS ORDER BY bucket";

        string[] names = [];
        object?[] totals = [];
        int calls = 0;

        var options = new ClickHouseTcpQueryOptions
        {
            Callbacks = new ClickHouseTcpQueryCallbacks
            {
                OnTotals = block =>
                {
                    calls++;

                    // ColumnNames is computed and owned, so it is safe to keep; the columns are not. One row, and
                    // every column here is a scalar, so GetValue boxes a copy of the value rather than a view of
                    // the buffer. A composite column would need materializing on purpose.
                    names = [.. block.ColumnNames];
                    totals = [.. block.Columns.Select(column => column.GetValue(0))];
                },
            },
        };

        Console.WriteLine($"   {sql}\n");
        await foreach (object[] row in client.QueryAsync(sql, options))
        {
            Console.WriteLine($"     row     {string.Join("  ", row.Select(v => $"{v,8}"))}");
        }

        Console.WriteLine($"     names   {string.Join("  ", names.Select(n => $"{n,8}"))}");
        Console.WriteLine($"     totals  {string.Join("  ", totals.Select(v => $"{v,8}"))}");
        Console.WriteLine();
        Console.WriteLine($"   Called {calls} time, after the last row: the server sends the totals block once the result");
        Console.WriteLine("   is complete. The shape is the query's own, so the aggregate columns hold the totals over");
        Console.WriteLine("   every group, and the grouping key holds a default rather than anything meaningful.");
        Console.WriteLine();
        Console.WriteLine("   It arrives on its own packet, not as an extra row, so a caller reading rows never has to");
        Console.WriteLine("   filter it out — which is the difference from reading WITH TOTALS over HTTP in a row-shaped");
        Console.WriteLine("   format.");
    }

    private static async Task TheExtremesRows(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n5. OnExtremes: two rows, the minimum and the maximum\n");

        const string sql = "SELECT number AS n, toString(number) AS text FROM numbers(1, 12)";

        var rows = new List<object?[]>();
        string[] names = [];

        var options = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["extremes"] = "1" },
            Callbacks = new ClickHouseTcpQueryCallbacks
            {
                OnExtremes = block =>
                {
                    names = [.. block.ColumnNames];
                    for (int row = 0; row < block.RowCount; row++)
                    {
                        rows.Add([.. block.Columns.Select(column => column.GetValue(row))]);
                    }
                },
            },
        };

        int count = 0;
        await foreach (object[] row in client.QueryAsync(sql, options))
        {
            count++;
        }

        Console.WriteLine($"   {sql}   ({count} rows)\n");
        Console.WriteLine($"     {"",-8} {string.Join("  ", names.Select(n => $"{n,6}"))}");
        Console.WriteLine($"     {"minimum",-8} {string.Join("  ", rows[0].Select(v => $"{v,6}"))}");
        Console.WriteLine($"     {"maximum",-8} {string.Join("  ", rows[1].Select(v => $"{v,6}"))}");
        Console.WriteLine();
        Console.WriteLine("   Row 0 is the minimum and row 1 the maximum, per column and independently, so the pair is");
        Console.WriteLine("   not two rows of the result. Each column is compared in its own type's order, which for");
        Console.WriteLine($"   the String column above is lexicographic — hence \"{rows[1][1]}\" as the maximum of 1..12.");
    }

    private static async Task NothingFiresWhenThereIsNothingToSend(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n6. A callback that never fires\n");

        int log = 0;
        int totals = 0;
        int extremes = 0;

        var callbacks = new ClickHouseTcpQueryCallbacks
        {
            OnLog = _ => log++,
            OnTotals = _ => totals++,
            OnExtremes = _ => extremes++,
        };

        // Nothing turned on: no send_logs_level, no WITH TOTALS, no extremes.
        await foreach (object[] row in client.QueryAsync(
            "SELECT number FROM numbers(5)", new ClickHouseTcpQueryOptions { Callbacks = callbacks }))
        {
        }

        Console.WriteLine($"   A plain query with all three set: OnLog {log}, OnTotals {totals}, OnExtremes {extremes} calls.");

        // All three turned on at once, on one query.
        await foreach (object[] row in client.QueryAsync(
            "SELECT number % 2 AS bucket, count() AS rows FROM numbers(20) GROUP BY bucket WITH TOTALS ORDER BY bucket",
            new ClickHouseTcpQueryOptions
            {
                Settings = new Dictionary<string, string>
                {
                    ["send_logs_level"] = "debug",
                    ["extremes"] = "1",
                },
                Callbacks = callbacks,
            }))
        {
        }

        Console.WriteLine($"   The same callbacks on a query with all three on:  OnLog {log}, OnTotals {totals}, OnExtremes {extremes} calls.");
        Console.WriteLine();
        Console.WriteLine("   There is no \"none arrived\" reading to look for, because there is no block to hand over,");
        Console.WriteLine("   so a caller that needs to know whether totals came keeps its own flag or counter — the");
        Console.WriteLine("   same shape Tcp_021 uses to show OnProfileInfo is called exactly once.");
    }

    /// <summary>
    /// Maps a server log line's Poco severity onto an <see cref="ILogger"/> level. Unknown numbers become
    /// <see cref="LogLevel.Information"/> rather than something alarming.
    /// </summary>
    private static LogLevel ToLogLevel(sbyte priority) => priority switch
    {
        1 => LogLevel.Critical,
        2 => LogLevel.Critical,
        3 => LogLevel.Error,
        4 => LogLevel.Warning,
        5 or 6 => LogLevel.Information,
        7 => LogLevel.Debug,
        8 or 9 => LogLevel.Trace,
        _ => LogLevel.Information,
    };
}
