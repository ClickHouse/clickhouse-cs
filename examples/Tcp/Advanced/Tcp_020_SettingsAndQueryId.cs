using System.Diagnostics;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// The two places a ClickHouse setting can be set — <see cref="ClickHouseTcpClientOptions.CustomSettings"/> for
/// every operation the client runs, <see cref="ClickHouseTcpQueryOptions.Settings"/> for one — and
/// <see cref="ClickHouseTcpQueryOptions.QueryId"/>, which is how a query is found again in
/// <c>system.query_log</c> or stopped with <c>KILL QUERY</c>.
///
/// <para>
/// <c>Tcp_002_ConnectionString</c> sets a client-level setting from a connection string key and reads it back;
/// this example is about what happens when both levels name the same setting, and about the settings that change
/// how an operation behaves rather than only what it reports. <c>async_insert</c> is the worked example.
/// </para>
/// </summary>
public static class TcpSettingsAndQueryId
{
    private const string TableName = "example_tcp_async_insert";

    public static async Task Run()
    {
        // Two client-level settings, from the set_<name> keys of the connection string. Tcp_002 covers the
        // spelling; what matters here is that they are the client's defaults for every operation.
        var builder = ExampleConfig.TcpBuilder();
        builder["set_max_threads"] = 2;
        builder["set_max_block_size"] = 4096;

        await using var client = new ClickHouseTcpClient(builder.ToOptions());

        await TwoLevels(client);
        await AMisspelledNameIsIgnored(client);
        await QueryIdInTheLog(client);
        await ReusingAQueryId(client);

        try
        {
            await AsyncInsert(client);
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
            Console.WriteLine($"\nDropped {TableName}.");
        }
    }

    private static async Task TwoLevels(ClickHouseTcpClient client)
    {
        Console.WriteLine("1. Client-level settings against per-query settings\n");

        // getSetting reports the value in force for the query asking, which makes the precedence observable.
        const string sql = "SELECT getSetting('max_threads')::String, getSetting('max_block_size')::String";

        Console.WriteLine($"   Options.CustomSettings   {string.Join(", ", client.Options.CustomSettings.Select(s => $"{s.Key}={s.Value}"))}");
        Console.WriteLine($"   no per-query options     {await Pair(client, sql, null)}");

        // Only max_threads is named twice, and only max_threads changes.
        var oneKey = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["max_threads"] = "7" },
        };
        Console.WriteLine($"   Settings max_threads=7   {await Pair(client, sql, oneKey)}");
        Console.WriteLine($"   the next query           {await Pair(client, sql, null)}");
        Console.WriteLine();
        Console.WriteLine("   A per-query value replaces the client-level one for that key alone: max_block_size");
        Console.WriteLine("   kept the client's 4096. And it applies to one operation — nothing is left behind on");
        Console.WriteLine("   the connection, because the settings travel in the query packet rather than as a SET.");
        Console.WriteLine();
        Console.WriteLine("   To carry a setting across operations, put it on the client, or run SET inside a");
        Console.WriteLine("   session (Tcp_016), which pins one connection and so can hold session state.");
        Console.WriteLine();
        Console.WriteLine("   Settings is IReadOnlyDictionary<string, string>: every value is text, so a number is");
        Console.WriteLine("   spelled \"7\". HTTP's QueryOptions.CustomSettings takes object instead.");
    }

    private static async Task<string> Pair(ClickHouseTcpClient client, string sql, ClickHouseTcpQueryOptions? options)
    {
        await foreach (object[] row in client.QueryAsync(sql, options))
        {
            return $"max_threads={row[0],-3} max_block_size={row[1]}";
        }

        return "(no row)";
    }

    private static async Task AMisspelledNameIsIgnored(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n2. A name the server does not know is ignored, not refused\n");

        // The settings list is not validated against the server's setting names, so a typo costs nothing and
        // does nothing. There is no client-side check either: the name is whatever string you passed.
        object value = await client.ExecuteScalarAsync("SELECT 1", new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["maxx_threads"] = "7" },
        });

        Console.WriteLine($"   Settings[\"maxx_threads\"] = \"7\", then SELECT 1 -> {value}, no error at all.");

        // A value that cannot be parsed as the setting's type does fail, which is the only feedback there is.
        //
        // On its own throwaway client, deliberately. The server raises this error while it is still reading the
        // settings list — before it has accepted the query — and closes the socket, which the pool does not
        // notice, so the connection goes back into the pool dead and the *next* operation on this client fails
        // with a ClickHouseTcpTransportException. Scoping it to a client that is disposed here disposes the dead
        // connection with it. Tcp_007 does the same for the same reason.
        await using (var throwaway = new ClickHouseTcpClient(client.Options))
        {
            try
            {
                await throwaway.ExecuteScalarAsync("SELECT 1", new ClickHouseTcpQueryOptions
                {
                    Settings = new Dictionary<string, string> { ["max_threads"] = "lots" },
                });
                Console.WriteLine("   max_threads = \"lots\" was accepted, which is not what this example expected");
            }
            catch (ClickHouseTcpServerException ex)
            {
                Console.WriteLine($"   Settings[\"max_threads\"] = \"lots\" -> {ex.Code} ({ex.RawCode}): {FirstLine(ex.Message)}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   So a wrong name is silent and a wrong value is loud. Read a setting back with");
        Console.WriteLine("   getSetting('name') when it matters that it arrived.");
        Console.WriteLine();
        Console.WriteLine("   That second query ran on a client of its own, because a bad setting value is refused");
        Console.WriteLine("   before the query is accepted and the server closes the connection on its way out. An");
        Console.WriteLine("   ordinary query error — a syntax error, an unknown table — leaves the connection usable,");
        Console.WriteLine("   and Tcp_023 shows that; this one does not.");
    }

    private static async Task QueryIdInTheLog(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n3. QueryId, and finding the query again\n");

        // Unique per run: a query id is the key of a system.query_log row, and two runs of this example against
        // one server must not collide.
        string queryId = $"example-tcp-020-{Guid.NewGuid():N}";
        var options = new ClickHouseTcpQueryOptions { QueryId = queryId };

        object rows = await client.ExecuteScalarAsync("SELECT count() FROM numbers(100000)", options);
        Console.WriteLine($"   Ran SELECT count() FROM numbers(100000) as query_id = {queryId}");
        Console.WriteLine($"   result {rows}");

        // The QueryFinish record is queued independently of the response reaching the client, so a flush issued
        // straight after the query can miss it. Retry the flush and the read rather than sleeping.
        string found = await ReadLog(
            client,
            "SELECT type::String || ' read_rows=' || toString(read_rows) || ' threads=' || Settings['max_threads'] " +
            "FROM system.query_log WHERE query_id = {id:String} AND type = 'QueryFinish'",
            queryId);

        Console.WriteLine($"   system.query_log by query_id: {found}");
        Console.WriteLine();
        Console.WriteLine("   The Settings column holds the settings the query ran with, client-level ones");
        Console.WriteLine("   included, which is the other reason to set a query id: it is the only handle that");
        Console.WriteLine("   ties an application's own request to a server-side row. It is also what");
        Console.WriteLine("   KILL QUERY WHERE query_id = '...' takes.");
    }

    private static async Task ReusingAQueryId(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n4. Reusing one\n");

        string queryId = $"example-tcp-020-reuse-{Guid.NewGuid():N}";
        var options = new ClickHouseTcpQueryOptions { QueryId = queryId };

        await client.ExecuteScalarAsync("SELECT 1", options);
        await client.ExecuteScalarAsync("SELECT 2", options);
        Console.WriteLine("   Two queries, one after the other, under the same id: both accepted. The id is not");
        Console.WriteLine("   unique — the log now holds two rows for it, and telling them apart means reading");
        Console.WriteLine("   event_time_microseconds.");

        // While one is still running, the server refuses the second. Its own client, so that the two queries are
        // genuinely concurrent rather than queued behind one connection.
        await using var second = new ClickHouseTcpClient(client.Options);
        string busyId = $"example-tcp-020-busy-{Guid.NewGuid():N}";
        var busy = new ClickHouseTcpQueryOptions { QueryId = busyId };

        // Started without Task.Run, so the query packet goes out on this thread rather than whenever the thread
        // pool gets to it. That ordering is what decides which of the two the server refuses.
        Task<object> slow = client
            .ExecuteScalarAsync("SELECT sleepEachRow(0.05) FROM numbers(6)", busy)
            .AsTask();

        // Waits until the server really is running it, rather than guessing with a delay. Until this returns, the
        // id is not yet claimed and it is undecided which query would be the duplicate.
        await WaitUntilRunning(second, busyId);

        try
        {
            await second.ExecuteScalarAsync("SELECT 1", busy);
            Console.WriteLine("   A concurrent reuse was accepted, which is not what this example expected");
        }
        catch (ClickHouseTcpServerException ex)
        {
            Console.WriteLine($"\n   The same id while the first is still running -> {ex.Code} (RawCode {ex.RawCode})");
            Console.WriteLine($"     {FirstLine(ex.Message)}");
            Console.WriteLine("     Code reads Unknown because ClickHouseErrorCode does not name 216; RawCode");
            Console.WriteLine("     always carries the server's number. Tcp_023 is about that pair.");
        }

        await slow;
        Console.WriteLine("\n   Use a fresh id per attempt (a Guid, or your own request id) unless you want the");
        Console.WriteLine("   server to reject a duplicate submission for you — which, with a retry, is the one");
        Console.WriteLine("   case where reusing an id is the point rather than a mistake.");
    }

    private static async Task AsyncInsert(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n5. async_insert: a setting that changes what an insert means\n");

        await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        await client.ExecuteAsync($"CREATE TABLE {TableName} (id UInt64, note String) ENGINE = MergeTree ORDER BY id");

        object[][] first = [[1UL, "a"], [2UL, "b"]];
        object[][] second = [[3UL, "c"], [4UL, "d"]];

        // Settings live on ClickHouseTcpInsertOptions too: it derives from ClickHouseTcpQueryOptions and adds
        // MaxRowsPerBlock (Tcp_009).
        var waits = new ClickHouseTcpInsertOptions
        {
            Settings = new Dictionary<string, string> { ["async_insert"] = "1", ["wait_for_async_insert"] = "1" },
        };
        var doesNotWait = new ClickHouseTcpInsertOptions
        {
            Settings = new Dictionary<string, string> { ["async_insert"] = "1", ["wait_for_async_insert"] = "0" },
        };

        var clock = Stopwatch.StartNew();
        await client.InsertRowsAsync($"INSERT INTO {TableName} (id, note) VALUES", first, waits);
        long waited = clock.ElapsedMilliseconds;
        object afterWaiting = await client.ExecuteScalarAsync($"SELECT count() FROM {TableName}");

        Console.WriteLine($"   async_insert=1, wait_for_async_insert=1: returned after {waited} ms, and the rows are");
        Console.WriteLine($"     already queryable — count() = {afterWaiting}. The rows went into a server-side buffer");
        Console.WriteLine("     shared with other clients' inserts, and the call waited for that buffer to be written.");

        clock.Restart();
        await client.InsertRowsAsync($"INSERT INTO {TableName} (id, note) VALUES", second, doesNotWait);
        long notWaited = clock.ElapsedMilliseconds;
        object immediately = await client.ExecuteScalarAsync($"SELECT count() FROM {TableName}");

        // The only way to make the second batch's visibility deterministic. Without it, the count above is 2 or 4
        // depending on whether the buffer happened to flush, which is exactly the guarantee being given up.
        await client.ExecuteAsync("SYSTEM FLUSH ASYNC INSERT QUEUE");
        object afterFlush = await client.ExecuteScalarAsync($"SELECT count() FROM {TableName}");

        Console.WriteLine($"\n   async_insert=1, wait_for_async_insert=0: returned after {notWaited} ms.");
        Console.WriteLine($"     count() straight afterwards = {immediately}. That number is 2 on one run and 4 on the next:");
        Console.WriteLine("     the buffer flushes on its own schedule and the call no longer waits for it.");
        Console.WriteLine($"     After SYSTEM FLUSH ASYNC INSERT QUEUE: count() = {afterFlush}.");
        Console.WriteLine();
        Console.WriteLine("   Two things the pair changes, neither of which is visible in the API:");
        Console.WriteLine("     - a returned InsertRowsAsync no longer means the rows are stored, so a failure");
        Console.WriteLine("       after the return is reported to nobody;");
        Console.WriteLine("     - read-after-write stops holding, so a test that inserts and counts fails.");
        Console.WriteLine();
        Console.WriteLine("   It is worth it for many small inserts from many clients, which is what the server-side");
        Console.WriteLine("   buffer is for. For one large insert, MaxRowsPerBlock and a plain insert are better.");
    }

    /// <summary>
    /// Reads one scalar out of <c>system.query_log</c>, retrying the flush and the read. Pick an expression that
    /// is never NULL for a row that exists, so that "no row yet" and "row with an empty value" cannot be confused.
    /// </summary>
    private static async Task<string> ReadLog(ClickHouseTcpClient client, string sql, string queryId)
    {
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "id", queryId } },
        };

        for (int attempt = 1; attempt <= 5; attempt++)
        {
            await client.ExecuteAsync("SYSTEM FLUSH LOGS");
            await foreach (object[] row in client.QueryAsync(sql, options))
            {
                return $"{row[0]} (attempt {attempt})";
            }

            await Task.Delay(50);
        }

        return "no row appeared in system.query_log after 5 attempts";
    }

    /// <summary>Waits until <c>system.processes</c> shows the query, so a race cannot decide the next assertion.</summary>
    private static async Task WaitUntilRunning(ClickHouseTcpClient client, string queryId)
    {
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "id", queryId } },
        };

        for (int attempt = 0; attempt < 300; attempt++)
        {
            object running = await client.ExecuteScalarAsync(
                "SELECT count() FROM system.processes WHERE query_id = {id:String}",
                options);
            if (Convert.ToUInt64(running) > 0)
            {
                return;
            }

            await Task.Delay(10);
        }
    }

    /// <summary>The server's message is one long line with its own detail appended; the first line is the fact.</summary>
    private static string FirstLine(string message)
    {
        string text = message.Replace("DB::Exception: ", string.Empty);
        int newline = text.IndexOf('\n');
        if (newline >= 0)
        {
            text = text[..newline];
        }

        return text.Length <= 110 ? text : text[..110] + "...";
    }
}
