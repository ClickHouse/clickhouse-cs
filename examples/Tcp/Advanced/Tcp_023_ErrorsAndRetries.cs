using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// The native client's exception hierarchy — <see cref="ClickHouseTcpServerException"/>,
/// <see cref="ClickHouseTcpTransportException"/> and <see cref="ClickHouseTcpProtocolException"/> under
/// <see cref="ClickHouseTcpException"/> — how to branch on <see cref="ClickHouseErrorCode"/>, and which failures
/// are worth retrying.
///
/// <para>
/// Retrying is where the two halves meet. A read is idempotent, so a retry costs a round trip and nothing else. An
/// insert is not: the same batch sent twice lands twice, unless the target table can deduplicate it and the insert
/// carries a token that says which insert it is. This example measures both.
/// </para>
/// </summary>
public static class TcpErrorsAndRetries
{
    private const string PlainTable = "example_tcp_retry_plain";
    private const string DedupTable = "example_tcp_retry_dedup";

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();

        TheHierarchy();
        await ServerErrors(client);
        await NotServerErrors();
        await Transient(client);
        await RetryingARead(client);

        try
        {
            await RetryingAnInsert(client);
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {PlainTable}");
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {DedupTable}");
            Console.WriteLine($"\nDropped {PlainTable} and {DedupTable}.");
        }
    }

    private static void TheHierarchy()
    {
        Console.WriteLine("1. Three exception types, one base, and what is deliberately not in it\n");
        Console.WriteLine("   ClickHouseTcpException : DbException        catch this for 'anything between the");
        Console.WriteLine("                                              client and the server went wrong'");
        Console.WriteLine("     ClickHouseTcpServerException              the server reported an error for a query,");
        Console.WriteLine("                                              a handshake or a ping");
        Console.WriteLine("     ClickHouseTcpTransportException           the socket failed: refused, dropped, TLS");
        Console.WriteLine("     ClickHouseTcpProtocolException            the bytes did not match the protocol");
        Console.WriteLine();
        Console.WriteLine("   The hierarchy is closed — the constructors are not visible outside the assembly — so a");
        Console.WriteLine("   caught ClickHouseTcpException is always one of the three.");
        Console.WriteLine();
        Console.WriteLine("   Mistakes in the calling code keep the usual framework types, on purpose:");
        Console.WriteLine("     ArgumentException           a bad option or a null argument (Tcp_019 has nine)");
        Console.WriteLine("     InvalidOperationException    a misused object — a session running two operations");
        Console.WriteLine("     ObjectDisposedException      use after disposal");
        Console.WriteLine("     OperationCanceledException   the caller cancelled (Tcp_022)");
        Console.WriteLine("     TimeoutException             a deadline elapsed: PoolTimeout, DialTimeout,");
        Console.WriteLine("                                  ReadTimeout (Tcp_019)");
        Console.WriteLine();
        Console.WriteLine("   So `catch (ClickHouseTcpException)` never swallows a bug in your own code, and never");
        Console.WriteLine("   swallows a cancellation.");
    }

    private static async Task ServerErrors(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n2. ClickHouseTcpServerException: Code, RawCode, Name, ServerStackTrace\n");

        (string What, string Sql)[] cases =
        [
            ("a syntax error", "SELECT FROM WHERE"),
            ("an unknown table", "SELECT * FROM does_not_exist_example_tcp_023"),
            ("an unknown function", "SELECT no_such_function(1)"),
            ("an unparseable value", "SELECT toUInt8('abc')"),
            ("a division by zero", "SELECT intDiv(1, 0)"),
        ];

        Console.WriteLine($"   {"",-22} {"Code",-28} {"RawCode",7}  {"transient",9}  message");
        foreach ((string what, string sql) in cases)
        {
            try
            {
                _ = await client.ExecuteScalarAsync(sql);
                Console.WriteLine($"   {what,-22} succeeded, which is not what this example expected");
            }
            catch (ClickHouseTcpServerException ex)
            {
                Console.WriteLine($"   {what,-22} {ex.Code,-28} {ex.RawCode,7}  {ex.IsTransient,9}  {FirstLine(ex.Message, 40)}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   RawCode is always the number the server sent. Code is that number as a named");
        Console.WriteLine("   constant, or Unknown when this client does not name it — the enum carries the codes");
        Console.WriteLine("   worth branching on, not all ~660 of them. Division by zero (153) is one it does not");
        Console.WriteLine("   name, so branch on RawCode for anything outside the list.");
        Console.WriteLine();
        Console.WriteLine("   Every one of those left the connection usable: the server reported an error as part of");
        Console.WriteLine($"   a complete response, so the pool keeps it. Proof — {await client.ExecuteScalarAsync("SELECT 'still here'")}.");

        // The two fields an operator asks for, on one error.
        try
        {
            _ = await client.ExecuteScalarAsync("SELECT * FROM does_not_exist_example_tcp_023");
        }
        catch (ClickHouseTcpServerException ex)
        {
            Console.WriteLine("\n   The whole of one error:");
            Console.WriteLine($"     Code               {ex.Code}");
            Console.WriteLine($"     RawCode            {ex.RawCode}");
            Console.WriteLine($"     Name               {ex.Name}");
            Console.WriteLine($"     IsTransient        {ex.IsTransient}");
            Console.WriteLine($"     ErrorCode          {ex.ErrorCode}   (DbException's, the same number)");
            Console.WriteLine($"     ServerStackTrace   {ex.ServerStackTrace?.Length ?? 0} characters of the server's own C++ frames");
            Console.WriteLine($"     Message            {FirstLine(ex.Message, 90)}");
            Console.WriteLine();
            Console.WriteLine("     Message repeats Name, because the server puts its exception class in both.");
            Console.WriteLine("     ServerStackTrace is for a bug report, not for a log line.");

            // Branching. A switch on Code is the readable form; the default arm has to exist, because a code the
            // enum does not name arrives as Unknown.
            string advice = ex.Code switch
            {
                ClickHouseErrorCode.UnknownTable or ClickHouseErrorCode.UnknownDatabase => "check the name and the database the client is pointed at",
                ClickHouseErrorCode.SyntaxError => "the query text is wrong; do not retry it",
                ClickHouseErrorCode.AccessDenied or ClickHouseErrorCode.AuthenticationFailed => "a grant or a credential problem",
                ClickHouseErrorCode.TooManyParts or ClickHouseErrorCode.ServerOverloaded => "back off and try again",
                _ => $"unrecognized; RawCode {ex.RawCode}",
            };
            Console.WriteLine($"\n     switch (ex.Code) -> {advice}");
        }
    }

    private static async Task NotServerErrors()
    {
        Console.WriteLine("\n3. The other two: transport and protocol\n");

        ClickHouseTcpClientOptions options = ExampleConfig.TcpBuilder().ToOptions();

        // Nothing listening: a socket failure, and a fresh connection may well work, so IsTransient is true.
        try
        {
            await using var refused = new ClickHouseTcpClient(options with { Port = 1, DialTimeout = TimeSpan.FromSeconds(2) });
            await refused.PingAsync();
            Console.WriteLine("   Port 1 answered, which is not what this example expected");
        }
        catch (ClickHouseTcpException ex)
        {
            Console.WriteLine($"   nothing listening on the port  {ex.GetType().Name}");
            Console.WriteLine($"                                  IsTransient={ex.IsTransient}, InnerException={ex.InnerException?.GetType().Name}");
            Console.WriteLine("                                  Match the inner exception when the distinction");
            Console.WriteLine("                                  matters: SocketException, IOException,");
            Console.WriteLine("                                  EndOfStreamException, AuthenticationException.");
        }

        // The HTTP port: a peer that answers, but not in this protocol. Not transient — retrying a
        // misconfiguration just fails again.
        try
        {
            await using var wrongPort = new ClickHouseTcpClient(options with { Port = ExampleConfig.HttpEndpoint.Port });
            await wrongPort.PingAsync();
            Console.WriteLine("   The HTTP port spoke the native protocol, which is not what this example expected");
        }
        catch (ClickHouseTcpException ex)
        {
            Console.WriteLine($"\n   the HTTP port ({ExampleConfig.HttpEndpoint.Port})           {ex.GetType().Name}");
            Console.WriteLine($"                                  IsTransient={ex.IsTransient}");
            Console.WriteLine($"                                  {ex.Message}");
            Console.WriteLine("                                  72 is 'H', the first byte of an HTTP response.");
        }

        Console.WriteLine();
        Console.WriteLine("   Both terminate the connection and it is never reused, which is why neither needs a");
        Console.WriteLine("   'is the client still usable' check: the pool simply dials again.");
    }

    private static async Task Transient(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n4. IsTransient, and what it does and does not promise\n");

        // A real timeout: a scan far too large for the deadline. Transient by code, and completely deterministic.
        await Report(
            client,
            "max_execution_time = 0.2 over a huge scan",
            "SELECT count() FROM numbers(50000000000)",
            new Dictionary<string, string> { ["max_execution_time"] = "0.2" });

        // Looks temporary, is not: the same query at the same size needs the same memory every time.
        await Report(
            client,
            "max_memory_usage = 1 MB",
            "SELECT groupArray(number) FROM numbers(10000000)",
            new Dictionary<string, string> { ["max_memory_usage"] = "1000000" });

        Console.WriteLine();
        Console.WriteLine("   IsTransient reads the code, and it means 'retrying could plausibly succeed' — not");
        Console.WriteLine("   'will'. TimeoutExceeded is transient because the server may be less busy next time,");
        Console.WriteLine("   and yet the query above will time out on every attempt, because the cause is its own");
        Console.WriteLine("   size. MemoryLimitExceeded is the opposite reading: it looks temporary and is judged");
        Console.WriteLine("   not transient, because the same query at the same size repeats it.");
        Console.WriteLine();
        Console.WriteLine("   So cap the attempts, and prefer a failure whose cause is outside your query:");
        Console.WriteLine("     transient   TimeoutExceeded(159) TooManySimultaneousQueries(202) NoFreeConnection(203)");
        Console.WriteLine("                 SocketTimeout(209) NetworkError(210) TooManyParts(252)");
        Console.WriteLine("                 AllConnectionTriesFailed(279) ServerOverloaded(745) KeeperException(999)");
        Console.WriteLine("                 and every ClickHouseTcpTransportException");
        Console.WriteLine("     not         syntax, unknown table or column, type mismatch, access denied,");
        Console.WriteLine("                 memory limit, and every ClickHouseTcpProtocolException");
    }

    private static async Task Report(
        ClickHouseTcpClient client,
        string label,
        string sql,
        Dictionary<string, string> settings)
    {
        try
        {
            _ = await client.ExecuteScalarAsync(sql, new ClickHouseTcpQueryOptions { Settings = settings });
            Console.WriteLine($"   {label,-42} succeeded, which is not what this example expected");
        }
        catch (ClickHouseTcpServerException ex)
        {
            Console.WriteLine($"   {label,-42} {ex.Code} ({ex.RawCode}), IsTransient={ex.IsTransient}");
        }
    }

    private static async Task RetryingARead(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n5. Retrying a read, which is free\n");

        // A real transient failure with nothing injected. max_concurrent_queries_for_user is checked when a query
        // starts, and only against the query that declares it: the slow query below declares nothing, so it holds
        // a slot and can never itself be refused, and only the retry loop can lose. That one-sidedness is what
        // makes the outcome determined rather than raced.
        // Started without Task.Run, so the query packet goes out on this thread before the poll below rather than
        // whenever the thread pool gets to it. AsTask only wraps the operation already in flight.
        string holderId = $"example-tcp-023-holder-{Guid.NewGuid():N}";
        Task<object> holder = client
            .ExecuteScalarAsync("SELECT sleepEachRow(0.08) FROM numbers(6)", new ClickHouseTcpQueryOptions { QueryId = holderId })
            .AsTask();

        // A separate client, so that the two queries are really concurrent rather than queued behind one
        // connection.
        await using var second = new ClickHouseTcpClient(client.Options);

        // Waits until the server is really running it, so the first attempt below is refused rather than usually
        // refused. Polling rather than a delay: a delay is the same race with a longer window.
        await WaitUntilRunning(second, holderId);

        Console.WriteLine("   A slow query is running. A second one asks with max_concurrent_queries_for_user = 1,");
        Console.WriteLine("   so the server refuses it until the first has finished.\n");

        var oneAtATime = new ClickHouseTcpQueryOptions
        {
            Settings = new Dictionary<string, string> { ["max_concurrent_queries_for_user"] = "1" },
        };

        const int attempts = 8;

        for (int attempt = 1; attempt <= attempts; attempt++)
        {
            try
            {
                object counted = await second.ExecuteScalarAsync("SELECT count() FROM numbers(1000)", oneAtATime);
                Console.WriteLine($"     attempt {attempt}: {counted}");
                break;
            }
            catch (ClickHouseTcpException ex) when (ex.IsTransient)
            {
                string reason = ex is ClickHouseTcpServerException server ? $"{server.Code} ({server.RawCode})" : ex.GetType().Name;
                Console.WriteLine($"     attempt {attempt}: {reason} — transient, so try again");

                // The last attempt is caught too. The limit counts every query this user is running, so anything
                // else on the server under the same user can keep refusing this one, and an example must report
                // that rather than throw out of the demonstration.
                if (attempt == attempts)
                {
                    Console.WriteLine($"     gave up after {attempts}: the cap is what stops a retry becoming a loop.");
                    break;
                }

                await Task.Delay(100 * attempt);
            }
        }

        await holder;

        Console.WriteLine();
        Console.WriteLine("   `catch (ClickHouseTcpException ex) when (ex.IsTransient)` is the whole filter, and the");
        Console.WriteLine("   attempt cap is what keeps a deterministic failure from becoming a loop. The read is");
        Console.WriteLine("   idempotent, so nothing had to be checked before trying again — which is the only");
        Console.WriteLine("   reason this retry is safe to write in three lines.");
        Console.WriteLine();
        Console.WriteLine("   The limit travelled in the query packet, so it bounded those attempts and nothing");
        Console.WriteLine("   else — not the query it was waiting for, and not whatever runs next. Tcp_020 is about");
        Console.WriteLine("   that.");
    }

    /// <summary>
    /// Waits until <c>system.processes</c> shows the query. Deliberately sets nothing of its own, so this poll can
    /// never be the query a concurrency limit refuses.
    /// </summary>
    private static async Task WaitUntilRunning(ClickHouseTcpClient client, string queryId)
    {
        var options = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "id", queryId } },
        };

        for (int attempt = 0; attempt < 200; attempt++)
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

        throw new TimeoutException(
            $"query {queryId} did not appear in system.processes, so the next step's precondition does not hold");
    }

    private static async Task RetryingAnInsert(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n6. Retrying an insert, which is not\n");

        await client.ExecuteAsync($"DROP TABLE IF EXISTS {PlainTable}");
        await client.ExecuteAsync($"DROP TABLE IF EXISTS {DedupTable}");
        await client.ExecuteAsync($"CREATE TABLE {PlainTable} (id UInt64) ENGINE = MergeTree ORDER BY id");

        // A non-replicated MergeTree deduplicates nothing unless it is told to keep a window of insert hashes.
        await client.ExecuteAsync(
            $"CREATE TABLE {DedupTable} (id UInt64) ENGINE = MergeTree ORDER BY id " +
            "SETTINGS non_replicated_deduplication_window = 100");

        object[][] batch = [[1UL], [2UL], [3UL]];

        // The failure mode: a retry after a transport error that in fact delivered the rows.
        await client.InsertRowsAsync($"INSERT INTO {PlainTable} (id) VALUES", batch);
        await client.InsertRowsAsync($"INSERT INTO {PlainTable} (id) VALUES", batch);
        Console.WriteLine($"   plain MergeTree, the same 3 rows sent twice        count() = {await client.ExecuteScalarAsync($"SELECT count() FROM {PlainTable}")}");

        var token = new ClickHouseTcpInsertOptions
        {
            Settings = new Dictionary<string, string> { ["insert_deduplication_token"] = "example-tcp-023-batch-1" },
        };

        await client.InsertRowsAsync($"INSERT INTO {PlainTable} (id) VALUES", batch, token);
        await client.InsertRowsAsync($"INSERT INTO {PlainTable} (id) VALUES", batch, token);
        Console.WriteLine($"   ... twice more with one insert_deduplication_token count() = {await client.ExecuteScalarAsync($"SELECT count() FROM {PlainTable}")}");
        Console.WriteLine("       The token did nothing: this table keeps no window of insert hashes to compare it");
        Console.WriteLine("       against, so there is nothing for it to match.");

        await client.InsertRowsAsync($"INSERT INTO {DedupTable} (id) VALUES", batch, token);
        await client.InsertRowsAsync($"INSERT INTO {DedupTable} (id) VALUES", batch, token);
        Console.WriteLine($"\n   non_replicated_deduplication_window = 100, same token  count() = {await client.ExecuteScalarAsync($"SELECT count() FROM {DedupTable}")}");

        // The token identifies the insert, not the bytes: a second attempt that produced different rows is still
        // dropped, which is what makes a retry safe even when the data was rebuilt.
        object[][] different = [[4UL], [5UL], [6UL]];
        await client.InsertRowsAsync($"INSERT INTO {DedupTable} (id) VALUES", different, token);
        Console.WriteLine($"   ... and again with different rows, same token          count() = {await client.ExecuteScalarAsync($"SELECT count() FROM {DedupTable}")}");

        // No token at all on the same table: the block's own hash is still compared, so a byte-identical retry is
        // dropped too. Worth knowing, and not worth relying on.
        await client.ExecuteAsync($"TRUNCATE TABLE {DedupTable}");
        await client.InsertRowsAsync($"INSERT INTO {DedupTable} (id) VALUES", batch);
        await client.InsertRowsAsync($"INSERT INTO {DedupTable} (id) VALUES", batch);
        Console.WriteLine($"   ... and the same batch twice with no token at all      count() = {await client.ExecuteScalarAsync($"SELECT count() FROM {DedupTable}")}");

        Console.WriteLine();
        Console.WriteLine("   So a safe insert retry needs two things, and one of them is not in the client:");
        Console.WriteLine("     - the table must deduplicate — a Replicated engine, or");
        Console.WriteLine("       non_replicated_deduplication_window on a plain MergeTree;");
        Console.WriteLine("     - the insert must carry insert_deduplication_token, one value per logical batch,");
        Console.WriteLine("       reused by every retry of it.");
        Console.WriteLine();
        Console.WriteLine("   The last line is why the token matters even though a window alone dropped the");
        Console.WriteLine("   duplicate: that was the block's own hash matching, and a retry that rebuilt the batch");
        Console.WriteLine("   in a different order, or split it differently, hashes differently and lands twice.");
        Console.WriteLine();
        Console.WriteLine("   And an insert that fails with a ClickHouseTcpTransportException may or may not have");
        Console.WriteLine("   been applied — the client cannot tell which side of the socket the failure was. That");
        Console.WriteLine("   is the case the token exists for.");
    }

    /// <summary>
    /// The server's message is one long line with its own multi-line detail appended, and it starts with the class
    /// name that <see cref="ClickHouseTcpServerException.Name"/> already carries.
    /// </summary>
    private static string FirstLine(string message, int width)
    {
        string text = message.Replace("DB::Exception: ", string.Empty);
        int newline = text.IndexOf('\n');
        if (newline >= 0)
        {
            text = text[..newline];
        }

        return text.Length <= width ? text : text[..width] + "...";
    }
}
