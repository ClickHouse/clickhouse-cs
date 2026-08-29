using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Binding values into a query with <see cref="ClickHouseTcpParameterCollection"/> and
/// <c>ClickHouseTcpQueryOptions.Parameters</c> — and the three ways it goes wrong, which are worth more of your
/// attention than the happy path.
///
/// <para>
/// The query text must carry each parameter's type (<c>{id:Int32}</c>): there is no <c>@name</c> rewriting on this
/// transport. A value that names an instant is refused unless the placeholder declares a timezone. And a
/// parameter named after a server setting is applied as that setting rather than bound, which fails with an error
/// that names neither.
/// </para>
/// </summary>
public static class TcpParameters
{
    private const string TableName = "example_tcp_parameters";

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();

        try
        {
            await Seed(client);
            await BindingValues(client);
            await TheCollection(client);
            await NoAtNameRewriting(client);
            await Identifiers(client);
            await InstantsNeedATimezone(client);
            await NamesThatCollideWithSettings(client);
            ShowWhatIsAbsent();
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
            Console.WriteLine($"\nDropped '{TableName}'");
        }
    }

    private static async Task Seed(ClickHouseTcpClient client)
    {
        await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        await client.ExecuteAsync($@"
            CREATE TABLE {TableName}
            (
                id UInt64,
                city String,
                temperature Float64,
                recorded_at DateTime('UTC')
            )
            ENGINE = MergeTree()
            ORDER BY id");

        var midnight = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc);

        await client.InsertRowsAsync(
            $"INSERT INTO {TableName} (id, city, temperature, recorded_at) VALUES",
            new List<object[]>
            {
                new object[] { 1UL, "Amsterdam", 17.5, midnight.AddHours(6) },
                new object[] { 2UL, "Amsterdam", 21.0, midnight.AddHours(14) },
                new object[] { 3UL, "Reykjavik", 9.5, midnight.AddHours(6) },
                new object[] { 4UL, "Reykjavik", 11.25, midnight.AddHours(14) },
                new object[] { 5UL, "Singapore", 28.0, midnight.AddHours(6) },
                new object[] { 6UL, "Singapore", 31.75, midnight.AddHours(14) },
            });

        Console.WriteLine($"Seeded '{TableName}' with 6 rows (id, city, temperature, recorded_at DateTime('UTC'))");

        // Parameters travel in the Query packet's settings list, which is why they need a protocol revision that
        // knows about them. An older server rejects the query rather than run it unparameterized.
        ClickHouseTcpServerInfo server = await client.GetServerInfoAsync();
        Console.WriteLine($"Server protocol revision {server.ProtocolRevision}; parameters need 54459 or above");
    }

    private static async Task BindingValues(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n1. Binding values\n");

        // Names, not positions. The collection keeps insertion order, but the query refers to each by name.
        var parameters = new ClickHouseTcpParameterCollection();
        parameters.Add("city", "Amsterdam");
        parameters.Add("floor", 18.0);
        parameters.Add("wanted", new[] { 1UL, 2UL, 5UL });

        // Every placeholder states its type. That is what the server parses the value as, and what the client
        // formats it as, so the two cannot disagree.
        string sql = $@"
            SELECT id, city, temperature
            FROM {TableName}
            WHERE (city = {{city:String}} OR temperature >= {{floor:Float64}})
              AND id IN {{wanted:Array(UInt64)}}
            ORDER BY id";

        Console.WriteLine("   SELECT ... WHERE (city = {city:String} OR temperature >= {floor:Float64})");
        Console.WriteLine("                AND id IN {wanted:Array(UInt64)}");
        Console.WriteLine("   city='Amsterdam', floor=18.0, wanted=[1, 2, 5]\n");

        await foreach (object[] row in client.QueryAsync(sql, new ClickHouseTcpQueryOptions { Parameters = parameters }))
        {
            Console.WriteLine($"     id {row[0],2}  {(string)row[1],-9}  {row[2]}");
        }

        Console.WriteLine();
        Console.WriteLine("   A collection works on any operation that takes ClickHouseTcpQueryOptions, so the same");
        Console.WriteLine("   parameters bind on ExecuteAsync, ExecuteScalarAsync, QueryAsync<T>, StreamAsync and");
        Console.WriteLine("   InsertAsync — an INSERT ... SELECT can be parameterized too.");

        object count = await client.ExecuteScalarAsync(
            $"SELECT count() FROM {TableName} WHERE city = {{city:String}}",
            new ClickHouseTcpQueryOptions { Parameters = new ClickHouseTcpParameterCollection { { "city", "Reykjavik" } } });
        Console.WriteLine($"     ExecuteScalarAsync(count() WHERE city = {{city:String}}) with city='Reykjavik' = {count}");
    }

    private static async Task TheCollection(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n2. The collection itself\n");

        var parameters = new ClickHouseTcpParameterCollection
        {
            { "city", "Singapore" },
            { "floor", 30.0 },
        };

        Console.WriteLine($"   Count                      {parameters.Count}");
        Console.WriteLine($"   Contains(\"city\")           {parameters.Contains("city")}");
        Console.WriteLine($"   Contains(\"City\")           {parameters.Contains("City")}   (names are ordinal, like the server's)");
        Console.WriteLine($"   this[\"floor\"].Value        {parameters["floor"].Value}");
        Console.WriteLine($"   TryGetValue(\"nope\", out _)  {parameters.TryGetValue("nope", out _)}");
        Console.WriteLine($"   enumerates in order        {string.Join(", ", parameters.Select(p => p.Name))}");

        // The wire format is a name/value list, so a repeated name has no meaning and is refused rather than
        // silently taking one of the two values.
        try
        {
            parameters.Add("city", "Reykjavik");
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine($"   Adding 'city' twice throws: {ex.Message.Split(" (Parameter")[0]}");
        }

        Console.WriteLine();
        Console.WriteLine("   The collection is mutable and not thread-safe, and a client is meant to be shared, so");
        Console.WriteLine("   build one per operation and then leave it alone.");

        // The options record makes that cheap: keep the shared settings in one instance and derive the variant.
        var shared = new ClickHouseTcpQueryOptions { Settings = new Dictionary<string, string> { ["max_threads"] = "2" } };
        object hottest = await client.ExecuteScalarAsync(
            $"SELECT max(temperature) FROM {TableName} WHERE city = {{city:String}}",
            shared with { Parameters = parameters });

        Console.WriteLine($"   shared with {{ Parameters = parameters }} -> max(temperature) in Singapore = {hottest}");
    }

    private static async Task NoAtNameRewriting(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n3. Trap one: the query text carries the type, and @name is not rewritten\n");
        Console.WriteLine("   The HTTP client rewrites @city into {city:String} before sending. Nothing rewrites");
        Console.WriteLine("   anything here — the text goes to the server as you wrote it:\n");

        var parameters = new ClickHouseTcpParameterCollection { { "city", "Amsterdam" } };
        var options = new ClickHouseTcpQueryOptions { Parameters = parameters };

        try
        {
            await client.ExecuteScalarAsync($"SELECT count() FROM {TableName} WHERE city = @city", options);
        }
        catch (ClickHouseTcpServerException ex)
        {
            Console.WriteLine($"     WHERE city = @city  ->  {Describe(ex)}");
        }

        // Without the type the server cannot parse the placeholder either.
        try
        {
            await client.ExecuteScalarAsync($"SELECT count() FROM {TableName} WHERE city = {{city}}", options);
        }
        catch (ClickHouseTcpServerException ex)
        {
            Console.WriteLine($"     WHERE city = {{city}}   ->  {Describe(ex)}");
        }

        object ok = await client.ExecuteScalarAsync($"SELECT count() FROM {TableName} WHERE city = {{city:String}}", options);
        Console.WriteLine($"     WHERE city = {{city:String}}  ->  {ok}");
        Console.WriteLine();
        Console.WriteLine("   So a query written for Dapper does not port over unchanged, and neither does one that");
        Console.WriteLine("   relied on the HTTP client inferring a type from the .NET value.");
    }

    private static async Task Identifiers(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n4. Where the type comes from, and the Identifier placeholder\n");
        Console.WriteLine("   Three places, first match wins:");
        Console.WriteLine("     1. ClickHouseTcpParameter.ClickHouseType, set on the parameter");
        Console.WriteLine("     2. the query's {name:Type} placeholder");
        Console.WriteLine("     3. the value's CLR type — which only ever applies to a parameter the query does");
        Console.WriteLine("        not name, because a query that does name it must state the type for the server");
        Console.WriteLine();
        Console.WriteLine("   So rung 1 exists for the case where the placeholder is not the format the value should");
        Console.WriteLine("   be written in. The server still reads the type from the query text, so an override that");
        Console.WriteLine("   disagrees with the placeholder makes the server parse text it did not expect: most");
        Console.WriteLine("   queries want rung 2 and nothing else.");
        Console.WriteLine();

        // Identifier is not a data type: the server splices the value in as a name rather than as a literal, so a
        // table or column can be bound instead of concatenated into the query text.
        var parameters = new ClickHouseTcpParameterCollection { { "tbl", TableName }, { "col", "temperature" } };
        object rows = await client.ExecuteScalarAsync(
            "SELECT count({col:Identifier}) FROM {tbl:Identifier}",
            new ClickHouseTcpQueryOptions { Parameters = parameters });

        Console.WriteLine($"   Identifier binds a name rather than a value — the one placeholder that is not a type:");
        Console.WriteLine($"     SELECT count({{col:Identifier}}) FROM {{tbl:Identifier}}  with tbl='{TableName}', col='temperature' = {rows}");
    }

    private static async Task InstantsNeedATimezone(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n5. Trap two: a value that names an instant needs a timezone in the placeholder\n");
        Console.WriteLine("   The wire carries a wall-clock time and no timezone, so the server reads the value in");
        Console.WriteLine("   its session timezone. For a value that names a point in time that silently moves the");
        Console.WriteLine("   instant, so the client refuses to send it rather than let it move:\n");

        var noon = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc);

        // Kind=Utc names an instant, and DateTime with no timezone argument declares none.
        await Refused("DateTime Kind=Utc into {t:DateTime}", client, $"SELECT count() FROM {TableName} WHERE recorded_at >= {{t:DateTime}}", noon);

        // A DateTimeOffset always names an instant, whatever its offset is.
        await Refused(
            "DateTimeOffset into {t:DateTime}",
            client,
            $"SELECT count() FROM {TableName} WHERE recorded_at >= {{t:DateTime}}",
            new DateTimeOffset(noon));

        Console.WriteLine();
        Console.WriteLine("   Two fixes. Declare the timezone in the placeholder, which is what you want whenever");
        Console.WriteLine("   the value really is an instant:");

        object declared = await Count(client, $"SELECT count() FROM {TableName} WHERE recorded_at >= {{t:DateTime('UTC')}}", noon);
        Console.WriteLine($"     {{t:DateTime('UTC')}} with Kind=Utc          -> {declared} rows");

        object offsetDeclared = await Count(client, $"SELECT count() FROM {TableName} WHERE recorded_at >= {{t:DateTime('UTC')}}", new DateTimeOffset(noon).ToOffset(TimeSpan.FromHours(5)));
        Console.WriteLine($"     {{t:DateTime('UTC')}} with a +05:00 offset   -> {offsetDeclared} rows (the same instant, moved into UTC)");

        Console.WriteLine();
        Console.WriteLine("   Or pass Kind=Unspecified, which says \"this wall-clock time, in whatever timezone the");
        Console.WriteLine("   server reads it in\" — no instant is claimed, so nothing can be lost:");

        var wallClock = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Unspecified);
        object unspecified = await Count(client, $"SELECT count() FROM {TableName} WHERE recorded_at >= {{t:DateTime}}", wallClock);
        Console.WriteLine($"     {{t:DateTime}} with Kind=Unspecified         -> {unspecified} rows");
        Console.WriteLine();
        Console.WriteLine("   DateTime64 is the same rule: {t:DateTime64(3, 'UTC')} declares one, {t:DateTime64(3)}");
        Console.WriteLine("   does not.");
    }

    private static async Task NamesThatCollideWithSettings(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n6. Trap three: a parameter named after a server setting\n");
        Console.WriteLine("   Parameters ride in the Query packet's settings list. A server that reads the name as a");
        Console.WriteLine("   setting applies it as that setting instead of binding it, and the query then fails while");
        Console.WriteLine("   the server is reading the setting's value. The names to avoid are the ordinary setting");
        Console.WriteLine("   names: limit and offset above all, and max_threads, readonly and log_comment too.\n");

        string sql = $"SELECT id FROM {TableName} ORDER BY id LIMIT {{limit:UInt64}}";
        var collided = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "limit", 2UL } },
        };

        Console.WriteLine($"   Server {(await client.GetServerInfoAsync()).Version}, parameter named 'limit':");

        // A client of its own for the query that is meant to fail. The server rejects this one while it is still
        // reading the settings list, and then closes the socket, so the connection it was on is dead even though
        // the client saw an ordinary server error. The pool checks a connection for a closed socket both on
        // return and on checkout, so it usually discards this one; a close notice that arrives after both checks
        // can still be handed out. Disposing a throwaway client keeps that race out of the shared pool.
        await using (ClickHouseTcpClient throwaway = ExampleConfig.CreateTcpClient())
        {
            try
            {
                var ids = new List<object>();
                await foreach (object[] row in throwaway.QueryAsync(sql, collided))
                {
                    ids.Add(row[0]);
                }

                Console.WriteLine($"     bound correctly — LIMIT {{limit:UInt64}} returned {ids.Count} row(s): {string.Join(", ", ids)}");
                Console.WriteLine("     This server is new enough to tell a parameter from a setting.");
            }
            catch (ClickHouseTcpException ex)
            {
                Console.WriteLine($"     {Describe(ex)}");
                Console.WriteLine("     The error names neither the parameter nor the setting, so nothing in it points at");
                Console.WriteLine("     the name as the cause. (Code prints as Unknown when ClickHouseErrorCode has no");
                Console.WriteLine("     name for the raw number; the raw number is always there.)");
                Console.WriteLine("     The server also closes the connection after this one, so the next operation on");
                Console.WriteLine("     that connection can fail with a transport error instead — which is why this");
                Console.WriteLine("     example runs the failing query on a client of its own.");
            }
        }

        Console.WriteLine();
        Console.WriteLine("   The fix is a rename, and it always works:");

        var renamed = new ClickHouseTcpQueryOptions
        {
            Parameters = new ClickHouseTcpParameterCollection { { "row_limit", 2UL } },
        };

        var kept = new List<object>();
        await foreach (object[] row in client.QueryAsync($"SELECT id FROM {TableName} ORDER BY id LIMIT {{row_limit:UInt64}}", renamed))
        {
            kept.Add(row[0]);
        }

        Console.WriteLine($"     LIMIT {{row_limit:UInt64}} returned {kept.Count} row(s): {string.Join(", ", kept)}");
        Console.WriteLine();
        Console.WriteLine("   This is the server's behaviour and it is version-dependent — 25.8 through 26.6 apply");
        Console.WriteLine("   the name as a setting, newer servers bind it. clickhouse-client --param_limit= fails");
        Console.WriteLine("   the same way, and the driver's HTTP transport is unaffected because it carries the");
        Console.WriteLine("   name separately. So avoid a setting name for a parameter if you support any server in");
        Console.WriteLine("   that range, whatever the server in front of you does today.");
    }

    private static void ShowWhatIsAbsent()
    {
        Console.WriteLine("\n7. What the HTTP client has here and this one does not\n");
        Console.WriteLine("   @name placeholders, rewritten client-side. Write {name:Type}.");
        Console.WriteLine("   IParameterTypeResolver. The type comes from the placeholder, or from");
        Console.WriteLine("     ClickHouseTcpParameter.ClickHouseType, or — only for a parameter the query does not");
        Console.WriteLine("     name — from the value's CLR type.");
        Console.WriteLine("   IParameterFormatter. There is no hook for how a value is written.");
        Console.WriteLine("   DbParameter and DbType. This client is not an ADO.NET provider.");
        Console.WriteLine();
        Console.WriteLine("   Null and DBNull both send the null marker, so a Nullable placeholder is the way to");
        Console.WriteLine("   bind an absent value: {city:Nullable(String)}.");
    }

    private static async Task Refused(string label, ClickHouseTcpClient client, string sql, object value)
    {
        try
        {
            await Count(client, sql, value);
            Console.WriteLine($"     {label,-38} -> accepted (unexpected)");
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine($"     {label}:");
            Console.WriteLine($"       {Wrap(ex.Message.Split(" (Parameter")[0])}");
        }
    }

    private static ValueTask<object> Count(ClickHouseTcpClient client, string sql, object value)
        => client.ExecuteScalarAsync(
            sql,
            new ClickHouseTcpQueryOptions { Parameters = new ClickHouseTcpParameterCollection { { "t", value } } });

    // The mapped error code, the number the server actually sent, and the first line of the message. Code is
    // Unknown for a code the enum does not name, which is why RawCode is worth printing next to it.
    private static string Describe(ClickHouseTcpException exception)
    {
        string message = exception.Message;
        int newline = message.IndexOf('\n');
        if (newline >= 0)
        {
            message = message[..newline];
        }

        const string prefix = "DB::Exception: ";
        if (message.StartsWith(prefix, StringComparison.Ordinal))
        {
            message = message[prefix.Length..];
        }

        if (message.Length > 120)
        {
            message = message[..120] + " ...";
        }

        return exception is ClickHouseTcpServerException server
            ? $"{server.Code} (code {server.RawCode}): {message}"
            : $"{exception.GetType().Name}: {message}";
    }

    // Reflows a long driver message so the console output stays readable.
    private static string Wrap(string message)
    {
        var lines = new List<string>();
        var line = new System.Text.StringBuilder();
        foreach (string word in message.Split(' '))
        {
            if (line.Length + word.Length + 1 > 92)
            {
                lines.Add(line.ToString());
                line.Clear();
            }

            line.Append(line.Length == 0 ? word : " " + word);
        }

        lines.Add(line.ToString());
        return string.Join("\n       ", lines);
    }
}
