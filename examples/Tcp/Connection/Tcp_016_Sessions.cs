using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// <c>OpenSessionAsync</c>: one connection out of the pool, pinned until the session is disposed, so state a
/// single connection holds — a temporary table, what a <c>SET</c> changed, which roles are active — survives from
/// one operation to the next.
///
/// <para>
/// Three rules come with that. One operation runs at a time, because the protocol carries one query per
/// connection. Disposal <b>closes</b> the connection instead of returning it to the pool, so no unrelated caller
/// can inherit the session's state. And a session holds one of the pool's slots for its whole lifetime, so keep it
/// short — <c>Tcp_017_PoolTuning</c> shows what happens when sessions outnumber the pool.
/// </para>
///
/// <para>
/// A session is also the native answer to HTTP's per-query <c>Roles</c>, which this transport does not have:
/// <c>SET ROLE</c> inside a session applies to every operation that follows it and to nothing else.
/// </para>
/// </summary>
public static class TcpSessions
{
    private const string RoleTable = "example_tcp_sessions_orders";
    private const string RoleName = "example_tcp_sessions_reader";
    private const string RoleUser = "example_tcp_sessions_user";

    // The password of the user this example creates to demonstrate SET ROLE. Not a connection string: the
    // endpoint still comes from ExampleConfig.
    private const string RoleUserPassword = "example_tcp_sessions_pw";

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();

        await StateThatSurvives(client);
        await OneOperationAtATime(client);
        await DisposalClosesTheConnection();
        await SetRoleInASession(client);
        WhatToRemember();
    }

    private static async Task StateThatSurvives(ClickHouseTcpClient client)
    {
        Console.WriteLine("1. One pinned connection, so connection-local state survives\n");

        await using IClickHouseTcpSession session = await client.OpenSessionAsync();
        Console.WriteLine($"   Opened a session. IsOpen = {session.IsOpen}");

        // A temporary table belongs to the connection that created it, which is why it needs a session at all.
        // It also needs no cleanup: the server drops it when the connection closes, and disposing the session
        // closes the connection.
        await session.ExecuteAsync("CREATE TEMPORARY TABLE example_tcp_sessions_scratch (id UInt64, note String) ENGINE = Memory");
        await session.InsertRowsAsync(
            "INSERT INTO example_tcp_sessions_scratch (id, note) VALUES",
            new[]
            {
                new object[] { 1UL, "first" },
                new object[] { 2UL, "second" },
            });

        object rows = await session.ExecuteScalarAsync("SELECT count() FROM example_tcp_sessions_scratch");
        Console.WriteLine($"   Created a TEMPORARY TABLE, inserted 2 rows, read back {rows} — three operations, one connection");

        const string visible = "SELECT count() FROM system.tables WHERE is_temporary AND name = 'example_tcp_sessions_scratch'";
        Console.WriteLine($"   system.tables sees it inside the session: {await session.ExecuteScalarAsync(visible)}");

        // The same client, but this operation takes whatever connection the pool hands out, so it is a different
        // session on the server and the table is not there.
        Console.WriteLine($"   ... and not from the client's pool:         {await client.ExecuteScalarAsync(visible)}");

        // A SET is connection state too, so it lasts exactly as long as the session.
        await session.ExecuteAsync("SET max_threads = 7");
        Console.WriteLine("\n   After SET max_threads = 7 in the session:");
        Console.WriteLine($"     getSetting('max_threads') in the session = {await session.ExecuteScalarAsync("SELECT getSetting('max_threads')")}");
        Console.WriteLine($"     getSetting('max_threads') on the client   = {await client.ExecuteScalarAsync("SELECT getSetting('max_threads')")}");
        Console.WriteLine("   A client-level setting reaches every operation instead; see Tcp_002's set_<name> keys.");
    }

    private static async Task OneOperationAtATime(ClickHouseTcpClient client)
    {
        Console.WriteLine("\n2. One operation at a time\n");

        await using IClickHouseTcpSession session = await client.OpenSessionAsync();

        // Not awaited yet: an async method body starts on the calling thread, so by the time this returns the
        // session has already claimed its connection for this query.
        ValueTask<object> running = session.ExecuteScalarAsync("SELECT sleep(0.2), 'slow'");

        Console.WriteLine($"   A query is in flight. IsOpen = {session.IsOpen} (busy is not closed)");

        try
        {
            _ = await session.ExecuteScalarAsync("SELECT 'me too'");
        }
        catch (InvalidOperationException ex)
        {
            Console.WriteLine($"   A second operation on the same session throws {ex.GetType().Name}:");
            Console.WriteLine($"     {ex.Message}");
        }

        _ = await running;
        Console.WriteLine($"\n   The first one finished, so the session is free again: {await session.ExecuteScalarAsync("SELECT 'next'")}");
        Console.WriteLine("   To run operations at once, run them on the client: each takes its own connection.");
    }

    private static async Task DisposalClosesTheConnection()
    {
        Console.WriteLine("\n3. Disposal closes the connection, it does not pool it\n");

        // MaxPoolSize = 1 makes the experiment decisive. If disposal returned the connection to the pool, the
        // second session would be handed the same one and would find the first session's temporary table.
        await using var client = new ClickHouseTcpClient(ExampleConfig.TcpBuilder().ToOptions() with { MaxPoolSize = 1 });

        const string visible = "SELECT count() FROM system.tables WHERE is_temporary AND name = 'example_tcp_sessions_handover'";

        IClickHouseTcpSession first = await client.OpenSessionAsync();
        await using (first)
        {
            await first.ExecuteAsync("CREATE TEMPORARY TABLE example_tcp_sessions_handover (id UInt64) ENGINE = Memory");
            Console.WriteLine($"   First session created a temporary table and sees it: {await first.ExecuteScalarAsync(visible)}");
        }

        Console.WriteLine($"   Disposed it. IsOpen = {first.IsOpen}");

        await using IClickHouseTcpSession second = await client.OpenSessionAsync();
        Console.WriteLine($"   Second session, from a pool of exactly one connection, sees: {await second.ExecuteScalarAsync(visible)}");
        Console.WriteLine("   Zero, so the first connection was closed rather than reused. That is the point: a");
        Console.WriteLine("   caller must never inherit another caller's temporary tables and settings.");
        Console.WriteLine("   It also means nothing has to be dropped — and that a session costs a reconnect.");
    }

    private static async Task SetRoleInASession(ClickHouseTcpClient admin)
    {
        Console.WriteLine("\n4. SET ROLE, which is what this transport has instead of a per-query role\n");
        Console.WriteLine("   ClickHouseTcpQueryOptions carries no Roles: there is nowhere on the wire to put one per");
        Console.WriteLine("   query. A session is the equivalent, and it is per connection rather than per query.\n");

        await admin.ExecuteAsync($"CREATE OR REPLACE TABLE {RoleTable} (id UInt64) ENGINE = MergeTree ORDER BY id");
        await admin.ExecuteAsync($"CREATE ROLE OR REPLACE {RoleName}");
        await admin.ExecuteAsync($"GRANT SELECT ON {RoleTable} TO {RoleName}");
        await admin.ExecuteAsync($"CREATE USER OR REPLACE {RoleUser} IDENTIFIED WITH plaintext_password BY '{RoleUserPassword}'");
        await admin.ExecuteAsync($"GRANT {RoleName} TO {RoleUser}");
        Console.WriteLine($"   Created user '{RoleUser}', role '{RoleName}' holding SELECT on '{RoleTable}'");

        try
        {
            // Same server as everything else here; only the credentials differ.
            var asUser = ExampleConfig.TcpBuilder();
            asUser.Username = RoleUser;
            asUser.Password = RoleUserPassword;

            await using var client = new ClickHouseTcpClient(asUser.ToOptions());
            await using IClickHouseTcpSession session = await client.OpenSessionAsync();

            Console.WriteLine($"\n   Fresh session, granted roles active by default: {await session.ExecuteScalarAsync("SELECT toString(currentRoles())")}");

            await session.ExecuteAsync("SET ROLE NONE");
            Console.WriteLine($"   After SET ROLE NONE:                            {await session.ExecuteScalarAsync("SELECT toString(currentRoles())")}");

            try
            {
                _ = await session.ExecuteScalarAsync($"SELECT count() FROM {RoleTable}");
            }
            catch (ClickHouseTcpServerException ex)
            {
                Console.WriteLine($"   Reading the table now fails with {ex.Code} ({ex.RawCode}), so the grant really came from the role.");

                // A server-side error in a query the server accepted does not end the session: the connection is
                // still good, only the query failed.
                Console.WriteLine($"   IsOpen after that error = {session.IsOpen}, so the session carries on");
            }

            await session.ExecuteAsync($"SET ROLE {RoleName}");
            Console.WriteLine($"   After SET ROLE {RoleName}, the read works again: {await session.ExecuteScalarAsync($"SELECT count() FROM {RoleTable}")} rows");

            // The client's own operations run over other connections, which never saw either SET ROLE.
            Console.WriteLine($"\n   Meanwhile an operation on the client, over a pooled connection: {await client.ExecuteScalarAsync("SELECT toString(currentRoles())")}");
            Console.WriteLine("   Untouched. A SET ROLE reaches exactly the connection it ran on.");
        }
        finally
        {
            await admin.ExecuteAsync($"DROP USER IF EXISTS {RoleUser}");
            await admin.ExecuteAsync($"DROP ROLE IF EXISTS {RoleName}");
            await admin.ExecuteAsync($"DROP TABLE IF EXISTS {RoleTable}");
            Console.WriteLine($"\n   Dropped the user, the role and '{RoleTable}'. The temporary tables above needed no cleanup.");
        }
    }

    private static void WhatToRemember()
    {
        Console.WriteLine("\n5. Worth knowing before you open one\n");
        Console.WriteLine("   Keep it short. A session holds one of MaxPoolSize connections from OpenSessionAsync");
        Console.WriteLine("   until disposal, so as many sessions as the pool is wide leaves nothing for anything");
        Console.WriteLine("   else, and the next caller waits out PoolTimeout and then fails. See Tcp_017.");
        Console.WriteLine();
        Console.WriteLine("   Finish what you stream. A StreamAsync or QueryAsync result holds the session until it");
        Console.WriteLine("   is read to the end or its enumerator is disposed — 'await foreach' does that for you.");
        Console.WriteLine("   One left suspended mid-enumeration and never disposed cannot give its slot back at all,");
        Console.WriteLine("   which is the one thing here not demonstrated: showing it means leaking a connection.");
        Console.WriteLine();
        Console.WriteLine("   Read IsOpen as a floor, not a promise. False is certain: the session is finished, its");
        Console.WriteLine("   server-side state is gone, and the answer is a new session rather than a retry. True");
        Console.WriteLine("   only means nothing is known to be wrong. A failed transport, a cancellation, or a");
        Console.WriteLine("   half-read stream ends a session; a server error in a query the server accepted does not.");
    }
}
