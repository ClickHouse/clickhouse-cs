using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Keeps temporary tables and settings on one pinned connection.</summary>
public static class TcpSessions
{
    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();
        object clientDefault = await client.ExecuteScalarAsync("SELECT getSetting('max_threads')");

        // A session pins one connection, so temporary state survives between its operations.
        await using IClickHouseTcpSession session = await client.OpenSessionAsync();

        Console.WriteLine($"Session open: {session.IsOpen}");

        await session.ExecuteAsync("""
            CREATE TEMPORARY TABLE example_tcp_session_values
            (id UInt64, note String)
            ENGINE = Memory
            """);
        await session.InsertRowsAsync(
            "INSERT INTO example_tcp_session_values (id, note) VALUES",
            new[]
            {
                new object[] { 1UL, "first" },
                new object[] { 2UL, "second" },
            });

        object count = await session.ExecuteScalarAsync(
            "SELECT count() FROM example_tcp_session_values");
        Console.WriteLine($"Temporary table rows: {count}");

        await session.ExecuteAsync("SET max_threads = 2");
        object sessionValue = await session.ExecuteScalarAsync("SELECT getSetting('max_threads')");

        Console.WriteLine($"max_threads in session: {sessionValue}");
        Console.WriteLine($"max_threads before the session: {clientDefault}");

        // Sessions accept one operation at a time. Disposal closes the pinned connection and its state.
    }
}
