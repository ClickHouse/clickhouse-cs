using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Applies client and query settings and assigns a query ID.</summary>
public static class TcpSettingsAndQueryId
{
    public static async Task Run()
    {
        var builder = ExampleConfig.TcpBuilder();

        // A set_ connection-string key becomes a default setting for every query.
        builder["set_max_threads"] = 2;

        await using var client = new ClickHouseTcpClient(builder.ToOptions());

        object clientSetting = await client.ExecuteScalarAsync("SELECT getSetting('max_threads')");
        Console.WriteLine($"Client default max_threads: {clientSetting}");

        // Query options override client defaults for this operation only.
        var queryOptions = new ClickHouseTcpQueryOptions
        {
            QueryId = $"example-tcp-settings-{Guid.NewGuid():N}",
            Settings = new Dictionary<string, string>
            {
                ["max_threads"] = "4",
                ["max_execution_time"] = "10",
            },
        };

        object querySetting = await client.ExecuteScalarAsync(
            "SELECT getSetting('max_threads')",
            queryOptions);
        object queryId = await client.ExecuteScalarAsync("SELECT currentQueryID()", queryOptions);

        Console.WriteLine($"Per-query max_threads: {querySetting}");
        Console.WriteLine($"Requested query ID: {queryOptions.QueryId}");
        Console.WriteLine($"Server query ID:    {queryId}");

        object nextSetting = await client.ExecuteScalarAsync("SELECT getSetting('max_threads')");
        Console.WriteLine($"Next query uses the client default again: {nextSetting}");

        // With no QueryId the client generates one, so every operation is correlatable with
        // system.query_log. The protocol never sends the server's own id back.
        object generated = await client.ExecuteScalarAsync("SELECT currentQueryID()");
        Console.WriteLine($"Client-generated query ID: {generated}");
    }
}
