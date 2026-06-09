namespace ClickHouse.Driver.Examples;

/// <summary>
/// Demonstrates connecting over the ClickHouse <b>Native (TCP) protocol</b> instead of HTTP and
/// running basic SELECT queries.
///
/// The only difference from an HTTP client is the connection string: <c>Protocol=native</c>.
/// This makes the driver speak the binary TCP protocol on port 9000 (the default native port)
/// rather than HTTP on 8123. Everything else — ExecuteReaderAsync, ExecuteScalarAsync, the typed
/// getters on the reader — works the same way.
///
/// Native protocol support is an MVP: it covers the handshake and SELECT result reading for scalar,
/// String, Nullable and Array columns. Query parameters and bulk insert still require the HTTP
/// protocol.
/// </summary>
public static class NativeBasicSelect
{
    public static async Task Run()
    {
        // Protocol=native switches the transport to the binary TCP protocol.
        // Port defaults to 9000 for native (8123 for http).
        using var client = new ClickHouseClient("Host=localhost;Protocol=native");

        // 1. A simple scalar query via ExecuteScalarAsync.
        var total = await client.ExecuteScalarAsync("SELECT count() FROM numbers(100)");
        Console.WriteLine($"1. count() over numbers(100) = {total} (type {total?.GetType().Name})\n");

        // 2. A multi-row, multi-column SELECT read through the data reader.
        Console.WriteLine("2. Reading rows from numbers(5):");
        Console.WriteLine("   n    label");
        Console.WriteLine("   ---  --------");
        using (var reader = await client.ExecuteReaderAsync(
            "SELECT number AS n, concat('row-', toString(number)) AS label FROM numbers(5) ORDER BY n"))
        {
            while (await reader.ReadAsync())
            {
                var n = reader.GetUInt64(0);
                var label = reader.GetString(1);
                Console.WriteLine($"   {n,-4} {label}");
            }
        }

        Console.WriteLine("\nDone — data above was served over the Native TCP protocol.");
    }
}
