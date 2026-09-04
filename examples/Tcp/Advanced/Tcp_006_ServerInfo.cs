using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>Reads server identity and negotiated protocol details from the handshake.</summary>
public static class TcpServerInfo
{
    private const int ParametersRevision = 54459;
    private static readonly Version QBitFrom = new(25, 11);

    public static async Task Run()
    {
        await using var client = ExampleConfig.CreateTcpClient();
        ClickHouseTcpServerInfo server = await client.GetServerInfoAsync();

        Console.WriteLine($"Name: {server.Name}");
        Console.WriteLine($"Version: {server.Version}");
        // The negotiated revision is the lower of the two the client and the server support.
        Console.WriteLine($"Protocol revision: {server.ProtocolRevision} in force");
        Console.WriteLine($"  server advertised {server.ServerProtocolRevision}, client supports {server.ClientProtocolRevision}");
        Console.WriteLine($"Timezone: {server.Timezone}");
        Console.WriteLine($"Display name: {server.DisplayName}");

        // Gate wire-level features on the negotiated protocol revision.
        if (server.ProtocolRevision >= ParametersRevision)
        {
            var options = new ClickHouseTcpQueryOptions
            {
                Parameters = new ClickHouseTcpParameterCollection { { "minimum", 90UL } },
            };
            object count = await client.ExecuteScalarAsync(
                "SELECT count() FROM numbers(100) WHERE number >= {minimum:UInt64}",
                options);
            Console.WriteLine($"Parameterized query result: {count}");
        }
        else
        {
            Console.WriteLine("This connection does not support query parameters.");
        }

        // Gate SQL features, such as data types and functions, on the server version.
        Console.WriteLine(
            server.Version >= QBitFrom
                ? "QBit is available on this server."
                : $"QBit requires ClickHouse {QBitFrom} or newer.");

        object exactVersion = await client.ExecuteScalarAsync("SELECT version()");
        Console.WriteLine($"Exact server build: {exactVersion}");
    }
}
