using System.Security.Cryptography;
using System.Text;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Sends related work to one ClickHouse Cloud replica with a replica tag. Needs a Cloud service with two or more
/// replicas and replica-aware routing for the native protocol turned on, so it runs only by explicit filter:
/// set CLICKHOUSE_CLOUD_HOST and CLICKHOUSE_CLOUD_PASSWORD (and CLICKHOUSE_CLOUD_USER when it is not "default"),
/// then run <c>dotnet run -- --filter TcpReplicaRouting</c>.
/// </summary>
public static class TcpReplicaRouting
{
    public static async Task Run()
    {
        string host = Environment.GetEnvironmentVariable("CLICKHOUSE_CLOUD_HOST");
        if (string.IsNullOrEmpty(host))
        {
            Console.WriteLine("Set CLICKHOUSE_CLOUD_HOST and CLICKHOUSE_CLOUD_PASSWORD to run this example.");
            return;
        }

        // A tag is sent in the TLS server name, so it needs TLS and must be one DNS label. Hash a value that has
        // other characters, such as an email address.
        string tag = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes("ada@example.com")))[..32];
        Console.WriteLine($"Replica tag for ada@example.com: {tag}");

        var options = new ClickHouseTcpClientOptions
        {
            Host = host,
            UseTls = true,
            Username = Environment.GetEnvironmentVariable("CLICKHOUSE_CLOUD_USER") ?? "default",
            Password = Environment.GetEnvironmentVariable("CLICKHOUSE_CLOUD_PASSWORD") ?? string.Empty,
        };

        // Each client opens its own connection, so each line below is a separate trip through the load balancer.
        Console.WriteLine("\nWithout a tag, connections are load balanced:");
        for (int i = 0; i < 4; i++)
        {
            await using var client = new ClickHouseTcpClient(options);
            Console.WriteLine($"  {await client.ExecuteScalarAsync("SELECT hostName()")}");
        }

        Console.WriteLine("\nWith the same tag, connections reach the same replica:");
        for (int i = 0; i < 4; i++)
        {
            await using var client = new ClickHouseTcpClient(options);
            object replica = await client.ExecuteScalarAsync(
                "SELECT hostName()",
                new ClickHouseTcpQueryOptions { ReplicaTag = tag });
            Console.WriteLine($"  {replica}");
        }

        // A session keeps temporary tables and settings on one connection. The tag chooses the replica it runs on.
        await using var shared = new ClickHouseTcpClient(options);
        await using IClickHouseTcpSession session = await shared.OpenSessionAsync(
            new ClickHouseTcpSessionOptions { ReplicaTag = tag });
        object sessionReplica = await session.ExecuteScalarAsync("SELECT hostName()");
        Console.WriteLine($"\nSession with the same tag runs on: {sessionReplica}");
    }
}
