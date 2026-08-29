using System.Net;
using System.Net.Sockets;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Block compression on the native protocol: the <c>Compression=lz4|zstd|none</c> connection-string key,
/// <see cref="ClickHouseTcpClientOptions.Compressor"/>, and what each setting is worth in bytes on the wire.
///
/// <para>
/// LZ4 is the default, so blocks are compressed in both directions unless you say otherwise. The HTTP transport's
/// <c>Compression</c> key is a boolean; this one names a codec.
/// </para>
///
/// <para>
/// <b>How this example measures.</b> It forwards the connection through a local socket that counts the bytes each
/// way, which is the only thing a client can observe honestly. Wall-clock time is not measured: everything here
/// runs over loopback, where there is no bandwidth to save, so a timing comparison would report the CPU cost of
/// compressing and none of the benefit. Wire size is deterministic and is the thing compression actually buys.
/// </para>
/// </summary>
public static class TcpCompression
{
    private const string TableName = "example_tcp_compression";

    // Big enough that the codec dominates the fixed cost of a handshake, small enough to stay quick.
    private const int SelectRows = 200_000;
    private const int InsertRows = 100_000;

    // Two columns, one of them compressible text, so the ratio is representative rather than a best case.
    private static readonly string Query = $"SELECT number, toString(number % 97) AS text FROM numbers({SelectRows})";

    public static async Task Run()
    {
        WhatIsInForce();

        await using var client = ExampleConfig.CreateTcpClient();
        await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
        await client.ExecuteAsync($"CREATE TABLE {TableName} (id UInt64, text String) ENGINE = MergeTree ORDER BY id");

        try
        {
            await ReadingDirection();
            await WhoChoosesTheServersCodec();
            await WritingDirection();
            WhatItBuys();
        }
        finally
        {
            await client.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}");
            Console.WriteLine($"\nDropped {TableName}.");
        }
    }

    private static void WhatIsInForce()
    {
        Console.WriteLine("1. Which codec a client is using\n");

        // The assembled connection string carries no Compression key, so this is the default.
        ClickHouseTcpClientOptions fromDefaults = ClickHouseTcpClientOptions.FromConnectionString(ExampleConfig.TcpConnectionString);

        Console.WriteLine($"   no Compression key          Compressor = {Describe(fromDefaults.Compressor)}");
        foreach (string codec in new[] { "lz4", "zstd", "none" })
        {
            ClickHouseTcpClientOptions options = Options(codec);
            Console.WriteLine($"   Compression={codec,-16}Compressor = {Describe(options.Compressor)}");
        }

        Console.WriteLine();
        Console.WriteLine("   'none' leaves Compressor null, and null means the query asks the server for no");
        Console.WriteLine("   compression at all — which is not the same as a frame whose method byte says NONE.");
        Console.WriteLine();
        Console.WriteLine("   Setting it in code instead of in a connection string takes the codec object:");
        Console.WriteLine("     options with { Compressor = ZstdCompressor.Default }");
        Console.WriteLine("     options with { Compressor = new ZstdCompressor(level: 9) }");
        Console.WriteLine("     options with { Compressor = null }                        // off");
        Console.WriteLine();

        // A codec that only implements the HTTP body path cannot frame a block, and the client says so at
        // construction rather than mid-query.
        try
        {
            using var refused = new ClickHouseTcpClient(Options("lz4") with { Compressor = GZipCompressor.Default });
            Console.WriteLine("   A GZip codec was accepted, which is not what this example expected");
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine($"   Not every IClickHouseCompressor will do: {ex.Message.Split(" (Parameter")[0]}");
        }
    }

    private static async Task ReadingDirection()
    {
        Console.WriteLine($"\n2. Server to client: {SelectRows:N0} rows, measured on the wire\n");
        Console.WriteLine($"   {Query}\n");
        Console.WriteLine($"   {"client codec",-14}{"bytes from the server",22}  {"vs none",8}");

        long baseline = 0;
        foreach (string codec in new[] { "none", "lz4", "zstd" })
        {
            (long fromServer, _, long rows) = await Measure(Options(codec), null);
            if (codec == "none")
            {
                baseline = fromServer;
            }

            Console.WriteLine($"   {codec,-14}{fromServer,22:N0}  {(double)baseline / fromServer,7:0.00}x   ({rows:N0} rows)");
        }

        Console.WriteLine();
        Console.WriteLine("   LZ4 cut it to about a third. ZSTD produced the same count as LZ4, to within the few");
        Console.WriteLine("   bytes of progress packets that vary between runs — which is the thing to understand");
        Console.WriteLine("   about this key.");
    }

    private static async Task WhoChoosesTheServersCodec()
    {
        Console.WriteLine("\n3. The client's codec does not choose what the server sends\n");
        Console.WriteLine("   The query packet carries one flag: compressed, or not. Which codec the server then");
        Console.WriteLine("   frames its blocks with is the server's own choice, from its network_compression_method");
        Console.WriteLine("   setting — LZ4 by default. So asking for ZSTD on the client changed nothing above.");
        Console.WriteLine("   Set it as a per-query setting to change it:\n");

        Console.WriteLine($"   {"network_compression_method",-30}{"bytes from the server",22}");
        foreach (string method in new[] { "LZ4", "ZSTD" })
        {
            var options = new ClickHouseTcpQueryOptions
            {
                Settings = new Dictionary<string, string> { ["network_compression_method"] = method },
            };
            (long fromServer, _, _) = await Measure(Options("lz4"), options);
            Console.WriteLine($"   {method,-30}{fromServer,22:N0}");
        }

        Console.WriteLine();
        Console.WriteLine("   The client decodes whatever arrives, whichever codec it asked for, so this is safe to");
        Console.WriteLine("   set per query. What the client's own Compressor decides is the direction the client");
        Console.WriteLine("   writes — which is an insert.");
    }

    private static async Task WritingDirection()
    {
        Console.WriteLine($"\n4. Client to server: an insert of {InsertRows:N0} rows\n");
        Console.WriteLine($"   {"client codec",-14}{"bytes to the server",22}  {"vs none",8}");

        long baseline = 0;
        foreach (string codec in new[] { "none", "lz4", "zstd" })
        {
            long toServer = await MeasureInsert(Options(codec));
            if (codec == "none")
            {
                baseline = toServer;
            }

            Console.WriteLine($"   {codec,-14}{toServer,22:N0}  {(double)baseline / toServer,7:0.00}x");
        }

        Console.WriteLine();
        Console.WriteLine("   Here the key does what its name suggests, because these are the client's own frames.");
        Console.WriteLine("   ZSTD is the smaller of the two and costs more CPU on the client to produce; LZ4 is the");
        Console.WriteLine("   cheaper one and is what the default gives you.");
    }

    private static void WhatItBuys()
    {
        Console.WriteLine("\n5. What the numbers above do and do not tell you\n");
        Console.WriteLine("   They are bytes, and bytes are the honest measurement: run this example twice and the");
        Console.WriteLine("   counts differ only by the handful of progress packets the server chose to send.");
        Console.WriteLine();
        Console.WriteLine("   There is deliberately no timing here. Every connection in this example is loopback,");
        Console.WriteLine("   where a saved byte saves nothing, so a wall-clock ranking of none/lz4/zstd would");
        Console.WriteLine("   measure the cost of compressing and none of the benefit — and would then read as an");
        Console.WriteLine("   argument for turning compression off. Where it pays is where the bytes have somewhere");
        Console.WriteLine("   to go: a link between availability zones, a metered egress bill, a saturated uplink,");
        Console.WriteLine("   or a server whose network is busier than its CPU.");
        Console.WriteLine();
        Console.WriteLine("   Reasonable defaults, then:");
        Console.WriteLine("     lz4    leave it alone. Cheapest in CPU, lightest on the server, ~3x here.");
        Console.WriteLine("     zstd   a slow or metered link, and inserts large enough for the ratio to matter.");
        Console.WriteLine("     none   a client and a server on the same host, where the CPU is the scarce thing.");
        Console.WriteLine();
        Console.WriteLine("   Compression is per query, not per connection, so nothing has to be restarted to change");
        Console.WriteLine("   it — but the codec lives on the client, so it takes a second client to run two.");
    }

    /// <summary>Options for the configured server with one <c>Compression</c> value.</summary>
    private static ClickHouseTcpClientOptions Options(string codec)
    {
        var builder = ExampleConfig.TcpBuilder();
        builder.Compression = codec;
        return builder.ToOptions();
    }

    private static string Describe(IClickHouseCompressor compressor)
        => compressor is null ? "null (no compression)" : compressor.GetType().Name;

    /// <summary>Runs the query through a counting proxy and reports the bytes each way.</summary>
    private static async Task<(long FromServer, long ToServer, long Rows)> Measure(
        ClickHouseTcpClientOptions options,
        ClickHouseTcpQueryOptions? queryOptions)
    {
        await using var proxy = new CountingProxy(ExampleConfig.TcpEndpoint.Host, ExampleConfig.TcpEndpoint.Port);

        long rows = 0;

        // The client is disposed before the counters are read, so every byte of the handshake, the query and the
        // close is included. The handshake is a few hundred bytes and identical between the runs.
        await using (var client = new ClickHouseTcpClient(options with { Host = "127.0.0.1", Port = proxy.Port }))
        {
            await foreach (Block block in client.StreamAsync(Query, queryOptions))
            {
                rows += block.RowCount;
            }
        }

        return (proxy.FromServer, proxy.ToServer, rows);
    }

    private static async Task<long> MeasureInsert(ClickHouseTcpClientOptions options)
    {
        await using var proxy = new CountingProxy(ExampleConfig.TcpEndpoint.Host, ExampleConfig.TcpEndpoint.Port);

        var ids = new ulong[InsertRows];
        var text = new string[InsertRows];
        for (int i = 0; i < InsertRows; i++)
        {
            ids[i] = (ulong)i;
            text[i] = (i % 97).ToString();
        }

        await using (var client = new ClickHouseTcpClient(options with { Host = "127.0.0.1", Port = proxy.Port }))
        {
            await client.InsertAsync(
                $"INSERT INTO {TableName} (id, text) VALUES",
                [ClickHouseTcpColumn.Create("id", ids), ClickHouseTcpColumn.Create("text", text)]);
        }

        return proxy.ToServer;
    }

    /// <summary>
    /// A local socket that forwards to the real server and counts the bytes each way. Nothing an application needs
    /// — it is here because wire size is not otherwise observable from the client, and a byte count is the only
    /// claim about compression that loopback can support.
    /// </summary>
    private sealed class CountingProxy : IAsyncDisposable
    {
        private readonly TcpListener listener;
        private readonly CancellationTokenSource stopping = new();
        private readonly Task accepting;
        private long fromServer;
        private long toServer;

        public CountingProxy(string host, int port)
        {
            listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            Port = ((IPEndPoint)listener.LocalEndpoint).Port;
            accepting = AcceptLoop(host, port);
        }

        /// <summary>The loopback port to point a client at.</summary>
        public int Port { get; }

        public long FromServer => Interlocked.Read(ref fromServer);

        public long ToServer => Interlocked.Read(ref toServer);

        public async ValueTask DisposeAsync()
        {
            await stopping.CancelAsync();
            listener.Stop();
            try
            {
                await accepting;
            }
            catch (Exception ex) when (ex is OperationCanceledException or SocketException or ObjectDisposedException)
            {
            }

            stopping.Dispose();
        }

        private async Task AcceptLoop(string host, int port)
        {
            var sessions = new List<Task>();
            try
            {
                while (!stopping.IsCancellationRequested)
                {
                    TcpClient accepted = await listener.AcceptTcpClientAsync(stopping.Token);
                    sessions.Add(Forward(accepted, host, port));
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or SocketException or ObjectDisposedException)
            {
                // The listener was stopped, which is the normal ending here.
            }

            foreach (Task session in sessions)
            {
                try
                {
                    await session;
                }
                catch (Exception ex) when (ex is OperationCanceledException or IOException or SocketException or ObjectDisposedException)
                {
                }
            }
        }

        private async Task Forward(TcpClient downstream, string host, int port)
        {
            using TcpClient upstream = new();
            using (downstream)
            {
                await upstream.ConnectAsync(host, port, stopping.Token);
                await Task.WhenAll(
                    Copy(downstream.GetStream(), upstream.GetStream(), towardsServer: true),
                    Copy(upstream.GetStream(), downstream.GetStream(), towardsServer: false));
            }
        }

        private async Task Copy(Stream from, Stream to, bool towardsServer)
        {
            byte[] buffer = new byte[64 * 1024];
            try
            {
                while (true)
                {
                    int read = await from.ReadAsync(buffer, stopping.Token);
                    if (read == 0)
                    {
                        break;
                    }

                    Interlocked.Add(ref towardsServer ? ref toServer : ref fromServer, read);
                    await to.WriteAsync(buffer.AsMemory(0, read), stopping.Token);
                    await to.FlushAsync(stopping.Token);
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or IOException or SocketException or ObjectDisposedException)
            {
                // Either side closing ends the copy; the counts up to that point are what matters.
            }
        }
    }
}
