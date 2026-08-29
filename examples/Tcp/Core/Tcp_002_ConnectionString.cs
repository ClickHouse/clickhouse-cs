using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Configuring the native-protocol client: what a native connection string holds, how
/// <see cref="ClickHouseTcpConnectionStringBuilder"/> builds one, and how it becomes a
/// <see cref="ClickHouseTcpClientOptions"/> — the record every client is built from.
///
/// <para>
/// The key set is not the HTTP one. There is no <c>Protocol</c> key, <c>Compression</c> names a codec instead of
/// switching a boolean, and the pool and TLS keys have no HTTP counterpart at all.
/// </para>
///
/// <para>
/// Configuration is this example's subject, so the connection strings in its output are literals. The client it
/// connects with still comes from <c>ExampleConfig</c>.
/// </para>
/// </summary>
public static class TcpConnectionString
{
    public static async Task Run()
    {
        ShowTheKeys();
        ClickHouseTcpClientOptions options = BuildOptions();
        DeriveAVariant(options);
        ShowTlsKeys();
        await ConnectWithThem(options);
    }

    private static void ShowTheKeys()
    {
        Console.WriteLine("1. A native-protocol connection string:\n");
        Console.WriteLine("   Host=localhost;Port=9000;Username=default;Password=secret;Database=default");
        Console.WriteLine();
        Console.WriteLine("   Every key is optional: Host defaults to localhost, Username to default, Database to");
        Console.WriteLine("   default, Password to empty. Port is the one to watch — the native protocol listens on");
        Console.WriteLine("   9000, not on the HTTP interface's 8123.");
        Console.WriteLine();
        Console.WriteLine("   There is no Protocol key. UseTls=true selects TLS, and an unset Port then resolves to");
        Console.WriteLine("   9440, the secure native port, instead of 9000.");

        Console.WriteLine("\n2. The keys that are not in the HTTP set:\n");
        Console.WriteLine("   Compression=lz4|zstd|none   A codec name, where the HTTP client's Compression is a");
        Console.WriteLine("                               boolean. lz4 is the default, so wire blocks are");
        Console.WriteLine("                               compressed in both directions unless this says none.");
        Console.WriteLine("   Pool                        MinPoolSize, MaxPoolSize, PoolTimeout, IdleTimeout,");
        Console.WriteLine("                               MaxConnectionLifetime, SweepInterval, PoolReusePolicy");
        Console.WriteLine("   TLS                         UseTls, TlsServerName, TlsCaCertificatePath,");
        Console.WriteLine("                               TlsAllowInvalidCertificates");
        Console.WriteLine("   Deadlines                   DialTimeout, ReadTimeout (both in seconds)");
        Console.WriteLine("   Other                       QuotaKey, MaxSendBufferBytes, and set_<name>=<value>");
        Console.WriteLine("                               for a ClickHouse setting sent with every operation");
    }

    private static ClickHouseTcpClientOptions BuildOptions()
    {
        // Every key the builder knows has a typed property, so a name is checked at compile time rather than
        // kept as an unknown key and ignored. An unreadable UseTls, TLS-authority or PoolReusePolicy value throws;
        // an unreadable number falls back to its default.
        var builder = ExampleConfig.TcpBuilder();
        builder.Compression = "zstd";
        builder.MaxPoolSize = 4;
        builder.IdleTimeout = TimeSpan.FromSeconds(60);

        // Custom settings have no typed property: any set_<name> key becomes a client-level ClickHouse setting.
        builder["set_max_threads"] = 2;

        Console.WriteLine("\n3. ClickHouseTcpConnectionStringBuilder:\n");
        Console.WriteLine($"   Host             {builder.Host}");
        Console.WriteLine($"   Port             {builder.Port?.ToString() ?? "(unset: resolved from UseTls)"}");
        Console.WriteLine($"   Username         {builder.Username}");
        Console.WriteLine($"   Password         {(builder.Password.Length == 0 ? "(empty)" : "(set — not printed)")}");
        Console.WriteLine($"   Database         {builder.Database}");
        Console.WriteLine($"   Compression      {builder.Compression}");
        Console.WriteLine($"   MaxPoolSize      {builder.MaxPoolSize}");
        Console.WriteLine($"   IdleTimeout      {builder.IdleTimeout.TotalSeconds}s");
        Console.WriteLine($"   UseTls           {builder.UseTls}");
        Console.WriteLine();
        Console.WriteLine("   builder.ToString() would render all of that back as a connection string, password");
        Console.WriteLine("   included, so it is not something to log.");

        // ToOptions() materializes the keys; FromConnectionString(text) is the same thing in one call for a string
        // that came from configuration.
        ClickHouseTcpClientOptions options = builder.ToOptions();
        ClickHouseTcpClientOptions fromText = ClickHouseTcpClientOptions.FromConnectionString(ExampleConfig.TcpConnectionString);

        Console.WriteLine("\n4. ClickHouseTcpClientOptions — what a client is really built from:\n");

        // The record's generated ToString would print the password; this override names only the safe properties,
        // so options are safe to log. The port it shows is the resolved one.
        Console.WriteLine($"   builder.ToOptions()   {options}");
        Console.WriteLine($"      Compressor       {Describe(options.Compressor)}");
        Console.WriteLine($"      MaxPoolSize      {options.MaxPoolSize}");
        Console.WriteLine($"      IdleTimeout      {options.IdleTimeout}");
        Console.WriteLine($"      CustomSettings   {string.Join(", ", options.CustomSettings.Select(s => $"{s.Key}={s.Value}"))}");
        Console.WriteLine();
        Console.WriteLine($"   FromConnectionString(ExampleConfig.TcpConnectionString)   {fromText}");
        Console.WriteLine($"      Compressor       {Describe(fromText.Compressor)}");
        Console.WriteLine("      That string carries no Compression key, so the lz4 default stands. Only 'none'");
        Console.WriteLine("      leaves the codec null, and null means the query asks for no compression at all.");

        return options;
    }

    private static string Describe(IClickHouseCompressor compressor)
        => compressor?.GetType().Name ?? "(none)";

    private static void DeriveAVariant(ClickHouseTcpClientOptions options)
    {
        // Options are an init-only record, so one instance can hold what every client shares and a 'with'
        // expression derives the variant. The original is untouched.
        ClickHouseTcpClientOptions wide = options with { MaxPoolSize = 32, Database = "system" };

        Console.WriteLine("\n5. Options is a record, so 'with' derives a variant:\n");
        Console.WriteLine($"   options with {{ MaxPoolSize = 32, Database = \"system\" }}");
        Console.WriteLine($"   original   MaxPoolSize={options.MaxPoolSize}, Database={options.Database}");
        Console.WriteLine($"   variant    MaxPoolSize={wide.MaxPoolSize}, Database={wide.Database}");
    }

    private static void ShowTlsKeys()
    {
        Console.WriteLine("\n6. The TLS keys, and how they are checked:\n");
        Console.WriteLine("   UseTls=true                        encrypt the transport, and dial 9440 unless Port says");
        Console.WriteLine("                                      otherwise. The handshake carries the password in the");
        Console.WriteLine("                                      clear, so this is what protects it.");
        Console.WriteLine("   TlsServerName=host                 the name to match the certificate against, when Host");
        Console.WriteLine("                                      is an address or an internal alias");
        Console.WriteLine("   TlsCaCertificatePath=ca.pem        validate against these authorities instead of the");
        Console.WriteLine("                                      host trust store");
        Console.WriteLine("   TlsAllowInvalidCertificates=true   accept any certificate — development only");

        // A TLS key with UseTls left false is refused at construction. Silently ignoring it is how a connection
        // meant to be encrypted ends up in the clear.
        try
        {
            _ = new ClickHouseTcpClient(new ClickHouseTcpClientOptions
            {
                Host = ExampleConfig.Host,
                TlsAllowInvalidCertificates = true,
            });
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine();
            Console.WriteLine("   A TLS key set while UseTls is false is rejected, not ignored:");
            Console.WriteLine($"     {ex.Message}");
        }
    }

    private static async Task ConnectWithThem(ClickHouseTcpClientOptions options)
    {
        Console.WriteLine("\n7. Running with those options:\n");

        await using var client = new ClickHouseTcpClient(options);

        var server = await client.GetServerInfoAsync();
        Console.WriteLine($"   Connected to {server}, blocks framed with {Describe(options.Compressor)}");

        // set_max_threads became a client-level setting, so the server sees it on every operation.
        object maxThreads = await client.ExecuteScalarAsync("SELECT getSetting('max_threads')");
        Console.WriteLine($"   getSetting('max_threads') = {maxThreads} — the set_max_threads key reached the server");
    }
}
