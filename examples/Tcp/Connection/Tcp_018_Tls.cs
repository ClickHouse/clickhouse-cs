using System.Net.Security;
using System.Security.Authentication;
using ClickHouse.Driver.Tcp;

namespace ClickHouse.Driver.Examples;

/// <summary>
/// Encrypting the native transport: <c>UseTls</c>, <c>TlsServerName</c>, <c>TlsCaCertificatePath</c>,
/// <c>TlsAllowInvalidCertificates</c> and the <c>ConfigureTls</c> hook — plus the port, which follows
/// <c>UseTls</c> to 9440 unless one is given.
///
/// <para>
/// The native handshake sends the password as plaintext, so on any untrusted network TLS is the only thing
/// protecting the credentials. Nothing above the transport changes: the protocol sends the same bytes either way.
/// </para>
///
/// <para>
/// The examples' server is plaintext, so the connecting part of this example is opt-in: set
/// <c>CLICKHOUSE_TCP_TLS_CONNECTION_STRING</c> to a secure endpoint and it runs. Everything else here needs no
/// server at all, because the client checks its TLS configuration when it is constructed — including reading the
/// certificate authority file.
/// </para>
/// </summary>
public static class TcpTls
{
    private const string TlsConnectionStringVariable = "CLICKHOUSE_TCP_TLS_CONNECTION_STRING";

    public static async Task Run()
    {
        WhatTlsIsFor();
        ThePortFollowsUseTls();
        BothWaysToSetIt();
        CheckedAtConstruction();
        PinningAnAuthorityReplacesTheTrustStore();
        TheEscapeHatch();
        await ConnectIfAnEndpointWasGiven();
    }

    private static void WhatTlsIsFor()
    {
        Console.WriteLine("1. Why TLS, on this transport\n");
        Console.WriteLine("   The native protocol's first packet carries the username and password in the clear, and");
        Console.WriteLine("   then every block of data. UseTls encrypts the socket underneath all of it. The protocol");
        Console.WriteLine("   bytes are identical either way, so nothing above the transport changes.");
        Console.WriteLine();
        Console.WriteLine("   TLS is not negotiated in band. The server has to be listening for secure native");
        Console.WriteLine("   connections (tcp_port_secure, conventionally 9440), and a TLS client pointed at a");
        Console.WriteLine("   plaintext port fails its handshake rather than falling back to plaintext.");
    }

    private static void ThePortFollowsUseTls()
    {
        Console.WriteLine("\n2. The port comes from UseTls when you do not give one\n");

        // Port is int?, and null is not "0" but "derive it". ToString shows the port a connection would dial.
        ClickHouseTcpClientOptions plain = ExampleConfig.TcpBuilder().ToOptions() with { Port = null };
        ClickHouseTcpClientOptions secure = plain with { UseTls = true };
        ClickHouseTcpClientOptions explicitPort = secure with { Port = 19440 };

        Console.WriteLine($"   UseTls = false, Port unset : {plain}");
        Console.WriteLine($"   UseTls = true,  Port unset : {secure}");
        Console.WriteLine($"   UseTls = true,  Port 19440 : {explicitPort}");
        Console.WriteLine();
        Console.WriteLine("   So switching a deployment to TLS is one key, as long as the server uses the conventional");
        Console.WriteLine("   port. An explicit Port is always used as given.");
    }

    private static void BothWaysToSetIt()
    {
        Console.WriteLine("\n3. The same four keys, in a connection string and on the options record\n");

        var builder = ExampleConfig.TcpBuilder();
        builder.Port = null;
        builder.UseTls = true;
        builder.TlsServerName = "clickhouse.internal";

        // Naming an authority file here touches nothing: it is read when a client is constructed, and this
        // example never constructs one from these options.
        builder.TlsCaCertificatePath = "/etc/ssl/ca.pem";

        ClickHouseTcpClientOptions fromBuilder = builder.ToOptions();

        Console.WriteLine("   UseTls=true;TlsServerName=clickhouse.internal;TlsCaCertificatePath=/etc/ssl/ca.pem");
        Console.WriteLine("   TlsAllowInvalidCertificates=false");
        Console.WriteLine();
        Console.WriteLine($"   builder.ToOptions() : {fromBuilder}");
        Console.WriteLine($"     TlsServerName               {fromBuilder.TlsServerName}");
        Console.WriteLine($"     TlsCaCertificatePath        {fromBuilder.TlsCaCertificatePath ?? "(null: the host trust store)"}");
        Console.WriteLine($"     TlsAllowInvalidCertificates {fromBuilder.TlsAllowInvalidCertificates}");
        Console.WriteLine("     Port left unset, so the rendered options above show the 9440 it resolved to");
        Console.WriteLine();
        Console.WriteLine("   TlsServerName is the name presented as SNI and matched against the certificate; it");
        Console.WriteLine("   defaults to Host, so set it only when Host is an address or an alias the certificate");
        Console.WriteLine("   does not name. ConfigureTls is the one TLS setting with no connection-string key: it is");
        Console.WriteLine("   a delegate, so it can only be set in code.");
    }

    private static void CheckedAtConstruction()
    {
        Console.WriteLine("\n4. What is refused before anything connects\n");
        Console.WriteLine("   Every line below comes from constructing a client, with no server involved.\n");

        ClickHouseTcpClientOptions plaintext = ExampleConfig.TcpBuilder().ToOptions();

        // A TLS setting on a client that does not use TLS is refused rather than ignored. Ignoring it is how a
        // connection meant to be encrypted ends up in the clear with a configured authority as the only evidence.
        Refused("TlsServerName, UseTls left false", plaintext with { TlsServerName = "clickhouse.internal" });
        Refused("TlsAllowInvalidCertificates, UseTls left false", plaintext with { TlsAllowInvalidCertificates = true });
        Refused("TlsCaCertificatePath, UseTls left false", plaintext with { TlsCaCertificatePath = "/etc/ssl/ca.pem" });
        Refused("ConfigureTls, UseTls left false", plaintext with { ConfigureTls = _ => { } });

        ClickHouseTcpClientOptions tls = plaintext with { UseTls = true };

        // Contradictory rather than merely redundant: with validation off, the authority would be read and never
        // consulted.
        Refused("TlsAllowInvalidCertificates and TlsCaCertificatePath together", tls with
        {
            TlsAllowInvalidCertificates = true,
            TlsCaCertificatePath = "/etc/ssl/ca.pem",
        });

        Refused("A blank TlsCaCertificatePath", tls with { TlsCaCertificatePath = "   " });

        // The authority file is read once, when the client is constructed, so a wrong path or an unparseable file
        // fails here instead of on the first connection — or worse, on the first reconnect at 3am.
        string missing = Path.Combine(Path.GetTempPath(), "example-tcp-tls-no-such-ca.pem");
        Refused("A TlsCaCertificatePath that does not exist", tls with { TlsCaCertificatePath = missing });

        string notACertificate = Path.Combine(Path.GetTempPath(), "example-tcp-tls-not-a-ca.pem");
        try
        {
            File.WriteAllText(notACertificate, "these are not the certificates you are looking for\n");
            Refused("A TlsCaCertificatePath that is not a PEM certificate", tls with { TlsCaCertificatePath = notACertificate });
        }
        finally
        {
            File.Delete(notACertificate);
        }

        // The connection-string parser is strict about these two keys for the same reason: a value it cannot read
        // must not quietly become the plaintext default.
        try
        {
            _ = ClickHouseTcpClientOptions.FromConnectionString("Host=clickhouse.example;UseTls=perhaps");
        }
        catch (ArgumentException ex)
        {
            Console.WriteLine($"   UseTls=perhaps in a connection string  -> {ex.GetType().Name}");
            Console.WriteLine($"     {ex.Message}");
        }
    }

    private static void Refused(string what, ClickHouseTcpClientOptions options)
    {
        try
        {
            // Constructed only to be refused: nothing here reaches a socket.
            using var client = new ClickHouseTcpClient(options);
            Console.WriteLine($"   {what} -> accepted, which is not what this example expected");
        }
        catch (Exception ex) when (ex is ArgumentException or IOException)
        {
            Console.WriteLine($"   {what} -> {ex.GetType().Name}");
            Console.WriteLine($"     {ex.Message.Split(" (Parameter")[0]}");
        }
    }

    private static void PinningAnAuthorityReplacesTheTrustStore()
    {
        Console.WriteLine("\n5. TlsCaCertificatePath replaces the host's trust store — it does not add to it\n");
        Console.WriteLine("   Set it and the server must chain to one of the authorities in that file. A certificate");
        Console.WriteLine("   the host would have accepted on its own is then refused. That is the point of naming an");
        Console.WriteLine("   authority: an additive check would still accept a certificate mis-issued by any of the");
        Console.WriteLine("   hundred-odd public authorities the host trusts.");
        Console.WriteLine();
        Console.WriteLine("   So a private authority is the case it serves. Pointing it at a public root to 'also");
        Console.WriteLine("   allow' a private one does not work, and pinning it in front of a server whose");
        Console.WriteLine("   certificate is publicly issued breaks that server.");
        Console.WriteLine();
        Console.WriteLine("   The file must hold at least one self-issued root, which is what the chain is anchored");
        Console.WriteLine("   to; it may also hold intermediates, which are used only to build a chain to an anchor.");
        Console.WriteLine("   Host name matching still happens either way — pinning roots does not replace it.");
        Console.WriteLine();
        Console.WriteLine("   TlsAllowInvalidCertificates is the other thing entirely: it stops the client checking");
        Console.WriteLine("   that the peer is the server it asked for, so anyone who can intercept the connection can");
        Console.WriteLine("   present any certificate and read the handshake password. For a private authority, pin");
        Console.WriteLine("   the root and keep the check.");
    }

    private static void TheEscapeHatch()
    {
        Console.WriteLine("\n6. ConfigureTls, for what the four keys do not cover\n");

        ClickHouseTcpClientOptions options = ExampleConfig.TcpBuilder().ToOptions() with
        {
            UseTls = true,
            ConfigureTls = tls =>
            {
                tls.EnabledSslProtocols = SslProtocols.Tls13;

                // Where a client certificate would go: tls.ClientCertificates = new X509Certificate2Collection(cert);
            },
        };

        // The client calls this once per connection, just before the handshake. Calling it here, on a fresh
        // options object, is only to show what it sets.
        var authentication = new SslClientAuthenticationOptions();
        options.ConfigureTls(authentication);

        Console.WriteLine($"   The hook set EnabledSslProtocols = {authentication.EnabledSslProtocols}");
        Console.WriteLine();
        Console.WriteLine("   It runs last, after everything the four keys set, which is what makes it an escape");
        Console.WriteLine("   hatch — client certificates, a protocol floor, cipher suites, a validation callback of");
        Console.WriteLine("   your own — and also what lets it weaken the transport in two ways that are easy to miss:");
        Console.WriteLine();
        Console.WriteLine("     replacing RemoteCertificateValidationCallback drops the check the keys configured;");
        Console.WriteLine("     clearing TargetHost stops the server name being matched at all, while chain");
        Console.WriteLine("     validation still appears to run.");
        Console.WriteLine();
        Console.WriteLine("   With TlsCaCertificatePath set, a CertificateChainPolicy is already in place and the hook");
        Console.WriteLine("   receives it and may edit it. .NET then ignores CertificateRevocationCheckMode, so");
        Console.WriteLine("   revocation goes through that policy's own RevocationMode.");
    }

    private static async Task ConnectIfAnEndpointWasGiven()
    {
        Console.WriteLine("\n7. Connecting over TLS\n");

        string? connectionString = Environment.GetEnvironmentVariable(TlsConnectionStringVariable);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            Console.WriteLine($"   Skipped: {TlsConnectionStringVariable} is not set.");
            Console.WriteLine();
            Console.WriteLine("   The server these examples run against speaks plaintext on 9000, and the one CI");
            Console.WriteLine("   starts publishes 8123 and 9000 only, so there is no secure port to dial. Nothing");
            Console.WriteLine("   here fakes one: a certificate invented to make a connection succeed would teach the");
            Console.WriteLine("   wrong thing, and turning validation off to get past it would teach something worse.");
            Console.WriteLine();
            Console.WriteLine("   To run this section, point it at a server listening on tcp_port_secure:");
            Console.WriteLine($"     export {TlsConnectionStringVariable}=\"Host=my-host;UseTls=true;Username=default;Password=...\"");
            Console.WriteLine("   A ClickHouse Cloud service is the easy case: its native endpoint is TLS on 9440 with");
            Console.WriteLine("   a publicly issued certificate, so UseTls=true and no other TLS key is needed.");
            return;
        }

        Console.WriteLine($"   {TlsConnectionStringVariable} is set, so connecting over TLS.");

        await using var client = new ClickHouseTcpClient(connectionString);
        ClickHouseTcpClientOptions options = client.Options;

        Console.WriteLine($"   {options}");
        Console.WriteLine($"     UseTls {options.UseTls}, TlsServerName {options.TlsServerName ?? "(Host)"}, " +
            $"CA {options.TlsCaCertificatePath ?? "(host trust store)"}, AllowInvalid {options.TlsAllowInvalidCertificates}");

        ClickHouseTcpServerInfo server = await client.GetServerInfoAsync();
        object now = await client.ExecuteScalarAsync("SELECT 'encrypted'");
        Console.WriteLine($"   Connected to {server}: SELECT returned {now}");
    }
}
