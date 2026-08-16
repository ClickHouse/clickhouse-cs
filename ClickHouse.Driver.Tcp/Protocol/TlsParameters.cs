using System;
using System.IO;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouse.Driver.Tcp.Protocol;

/// <summary>
/// How a connection negotiates TLS: the name to authenticate the server as, and how to validate the certificate
/// chain it presents. The connection layer builds this from client options once; a connect uses it to wrap its
/// socket before the handshake writes a byte.
///
/// <para>
/// TLS is below the protocol, so nothing above the transport changes: the native protocol sends the same bytes
/// encrypted or not. TLS keeps the ClientHello private, and that message carries the password as plaintext.
/// </para>
/// </summary>
internal sealed class TlsParameters
{
    // id-kp-serverAuth: the purpose a certificate must declare to authenticate a server.
    private static readonly Oid ServerAuthentication = new("1.3.6.1.5.5.7.3.1");

    /// <summary>The host name presented as SNI and matched against the server certificate.</summary>
    required public string TargetHost { get; init; }

    /// <summary>Whether to accept a certificate that fails validation. Development only — see the option that sets it.</summary>
    public bool AllowInvalidCertificates { get; init; }

    /// <summary>Certificate authorities to validate against instead of the host's trust store, or null to use the host's.</summary>
    public X509Certificate2Collection CaCertificates { get; init; }

    /// <summary>A hook applied to the TLS options last, able to override everything else here. Null for none.</summary>
    public Action<SslClientAuthenticationOptions> Configure { get; init; }

    /// <summary>
    /// Loads a PEM file of certificate authorities. Called once per client so a bad path fails at construction
    /// rather than on every connect.
    /// </summary>
    /// <param name="path">Path to a PEM file holding one or more certificates.</param>
    /// <returns>The certificates in the file.</returns>
    /// <exception cref="ArgumentException">The file holds no certificate.</exception>
    internal static X509Certificate2Collection LoadCaCertificates(string path)
    {
        var certificates = new X509Certificate2Collection();
        certificates.ImportFromPemFile(path);
        if (certificates.Count == 0)
        {
            throw new ArgumentException($"The certificate authority file '{path}' contains no PEM certificate.", nameof(path));
        }

        return certificates;
    }

    /// <summary>
    /// Wraps a transport stream in TLS and completes the handshake. The wrapper owns <paramref name="inner"/>
    /// from the moment it is built, either way: on success disposing the returned stream disposes both, and on
    /// failure both are disposed here before the exception propagates.
    /// </summary>
    /// <param name="inner">The plaintext transport stream to encrypt.</param>
    /// <param name="cancellationToken">A token to observe while handshaking.</param>
    /// <returns>The encrypted stream.</returns>
    internal async ValueTask<Stream> WrapAsync(Stream inner, CancellationToken cancellationToken)
    {
        var ssl = new SslStream(inner, leaveInnerStreamOpen: false);
        try
        {
            var authentication = new SslClientAuthenticationOptions { TargetHost = TargetHost };

            if (AllowInvalidCertificates)
            {
                // CA5359 is exactly what this option asks for, and the property that sets it documents the cost.
                // The analyzer cannot tell an opt-in development switch from an accident, so the suppression is
                // kept to this one statement rather than the file.
#pragma warning disable CA5359 // Do Not Disable Certificate Validation
                authentication.RemoteCertificateValidationCallback = static (_, _, _, _) => true;
#pragma warning restore CA5359
            }
            else if (CaCertificates is { Count: > 0 } roots)
            {
                // The revocation mode is read when the callback runs, not now, so a hook that asks for revocation
                // checking still gets it — the rebuild would otherwise re-decide with revocation off and accept a
                // certificate the platform had just refused as revoked.
                authentication.RemoteCertificateValidationCallback =
                    (_, certificate, chain, errors) => ValidateAgainstCustomRoots(
                        roots, certificate, chain, errors, authentication.CertificateRevocationCheckMode);
            }

            // Last, so a caller can override any of the above.
            Configure?.Invoke(authentication);

            await ssl.AuthenticateAsClientAsync(authentication, cancellationToken).ConfigureAwait(false);
            return ssl;
        }
        catch
        {
            // Built with leaveInnerStreamOpen false, so this closes the inner stream as well.
            ssl.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Replaces the platform's trust decision with one taken against a private set of roots, leaving every other
    /// check alone. A name mismatch or a missing certificate still fails: naming a root says who to trust, not
    /// that identity stops mattering.
    /// </summary>
    /// <remarks>
    /// The rebuild runs even when the platform was satisfied. Naming a certificate authority means the server
    /// must chain to <i>that</i> authority — accepting whatever the host's trust store already believes would
    /// make the option additive, so a certificate mis-issued by any public authority would still be taken, which
    /// is the attack pinning a private root exists to stop.
    /// </remarks>
    /// <param name="roots">The certificate authorities to trust.</param>
    /// <param name="certificate">The server certificate the platform validated.</param>
    /// <param name="chain">The chain the platform built, whose intermediates the rebuild reuses.</param>
    /// <param name="errors">The platform's verdict.</param>
    /// <param name="revocationMode">The revocation checking the TLS options ask for.</param>
    /// <returns>Whether to accept the certificate.</returns>
    private static bool ValidateAgainstCustomRoots(
        X509Certificate2Collection roots,
        X509Certificate certificate,
        X509Chain chain,
        SslPolicyErrors errors,
        X509RevocationMode revocationMode)
    {
        // Only the trust decision is ours to re-take; anything else the platform flagged stands.
        if ((errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != SslPolicyErrors.None)
        {
            return false;
        }

        // The runtime always hands the callback an X509Certificate2. Refuse rather than guess if that changes:
        // a chain cannot be rebuilt from the base type without an API that is obsolete on the newer targets.
        if (certificate is not X509Certificate2 leaf)
        {
            return false;
        }

        using var rebuilt = new X509Chain();
        rebuilt.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        rebuilt.ChainPolicy.CustomTrustStore.AddRange(roots);
        rebuilt.ChainPolicy.RevocationMode = revocationMode;

        // The platform requires the certificate to be valid for server authentication. This rebuild replaces the
        // platform's check, so it must apply the same requirement: without it, any certificate under the named
        // authority whose name covers this host is accepted, including one issued to a client.
        rebuilt.ChainPolicy.ApplicationPolicy.Add(ServerAuthentication);

        // Intermediates the server sent live in the platform's chain; without them a path to a custom root
        // that is not the direct issuer cannot be completed.
        if (chain is not null)
        {
            foreach (X509ChainElement element in chain.ChainElements)
            {
                rebuilt.ChainPolicy.ExtraStore.Add(element.Certificate);
            }
        }

        return rebuilt.Build(leaf);
    }
}
