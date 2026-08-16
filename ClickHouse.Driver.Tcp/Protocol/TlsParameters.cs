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
                authentication.CertificateChainPolicy = PinnedRootPolicy(roots);
            }

            // Last, so a caller can override any of the above. The pinned roots are expressed as a chain policy
            // rather than a validation callback partly for this: the hook receives that policy and can adjust it —
            // the verification time, the revocation mode, the flags — and every change takes effect, because this
            // is the policy the handshake builds the chain with.
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
    /// The chain policy that pins the server to a private set of certificate authorities. The handshake builds
    /// the chain with this policy, so the host-name match it always applies is unaffected.
    /// </summary>
    /// <remarks>
    /// Pinning replaces the host's trust store rather than adding to it: the server must chain to one of these
    /// authorities, and a certificate the host would accept on its own is refused. An additive check would still
    /// take a certificate mis-issued by any public authority, which is what pinning a private root prevents.
    /// </remarks>
    /// <param name="roots">The certificate authorities to trust.</param>
    /// <returns>The policy to hand the handshake.</returns>
    private static X509ChainPolicy PinnedRootPolicy(X509Certificate2Collection roots)
    {
        var policy = new X509ChainPolicy
        {
            TrustMode = X509ChainTrustMode.CustomRootTrust,

            // The SslStream default. Named here because this policy replaces the one the handshake would have
            // built, so anything left out is not inherited - it is simply absent.
            RevocationMode = X509RevocationMode.NoCheck,
        };

        policy.CustomTrustStore.AddRange(roots);

        // A server certificate must say it is for server authentication, so that a private authority which also
        // issues client certificates cannot let the holder of one impersonate a host its name covers. The
        // handshake applies this requirement to a client-side chain anyway; naming it keeps the requirement in the
        // policy a caller can read and does not leave it resting on that behaviour.
        policy.ApplicationPolicy.Add(ServerAuthentication);
        return policy;
    }
}
