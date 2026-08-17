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
/// encrypted or not. What TLS keeps private is the native protocol's client Hello packet, which carries the
/// password as plaintext. (That packet is named ClientHello in the protocol spec; it is not the TLS ClientHello,
/// which is part of the visible handshake below it and carries no credentials.)
/// </para>
/// </summary>
internal sealed class TlsParameters : IDisposable
{
    // id-kp-serverAuth: the extended key usage a certificate must permit to authenticate a server. A certificate
    // carrying no such extension is unrestricted and satisfies this (RFC 5280 4.2.1.12); it constrains the
    // extension when present rather than requiring it.
    private static readonly Oid ServerAuthentication = new("1.3.6.1.5.5.7.3.1");

    private bool disposed;

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
        // Refused rather than continued: the pinned authorities are gone once this is disposed, and the branch
        // below that selects them tests for a non-empty collection. Carrying on would fall through to the host
        // trust store, quietly turning exclusive pinning into whatever the machine happens to trust.
        ObjectDisposedException.ThrowIf(disposed, this);

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

            NormalizeRevocationMode(authentication);

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
    /// Carries a revocation mode set on the options into the chain policy, which is the only place the handshake
    /// reads it from once a policy is present.
    /// </summary>
    /// <remarks>
    /// The platform copies <c>CertificateRevocationCheckMode</c> into the chain policy it builds, but only when it
    /// builds one. Once a policy is supplied — which pinning does — the top-level property is ignored, so
    /// <c>ConfigureTls</c> setting the obvious property would silently do nothing and a revoked certificate would
    /// be accepted. Both values start at <c>NoCheck</c>, so a change to either is visible; a nested edit wins,
    /// because a caller who reached into the policy meant that policy.
    /// </remarks>
    /// <param name="authentication">The options about to be handed to the handshake.</param>
    private static void NormalizeRevocationMode(SslClientAuthenticationOptions authentication)
    {
        if (authentication.CertificateChainPolicy is { RevocationMode: X509RevocationMode.NoCheck } policy
            && authentication.CertificateRevocationCheckMode != X509RevocationMode.NoCheck)
        {
            policy.RevocationMode = authentication.CertificateRevocationCheckMode;
        }
    }

    /// <summary>
    /// Releases the loaded certificate authorities, which hold native key handles. Idempotent, and a no-op when
    /// none were loaded. The owner calls this once nothing can still be handshaking: <see cref="WrapAsync"/>
    /// refuses afterwards rather than negotiating without them.
    /// </summary>
    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        // Set first, so a use racing this disposal is refused rather than handed certificates being freed.
        disposed = true;

        if (CaCertificates is not { } certificates)
        {
            return;
        }

        foreach (X509Certificate2 certificate in certificates)
        {
            certificate.Dispose();
        }

        // The collection is deliberately left populated. Clearing it would make the pinned-roots branch in
        // WrapAsync see an empty set and skip, which is the fall-through to host trust the flag above prevents.
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

        // A server certificate must not exclude server authentication, so that a private authority which also
        // issues client certificates cannot let the holder of one impersonate a host its name covers. The
        // handshake applies this requirement to a client-side chain anyway; naming it keeps the requirement in the
        // policy a caller can read and does not leave it resting on that behaviour.
        policy.ApplicationPolicy.Add(ServerAuthentication);
        return policy;
    }
}
