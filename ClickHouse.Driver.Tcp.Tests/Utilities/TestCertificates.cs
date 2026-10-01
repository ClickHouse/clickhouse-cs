using System;
using System.Collections.Concurrent;
using System.IO;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace ClickHouse.Driver.Tcp.Tests.Utilities;

/// <summary>
/// Builds throwaway certificate chains for the TLS tests: a private authority, optional intermediates, and server
/// certificates issued under them. Shared so a test of the transport and a test of the client wiring present the
/// same certificates to the same validation code.
/// </summary>
/// <remarks>
/// One clock reading for the whole process, and an authority whose validity window strictly contains the leaf's.
/// Reading the clock per certificate lets a leaf issued a second later ask for a <c>notAfter</c> past its issuer's,
/// which <see cref="CertificateRequest.Create(X509Certificate2, DateTimeOffset, DateTimeOffset, byte[])"/> rejects.
/// </remarks>
internal static class TestCertificates
{
    // id-kp-serverAuth and id-kp-clientAuth.
    private const string ServerAuthenticationOid = "1.3.6.1.5.5.7.3.1";
    private const string ClientAuthenticationOid = "1.3.6.1.5.5.7.3.2";

    private static readonly DateTimeOffset Now = DateTimeOffset.UtcNow;
    private static readonly DateTimeOffset AuthorityNotBefore = Now.AddDays(-3);
    private static readonly DateTimeOffset AuthorityNotAfter = Now.AddDays(3);
    private static readonly DateTimeOffset LeafNotBefore = Now.AddDays(-1);
    private static readonly DateTimeOffset LeafNotAfter = Now.AddDays(1);

    private static readonly ConcurrentBag<string> WrittenFiles = [];

    /// <summary>A self-signed certificate authority, able to sign both certificates and other authorities.</summary>
    /// <returns>The authority, with its private key.</returns>
    internal static X509Certificate2 CreateAuthority()
    {
        using RSA key = RSA.Create(2048);
        var request = new CertificateRequest($"CN=ClickHouse TCP test CA {Guid.NewGuid():N}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(certificateAuthority: true, hasPathLengthConstraint: false, pathLengthConstraint: 0, critical: true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.DigitalSignature, critical: true));
        return request.CreateSelfSigned(AuthorityNotBefore, AuthorityNotAfter);
    }

    /// <summary>An intermediate authority signed by <paramref name="root"/>, for testing chains longer than one hop.</summary>
    /// <param name="root">The authority that signs it.</param>
    /// <returns>The intermediate, with its private key.</returns>
    internal static X509Certificate2 CreateIntermediate(X509Certificate2 root)
    {
        using RSA key = RSA.Create(2048);
        var request = new CertificateRequest($"CN=ClickHouse TCP test intermediate {Guid.NewGuid():N}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(certificateAuthority: true, hasPathLengthConstraint: false, pathLengthConstraint: 0, critical: true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.DigitalSignature, critical: true));

        // Between the root's window and the leaf's, so both containments hold.
        using X509Certificate2 issued = request.Create(root, AuthorityNotBefore.AddDays(1), AuthorityNotAfter.AddDays(-1), Guid.NewGuid().ToByteArray());
        return issued.CopyWithPrivateKey(key);
    }

    /// <summary>A server certificate issued by <paramref name="issuer"/> and usable by an <c>SslStream</c> server.</summary>
    /// <param name="issuer">The authority that signs it.</param>
    /// <param name="dnsName">The name to put in the subject alternative name.</param>
    /// <param name="purpose">Which extended key usage to declare.</param>
    /// <returns>The certificate, with a private key every platform accepts.</returns>
    internal static X509Certificate2 IssueServerCertificate(X509Certificate2 issuer, string dnsName, CertificatePurpose purpose = CertificatePurpose.ServerAuthentication)
    {
        using RSA key = RSA.Create(2048);
        CertificateRequest request = ServerCertificateRequest(key, dnsName, purpose);
        using X509Certificate2 issued = request.Create(issuer, LeafNotBefore, LeafNotAfter, Guid.NewGuid().ToByteArray());
        using X509Certificate2 withKey = issued.CopyWithPrivateKey(key);
        return MakeUsableAsServerCertificate(withKey);
    }

    /// <summary>A self-signed server certificate, which no authority vouches for.</summary>
    /// <param name="dnsName">The name to put in the subject alternative name.</param>
    /// <returns>The certificate, with a private key every platform accepts.</returns>
    internal static X509Certificate2 CreateSelfSignedServerCertificate(string dnsName)
    {
        using RSA key = RSA.Create(2048);
        using X509Certificate2 created = ServerCertificateRequest(key, dnsName, CertificatePurpose.ServerAuthentication)
            .CreateSelfSigned(LeafNotBefore, LeafNotAfter);
        return MakeUsableAsServerCertificate(created);
    }

    /// <summary>Writes certificates to a temporary PEM file, as a certificate-authority option expects one.</summary>
    /// <param name="certificates">The certificates to write, in order.</param>
    /// <returns>The path written.</returns>
    internal static string WritePemFile(params X509Certificate2[] certificates)
    {
        string path = Path.Combine(Path.GetTempPath(), $"tcp-tls-ca-{Guid.NewGuid():N}.pem");
        var pem = new StringBuilder();
        foreach (X509Certificate2 certificate in certificates)
        {
            pem.AppendLine(certificate.ExportCertificatePem());
        }

        File.WriteAllText(path, pem.ToString());
        WrittenFiles.Add(path);
        return path;
    }

    /// <summary>Records a path this class did not write, so a fixture's teardown removes it too.</summary>
    /// <param name="path">The file to delete on cleanup.</param>
    internal static void TrackForCleanUp(string path) => WrittenFiles.Add(path);

    /// <summary>Deletes every temporary file written so far. Call from a fixture's one-time teardown.</summary>
    internal static void DeleteTemporaryFiles()
    {
        while (WrittenFiles.TryTake(out string path))
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // A leftover temp file is not worth failing a run over.
            }
        }
    }

    private static CertificateRequest ServerCertificateRequest(RSA key, string dnsName, CertificatePurpose purpose)
    {
        var request = new CertificateRequest($"CN={dnsName}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(certificateAuthority: false, hasPathLengthConstraint: false, pathLengthConstraint: 0, critical: true));

        string oid = purpose == CertificatePurpose.ServerAuthentication ? ServerAuthenticationOid : ClientAuthenticationOid;
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid(oid)], critical: false));

        // Modern TLS stacks match the name against the SAN, never the subject alone.
        var alternativeNames = new SubjectAlternativeNameBuilder();
        alternativeNames.AddDnsName(dnsName);
        request.CertificateExtensions.Add(alternativeNames.Build());
        return request;
    }

    // A certificate built in memory carries an ephemeral key, which SslStream refuses to serve with on Windows.
    // Round-tripping through PKCS#12 gives it a key handle every platform accepts.
    private static X509Certificate2 MakeUsableAsServerCertificate(X509Certificate2 certificate)
    {
        byte[] exported = certificate.Export(X509ContentType.Pfx);
#if NET9_0_OR_GREATER
        return X509CertificateLoader.LoadPkcs12(exported, password: null);
#else
        return new X509Certificate2(exported);
#endif
    }
}

/// <summary>Which extended key usage a test certificate declares.</summary>
internal enum CertificatePurpose
{
    /// <summary>Valid for authenticating a server, which is what a TLS client requires.</summary>
    ServerAuthentication,

    /// <summary>Valid for authenticating a client only, so a TLS client must refuse it.</summary>
    ClientAuthentication,
}
