#!/usr/bin/env dotnet
// Signs the JWTs for the ClickHouse Cloud JWT authentication tests, and writes the public key set
// (jwks.json) that the Cloud JWT provider reads. Both commands read the RSA private key (PEM) from
// the JWT_PKEY environment variable. README.md in this directory describes the Cloud setup.
//
//   dotnet run .github/scripts/jwt.cs -- token                          # valid for 30 minutes
//   dotnet run .github/scripts/jwt.cs -- token --ttl 2700
//   dotnet run .github/scripts/jwt.cs -- token --expired                # expired 5 minutes ago
//   dotnet run .github/scripts/jwt.cs -- jwks > .github/jwks.json
//
// Options: --issuer (default mydomain.com), --audience (default ci-test-service), --subject (default
// ci-test), --ttl in seconds (default 1800, token only), --expired (token only: iat = now - 600,
// exp = now - 300, and --ttl is ignored). The claims must match the JWT provider on the Cloud
// service. The jwks output holds only the public key and a self-signed x5c certificate.
//
// The script uses only the .NET base class library. PublishAot=false stops the restore of the native
// AOT compiler packages that a file-based app gets by default, so the build restores no package.

#:property PublishAot=false

using System.Buffers.Text;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

var command = args.Length > 0 ? args[0] : null;
switch (command)
{
    case null:
        return Usage(1);
    case "--help" or "-h":
        return Usage(0);
    case not ("token" or "jwks"):
        return Fail($"unknown command '{command}'");
}

var issuer = "mydomain.com";
var audience = "ci-test-service";
var subject = "ci-test";
var ttl = 1800;
var expired = false;

for (var i = 1; i < args.Length; i++)
{
    var option = args[i];
    if (option == "--expired")
    {
        expired = true;
        continue;
    }

    if (option is not ("--issuer" or "--audience" or "--subject" or "--ttl"))
    {
        return Fail($"unknown option '{option}'");
    }

    if (++i >= args.Length)
    {
        return Fail($"'{option}' needs a value");
    }

    var value = args[i];
    switch (option)
    {
        case "--issuer":
            issuer = value;
            break;
        case "--audience":
            audience = value;
            break;
        case "--subject":
            subject = value;
            break;
        case "--ttl" when int.TryParse(value, out var seconds) && seconds > 0:
            ttl = seconds;
            break;
        default:
            return Fail($"--ttl must be a positive number of seconds, not '{value}'");
    }
}

var pem = Environment.GetEnvironmentVariable("JWT_PKEY");
if (string.IsNullOrWhiteSpace(pem))
{
    return Fail("the private key is not set. Set the JWT_PKEY environment variable.");
}

using var rsa = RSA.Create();
try
{
    rsa.ImportFromPem(pem);

    // Fails for a public key.
    _ = rsa.ExportParameters(includePrivateParameters: true);
}
catch (Exception ex) when (ex is ArgumentException or CryptographicException)
{
    return Fail("JWT_PKEY is not an RSA private key in PEM format.");
}

var publicKey = rsa.ExportParameters(includePrivateParameters: false);
var modulus = Base64Url.EncodeToString(TrimLeadingZeros(publicKey.Modulus!));
var exponent = Base64Url.EncodeToString(TrimLeadingZeros(publicKey.Exponent!));
var kid = Thumbprint(modulus, exponent);

if (command == "token")
{
    Console.Out.Write(SignToken());
}
else
{
    Console.Out.WriteLine(PublicJwks());
}

return 0;

// An RS256 JWT that is valid for ttl seconds, or that expired 5 minutes ago with --expired. The kid
// in the header is the kid in jwks.json.
string SignToken()
{
    var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    var (issuedAt, expiresAt) = expired ? (now - 600, now - 300) : (now, now + ttl);
    var header = Json(w =>
    {
        w.WriteString("alg", "RS256");
        w.WriteString("typ", "JWT");
        w.WriteString("kid", kid);
    });
    var claims = Json(w =>
    {
        w.WriteString("iss", issuer);
        w.WriteString("sub", subject);
        w.WriteString("aud", audience);
        w.WriteNumber("iat", issuedAt);
        w.WriteNumber("exp", expiresAt);
    });

    var signingInput = $"{Base64Url.EncodeToString(header)}.{Base64Url.EncodeToString(claims)}";
    var signature = rsa.SignData(Encoding.ASCII.GetBytes(signingInput), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
    return $"{signingInput}.{Base64Url.EncodeToString(signature)}";
}

// The public JWKS: the public key properties (no private key parameters), and an x5c chain with
// one self-signed certificate that is valid for 365 days.
string PublicJwks()
{
    // Build() encodes the attributes in the reverse order of the Add calls: C, ST, L, O, CN.
    var name = new X500DistinguishedNameBuilder();
    name.AddCommonName(issuer);
    name.AddOrganizationName("My Organization");
    name.AddLocalityName("San Francisco");
    name.AddStateOrProvinceName("California");
    name.AddCountryOrRegion("US");

    var request = new CertificateRequest(name.Build(), rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
    var now = DateTimeOffset.UtcNow;
    using var certificate = request.CreateSelfSigned(now, now.AddDays(365));

    var jwks = Json(
        w =>
        {
            w.WriteStartArray("keys");
            w.WriteStartObject();
            w.WriteString("e", exponent);
            w.WriteString("kid", kid);
            w.WriteString("kty", "RSA");
            w.WriteString("n", modulus);
            w.WriteString("use", "sig");
            w.WriteStartArray("x5c");
            w.WriteStringValue(Convert.ToBase64String(certificate.RawData));
            w.WriteEndArray();
            w.WriteEndObject();
            w.WriteEndArray();
        },
        indented: true);
    return Encoding.UTF8.GetString(jwks);
}

// The RFC 7638 JWK thumbprint (SHA-256): the required RSA members in lexicographic order, without
// white space.
static string Thumbprint(string n, string e) =>
    Base64Url.EncodeToString(SHA256.HashData(Encoding.UTF8.GetBytes($$"""{"e":"{{e}}","kty":"RSA","n":"{{n}}"}""")));

// RFC 7518 section 6.3.1: n and e use the minimum number of octets.
static byte[] TrimLeadingZeros(byte[] value)
{
    var start = Array.FindIndex(value, b => b != 0);
    return start <= 0 ? value : value[start..];
}

static byte[] Json(Action<Utf8JsonWriter> write, bool indented = false)
{
    using var buffer = new MemoryStream();
    // The relaxed encoder writes the '+' of the x5c base64 as is. The output is still valid JSON.
    using (var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions { Indented = indented, Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping }))
    {
        writer.WriteStartObject();
        write(writer);
        writer.WriteEndObject();
    }

    return buffer.ToArray();
}

static int Usage(int exitCode)
{
    (exitCode == 0 ? Console.Out : Console.Error).WriteLine(
        "usage: dotnet run .github/scripts/jwt.cs -- (token | jwks) [--issuer <iss>] [--audience <aud>] [--subject <sub>] [--ttl <seconds>] [--expired]");
    return exitCode;
}

static int Fail(string message)
{
    Console.Error.WriteLine($"jwt.cs: {message}");
    return 1;
}
