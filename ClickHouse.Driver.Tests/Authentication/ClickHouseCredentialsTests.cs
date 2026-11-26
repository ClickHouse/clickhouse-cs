using System;
using System.Collections.Generic;
using System.Text;

namespace ClickHouse.Driver.Tests.Authentication;

public class ClickHouseCredentialsTests
{
    private static readonly DateTimeOffset Exp = DateTimeOffset.FromUnixTimeSeconds(1_900_000_000);

    internal static string Jwt(string payloadJson, string header = "{\"alg\":\"RS256\",\"typ\":\"JWT\"}", string signature = "c2lnbmF0dXJl")
        => $"{Base64Url(header)}.{Base64Url(payloadJson)}.{signature}";

    internal static string Base64Url(string json)
        => Convert.ToBase64String(Encoding.UTF8.GetBytes(json)).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    [Test]
    public void CreateBearer_JwtWithExpClaimAndNoExpiry_UsesExpClaim()
    {
        var credentials = ClickHouseCredentials.CreateBearer(Jwt("{\"sub\":\"svc\",\"exp\":1900000000}"));

        Assert.Multiple(() =>
        {
            Assert.That(credentials.ExpiresAt, Is.Null);
            Assert.That(credentials.EffectiveExpiresAt, Is.EqualTo(Exp));
        });
    }

    [Test]
    public void CreateBearer_FractionalExpClaim_KeepsMilliseconds()
    {
        var credentials = ClickHouseCredentials.CreateBearer(Jwt("{\"exp\":1900000000.25}"));

        Assert.That(credentials.EffectiveExpiresAt, Is.EqualTo(Exp.AddMilliseconds(250)));
    }

    [Test]
    public void CreateBearer_ExplicitExpiry_HasPrecedenceOverExpClaim()
    {
        var explicitExpiry = Exp.AddHours(-1);

        var credentials = ClickHouseCredentials.CreateBearer(Jwt("{\"exp\":1900000000}"), explicitExpiry);

        Assert.Multiple(() =>
        {
            Assert.That(credentials.ExpiresAt, Is.EqualTo(explicitExpiry));
            Assert.That(credentials.EffectiveExpiresAt, Is.EqualTo(explicitExpiry));
        });
    }

    private static IEnumerable<TestCaseData> TokensWithoutExpiry()
    {
        yield return new TestCaseData("opaque-access-token").SetName("CreateBearer_OpaqueToken_ExpiryUnknown");
        yield return new TestCaseData($"{Base64Url("{}")}.{Base64Url("{\"exp\":1900000000}")}").SetName("CreateBearer_TwoSegments_ExpiryUnknown");
        yield return new TestCaseData(Jwt("{\"exp\":1900000000}") + ".x.y").SetName("CreateBearer_FiveSegments_ExpiryUnknown");
        yield return new TestCaseData(Jwt("{\"sub\":\"svc\"}")).SetName("CreateBearer_NoExpClaim_ExpiryUnknown");
        yield return new TestCaseData(Jwt("{\"exp\":\"1900000000\"}")).SetName("CreateBearer_StringExpClaim_ExpiryUnknown");
        yield return new TestCaseData(Jwt("{\"exp\":1e20}")).SetName("CreateBearer_ExpClaimOutOfRange_ExpiryUnknown");
        yield return new TestCaseData(Jwt("[1900000000]")).SetName("CreateBearer_PayloadNotAnObject_ExpiryUnknown");
        yield return new TestCaseData(Jwt("not json")).SetName("CreateBearer_PayloadNotJson_ExpiryUnknown");
        yield return new TestCaseData("eyJhbGciOiJub25lIn0.***.sig").SetName("CreateBearer_PayloadNotBase64Url_ExpiryUnknown");
    }

    [TestCaseSource(nameof(TokensWithoutExpiry))]
    public void CreateBearer_TokenWithoutReadableExp_ExpiryUnknown(string token)
    {
        var credentials = ClickHouseCredentials.CreateBearer(token);

        Assert.That(credentials.EffectiveExpiresAt, Is.Null);
    }

    [Test]
    public void CreateBasic_NullPassword_UsesEmptyPassword()
    {
        var credentials = ClickHouseCredentials.CreateBasic("alice", null);

        Assert.Multiple(() =>
        {
            Assert.That(credentials.Username, Is.EqualTo("alice"));
            Assert.That(credentials.Password, Is.Empty);
            Assert.That(credentials.BearerToken, Is.Null);
            Assert.That(credentials.EffectiveExpiresAt, Is.Null);
        });
    }

    [Test]
    public void CreateBasic_NullUsername_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => ClickHouseCredentials.CreateBasic(null, "pw"));
    }

    [TestCase(null)]
    [TestCase("")]
    public void CreateBearer_NullOrEmptyToken_Throws(string token)
    {
        Assert.Throws<ArgumentException>(() => ClickHouseCredentials.CreateBearer(token));
    }

    [Test]
    public void ToString_Basic_ShowsUsernameButNotPassword()
    {
        var text = ClickHouseCredentials.CreateBasic("alice", "s3cret-password").ToString();

        Assert.That(text, Does.Contain("alice").And.Not.Contain("s3cret-password"));
    }

    [Test]
    public void ToString_Bearer_ShowsExpiryButNotToken()
    {
        var token = Jwt("{\"exp\":1900000000}");

        var text = ClickHouseCredentials.CreateBearer(token).ToString();

        Assert.That(text, Does.Contain("2030-03-17T17:46:40").And.Not.Contain(token).And.Not.Contain(token.Split('.')[1]));
    }
}
