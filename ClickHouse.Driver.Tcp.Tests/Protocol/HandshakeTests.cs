using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Protocol;

// These cover only the handshake logic that live integration tests can't reach: version-gated decoding
// against a revision no supported server negotiates, and the nested-exception chain (has_nested is always
// false on the wire today). The happy-path encode/decode and orchestration are exercised end to end by the
// integration suite.
[TestFixture]
public class HandshakeTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static ClickHouseBinaryReader ReaderOver(byte[] bytes) => new(new MemoryStream(bytes), 16384);

    private static async Task<byte[]> WriteAsync(Action<ClickHouseBinaryWriter> write)
    {
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms))
        {
            write(writer);
            await writer.FlushAsync(None);
        }

        return ms.ToArray();
    }

    [Test]
    public void ToString_CredentialsAreSet_RedactsSecretsAndIncludesIdentity()
    {
        const string password = "distinctive-secret-password";
        const string quotaKey = "distinctive-secret-quota-key";
        var parameters = new ClientHandshakeParameters
        {
            VersionMajor = 12,
            VersionMinor = 34,
            Database = "test-database",
            Username = "test-user",
            Password = password,
            QuotaKey = quotaKey,
        };

        string description = parameters.ToString();
        Assert.Multiple(() =>
        {
            Assert.That(description, Does.Contain(nameof(ClientHandshakeParameters)));
            Assert.That(description, Does.Contain(ClientHandshakeParameters.DefaultClientName));
            Assert.That(description, Does.Contain("12.34"));
            Assert.That(description, Does.Contain("test-database"));
            Assert.That(description, Does.Contain("test-user"));
            Assert.That(description, Does.Not.Contain(password));
            Assert.That(description, Does.Not.Contain(quotaKey));
            Assert.That(description, Does.Contain("<redacted>"));
        });
    }

    [Test]
    public async Task ReadServerHelloAsync_OlderRevision_SkipsGatedFields()
    {
        // At revision 54000 the timezone (54058), display_name (54372) and version_patch (54401) gates are all
        // inactive, so the server writes none of them — and the decoder must not try to read them.
        byte[] body = await WriteAsync(w =>
        {
            w.WriteString("OldServer");
            w.WriteVarUInt(21);
            w.WriteVarUInt(1);
            w.WriteVarUInt(54000);
        });

        using var reader = ReaderOver(body);
        ServerHandshake server = await Handshake.ReadServerHelloAsync(reader, None);

        Assert.Multiple(() =>
        {
            Assert.That(server.Revision, Is.EqualTo(54000));
            Assert.That(server.Timezone, Is.Empty);
            Assert.That(server.DisplayName, Is.Empty);
            Assert.That(server.VersionPatch, Is.EqualTo(0));
            Assert.That(server.Negotiated.Version, Is.EqualTo(54000));
        });

        // The decode must have consumed exactly the four always-present fields — no more (which would throw
        // here) and no fewer (proven by the stream now being at its end).
        Assert.ThrowsAsync<EndOfStreamException>(async () => await reader.ReadByteAsync(None));
    }

    [Test]
    public async Task ExceptionReadAsync_NestedChain_LinksInnerException()
    {
        byte[] body = await WriteAsync(w =>
        {
            w.WriteInt32(10);
            w.WriteString("DB::Exception");
            w.WriteString("outer");
            w.WriteString("outer stack");
            w.WriteBool(true); // has_nested → another frame follows
            w.WriteInt32(20);
            w.WriteString("DB::NestedException");
            w.WriteString("inner cause");
            w.WriteString("inner stack");
            w.WriteBool(false);
        });

        using var reader = ReaderOver(body);
        ClickHouseServerException ex = await ClickHouseServerException.ReadAsync(reader, None);

        Assert.That(ex.Code, Is.EqualTo(10));
        Assert.That(ex.Message, Is.EqualTo("outer"));
        var inner = ex.InnerException as ClickHouseServerException;
        Assert.That(inner, Is.Not.Null);
        Assert.That(inner!.Code, Is.EqualTo(20));
        Assert.That(inner.Message, Is.EqualTo("inner cause"));
        Assert.That(inner.InnerException, Is.Null);
    }
}
