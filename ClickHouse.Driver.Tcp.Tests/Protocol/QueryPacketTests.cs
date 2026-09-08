using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Protocol;

// Exercises the Query packet writer directly, where the serialized bytes are easiest to compare.
[TestFixture]
public class QueryPacketTests
{
    private const string NativeBinaryTypesSetting = "output_format_native_encode_types_in_binary_format";

    private static readonly CancellationToken None = CancellationToken.None;

    private static readonly ClientMetadata Client = new()
    {
        Username = "default",
    };

    [Test]
    public async Task Write_NativeBinaryTypesSettingEnabled_ForcedToTextualForm()
    {
        // The block reader only decodes textual type headers, so a caller enabling binary type headers must be
        // neutralized. Forcing the value from "1" to "0" makes the two packets byte-for-byte identical.
        byte[] enabled = await WriteQueryAsync(new Dictionary<string, string> { [NativeBinaryTypesSetting] = "1" });
        byte[] disabled = await WriteQueryAsync(new Dictionary<string, string> { [NativeBinaryTypesSetting] = "0" });

        CollectionAssert.AreEqual(disabled, enabled);
    }

    [Test]
    public async Task Write_UnrelatedSettingValue_PassedThroughUnchanged()
    {
        // A guard that the override is scoped to the one setting: unrelated values still reach the wire verbatim.
        byte[] four = await WriteQueryAsync(new Dictionary<string, string> { ["max_threads"] = "4" });
        byte[] eight = await WriteQueryAsync(new Dictionary<string, string> { ["max_threads"] = "8" });

        CollectionAssert.AreNotEqual(eight, four);
    }

    [Test]
    public void Write_ParametersOnProtocolBelowParametersFeature_Throws()
    {
        // Servers negotiating below the parameters feature (but at or above the handshake floor) have no wire
        // slot for the parameters list; dropping the caller's parameters silently would run a different query.
        var parameters = new Dictionary<string, string> { ["id"] = "42" };
        using var ms = new MemoryStream();
        using var writer = new ClickHouseBinaryWriter(ms);

        Assert.Throws<System.NotSupportedException>(() =>
            Query.Write(writer, new NegotiatedProtocol((int)ProtocolFeature.Parameters - 1), Client, "qid", "SELECT {id:UInt8}", settings: null, parameters));
    }

    [Test]
    public async Task Write_EmptyParametersOnProtocolBelowParametersFeature_DoesNotThrow()
    {
        // Only a non-empty parameter set is unsatisfiable below the feature; none-to-send is always fine.
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms))
        {
            Query.Write(writer, new NegotiatedProtocol((int)ProtocolFeature.Parameters - 1), Client, "qid", "SELECT 1", settings: null, new Dictionary<string, string>());
            await writer.FlushAsync(None);
        }

        Assert.That(ms.Length, Is.GreaterThan(0));
    }

    private static async Task<byte[]> WriteQueryAsync(IReadOnlyDictionary<string, string> settings)
    {
        using var ms = new MemoryStream();
        using (var writer = new ClickHouseBinaryWriter(ms))
        {
            Query.Write(writer, new NegotiatedProtocol(54460), Client, "qid", "SELECT 1", settings, queryParameters: null);
            await writer.FlushAsync(None);
        }

        return ms.ToArray();
    }
}
