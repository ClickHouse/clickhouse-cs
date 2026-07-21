#nullable enable
namespace ClickHouse.Driver.Vendor.K4os.Compression.LZ4.Streams;

/// <summary>
/// Decoder settings.
/// </summary>
internal class LZ4DecoderSettings
{
	internal static LZ4DecoderSettings Default { get; } = new();

	/// <summary>Extra memory for decompression.</summary>
	public int ExtraMemory { get; set; }
}