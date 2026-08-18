using System;
using System.Buffers.Binary;
using System.IO;

namespace ClickHouse.Driver.Compression;

/// <summary>
/// Encodes and decodes a single ClickHouse compression frame. Pure span work: callers own the I/O and the
/// buffers, so the same codec serves the native protocol's per-packet framing and the HTTP
/// <c>compress</c>/<c>decompress</c> stream.
/// <para>
/// The layout is a 16-byte checksum, a 9-byte header, then the body:
/// </para>
/// <code>
/// [16 bytes: CityHash128 over the 9-byte header + body]
/// [1  byte : method]              0x02 NONE, 0x82 LZ4, 0x90 ZSTD
/// [4  bytes: compressed_size]     little-endian; counts the 9-byte header, not the checksum
/// [4  bytes: uncompressed_size]   little-endian
/// [N  bytes: body]                N = compressed_size - 9
/// </code>
/// <para>
/// The two spans differ and are easy to confuse: the checksum covers the header plus the body, while
/// <c>compressed_size</c> counts the header plus the body but excludes the checksum itself.
/// </para>
/// </summary>
internal static class CompressionFrame
{
    /// <summary>Size of the leading CityHash128 checksum.</summary>
    public const int ChecksumSize = 16;

    /// <summary>Size of the header the checksum covers: method plus the two sizes.</summary>
    public const int HeaderSize = 9;

    /// <summary>Bytes before the body: the checksum plus the header.</summary>
    public const int PrefixSize = ChecksumSize + HeaderSize;

    /// <summary>Method byte for an uncompressed body. The frame and its checksum are still written.</summary>
    public const byte MethodNone = 0x02;

    /// <summary>Method byte for an LZ4 body, in the LZ4 <b>block</b> format (no frame, no magic number).</summary>
    public const byte MethodLz4 = 0x82;

    /// <summary>Method byte for a ZSTD body, a raw single zstd frame including its magic number.</summary>
    public const byte MethodZstd = 0x90;

    // Both sizes arrive from the peer, so a corrupt or hostile stream can declare anything, and a decoder rents
    // a buffer for each before it can check the checksum. The server flushes at ~1 MiB, so 128 MiB is ample
    // headroom while keeping the worst case a peer can force to a size a process can absorb. The Go client caps
    // both at the same figure.
    private const int MaxBodySize = 128 * 1024 * 1024;
    private const int MaxPlaintextSize = 128 * 1024 * 1024;

    /// <summary>
    /// The largest frame <see cref="Write"/> can produce for <paramref name="plaintextLength"/> bytes, so a
    /// caller can size its destination buffer.
    /// </summary>
    /// <param name="codec">The codec that will encode the body.</param>
    /// <param name="plaintextLength">The number of plaintext bytes to be framed.</param>
    /// <returns>The maximum total frame size in bytes.</returns>
    public static int MaxFrameSize(IClickHouseCompressor codec, int plaintextLength)
    {
        ArgumentNullException.ThrowIfNull(codec);
        return PrefixSize + codec.MaxEncodedLength(plaintextLength);
    }

    /// <summary>
    /// Frames <paramref name="plaintext"/> into <paramref name="destination"/>, which must hold at least
    /// <see cref="MaxFrameSize"/> bytes.
    /// </summary>
    /// <param name="plaintext">The bytes to compress and frame.</param>
    /// <param name="codec">The codec supplying the method byte and the body encoding.</param>
    /// <param name="destination">The buffer to write the whole frame into.</param>
    /// <returns>The number of bytes written: <see cref="PrefixSize"/> plus the encoded body.</returns>
    /// <exception cref="ArgumentException"><paramref name="destination"/> is too small.</exception>
    public static int Write(ReadOnlySpan<byte> plaintext, IClickHouseCompressor codec, Span<byte> destination)
    {
        ArgumentNullException.ThrowIfNull(codec);

        int required = MaxFrameSize(codec, plaintext.Length);
        if (destination.Length < required)
        {
            throw new ArgumentException($"Destination holds {destination.Length} bytes; framing {plaintext.Length} plaintext bytes needs up to {required}.", nameof(destination));
        }

        // Encode straight into place after the prefix, so the body is never copied.
        int bodyLength = codec.Encode(plaintext, destination.Slice(PrefixSize));
        int compressedSize = HeaderSize + bodyLength;

        Span<byte> header = destination.Slice(ChecksumSize, HeaderSize);
        header[0] = codec.MethodByte;
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(1, 4), compressedSize);
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(5, 4), plaintext.Length);

        // The checksum covers the header and the body, but not itself.
        var (low, high) = CityHash102.Hash128(destination.Slice(ChecksumSize, compressedSize));
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(0, 8), low);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(8, 8), high);

        return ChecksumSize + compressedSize;
    }

    /// <summary>Reads the 9-byte header, validating both declared sizes.</summary>
    /// <param name="header">Exactly <see cref="HeaderSize"/> bytes, the ones following the checksum.</param>
    /// <param name="method">The declared method byte.</param>
    /// <param name="bodySize">The body length, that is <c>compressed_size</c> minus the header.</param>
    /// <param name="plaintextSize">The declared uncompressed size.</param>
    /// <exception cref="InvalidDataException">A declared size is negative, too small to include the header, or implausibly large.</exception>
    public static void ReadHeader(ReadOnlySpan<byte> header, out byte method, out int bodySize, out int plaintextSize)
    {
        if (header.Length != HeaderSize)
        {
            throw new ArgumentException($"A frame header is exactly {HeaderSize} bytes.", nameof(header));
        }

        method = header[0];
        int compressedSize = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(1, 4));
        plaintextSize = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(5, 4));

        // compressed_size counts the header, so anything below it is corrupt rather than merely empty.
        if (compressedSize < HeaderSize)
        {
            throw new InvalidDataException($"Compression frame declares compressed_size {compressedSize}, which does not cover its own {HeaderSize}-byte header (corrupt stream).");
        }

        bodySize = compressedSize - HeaderSize;
        if (bodySize > MaxBodySize)
        {
            throw new InvalidDataException($"Compression frame declares a {bodySize}-byte body, above the {MaxBodySize}-byte limit (corrupt stream).");
        }

        if (plaintextSize < 0 || plaintextSize > MaxPlaintextSize)
        {
            throw new InvalidDataException($"Compression frame declares uncompressed_size {plaintextSize}, outside the supported range (corrupt stream).");
        }
    }

    /// <summary>
    /// Recomputes the checksum over <paramref name="headerAndBody"/> and compares it with the frame's.
    /// </summary>
    /// <param name="checksum">The frame's leading <see cref="ChecksumSize"/> bytes.</param>
    /// <param name="headerAndBody">The 9 header bytes followed by the body, contiguous.</param>
    /// <exception cref="InvalidDataException">The recomputed checksum differs, so the frame is corrupt.</exception>
    public static void VerifyChecksum(ReadOnlySpan<byte> checksum, ReadOnlySpan<byte> headerAndBody)
    {
        var (low, high) = CityHash102.Hash128(headerAndBody);
        ulong actualLow = BinaryPrimitives.ReadUInt64LittleEndian(checksum.Slice(0, 8));
        ulong actualHigh = BinaryPrimitives.ReadUInt64LittleEndian(checksum.Slice(8, 8));

        if (low != actualLow || high != actualHigh)
        {
            throw new InvalidDataException(
                $"Compression frame checksum mismatch: the frame carries {actualHigh:x16}{actualLow:x16} but its bytes hash to {high:x16}{low:x16}. " +
                "The stream is corrupt, or the peer used a CityHash other than v1.0.2.");
        }
    }

    /// <summary>
    /// Decodes a frame body into <paramref name="plaintext"/>, which must be exactly the declared
    /// uncompressed size.
    /// </summary>
    /// <param name="method">The frame's method byte.</param>
    /// <param name="body">The frame body.</param>
    /// <param name="plaintext">The destination, sized to the declared uncompressed size.</param>
    /// <exception cref="InvalidDataException">The method byte is unknown, or the body decoded to the wrong length.</exception>
    public static void Decode(byte method, ReadOnlySpan<byte> body, Span<byte> plaintext)
    {
        if (method == MethodNone)
        {
            if (body.Length != plaintext.Length)
            {
                throw new InvalidDataException($"An uncompressed frame declares {plaintext.Length} plaintext bytes but carries a {body.Length}-byte body.");
            }

            body.CopyTo(plaintext);
            return;
        }

        IClickHouseCompressor codec = ResolveCodec(method);
        int written = codec.Decode(body, plaintext);
        if (written != plaintext.Length)
        {
            throw new InvalidDataException($"Compression frame declares {plaintext.Length} plaintext bytes but its body decoded to {written} (corrupt stream).");
        }
    }

    /// <summary>
    /// The codec for a frame's method byte. The peer chooses the codec per frame, so this must follow the
    /// frame rather than whatever the caller configured for its own writes.
    /// </summary>
    /// <param name="method">The frame's method byte.</param>
    /// <returns>The matching codec.</returns>
    /// <exception cref="InvalidDataException">The method byte is not one this client can decode.</exception>
    public static IClickHouseCompressor ResolveCodec(byte method) => method switch
    {
        MethodLz4 => Lz4Compressor.Default,
        MethodZstd => ZstdCompressor.Default,
        _ => throw new InvalidDataException(
            $"Compression frame declares method byte 0x{method:X2}, which this client cannot decode " +
            $"(supported: 0x{MethodNone:X2} NONE, 0x{MethodLz4:X2} LZ4, 0x{MethodZstd:X2} ZSTD)."),
    };
}
