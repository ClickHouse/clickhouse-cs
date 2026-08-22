using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Driver.Compression;
using ClickHouse.Driver.Tcp.Compression;
using ClickHouse.Driver.Tcp.Protocol;

namespace ClickHouse.Driver.Tcp.Tests.Compression;

/// <summary>
/// Unit tests for the streaming frame reader and writer over a memory stream. These cover what a server round
/// trip cannot pin down: how many frames a body is cut into, that a value split across a boundary reassembles,
/// that the reader stops at a boundary instead of reading into whatever follows, and the leftover assertion.
/// </summary>
[TestFixture]
public class CompressedFrameStreamingTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private static IEnumerable<TestCaseData> Codecs()
    {
        yield return new TestCaseData(Lz4Compressor.Default).SetName("{m}(LZ4)");
        yield return new TestCaseData(ZstdCompressor.Default).SetName("{m}(ZSTD)");
    }

    [TestCaseSource(nameof(Codecs))]
    public async Task WriteThenRead_ABodySpanningManyFrames_RecoversEveryByte(IClickHouseCompressor codec)
    {
        // A frame target far below the payload forces many frames, so values land across the boundaries.
        byte[] payload = Pattern(50_000);
        var stream = new MemoryStream();

        using (var rawWriter = new ClickHouseBinaryWriter(stream))
        using (var frames = new CompressedFrameWriter(rawWriter, codec, frameTarget: 700))
        {
            frames.Writer.WriteBytes(payload);
            await frames.EndBlockAsync(None);
        }

        stream.Position = 0;
        var readBack = new byte[payload.Length];
        using (var rawReader = new ClickHouseBinaryReader(stream))
        using (var frames = new CompressedFrameReader(rawReader))
        {
            // Both read paths: a few single bytes through the buffer, then the bulk path for the remainder.
            for (int i = 0; i < 5; i++)
            {
                readBack[i] = await frames.Reader.ReadByteAsync(None);
            }

            await frames.Reader.ReadBytesAsync(readBack.AsMemory(5), None);
            frames.EndBlock();
        }

        Assert.That(readBack, Is.EqualTo(payload));
    }

    [TestCaseSource(nameof(Codecs))]
    public async Task WriteThenRead_AFixedWidthValueStraddlingAFrameBoundary_DecodesWhole(IClickHouseCompressor codec)
    {
        // The frame target is deliberately not a multiple of 8, so UInt64s straddle boundaries.
        var stream = new MemoryStream();
        const int Count = 5000;

        using (var rawWriter = new ClickHouseBinaryWriter(stream))
        using (var frames = new CompressedFrameWriter(rawWriter, codec, frameTarget: 101))
        {
            for (ulong i = 0; i < Count; i++)
            {
                frames.Writer.WriteUInt64(i);
            }

            await frames.EndBlockAsync(None);
        }

        stream.Position = 0;
        using (var rawReader = new ClickHouseBinaryReader(stream))
        using (var frames = new CompressedFrameReader(rawReader))
        {
            for (ulong i = 0; i < Count; i++)
            {
                ulong value = await frames.Reader.ReadUInt64Async(None);
                if (value != i)
                {
                    Assert.Fail($"value {i} decoded as {value}, so a frame boundary split it wrongly");
                }
            }

            frames.EndBlock();
        }

        Assert.Pass();
    }

    [Test]
    public async Task Read_AtTheEndOfABody_LeavesWhateverFollowsOnTheRawStream()
    {
        // The hazard the short reads exist for: after a body, the next bytes are an uncompressed envelope. The
        // reader must not have consumed them while satisfying the last read.
        byte[] payload = Pattern(64);
        var stream = new MemoryStream();

        using (var rawWriter = new ClickHouseBinaryWriter(stream))
        {
            using (var frames = new CompressedFrameWriter(rawWriter, codec: Lz4Compressor.Default))
            {
                frames.Writer.WriteBytes(payload);
                await frames.EndBlockAsync(None);
            }

            // An uncompressed "next packet" straight after the frames, exactly as the server would write it.
            rawWriter.WriteVarUInt(42);
            rawWriter.WriteString("next-packet");
            await rawWriter.FlushAsync(None);
        }

        stream.Position = 0;
        using var rawReader = new ClickHouseBinaryReader(stream);
        using (var frames = new CompressedFrameReader(rawReader))
        {
            var readBack = new byte[payload.Length];
            await frames.Reader.ReadBytesAsync(readBack, None);
            frames.EndBlock();
            Assert.That(readBack, Is.EqualTo(payload));
        }

        // The envelope survived: it was never pulled in as frame bytes.
        Assert.Multiple(async () =>
        {
            Assert.That(await rawReader.ReadVarUIntAsync(None), Is.EqualTo(42UL));
            Assert.That(await rawReader.ReadStringAsync(None), Is.EqualTo("next-packet"));
        });
    }

    [Test]
    public async Task EndBlock_WithDecodedPlaintextLeftUnread_Throws()
    {
        // Stands in for a column decoder that consumed too few bytes: the frames carried more than the block's
        // dimensions accounted for, which must fail loudly rather than desync the next block.
        var stream = new MemoryStream();
        using (var rawWriter = new ClickHouseBinaryWriter(stream))
        using (var frames = new CompressedFrameWriter(rawWriter, Lz4Compressor.Default))
        {
            frames.Writer.WriteBytes(Pattern(500));
            await frames.EndBlockAsync(None);
        }

        stream.Position = 0;
        using var rawReader = new ClickHouseBinaryReader(stream);
        using var reader = new CompressedFrameReader(rawReader);

        var partial = new byte[100];
        await reader.Reader.ReadBytesAsync(partial, None);

        var failure = Assert.Throws<ClickHouseProtocolException>(() => reader.EndBlock());
        Assert.That(failure.Message, Does.Contain("unread"));
    }

    [Test]
    public async Task EndBlock_AfterTheWholeBody_DoesNotThrow()
    {
        var stream = new MemoryStream();
        using (var rawWriter = new ClickHouseBinaryWriter(stream))
        using (var frames = new CompressedFrameWriter(rawWriter, Lz4Compressor.Default))
        {
            frames.Writer.WriteBytes(Pattern(500));
            await frames.EndBlockAsync(None);
        }

        stream.Position = 0;
        using var rawReader = new ClickHouseBinaryReader(stream);
        using var reader = new CompressedFrameReader(rawReader);

        var all = new byte[500];
        await reader.Reader.ReadBytesAsync(all, None);

        Assert.Multiple(() =>
        {
            Assert.That(() => reader.EndBlock(), Throws.Nothing);
            Assert.That(reader.PendingPlaintext, Is.Zero);
        });
    }

    [Test]
    public async Task Read_AFrameWhoseBodyWasCorruptedInTransit_Throws()
    {
        var stream = new MemoryStream();
        using (var rawWriter = new ClickHouseBinaryWriter(stream))
        using (var frames = new CompressedFrameWriter(rawWriter, Lz4Compressor.Default))
        {
            frames.Writer.WriteBytes(Pattern(500));
            await frames.EndBlockAsync(None);
        }

        byte[] wire = stream.ToArray();
        wire[CompressionFrame.PrefixSize + 2] ^= 0x01;

        using var rawReader = new ClickHouseBinaryReader(new MemoryStream(wire));
        using var reader = new CompressedFrameReader(rawReader);

        var destination = new byte[500];
        Assert.That(
            async () => await reader.Reader.ReadBytesAsync(destination, None),
            Throws.TypeOf<InvalidDataException>().With.Message.Contains("checksum mismatch"));
    }

    [Test]
    public void Constructor_ANullRawReader_Throws()
        => Assert.That(() => new CompressedFrameReader(null), Throws.ArgumentNullException);

    [Test]
    public void Constructor_ANullCodecOrWriter_Throws()
    {
        using var stream = new MemoryStream();
        using var rawWriter = new ClickHouseBinaryWriter(stream);

        Assert.Multiple(() =>
        {
            Assert.That(() => new CompressedFrameWriter(null, Lz4Compressor.Default), Throws.ArgumentNullException);
            Assert.That(() => new CompressedFrameWriter(rawWriter, null), Throws.ArgumentNullException);
            Assert.That(() => new CompressedFrameWriter(rawWriter, Lz4Compressor.Default, frameTarget: 0), Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void Dispose_CalledTwice_IsANoOp()
    {
        var stream = new MemoryStream();
        using var rawWriter = new ClickHouseBinaryWriter(stream);
        using var rawReader = new ClickHouseBinaryReader(stream);

        var reader = new CompressedFrameReader(rawReader);
        var writer = new CompressedFrameWriter(rawWriter, Lz4Compressor.Default);

        reader.Dispose();
        writer.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(() => reader.Dispose(), Throws.Nothing);
            Assert.That(() => writer.Dispose(), Throws.Nothing);
        });
    }

    private static byte[] Pattern(int length)
    {
        var data = new byte[length];
        for (int i = 0; i < length; i++)
        {
            data[i] = unchecked((byte)((i * 31) + 7));
        }

        return data;
    }
}
