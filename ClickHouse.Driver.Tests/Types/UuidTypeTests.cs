using System;
using System.IO;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

/// <summary>
/// Pins the UUID wire layout, which is neither the .NET byte order nor plain big-endian: ClickHouse stores
/// the two 64-bit halves swapped, each big-endian. A round-trip test cannot see this — read and write
/// permute inversely, so the same wrong permutation on both sides still round-trips — so the expected bytes
/// come from the server instead.
/// </summary>
[TestFixture]
public class UuidTypeTests
{
    private static readonly Guid Uuid = new("2ee6b16f-1b03-4b1e-a1a5-99f7ae6a1c2c");

    // SELECT toUUID('2ee6b16f-1b03-4b1e-a1a5-99f7ae6a1c2c') FORMAT RowBinary, against ClickHouse 26.3.
    private static readonly byte[] WireBytes =
    [
        0x1e, 0x4b, 0x03, 0x1b, 0x6f, 0xb1, 0xe6, 0x2e,
        0x2c, 0x1c, 0x6a, 0xae, 0xf7, 0x99, 0xa5, 0xa1,
    ];

    private static ClickHouseType Type => TypeConverter.ParseClickHouseType("UUID", TypeSettings.Default);

    [Test]
    public void Write_Guid_EmitsTheBytesTheServerEmits()
    {
        using var stream = new MemoryStream();
        using var writer = new ExtendedBinaryWriter(stream);

        Type.Write(writer, Uuid);
        writer.Flush();

        Assert.That(stream.ToArray(), Is.EqualTo(WireBytes));
    }

    [Test]
    public void Read_ServerBytes_ReturnsTheGuid()
    {
        using var stream = new MemoryStream(WireBytes);
        using var reader = new ExtendedBinaryReader(stream);

        Assert.That(Type.Read(reader), Is.EqualTo(Uuid));
        Assert.That(stream.Position, Is.EqualTo(WireBytes.Length), "read must consume exactly the 16 bytes");
    }
}
