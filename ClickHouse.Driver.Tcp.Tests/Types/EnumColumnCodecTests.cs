using System;
using System.Threading.Tasks;
using ClickHouse.Driver.Tcp.Types;
using ClickHouse.Driver.Tcp.Types.Codecs;
using static ClickHouse.Driver.Tcp.Tests.Utilities.CodecTestHarness;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class EnumColumnCodecTests
{
    [Test]
    public void Create_Enum8_ParsesLabelMap()
    {
        var codec = (EnumColumnCodec<sbyte>)Enum8ColumnCodec.Create(TypeParser.Parse("Enum8('a' = -1, 'b' = 0, 'c' = 127)"));

        Assert.Multiple(() =>
        {
            Assert.That(codec.LabelToOrdinal["a"], Is.EqualTo((sbyte)-1));
            Assert.That(codec.LabelToOrdinal["c"], Is.EqualTo((sbyte)127));
            Assert.That(codec.OrdinalToLabel[(sbyte)0], Is.EqualTo("b"));
        });
    }

    [Test]
    public void Create_Enum8_OrdinalOutOfRange_Throws()
        => Assert.Throws<FormatException>(() => Enum8ColumnCodec.Create(TypeParser.Parse("Enum8('a' = 128)")));

    [Test]
    public void Create_Enum8_NoMembers_Throws()
        => Assert.Throws<FormatException>(() => Enum8ColumnCodec.Create(TypeParser.Parse("Enum8()")));

    [Test]
    public void Create_MalformedMember_Throws()
        => Assert.Throws<FormatException>(() => Enum8ColumnCodec.Create(TypeParser.Parse("Enum8('a')")));

    [Test]
    public void Create_DuplicateLabel_Throws()
        => Assert.Throws<FormatException>(() => Enum8ColumnCodec.Create(TypeParser.Parse("Enum8('a' = 1, 'a' = 2)")));

    [Test]
    public void Create_DuplicateOrdinal_Throws()
        => Assert.Throws<FormatException>(() => Enum8ColumnCodec.Create(TypeParser.Parse("Enum8('a' = 1, 'b' = 1)")));

    [Test]
    public void Create_LabelWithEscapedQuote_IsUnescaped()
    {
        var codec = (EnumColumnCodec<sbyte>)Enum8ColumnCodec.Create(TypeParser.Parse(@"Enum8('a\'b' = 1)"));
        Assert.That(codec.OrdinalToLabel[(sbyte)1], Is.EqualTo("a'b"));
    }

    [Test]
    public async Task Enum8_RoundTrip_SurfacesRawOrdinal()
    {
        IColumnCodec codec = Enum8ColumnCodec.Create(TypeParser.Parse("Enum8('a' = -1, 'b' = 127)"));
        var values = new sbyte[] { -1, 127 };

        using var column = (IColumn<sbyte>)await RoundTripAsync(codec, PrimitiveColumn<sbyte>.FromValues("c", "Enum8", values), "Enum8('a' = -1, 'b' = 127)", values.Length);

        CollectionAssert.AreEqual(values, column.Values.ToArray());
    }

    [Test]
    public async Task Enum16_RoundTrip_SurfacesRawOrdinal()
    {
        IColumnCodec codec = Enum16ColumnCodec.Create(TypeParser.Parse("Enum16('x' = -32768, 'y' = 32767)"));
        var values = new short[] { -32768, 32767 };

        using var column = (IColumn<short>)await RoundTripAsync(codec, PrimitiveColumn<short>.FromValues("c", "Enum16", values), "Enum16('x' = -32768, 'y' = 32767)", values.Length);

        CollectionAssert.AreEqual(values, column.Values.ToArray());
    }
}
