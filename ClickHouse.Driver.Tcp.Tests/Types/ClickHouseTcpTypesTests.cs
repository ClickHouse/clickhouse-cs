using System;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Covers the public entry points of <see cref="ClickHouseTcpTypes"/>: the argument contract and how a type string
/// this client cannot handle is reported. Which answers the codecs give is
/// <see cref="ColumnWriteAcceptanceTests"/> and <see cref="ColumnReadProjectionTests"/>; that those answers match
/// what a real insert and a real read do is
/// <c>ClickHouseTcpTypesIntegrationTests</c>.
/// </summary>
[TestFixture]
public class ClickHouseTcpTypesTests
{
    [Test]
    public void CanWrite_TheTypeAndACandidate_AnswersFromTheCodec()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClickHouseTcpTypes.CanWrite("Array(Nullable(DateTime('UTC')))", typeof(DateTime?[])), Is.True);
            Assert.That(ClickHouseTcpTypes.CanWrite("Array(Nullable(DateTime('UTC')))", typeof(DateTime[])), Is.False, "an Array(Nullable(T)) row has to carry the nulls");
            Assert.That(ClickHouseTcpTypes.CanWrite("FixedString(4)", typeof(byte[])), Is.True);
            Assert.That(ClickHouseTcpTypes.CanWrite("FixedString(4)", typeof(string)), Is.False);
        });
    }

    [Test]
    public void CanRead_TheTypeAndACandidate_AnswersFromTheCodec()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClickHouseTcpTypes.CanRead("Enum8('a' = 1)", typeof(string)), Is.True);
            Assert.That(ClickHouseTcpTypes.CanRead("Enum8('a' = 1)", typeof(sbyte)), Is.True);
            Assert.That(ClickHouseTcpTypes.CanRead("UInt32", typeof(uint)), Is.True);
            Assert.That(ClickHouseTcpTypes.CanRead("UInt32", typeof(long)), Is.False, "there is no numeric widening");
        });
    }

    [Test]
    public void CanReadAndCanWrite_AnAliasOrACaseVariant_AnswerForTheTypeItNames()
    {
        // A caller asking about a type writes it the way their query does, and the server takes any of these.
        Assert.Multiple(() =>
        {
            Assert.That(ClickHouseTcpTypes.CanWrite("VARCHAR", typeof(string)), Is.True);
            Assert.That(ClickHouseTcpTypes.CanRead("BIGINT", typeof(long)), Is.True);
            Assert.That(ClickHouseTcpTypes.CanRead("datetime64(3)", typeof(long)), Is.True);
            Assert.That(ClickHouseTcpTypes.CanWrite("Array(TINYINT UNSIGNED)", typeof(byte[])), Is.True);
            Assert.That(ClickHouseTcpTypes.CanWrite("VARCHAR", typeof(int)), Is.False, "the alias does not change what fits");
        });
    }

    [Test]
    public void CanReadAndCanWrite_NullArgument_Throws()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpTypes.CanWrite(null, typeof(int)));
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpTypes.CanWrite("Int32", null));
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpTypes.CanRead(null, typeof(int)));
            Assert.Throws<ArgumentNullException>(() => ClickHouseTcpTypes.CanRead("Int32", null));
        });
    }

    /// <summary>
    /// A type string the client cannot resolve is not a "no": it is a different failure, and reporting it as false
    /// would send a caller looking for a CLR type that would satisfy it.
    /// </summary>
    [Test]
    public void CanReadAndCanWrite_TypeThisClientCannotResolve_ThrowsRatherThanAnsweringFalse()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<FormatException>(() => ClickHouseTcpTypes.CanWrite("Array(", typeof(int[])));
            Assert.Throws<NotSupportedException>(() => ClickHouseTcpTypes.CanWrite("NoSuchType", typeof(int)));
            Assert.Throws<FormatException>(() => ClickHouseTcpTypes.CanRead("Array(", typeof(int[])));
            Assert.Throws<NotSupportedException>(() => ClickHouseTcpTypes.CanRead("NoSuchType", typeof(int)));
        });
    }
}
