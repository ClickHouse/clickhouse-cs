using System;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Covers the public argument and error contracts of <see cref="ClickHouseTcpTypes"/>.
/// </summary>
[TestFixture]
public class ClickHouseTcpTypesTests
{
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

    /// <summary>
    /// A zone this platform cannot represent is a property of the value, not of the type: the type does offer the
    /// calendar readings, and only a row actually projected needs the zone. So the answer is yes, and the read is
    /// where the zone is reported. Asking is also how the POCO tier discovers a mapping, so a throw here would
    /// fail a mapping that never asked for a calendar value.
    /// </summary>
    [TestCase("DateTime('Fixed/UTC+19:00:00')")]
    [TestCase("DateTime64(3, 'Fixed/UTC+05:30:15')")]
    public void CanRead_ACalendarReadingOfAZoneTimeZoneInfoCannotHold_AnswersYes(string clickHouseType)
    {
        Assert.Multiple(() =>
        {
            Assert.That(ClickHouseTcpTypes.CanRead(clickHouseType, typeof(DateTimeOffset)), Is.True);
            Assert.That(ClickHouseTcpTypes.CanRead(clickHouseType, typeof(DateTime)), Is.True);
        });
    }
}
