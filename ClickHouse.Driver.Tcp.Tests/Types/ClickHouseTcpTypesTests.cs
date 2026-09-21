using System;

namespace ClickHouse.Driver.Tcp.Tests.Types;

/// <summary>
/// Covers the public argument and error contracts of <see cref="ClickHouseTcpTypes"/>.
/// </summary>
[TestFixture]
public class ClickHouseTcpTypesTests
{
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
