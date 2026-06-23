using System;
using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

public class IdentifierTypeTests
{
    [Test]
    public void ParseClickHouseType_Identifier_ReturnsIdentifierType()
    {
        var type = TypeConverter.ParseClickHouseType("Identifier", TypeSettings.Default);
        Assert.That(type, Is.InstanceOf<IdentifierType>());
        Assert.That(type.ToString(), Is.EqualTo("Identifier"));
    }

    [Test]
    public void ToClickHouseType_String_RemainsStringNotIdentifier()
    {
        // Identifier is intentionally excluded from value->type inference: a .NET string must keep
        // inferring as String so ordinary string parameters are unaffected by the new pseudo-type.
        Assert.That(TypeConverter.ToClickHouseType(typeof(string)), Is.InstanceOf<StringType>());
    }

    [Test]
    public void Read_IdentifierType_ThrowsNotSupported()
    {
        // Identifier is a query-parameter-only pseudo-type; the server never emits it in a result set.
        Assert.Throws<NotSupportedException>(() => new IdentifierType().Read(null));
    }

    [Test]
    public void Write_IdentifierType_ThrowsNotSupported()
    {
        Assert.Throws<NotSupportedException>(() => new IdentifierType().Write(null, "x"));
    }
}
