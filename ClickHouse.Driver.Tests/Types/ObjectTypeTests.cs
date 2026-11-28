using ClickHouse.Driver.Types;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

public class ObjectTypeTests
{
    [Test]
    public void Parse_ObjectJson_ReturnsObjectType()
    {
        var type = TypeConverter.ParseClickHouseType("Object('json')", TypeSettings.Default);

        Assert.That(type, Is.TypeOf<ObjectType>());
        Assert.That(type.ToString(), Is.EqualTo("Object(Json)"));
    }
}
