using System;
using ClickHouse.Driver.ADO.Parameters;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.ADO;

public class SqlParameterTypeExtractorTests
{
    [Test]
    public void ExtractTypeHints_SimpleType_ReturnsType()
    {
        var expectedType = "UInt64";
        var sql = $"SELECT {{id:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["id"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_DateTimeWithTimezone_ReturnsFullType()
    {
        var expectedType = "DateTime('Europe/Amsterdam')";
        var sql = $"SELECT {{dt:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["dt"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_DateTime64WithScaleAndTimezone_ReturnsFullType()
    {
        var expectedType = "DateTime64(3, 'UTC')";
        var sql = $"SELECT {{dt:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["dt"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_NestedType_ReturnsFullType()
    {
        var expectedType = "Array(Nullable(String))";
        var sql = $"SELECT {{arr:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["arr"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_EnumWithMultipleValues_ReturnsFullType()
    {
        var expectedType = "Enum8('a' = 1, 'b' = 2)";
        var sql = $"SELECT {{e:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["e"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_MultipleParameters_ReturnsAllTypes()
    {
        var expectedIdType = "UInt64";
        var expectedNameType = "String";
        var expectedDtType = "DateTime('UTC')";
        var sql = $"SELECT {{id:{expectedIdType}}}, {{name:{expectedNameType}}}, {{dt:{expectedDtType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(3));
        Assert.That(hints["id"], Is.EqualTo(expectedIdType));
        Assert.That(hints["name"], Is.EqualTo(expectedNameType));
        Assert.That(hints["dt"], Is.EqualTo(expectedDtType));
    }

    [Test]
    public void ExtractTypeHints_EscapedQuotesInEnum_ReturnsFullType()
    {
        var expectedType = "Enum8('it''s' = 1, 'hello' = 2)";
        var sql = $"SELECT {{e:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["e"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_NullString_ReturnsEmptyDictionary()
    {
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(null);

        Assert.That(hints, Is.Empty);
    }

    [Test]
    public void ExtractTypeHints_EmptyString_ReturnsEmptyDictionary()
    {
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(string.Empty);

        Assert.That(hints, Is.Empty);
    }

    [Test]
    public void ExtractTypeHints_NoParameters_ReturnsEmptyDictionary()
    {
        var sql = "SELECT 1";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Is.Empty);
    }

    [Test]
    public void ExtractTypeHints_ComplexInsertQuery_ReturnsAllTypes()
    {
        var expectedIdType = "UInt64";
        var expectedNameType = "String";
        var expectedCreatedAtType = "DateTime('Europe/London')";
        var expectedTagsType = "Array(String)";
        var sql = $@"INSERT INTO test_table (id, name, created_at, tags)
                    VALUES ({{id:{expectedIdType}}}, {{name:{expectedNameType}}}, {{created_at:{expectedCreatedAtType}}}, {{tags:{expectedTagsType}}})";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(4));
        Assert.That(hints["id"], Is.EqualTo(expectedIdType));
        Assert.That(hints["name"], Is.EqualTo(expectedNameType));
        Assert.That(hints["created_at"], Is.EqualTo(expectedCreatedAtType));
        Assert.That(hints["tags"], Is.EqualTo(expectedTagsType));
    }

    [Test]
    public void ExtractTypeHints_DeeplyNestedType_ReturnsFullType()
    {
        var expectedType = "Array(Tuple(String, Array(Nullable(Int32))))";
        var sql = $"SELECT {{data:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["data"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_MapType_ReturnsFullType()
    {
        var expectedType = "Map(String, Int32)";
        var sql = $"SELECT {{m:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["m"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_BraceInsideQuotedString_ReturnsFullType()
    {
        var expectedType = "Enum8('}' = 1, '{' = 2)";
        var sql = $"SELECT {{e:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["e"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_FullTypeDefinitionInsideQuotedString_IgnoresQuotedString()
    {
        var expectedType = "Enum8('{type:value}' = 1, '{' = 2)";
        var sql = $"SELECT {{e:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["e"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_TrailingSpaceInType_Trimmed()
    {
        var expectedType = "Int32";
        var sql = $"SELECT {{a:{expectedType} }}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["a"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_SpacesAroundColon_Trimmed()
    {
        var expectedType = "Int32";
        var sql = $"SELECT {{a : {expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["a"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_LeadingSpaceInName_Trimmed()
    {
        var expectedType = "Int32";
        var sql = $"SELECT {{ a:{expectedType}}}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["a"], Is.EqualTo(expectedType));
    }

    [Test]
    public void ExtractTypeHints_UnterminatedParameter_ReturnsEmptyDictionary()
    {
        var sql = "SELECT {id:Int32";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Is.Empty);
    }

    [Test]
    public void ExtractTypeHints_SameParameterWithDifferentTypes_ThrowsArgumentException()
    {
        var sql = "SELECT {val:Int32}, {val:String}";

        var ex = Assert.Throws<ArgumentException>(() => SqlParameterTypeExtractor.ExtractTypeHints(sql));
        Assert.That(ex.Message, Does.Contain("Parameter 'val' has conflicting type hints"));
        Assert.That(ex.Message, Does.Contain("Int32"));
        Assert.That(ex.Message, Does.Contain("String"));
    }

    [Test]
    public void ExtractTypeHints_SameParameterWithSameType_ReturnsType()
    {
        var sql = "SELECT {val:Int32}, {val:Int32}";
        var hints = SqlParameterTypeExtractor.ExtractTypeHints(sql);

        Assert.That(hints, Has.Count.EqualTo(1));
        Assert.That(hints["val"], Is.EqualTo("Int32"));
    }
}
