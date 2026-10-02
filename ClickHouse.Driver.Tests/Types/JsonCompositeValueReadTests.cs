using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Tests.Attributes;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

/// <summary>
/// Pins how a JSON path holding a composite value is materialized: a tuple, and a value under a
/// Dynamic or Variant path whose own type is only known per value. Each case asserts the document
/// the server itself renders for the same expression, so the driver and the server cannot drift.
/// </summary>
[TestFixture]
public class JsonCompositeValueReadTests : AbstractConnectionTestFixture
{
    public static IEnumerable<TestCaseData> Cases()
    {
        // A dynamic path holding a heterogeneous array: the path is Array(Dynamic) and the nested
        // element is a Dynamic holding an unnamed Tuple.
        yield return Case("DynamicPathHoldingTuple",
            @"'{""a"": [""template"", ""x"", [""macro"", 20], ""y""]}'::JSON",
            @"{""a"":[""template"",""x"",[""macro"",20],""y""]}");

        // max_dynamic_types bounds what the server tracks, not how a value is encoded.
        yield return Case("DynamicPathHoldingTupleWithoutDynamicTypeBudget",
            @"'{""a"": [""template"", ""x"", [""macro"", 20], ""y""]}'::JSON(max_dynamic_types = 0)",
            @"{""a"":[""template"",""x"",[""macro"",20],""y""]}");

        // An unnamed tuple is an array, a named one is an object — under a hint, and under the
        // per-value type a Dynamic carries.
        yield return Case("UnnamedTuplePath",
            @"CAST('{""t"": [1, 2]}', 'JSON(t Tuple(Int64, Int64))')",
            @"{""t"":[1,2]}");

        yield return Case("NamedTuplePath",
            @"CAST('{""t"": {""x"": 1, ""y"": ""s""}}', 'JSON(t Tuple(x Int64, y String))')",
            @"{""t"":{""x"":1,""y"":""s""}}");

        yield return Case("DynamicPathHoldingUnnamedTuple",
            @"CAST(map('v', CAST(tuple(1::Int64, 's'), 'Dynamic')), 'JSON(v Dynamic)')",
            @"{""v"":[1,""s""]}");

        // A tuple nested in the containers which are read element by element.
        yield return Case("ArrayOfTuplesPath",
            @"CAST('{""t"": [[1, ""a""], [2, ""b""]]}', 'JSON(t Array(Tuple(Int64, String)))')",
            @"{""t"":[[1,""a""],[2,""b""]]}");

        yield return Case("MapOfTuplesPath",
            @"CAST('{""m"": {""k"": [1, ""a""]}}', 'JSON(m Map(String, Tuple(Int64, String)))')",
            @"{""m"":{""k"":[1,""a""]}}");

        // Nested reads as a repeated tuple, and the server renders it as an array of objects.
        yield return Case("NestedPath",
            @"CAST('{""n"": [{""x"": 1, ""y"": ""a""}, {""x"": 2, ""y"": ""b""}]}', 'JSON(n Nested(x Int64, y String))')",
            @"{""n"":[{""x"":1,""y"":""a""},{""x"":2,""y"":""b""}]}");

        // SimpleAggregateFunction reads as the type it wraps, so the value inside decides the shape.
        yield return Case("SimpleAggregateFunctionOfTuplePath",
            @"CAST('{""t"": [1, ""a""]}', 'JSON(t SimpleAggregateFunction(anyLast, Tuple(Int64, String)))')",
            @"{""t"":[1,""a""]}");

        // A Variant path names its alternatives up front, but which one a value holds is still
        // decided per value.
        yield return Case("VariantPathHoldingArray",
            @"CAST('{""v"": [1, 2]}', 'JSON(v Variant(Array(Int64), String))')",
            @"{""v"":[1,2]}");

        yield return Case("VariantPathHoldingTuple",
            @"CAST('{""v"": [1, ""a""]}', 'JSON(v Variant(Tuple(Int64, String), String))')",
            @"{""v"":[1,""a""]}");

        // Contrast: a scalar under the same per-value types keeps rendering as a scalar, and an
        // absent alternative as an explicit null.
        yield return Case("DynamicPathHoldingString",
            @"CAST('{""v"": ""str""}', 'JSON(v Dynamic)')",
            @"{""v"":""str""}");

        yield return Case("DynamicPathHoldingNull",
            @"CAST('{""n"": null}', 'JSON(n Dynamic)')",
            @"{""n"":null}");

        yield return Case("VariantPathHoldingNull",
            @"CAST('{""v"": null}', 'JSON(v Variant(Array(Int64), String))')",
            @"{""v"":null}");

        // A Nullable path is read by the type it wraps, so a fixed string under it stays text.
        yield return Case("NullableFixedStringPath",
            @"CAST('{""s"": ""abc""}', 'JSON(s Nullable(FixedString(3)))')",
            @"{""s"":""abc""}");
    }

    /// <summary>
    /// A Nullable path decides only whether a value is present; a present value still renders by
    /// the type Nullable wraps. A Tuple is the one composite type Nullable accepts, and only
    /// behind a server setting, so these cases carry it.
    /// </summary>
    public static IEnumerable<TestCaseData> NullableCases()
    {
        yield return Case("NullableNamedTuplePath",
            @"CAST('{""t"": {""x"": 1, ""y"": ""s""}}', 'JSON(t Nullable(Tuple(x Int64, y String)))')",
            @"{""t"":{""x"":1,""y"":""s""}}");

        yield return Case("NullableUnnamedTuplePath",
            @"CAST('{""t"": [1, ""a""]}', 'JSON(t Nullable(Tuple(Int64, String)))')",
            @"{""t"":[1,""a""]}");

        yield return Case("NullableTupleOfCompositePath",
            @"CAST('{""t"": {""a"": [1, 2], ""s"": ""x""}}', 'JSON(t Nullable(Tuple(a Array(Int64), s String)))')",
            @"{""t"":{""a"":[1,2],""s"":""x""}}");

        // The containers which are read element by element each hand their element type on.
        yield return Case("ArrayOfNullableTuplesPath",
            @"CAST('{""t"": [[1, ""a""], null]}', 'JSON(t Array(Nullable(Tuple(Int64, String))))')",
            @"{""t"":[[1,""a""],null]}");

        yield return Case("MapOfNullableTuplesPath",
            @"CAST('{""m"": {""k"": [1, ""a""], ""n"": null}}', 'JSON(m Map(String, Nullable(Tuple(Int64, String))))')",
            @"{""m"":{""k"":[1,""a""],""n"":null}}");

        // Contrast: the absent value, and a scalar under the same wrapper, keep their rendering.
        yield return Case("NullableTuplePathHoldingNull",
            @"CAST('{""t"": null}', 'JSON(t Nullable(Tuple(x Int64, y String)))')",
            @"{""t"":null}");

        yield return Case("NullableScalarPath",
            @"CAST('{""s"": ""v""}', 'JSON(s Nullable(String))')",
            @"{""s"":""v""}");
    }

    private static TestCaseData Case(string name, string valueSql, string expectedJson)
        => new TestCaseData(valueSql, expectedJson).SetName(name);

    [Test]
    [RequiredFeature(Feature.Json)]
    [TestCaseSource(nameof(Cases))]
    public Task ShouldReadJsonValueAsTheServerRendersIt(string valueSql, string expectedJson)
        => AssertReadsAsServerRenders(valueSql, expectedJson);

    [Test]
    [RequiredFeature(Feature.Json)]
    [FromVersion(26, 1)] // allow_experimental_nullable_tuple_type
    [TestCaseSource(nameof(NullableCases))]
    public Task ShouldReadNullableJsonValueAsTheServerRendersIt(string valueSql, string expectedJson)
        => AssertReadsAsServerRenders(valueSql, expectedJson, "allow_experimental_nullable_tuple_type");

    private async Task AssertReadsAsServerRenders(string valueSql, string expectedJson, string enablingSetting = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {valueSql} AS v, toJSONString({valueSql}) AS s";
        if (enablingSetting != null)
            command.CustomSettings[enablingSetting] = 1;

        using var reader = await command.ExecuteReaderAsync();
        Assert.That(reader.Read(), Is.True);

        var value = (JsonObject)reader.GetValue(0);
        var serverRendering = reader.GetString(1);

        Assert.Multiple(() =>
        {
            Assert.That(serverRendering, Is.EqualTo(expectedJson));
            Assert.That(value.ToJsonString(), Is.EqualTo(expectedJson));
        });
    }
}
