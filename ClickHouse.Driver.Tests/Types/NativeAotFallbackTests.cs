using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using ClickHouse.Driver.Json;
using ClickHouse.Driver.Types;
using ClickHouse.Driver.Utility;
using NUnit.Framework;

namespace ClickHouse.Driver.Tests.Types;

/// <summary>
/// The CLR representations the driver picks when dynamic code is not supported, as under native AOT. The test
/// process supports dynamic code, so these call the selection with <c>dynamicCode: false</c>. The
/// ClickHouse.Driver.AotSmokeTest project runs the same paths end to end in a native AOT binary.
/// </summary>
public class NativeAotFallbackTests
{
    private static readonly TypeSettings[] AllSettings =
    [
        TypeSettings.Default,
        TypeSettings.Default with { useBigDecimal = false, readStringsAsByteArrays = true, jsonReadMode = JsonReadMode.String },
    ];

    [Test]
    public void ClrTypes_EveryValueTypeOfTheTypeMatrix_IsListed()
    {
        var listed = (IDictionary)typeof(ClrTypes)
            .GetField("ValueTypes", BindingFlags.NonPublic | BindingFlags.Static)
            .GetValue(null);

        var found = new HashSet<Type>();
        foreach (var settings in AllSettings)
        {
            foreach (var sample in TestCases.GetDataTypeSamples())
                CollectValueTypes(TypeConverter.ParseClickHouseType(sample.ClickHouseType, settings).FrameworkType, found);
        }

        Assert.That(found, Is.Not.Empty, "found no value types at all — check the walk");
        Assert.That(found.Where(t => !listed.Contains(t)), Is.Empty,
            "native AOT cannot create an array of a value type ClrTypes does not list");
    }

    [Test]
    public void CreateArray_UnlistedValueTypeWithoutDynamicCode_ThrowsNotSupported()
    {
        var error = Assert.Throws<NotSupportedException>(() => ClrTypes.CreateArray(typeof(Half), 1, dynamicCode: false));
        Assert.That(error.Message, Does.Contain("System.Half"));
    }

    [Test]
    public void CreateArray_UnlistedReferenceTypeWithoutDynamicCode_CreatesTypedArray()
    {
        Assert.That(ClrTypes.CreateArray(typeof(LargeTuple), 2, dynamicCode: false), Is.TypeOf<LargeTuple[]>().And.Length.EqualTo(2));
    }

    [Test]
    public void CreateArray_ListedNullableValueType_CreatesTypedArray()
    {
        Assert.That(ClrTypes.CreateArray(typeof(int?), 2, dynamicCode: false), Is.TypeOf<int?[]>());
    }

    [Test]
    public void MapGetShape_WithoutDynamicCode_FallsBackToObjectEntries()
    {
        var shape = MapType.GetShape(typeof(string), typeof(int), dynamicCode: false);
        Assert.Multiple(() =>
        {
            Assert.That(shape.DictionaryType, Is.EqualTo(typeof(Dictionary<object, object>)));
            Assert.That(shape.NewDictionary(1), Is.TypeOf<Dictionary<object, object>>());
            Assert.That(shape.ListType, Is.EqualTo(typeof(List<KeyValuePair<object, object>>)));
            Assert.That(shape.NewList(1), Is.TypeOf<List<KeyValuePair<object, object>>>());
            Assert.That(shape.NewPair("k", 1), Is.EqualTo(new KeyValuePair<object, object>("k", 1)));
        });
    }

    [Test]
    public void MapGetShape_StringToStringWithoutDynamicCode_StaysTyped()
    {
        var shape = MapType.GetShape(typeof(string), typeof(string), dynamicCode: false);
        Assert.Multiple(() =>
        {
            Assert.That(shape.NewDictionary(1), Is.TypeOf<Dictionary<string, string>>());
            Assert.That(shape.NewList(1), Is.TypeOf<List<KeyValuePair<string, string>>>());
            Assert.That(shape.NewPair("k", "v"), Is.EqualTo(new KeyValuePair<string, string>("k", "v")));
        });
    }

    [TestCase(typeof(KeyValuePair<object, object>))]
    [TestCase(typeof(KeyValuePair<string, string>))]
    public void MapGetPairAccessors_CompiledPairTypeWithoutDynamicCode_ReadsKeyAndValue(Type pairType)
    {
        var pair = Activator.CreateInstance(pairType, "k", "v");
        var (key, value) = MapType.GetPairAccessors(pairType, dynamicCode: false);
        Assert.That((key(pair), value(pair)), Is.EqualTo(((object)"k", (object)"v")));
    }

    [Test]
    public void MapGetPairAccessors_OtherPairTypeWithoutDynamicCode_ThrowsNotSupported()
    {
        var error = Assert.Throws<NotSupportedException>(
            () => MapType.GetPairAccessors(typeof(KeyValuePair<string, int>), dynamicCode: false));
        Assert.That(error.Message, Does.Contain("IDictionary"));
    }

    [Test]
    public void TupleGetShape_WithoutDynamicCode_FallsBackToLargeTuple()
    {
        var (frameworkType, factory) = TupleType.GetShape([new Int32Type(), new StringType()], dynamicCode: false);
        Assert.Multiple(() =>
        {
            Assert.That(frameworkType, Is.EqualTo(typeof(LargeTuple)));
            Assert.That(factory, Is.Null);
        });
    }

    [Test]
    public void TupleGetShape_PointShapeWithoutDynamicCode_StaysTyped()
    {
        var (frameworkType, factory) = TupleType.GetShape([new Float64Type(), new Float64Type()], dynamicCode: false);
        Assert.Multiple(() =>
        {
            Assert.That(frameworkType, Is.EqualTo(typeof(Tuple<double, double>)));
            Assert.That(((Func<object, object, System.Runtime.CompilerServices.ITuple>)factory)(1.0, 2.0), Is.EqualTo(Tuple.Create(1.0, 2.0)));
        });
    }

    [Test]
    public void MakeTuple_LargeTupleShape_ReturnsLargeTupleWithValues()
    {
        var tupleType = new TupleType { UnderlyingTypes = Enumerable.Range(0, 8).Select(_ => (ClickHouseType)new Int32Type()).ToArray() };
        var tuple = tupleType.MakeTuple(Enumerable.Range(1, 8).Cast<object>().ToArray());
        Assert.Multiple(() =>
        {
            Assert.That(tuple, Is.TypeOf<LargeTuple>());
            Assert.That(tuple[7], Is.EqualTo(8));
        });
    }

    // Every value type reachable from a framework type: array elements and generic arguments, recursively
    private static void CollectValueTypes(Type type, HashSet<Type> found)
    {
        if (type.IsValueType && !found.Add(type))
            return;

        if (type.IsArray)
            CollectValueTypes(type.GetElementType(), found);
        if (type.IsGenericType)
        {
            foreach (var argument in type.GetGenericArguments())
                CollectValueTypes(argument, found);
        }
    }
}
