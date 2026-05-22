using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using ClickHouse.Driver.ADO.Parameters;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Types;
using Dapper;
using NUnit.Framework;
namespace ClickHouse.Driver.Tests.Types;

public class TypeMappingTests
{
    [Test]
    [TestCase("Nothing", ExpectedResult = typeof(DBNull))]

    [TestCase("Int8", ExpectedResult = typeof(sbyte))]
    [TestCase("Int16", ExpectedResult = typeof(short))]
    [TestCase("Int32", ExpectedResult = typeof(int))]
    [TestCase("Int64", ExpectedResult = typeof(long))]

    [TestCase("UInt8", ExpectedResult = typeof(byte))]
    [TestCase("UInt16", ExpectedResult = typeof(ushort))]
    [TestCase("UInt32", ExpectedResult = typeof(uint))]
    [TestCase("UInt64", ExpectedResult = typeof(ulong))]

    [TestCase("Float32", ExpectedResult = typeof(float))]
    [TestCase("Float64", ExpectedResult = typeof(double))]

    [TestCase("Decimal(18,3)", ExpectedResult = typeof(ClickHouseDecimal))]
    [TestCase("Decimal32(3)", ExpectedResult = typeof(ClickHouseDecimal))]
    [TestCase("Decimal64(3)", ExpectedResult = typeof(ClickHouseDecimal))]
    [TestCase("Decimal128(3)", ExpectedResult = typeof(ClickHouseDecimal))]

    [TestCase("String", ExpectedResult = typeof(string))]
    [TestCase("FixedString(5)", ExpectedResult = typeof(string))]

    [TestCase("UUID", ExpectedResult = typeof(Guid))]

    [TestCase("IPv4", ExpectedResult = typeof(IPAddress))]
    [TestCase("IPv6", ExpectedResult = typeof(IPAddress))]

    [TestCase("LowCardinality(String)", ExpectedResult = typeof(string))]

    [TestCase("Date", ExpectedResult = typeof(DateTime))]
    [TestCase("DateTime", ExpectedResult = typeof(DateTime))]
    [TestCase("DateTime('Etc/UTC')", ExpectedResult = typeof(DateTime))]
    [TestCase("DateTime64(3)", ExpectedResult = typeof(DateTime))]
    [TestCase("DateTime64(3, 'Etc/UTC')", ExpectedResult = typeof(DateTime))]
    [TestCase("Time", ExpectedResult = typeof(TimeSpan))]
    [TestCase("Time64(6)", ExpectedResult = typeof(TimeSpan))]

    [TestCase("Map(String, Int32)", ExpectedResult = typeof(Dictionary<string, int>))]
    [TestCase("Map(Tuple(Int32, Int32), Int32)", ExpectedResult = typeof(Dictionary<Tuple<int,int>, int>))]
    
    [TestCase("Nullable(UInt32)", ExpectedResult = typeof(uint?))]
    [TestCase("Array(Array(String))", ExpectedResult = typeof(string[][]))]
    [TestCase("Array(Array(Array(UInt8)))", ExpectedResult = typeof(byte[][][]))]
    [TestCase("Array(Array(Array(Array(Int32))))", ExpectedResult = typeof(int[][][][]))]
    [TestCase("Array(Array(Nullable(Int32)))", ExpectedResult = typeof(int?[][]))]
    [TestCase("Array(Nullable(UInt32))", ExpectedResult = typeof(uint?[]))]
    [TestCase("SimpleAggregateFunction(anyLast,Nullable(UInt32))", ExpectedResult = typeof(uint?))]
    [TestCase("Tuple(Int32,UInt8,Nullable(Float32),Array(String))", ExpectedResult = typeof(Tuple<int, byte, float?, string[]>))]
    public Type ShouldConvertFromClickHouseType(string clickHouseType) => TypeConverter.ParseClickHouseType(clickHouseType, TypeSettings.Default).FrameworkType;

    [Test]
    [TestCase(typeof(DBNull), ExpectedResult = "Nullable(Nothing)")]

    [TestCase(typeof(sbyte), ExpectedResult = "Int8")]
    [TestCase(typeof(short), ExpectedResult = "Int16")]
    [TestCase(typeof(int), ExpectedResult = "Int32")]
    [TestCase(typeof(long), ExpectedResult = "Int64")]

    [TestCase(typeof(byte), ExpectedResult = "UInt8")]
    [TestCase(typeof(ushort), ExpectedResult = "UInt16")]
    [TestCase(typeof(uint), ExpectedResult = "UInt32")]
    [TestCase(typeof(ulong), ExpectedResult = "UInt64")]

    [TestCase(typeof(float), ExpectedResult = "Float32")]
    [TestCase(typeof(double), ExpectedResult = "Float64")]

    [TestCase(typeof(string), ExpectedResult = "String")]

    [TestCase(typeof(DateTime), ExpectedResult = "DateTime")]
    [TestCase(typeof(TimeSpan), ExpectedResult = "Time64(7)")]

    [TestCase(typeof(IPAddress), ExpectedResult = "IPv4")]
    [TestCase(typeof(Guid), ExpectedResult = "UUID")]

    [TestCase(typeof(uint?), ExpectedResult = "Nullable(UInt32)")]
    [TestCase(typeof(uint?[]), ExpectedResult = "Array(Nullable(UInt32))")]
    [TestCase(typeof(string[][]), ExpectedResult = "Array(Array(String))")]
    [TestCase(typeof(int[][][]), ExpectedResult = "Array(Array(Array(Int32)))")]
    // Multidimensional CLR arrays map to nested Array types — the wire format is jagged regardless.
    [TestCase(typeof(byte[,]), ExpectedResult = "Array(Array(UInt8))")]
    [TestCase(typeof(string[,]), ExpectedResult = "Array(Array(String))")]
    [TestCase(typeof(int[,,]), ExpectedResult = "Array(Array(Array(Int32)))")]
    [TestCase(typeof(double[,,,]), ExpectedResult = "Array(Array(Array(Array(Float64))))")]
    [TestCase(typeof(Dictionary<string,int>), ExpectedResult = "Map(String, Int32)")]
    [TestCase(typeof(Dictionary<Tuple<int,int>,int>), ExpectedResult = "Map(Tuple(Int32,Int32), Int32)")]
    [TestCase(typeof(List<string>), ExpectedResult = "Array(String)")]
    [TestCase(typeof(List<List<string>>), ExpectedResult = "Array(Array(String))")]
#if NET6_0_OR_GREATER
    [TestCase(typeof(DateOnly), ExpectedResult = "Date")]
#endif
    [TestCase(typeof(Tuple<int, byte, float?, string[]>), ExpectedResult = "Tuple(Int32,UInt8,Nullable(Float32),Array(String))")]
    // System.Tuple with >7 elements (TRest nesting, same as ValueTuple)
    [TestCase(typeof(Tuple<int, int, int, int, int, int, int, Tuple<string>>), ExpectedResult = "Tuple(Int32,Int32,Int32,Int32,Int32,Int32,Int32,String)")]
    [TestCase(typeof(Tuple<int, int, int, int, int, int, int, Tuple<int, string>>), ExpectedResult = "Tuple(Int32,Int32,Int32,Int32,Int32,Int32,Int32,Int32,String)")]

    // ValueTuple → ClickHouse Tuple
    [TestCase(typeof(ValueTuple<int, string>), ExpectedResult = "Tuple(Int32,String)")]
    [TestCase(typeof(ValueTuple<int, byte, float?, string[]>), ExpectedResult = "Tuple(Int32,UInt8,Nullable(Float32),Array(String))")]
    [TestCase(typeof(ValueTuple<int, ValueTuple<string, byte>>), ExpectedResult = "Tuple(Int32,Tuple(String,UInt8))")]
    [TestCase(typeof(ValueTuple<int>), ExpectedResult = "Tuple(Int32)")]
    [TestCase(typeof(ValueTuple<int, string, float, double, byte, long, short>), ExpectedResult = "Tuple(Int32,String,Float32,Float64,UInt8,Int64,Int16)")]
    // ValueTuple with >7 elements (compiler generates nested TRest)
    [TestCase(typeof(ValueTuple<int, int, int, int, int, int, int, ValueTuple<string>>), ExpectedResult = "Tuple(Int32,Int32,Int32,Int32,Int32,Int32,Int32,String)")]
    [TestCase(typeof(ValueTuple<int, int, int, int, int, int, int, ValueTuple<int, string>>), ExpectedResult = "Tuple(Int32,Int32,Int32,Int32,Int32,Int32,Int32,Int32,String)")]
    // Double rest-nesting (15 elements)
    [TestCase(typeof(ValueTuple<int, int, int, int, int, int, int, ValueTuple<int, int, int, int, int, int, int, ValueTuple<string>>>), ExpectedResult = "Tuple(Int32,Int32,Int32,Int32,Int32,Int32,Int32,Int32,Int32,Int32,Int32,Int32,Int32,Int32,String)")]
    public string ShouldConvertToClickHouseType(Type type) => TypeConverter.ToClickHouseType(type).ToString();

    private static IEnumerable<TestCaseData> ValueToClickHouseTypeCases()
    {
        // Scalar
        yield return new TestCaseData(IPAddress.Parse("127.0.0.1")).Returns("IPv4");
        yield return new TestCaseData(IPAddress.Parse("::1")).Returns("IPv6");

        // Array (non-empty)
        yield return new TestCaseData((object)new[] { IPAddress.Parse("127.0.0.1") }).Returns("Array(IPv4)");
        yield return new TestCaseData((object)new[] { IPAddress.Parse("::1") }).Returns("Array(IPv6)");

        // Array (empty — falls back to type-based default)
        yield return new TestCaseData((object)Array.Empty<IPAddress>()).Returns("Array(IPv4)");

        // List (non-empty)
        yield return new TestCaseData(new List<IPAddress> { IPAddress.Parse("127.0.0.1") }).Returns("Array(IPv4)");
        yield return new TestCaseData(new List<IPAddress> { IPAddress.Parse("::1") }).Returns("Array(IPv6)");

        // List (empty)
        yield return new TestCaseData(new List<IPAddress>()).Returns("Array(IPv4)");

        // Tuple
        yield return new TestCaseData(Tuple.Create("hello", IPAddress.Parse("::1"))).Returns("Tuple(String,IPv6)");
        yield return new TestCaseData(Tuple.Create(IPAddress.Parse("1.2.3.4"), IPAddress.Parse("::1"))).Returns("Tuple(IPv4,IPv6)");

        // Map (IP as value, non-empty)
        yield return new TestCaseData(new Dictionary<string, IPAddress> { ["k"] = IPAddress.Parse("127.0.0.1") }).Returns("Map(String, IPv4)");
        yield return new TestCaseData(new Dictionary<string, IPAddress> { ["k"] = IPAddress.Parse("::1") }).Returns("Map(String, IPv6)");

        // Map (IP as key, non-empty)
        yield return new TestCaseData(new Dictionary<IPAddress, string> { [IPAddress.Parse("127.0.0.1")] = "v" }).Returns("Map(IPv4, String)");
        yield return new TestCaseData(new Dictionary<IPAddress, string> { [IPAddress.Parse("::1")] = "v" }).Returns("Map(IPv6, String)");

        // Map (empty)
        yield return new TestCaseData(new Dictionary<string, IPAddress>()).Returns("Map(String, IPv4)");
        yield return new TestCaseData(new Dictionary<IPAddress, string>()).Returns("Map(IPv4, String)");

        // Nested
        yield return new TestCaseData((object)new IPAddress[][] { new[] { IPAddress.Parse("::1") } }).Returns("Array(Array(IPv6))");
        yield return new TestCaseData((object)new IPAddress[][][] { new[] { new[] { IPAddress.Parse("::1") } } }).Returns("Array(Array(Array(IPv6)))");

        // Multidimensional CLR arrays — rank propagates through nested ArrayType layers.
        yield return new TestCaseData((object)new byte[2, 3]).Returns("Array(Array(UInt8))");
        yield return new TestCaseData((object)new int[1, 1, 1]).Returns("Array(Array(Array(Int32)))");
        // Multidim with non-empty value-peek; an inner IPv6 propagates.
        var ipMatrix = new IPAddress[1, 1];
        ipMatrix[0, 0] = IPAddress.Parse("::1");
        yield return new TestCaseData((object)ipMatrix).Returns("Array(Array(IPv6))");
        // Multidim with empty length (first dim 0) — falls back to element-type inference.
        yield return new TestCaseData((object)new byte[0, 3]).Returns("Array(Array(UInt8))");

        // Collections with null first element (falls back to type-based default)
        yield return new TestCaseData((object)new[] { null, IPAddress.Parse("::1") }).Returns("Array(IPv4)");
        yield return new TestCaseData(new List<IPAddress> { null, IPAddress.Parse("::1") }).Returns("Array(IPv4)");

        // Tuple with null item (falls back to type-based inference for that item)
        yield return new TestCaseData(Tuple.Create((IPAddress)null, IPAddress.Parse("::1"))).Returns("Tuple(IPv4,IPv6)");

        // Map with null value (falls back to type-based inference for value)
        yield return new TestCaseData(new Dictionary<string, IPAddress> { ["k"] = null }).Returns("Map(String, IPv4)");

        // ValueTuple
        yield return new TestCaseData((object)ValueTuple.Create("hello", IPAddress.Parse("::1"))).Returns("Tuple(String,IPv6)");
        yield return new TestCaseData((object)ValueTuple.Create(IPAddress.Parse("1.2.3.4"), IPAddress.Parse("::1"))).Returns("Tuple(IPv4,IPv6)");

        // ValueTuple with null item (falls back to type-based inference for that item)
        yield return new TestCaseData((object)ValueTuple.Create((IPAddress)null, IPAddress.Parse("::1"))).Returns("Tuple(IPv4,IPv6)");

        // ValueTuple with >7 elements (flattening)
        yield return new TestCaseData((object)(1, 2, 3, 4, 5, 6, 7, "eight")).Returns("Tuple(Int32,Int32,Int32,Int32,Int32,Int32,Int32,String)");
        yield return new TestCaseData((object)(1, 2, 3, 4, 5, 6, 7, 8, "nine")).Returns("Tuple(Int32,Int32,Int32,Int32,Int32,Int32,Int32,Int32,String)");
    }

    [TestCaseSource(nameof(ValueToClickHouseTypeCases))]
    public string ShouldConvertValueToClickHouseType(object value) => TypeConverter.ToClickHouseType(value).ToString();

    private static IEnumerable<TestCaseData> NonZeroBoundMultidimCases()
    {
        // Rank 2, single non-zero lower bound
        var rank2OneAxis = Array.CreateInstance(typeof(int), new[] { 2, 3 }, new[] { 0, 10 });
        rank2OneAxis.SetValue(42, 0, 10);
        yield return new TestCaseData(rank2OneAxis).Returns("Array(Array(Int32))")
            .SetName("ToClickHouseType_NonZeroBoundRank2OneAxis_InfersNestedArrayType");

        // Rank 2, non-zero lower bound on both axes
        var rank2BothAxes = Array.CreateInstance(typeof(int), new[] { 2, 3 }, new[] { 5, 10 });
        rank2BothAxes.SetValue(42, 5, 10);
        yield return new TestCaseData(rank2BothAxes).Returns("Array(Array(Int32))")
            .SetName("ToClickHouseType_NonZeroBoundRank2BothAxes_InfersNestedArrayType");

        // Rank 3, non-zero lower bound on all axes
        var rank3 = Array.CreateInstance(typeof(byte), new[] { 2, 2, 2 }, new[] { 100, 200, 300 });
        rank3.SetValue((byte)1, 100, 200, 300);
        yield return new TestCaseData(rank3).Returns("Array(Array(Array(UInt8)))")
            .SetName("ToClickHouseType_NonZeroBoundRank3_InfersNestedArrayType");

        // Value-based propagation: inner IPv6 should still be picked up via the first-element peek
        var ipMatrix = (IPAddress[,])Array.CreateInstance(typeof(IPAddress), new[] { 1, 1 }, new[] { 5, 10 });
        ipMatrix.SetValue(IPAddress.Parse("::1"), 5, 10);
        yield return new TestCaseData(ipMatrix).Returns("Array(Array(IPv6))")
            .SetName("ToClickHouseType_NonZeroBoundIpMatrix_PropagatesIPv6FromFirstElement");
    }

    [TestCaseSource(nameof(NonZeroBoundMultidimCases))]
    public string ToClickHouseType_NonZeroBoundMultidimArray_InfersNestedArrayType(object value)
        => TypeConverter.ToClickHouseType(value).ToString();

    private static IEnumerable<TestCaseData> HttpParameterFormatterIpCases()
    {
        // Scalar IPv4/IPv6
        yield return new TestCaseData(IPAddress.Parse("192.168.1.1")).Returns("192.168.1.1");
        yield return new TestCaseData(IPAddress.Parse("::1")).Returns("::1");
        yield return new TestCaseData(IPAddress.Parse("2001:db8::1")).Returns("2001:db8::1");

        // Array of IPv4
        yield return new TestCaseData((object)new[] { IPAddress.Parse("10.0.0.1"), IPAddress.Parse("10.0.0.2") })
            .Returns("['10.0.0.1','10.0.0.2']");

        // Array of IPv6
        yield return new TestCaseData((object)new[] { IPAddress.Parse("::1"), IPAddress.Parse("::2") })
            .Returns("['::1','::2']");

        // Tuple with mixed IP types
        yield return new TestCaseData(Tuple.Create(IPAddress.Parse("10.0.0.1"), IPAddress.Parse("::1")))
            .Returns("('10.0.0.1','::1')");

        // Map with IP value
        yield return new TestCaseData(new Dictionary<string, IPAddress> { ["host"] = IPAddress.Parse("::1") })
            .Returns("{'host' : '::1'}");

        // Map with IP key
        yield return new TestCaseData(new Dictionary<IPAddress, string> { [IPAddress.Parse("::1")] = "loopback" })
            .Returns("{'::1' : 'loopback'}");

        // ValueTuple with mixed IP types
        yield return new TestCaseData((object)ValueTuple.Create(IPAddress.Parse("10.0.0.1"), IPAddress.Parse("::1")))
            .Returns("('10.0.0.1','::1')");
    }

    [TestCaseSource(nameof(HttpParameterFormatterIpCases))]
    public string ShouldFormatIpParameterViaHttpFormatter(object value)
    {
        var parameter = new ClickHouseDbParameter { ParameterName = "p", Value = value };
        var typeName = ParameterTypeResolution.ResolveTypeName(parameter, null, null);
        return HttpParameterFormatter.Format(parameter, typeName, TypeSettings.Default);
    }

    private static IEnumerable<TestCaseData> HttpParameterFormatterNestedArrayCases()
    {
        // Jagged 2D — int
        yield return new TestCaseData(
            (object)new int[][] { new[] { 1, 2 }, new[] { 3, 4 } },
            "Array(Array(Int32))").Returns("[[1,2],[3,4]]");
        // Multidim 2D — byte; must emit identical wire format
        yield return new TestCaseData(
            (object)new byte[,] { { 1, 2 }, { 3, 4 } },
            "Array(Array(UInt8))").Returns("[[1,2],[3,4]]");
        // Multidim 2D — int
        yield return new TestCaseData(
            (object)new int[,] { { 10, 20, 30 }, { 40, 50, 60 } },
            "Array(Array(Int32))").Returns("[[10,20,30],[40,50,60]]");
        // Jagged 3D
        yield return new TestCaseData(
            (object)new int[][][] { new int[][] { new[] { 1, 2 } } },
            "Array(Array(Array(Int32)))").Returns("[[[1,2]]]");
        // Multidim 3D
        yield return new TestCaseData(
            (object)new int[1, 1, 2] { { { 1, 2 } } },
            "Array(Array(Array(Int32)))").Returns("[[[1,2]]]");
        // Jagged strings — quoting and escaping inside nested array
        yield return new TestCaseData(
            (object)new string[][] { new[] { "a", "b'c" }, new[] { "d" } },
            "Array(Array(String))").Returns(@"[['a','b\'c'],['d']]");
        // Multidim strings (rectangular)
        yield return new TestCaseData(
            (object)new string[,] { { "a", "b" }, { "c", "d" } },
            "Array(Array(String))").Returns("[['a','b'],['c','d']]");
        // Ragged jagged — inner lengths differ; we must not require rectangularity
        yield return new TestCaseData(
            (object)new int[][] { new[] { 1, 2, 3 }, new[] { 4 } },
            "Array(Array(Int32))").Returns("[[1,2,3],[4]]");
        // Nullable inner — quote=true on the recursive call should emit `null` not `\N`
        yield return new TestCaseData(
            (object)new int?[][] { new int?[] { 1, null, 3 } },
            "Array(Array(Nullable(Int32)))").Returns("[[1,null,3]]");
        // Empty outer
        yield return new TestCaseData(
            (object)new int[0][],
            "Array(Array(Int32))").Returns("[]");
        // Outer with empty inner
        yield return new TestCaseData(
            (object)new int[][] { new int[0], new[] { 1 } },
            "Array(Array(Int32))").Returns("[[],[1]]");
        // Multidim with zero outer dim — emits empty brackets
        yield return new TestCaseData(
            (object)new int[0, 5],
            "Array(Array(Int32))").Returns("[]");
        // Multidim with zero inner dim — each outer row is an empty slice
        yield return new TestCaseData(
            (object)new int[3, 0],
            "Array(Array(Int32))").Returns("[[],[],[]]");
        // List<List<int>>
        yield return new TestCaseData(
            (object)new List<List<int>> { new() { 1, 2 }, new() { 3 } },
            "Array(Array(Int32))").Returns("[[1,2],[3]]");
        // List<int[]> — mixed List + array
        yield return new TestCaseData(
            (object)new List<int[]> { new[] { 1, 2 }, new[] { 3 } },
            "Array(Array(Int32))").Returns("[[1,2],[3]]");
    }

    [TestCaseSource(nameof(HttpParameterFormatterNestedArrayCases))]
    public string ShouldFormatNestedArrayParameterViaHttpFormatter(object value, string typeName)
    {
        var parameter = new ClickHouseDbParameter { ParameterName = "p", Value = value };
        return HttpParameterFormatter.Format(parameter, typeName, TypeSettings.Default);
    }

    [Test]
    public void HttpParameterFormatter_ScalarPassedToNestedArrayType_ThrowsArgumentExceptionMentioningParameterAndOuterType()
    {
        var parameter = new ClickHouseDbParameter { ParameterName = "m_value", Value = (byte)219 };
        var ex = Assert.Throws<ArgumentException>(
            () => HttpParameterFormatter.Format(parameter, "Array(Array(UInt8))", TypeSettings.Default));
        Assert.That(ex!.Message, Does.Contain("m_value"));
        // Must include the full outer type, not just the leaf where recursion bottomed out.
        Assert.That(ex.Message, Does.Contain("Array(Array(UInt8))"));
    }

    [Test]
    public void HttpParameterFormatter_FlatArrayPassedToNestedArrayType_ThrowsArgumentExceptionMentioningParameterAndOuterType()
    {
        // A single-level int[] cannot satisfy Array(Array(Int32)); each element is a scalar where an array is expected.
        var parameter = new ClickHouseDbParameter { ParameterName = "p", Value = new[] { 1, 2, 3 } };
        var ex = Assert.Throws<ArgumentException>(
            () => HttpParameterFormatter.Format(parameter, "Array(Array(Int32))", TypeSettings.Default));
        Assert.That(ex!.Message, Does.Contain("'p'"));
        // Outer type must be preserved even though recursion bottoms out on the inner Array(Int32).
        Assert.That(ex.Message, Does.Contain("Array(Array(Int32))"));
    }

    [Test]
    public void HttpParameterFormatter_ScalarPassedToScalarMismatchedType_PreservesParameterAndType()
    {
        // Sanity: non-nested mismatch must still produce a useful message.
        var parameter = new ClickHouseDbParameter { ParameterName = "x", Value = new object() };
        var ex = Assert.Throws<ArgumentException>(
            () => HttpParameterFormatter.Format(parameter, "Array(Int32)", TypeSettings.Default));
        Assert.That(ex!.Message, Does.Contain("'x'"));
        Assert.That(ex.Message, Does.Contain("Array(Int32)"));
    }

    [Test]
    public void ShouldInferCorrectQueryFormForIPv4Parameter()
    {
        var parameter = new ClickHouseDbParameter { ParameterName = "addr", Value = IPAddress.Parse("10.0.0.1") };
        Assert.That(parameter.QueryForm, Is.EqualTo("{addr:IPv4}"));
    }

    [Test]
    public void ShouldInferCorrectQueryFormForIPv6Parameter()
    {
        var parameter = new ClickHouseDbParameter { ParameterName = "addr", Value = IPAddress.Parse("::1") };
        Assert.That(parameter.QueryForm, Is.EqualTo("{addr:IPv6}"));
    }

    [Test, Explicit]
    public void ShouldConvertClickHouseType()
    {
        using var connection = TestUtilities.GetTestClickHouseConnection();
        var types = connection.Query<string>("SELECT name FROM system.data_type_families").ToList();
        var exceptions = new List<Exception>();
        foreach (var type in types)
        {
            try
            {
                TypeConverter.ParseClickHouseType(type, TypeSettings.Default);
            }
            catch (ArgumentOutOfRangeException)
            {
            }
            catch (Exception e)
            {
                exceptions.Add(e);
            }
        }
        ClassicAssert.IsEmpty(exceptions);
    }
}
