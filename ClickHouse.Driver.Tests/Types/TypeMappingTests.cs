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
    [TestCase(typeof(Dictionary<string,int>), ExpectedResult = "Map(String, Int32)")]
    [TestCase(typeof(Dictionary<Tuple<int,int>,int>), ExpectedResult = "Map(Tuple(Int32,Int32), Int32)")]
    [TestCase(typeof(List<string>), ExpectedResult = "Array(String)")]
    [TestCase(typeof(List<List<string>>), ExpectedResult = "Array(Array(String))")]
#if NET6_0_OR_GREATER
    [TestCase(typeof(DateOnly), ExpectedResult = "Date")]
#endif
    [TestCase(typeof(Tuple<int, byte, float?, string[]>), ExpectedResult = "Tuple(Int32,UInt8,Nullable(Float32),Array(String))")]
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

        // Collections with null first element (falls back to type-based default)
        yield return new TestCaseData((object)new[] { null, IPAddress.Parse("::1") }).Returns("Array(IPv4)");
        yield return new TestCaseData(new List<IPAddress> { null, IPAddress.Parse("::1") }).Returns("Array(IPv4)");

        // Tuple with null item (falls back to type-based inference for that item)
        yield return new TestCaseData(Tuple.Create((IPAddress)null, IPAddress.Parse("::1"))).Returns("Tuple(IPv4,IPv6)");

        // Map with null value (falls back to type-based inference for value)
        yield return new TestCaseData(new Dictionary<string, IPAddress> { ["k"] = null }).Returns("Map(String, IPv4)");
    }

    [TestCaseSource(nameof(ValueToClickHouseTypeCases))]
    public string ShouldConvertValueToClickHouseType(object value) => TypeConverter.ToClickHouseType(value).ToString();

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
    }

    [TestCaseSource(nameof(HttpParameterFormatterIpCases))]
    public string ShouldFormatIpParameterViaHttpFormatter(object value)
    {
        var parameter = new ClickHouseDbParameter { ParameterName = "p", Value = value };
        var typeName = ParameterTypeResolution.ResolveTypeName(parameter, null, null);
        return HttpParameterFormatter.Format(parameter, typeName, TypeSettings.Default);
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
