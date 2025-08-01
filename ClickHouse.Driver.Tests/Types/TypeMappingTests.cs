﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
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

    [TestCase("Nullable(UInt32)", ExpectedResult = typeof(uint?))]
    [TestCase("Array(Array(String))", ExpectedResult = typeof(string[][]))]
    [TestCase("Array(Nullable(UInt32))", ExpectedResult = typeof(uint?[]))]
    [TestCase("SimpleAggregateFunction(anyLast,Nullable(UInt32))", ExpectedResult = typeof(uint?))]
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

    [TestCase(typeof(IPAddress), ExpectedResult = "IPv4")]
    [TestCase(typeof(Guid), ExpectedResult = "UUID")]

    [TestCase(typeof(uint?), ExpectedResult = "Nullable(UInt32)")]
    [TestCase(typeof(uint?[]), ExpectedResult = "Array(Nullable(UInt32))")]
    [TestCase(typeof(string[][]), ExpectedResult = "Array(Array(String))")]
#if NET6_0_OR_GREATER
    [TestCase(typeof(DateOnly), ExpectedResult = "Date")]
#endif
    [TestCase(typeof(Tuple<int, byte, float?, string[]>), ExpectedResult = "Tuple(Int32,UInt8,Nullable(Float32),Array(String))")]
    public string ShouldConvertToClickHouseType(Type type) => TypeConverter.ToClickHouseType(type).ToString();

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
