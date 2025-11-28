using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Threading.Tasks;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Numerics;
using ClickHouse.Driver.Utility;
using System.Text.Json.Nodes;
using NUnit.Framework.Constraints;

namespace ClickHouse.Driver.Tests;

public static class TestUtilities
{
    public static readonly Feature SupportedFeatures;
    public static readonly Version ServerVersion;

    static TestUtilities()
    {
        var versionString = Environment.GetEnvironmentVariable("CLICKHOUSE_VERSION");
        if (versionString is not null and not "latest" and not "head")
        {
            ServerVersion = Version.Parse(versionString.Split(':').Last().Trim());
            SupportedFeatures = ClickHouseFeatureMap.GetFeatureFlags(ServerVersion);
        }
        else
        {
            SupportedFeatures = Feature.All;
            ServerVersion = null;
        }
    }
    
    /// <summary>
    /// Equality assertion with special handling for certain object types
    /// </summary>
    /// <param name="expected"></param>
    /// <param name="result"></param>
    public static void AssertEqual(object expected, object result)
    {
        if (expected is JsonNode)
        {
            // Necessary because the ordering of the fields is not guaranteed to be the same
            Assert.That(result, Is.EqualTo(expected).Using<JsonObject,JsonObject>(JsonNode.DeepEquals));
        }
        else
        {
            Assert.That(result, Is.EqualTo(expected).UsingPropertiesComparer());
        }
    }

    [Test]
    public static async Task ExpectedFeaturesShouldMatchActualFeatures()
    {
        using var connection = GetTestClickHouseConnection();
        await connection.OpenAsync();
        Assert.That(connection.SupportedFeatures, Is.EqualTo(SupportedFeatures & connection.SupportedFeatures));
    }

    /// <summary>
    /// Utility method to allow to redirect ClickHouse connections to different machine, in case of Windows development environment
    /// </summary>
    /// <returns></returns>
    public static ClickHouseConnection GetTestClickHouseConnection(bool compression = true, bool session = false, bool customDecimals = true, string password = null, bool useFormDataParameters = false)
    {
        var builder = GetConnectionStringBuilder();
        builder.Compression = compression;
        builder.UseSession = session;
        builder.UseCustomDecimals = customDecimals;
        
        if (password is not null)
        {
            builder.Password = password;
        }
        builder["set_session_timeout"] = 1; // Expire sessions quickly after test
        builder["set_allow_experimental_geo_types"] = 1; // Allow support for geo types
        builder["set_flatten_nested"] = 0; // Nested should be a single column, see https://clickhouse.com/docs/en/operations/settings/settings#flatten-nested

        if (SupportedFeatures.HasFlag(Feature.Map))
        {
            builder["set_allow_experimental_map_type"] = 1;
        }
        if (SupportedFeatures.HasFlag(Feature.Variant))
        {
            builder["set_allow_experimental_variant_type"] = 1;
        }
        if (SupportedFeatures.HasFlag(Feature.Json))
        {
            builder["set_allow_experimental_json_type"] = 1;
        }
        if (SupportedFeatures.HasFlag(Feature.Dynamic))
        {
            builder["set_allow_experimental_dynamic_type"] = 1;
        }
        if (SupportedFeatures.HasFlag(Feature.Time))
        {
            builder["set_enable_time_time64_type"] = 1;
        }

        var settings = new ClickHouseClientSettings(builder)
        {
            UseFormDataParameters = useFormDataParameters
        };
        
        var connection = new ClickHouseConnection(settings);
        connection.Open();
        return connection;
    }

    public static ClickHouseConnectionStringBuilder GetConnectionStringBuilder()
    {
        // Connection string must be provided pointing to a test ClickHouse server
        var devConnectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION") ??
            throw new InvalidOperationException("Must set CLICKHOUSE_CONNECTION environment variable pointing at ClickHouse server");

        return new ClickHouseConnectionStringBuilder(devConnectionString);
    }

    public readonly struct DataTypeSample(string clickHouseType, Type frameworkType, string exampleExpression, object exampleValue)
    {
        public readonly string ClickHouseType = clickHouseType;
        public readonly Type FrameworkType = frameworkType;
        public readonly string ExampleExpression = exampleExpression;
        public readonly object ExampleValue = exampleValue;
    }

    /// <summary>
    /// Helper to generate composite type test cases from base type samples
    /// </summary>
    private static IEnumerable<DataTypeSample> GenerateCompositeTypeSamples(DataTypeSample baseSample)
    {
        var baseType = baseSample.ClickHouseType;
        var baseExpr = baseSample.ExampleExpression;
        var baseValue = baseSample.ExampleValue;

        // Array
        var arrayValue = Array.CreateInstance(baseSample.FrameworkType, 2);

        arrayValue.SetValue(baseValue, 0);
        arrayValue.SetValue(baseValue, 1);
        yield return new DataTypeSample(
            $"Array({baseType})",
            arrayValue.GetType(),
            $"array({baseExpr}, {baseExpr})",
            arrayValue
        );

        // Nullable (skip if value is already DBNull or if type is not a value type)
        if (baseValue is not DBNull && baseSample.FrameworkType.IsValueType)
        {
            var nullableType = typeof(Nullable<>).MakeGenericType(baseSample.FrameworkType);
            var nullableValue = Activator.CreateInstance(nullableType, baseValue);

            yield return new DataTypeSample(
                $"Nullable({baseType})",
                nullableType,
                baseExpr,
                nullableValue
            );
        }

        // Tuple with base type and String
        var tupleType = typeof(Tuple<,>).MakeGenericType(baseSample.FrameworkType, typeof(string));
        var tupleValue = Activator.CreateInstance(tupleType, baseValue, "test");
        yield return new DataTypeSample(
            $"Tuple({baseType}, String)",
            tupleType,
            $"tuple({baseExpr}, 'test')",
            tupleValue
        );

        // Map(String, baseType) - String as key
        var mapType = typeof(Dictionary<,>).MakeGenericType(typeof(string), baseSample.FrameworkType);
        var mapValue = Activator.CreateInstance(mapType);
        var addMethod = mapType.GetMethod("Add");
        addMethod.Invoke(mapValue, new object[] { "key", baseValue });
        yield return new DataTypeSample(
            $"Map(String, {baseType})",
            mapType,
            $"map('key', {baseExpr})",
            mapValue
        );

        // Variant
        // Some types have problems with parsing on the server side when it comes to variants
        // This should be fixed with https://github.com/ClickHouse/ClickHouse/pull/90430
        string[] noVariantTests = new[]
        {
            "Int32",
            "UInt32",
            "Int64",
            "UInt64",
            "Date",
            "Date32",
            "DateTime",
            "DateTime64",
            "Float32",
            "Bool",
            "BFloat16",
        };
        if (!noVariantTests.Contains(baseType) && !baseType.StartsWith("Enum"))
        {
            var variantSecondType = "String";
            // Some types can cause a database error due to suspicious variant types/wrong type inference, avoid that
            if (baseType.StartsWith("Enum") || baseType.StartsWith("FixedString") || baseType == "BFloat16" || baseType.StartsWith("Time"))
            {
                variantSecondType = "Date";
            }

            yield return new DataTypeSample(
                $"Variant({baseType}, {variantSecondType})",
                typeof(object),
                $"{baseExpr}::Variant({baseType}, {variantSecondType})",
                baseValue
            );
        }
    }

    public static IEnumerable<DataTypeSample> GetDataTypeSamples()
    {
        yield return new DataTypeSample("Nothing", typeof(DBNull), "NULL", DBNull.Value);

        yield return new DataTypeSample("Int8", typeof(sbyte), "toInt8(-8)", -8);
        yield return new DataTypeSample("UInt8", typeof(byte), "toUInt8(8)", 8);

        yield return new DataTypeSample("Int16", typeof(short), "toInt16(-16)", -16);
        yield return new DataTypeSample("UInt16", typeof(ushort), "toUInt16(16)", 16);

        yield return new DataTypeSample("Int32", typeof(int), "toInt16(-32)", -32);
        yield return new DataTypeSample("UInt32", typeof(uint), "toUInt16(32)", 32);

        yield return new DataTypeSample("Int64", typeof(long), "toInt64(-64)", -64);
        yield return new DataTypeSample("UInt64", typeof(ulong), "toUInt64(64)", 64);

        yield return new DataTypeSample("Float32", typeof(float), "toFloat32(32e6)", 32e6);
        yield return new DataTypeSample("Float32", typeof(float), "toFloat32(-32e6)", -32e6);
        
        yield return new DataTypeSample("Float32", typeof(float), "toFloat32(1.1)", 1.1f);
        yield return new DataTypeSample("Float32", typeof(float), "toFloat32(-1.1)", -1.1f);

        yield return new DataTypeSample("Float64", typeof(double), "toFloat64(64e6)", 64e6);
        yield return new DataTypeSample("Float64", typeof(double), "toFloat64(-64e6)", -64e6);

        yield return new DataTypeSample("BFloat16", typeof(float), "toBFloat16(3.14)", 3.125f);
        yield return new DataTypeSample("BFloat16", typeof(float), "toBFloat16(-2.5)", -2.5f);

        yield return new DataTypeSample("String", typeof(string), "'TestString'", "TestString");
        yield return new DataTypeSample("String", typeof(string), "'\t\r\n'", "\t\r\n");

        yield return new DataTypeSample("String", typeof(string), "'Добрый день'", "Добрый день");
        yield return new DataTypeSample("String", typeof(string), "'¿Qué tal?'", "¿Qué tal?");
        yield return new DataTypeSample("String", typeof(string), "'你好'", "你好");
        yield return new DataTypeSample("String", typeof(string), "'こんにちは'", "こんにちは");
        yield return new DataTypeSample("String", typeof(string), "'⌬⏣'", "⌬⏣");
        yield return new DataTypeSample("String", typeof(string), "'Çay'", "Çay");
        yield return new DataTypeSample("String", typeof(string), "'お茶'", "お茶");

        // yield return new DataTypeSample("String", typeof(string), "'1\t2\n3'", "1\t2\n3");
        yield return new DataTypeSample("FixedString(3)", typeof(string), "toFixedString('ASD',3)", "ASD");
        yield return new DataTypeSample("FixedString(5)", typeof(string), "toFixedString('ASD',5)", "ASD\0\0");

        yield return new DataTypeSample("UUID", typeof(Guid), "toUUID('00000000-0000-0000-0000-000000000000')", new Guid("00000000-0000-0000-0000-000000000000"));
        yield return new DataTypeSample("UUID", typeof(Guid), "toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')", new Guid("61f0c404-5cb3-11e7-907b-a6006ad3dba0"));

        yield return new DataTypeSample("IPv4", typeof(IPAddress), "toIPv4('1.2.3.4')", IPAddress.Parse("1.2.3.4"));
        yield return new DataTypeSample("IPv4", typeof(IPAddress), "toIPv4('255.255.255.255')", IPAddress.Parse("255.255.255.255"));

        yield return new DataTypeSample("Enum('a' = 1, 'b' = 2)", typeof(string), "CAST('a', 'Enum(\\'a\\' = 1, \\'b\\' = 2)')", "a");
        yield return new DataTypeSample("Enum8('a' = -1, 'b' = 127)", typeof(string), "CAST('a', 'Enum8(\\'a\\' = -1, \\'b\\' = 127)')", "a");
        yield return new DataTypeSample("Enum16('a' = -32768, 'b' = 32767)", typeof(string), "CAST('a', 'Enum16(\\'a\\' = -32768, \\'b\\' = 32767)')", "a");

        yield return new DataTypeSample("Array(Int32)", typeof(int[]), "array(1, 2, 3)", new[] { 1, 2, 3 });
        yield return new DataTypeSample("Array(String)", typeof(int[]), "array('a', 'b', 'c')", new[] { "a", "b", "c" });
        yield return new DataTypeSample("Array(Nullable(Int32))", typeof(int?[]), "array(1, 2, NULL)", new int?[] { 1, 2, null });

        yield return new DataTypeSample("Nullable(Int32)", typeof(int?), "toInt32OrNull('123')", 123);
        yield return new DataTypeSample("Nullable(Int32)", typeof(int?), "toInt32OrNull(NULL)", DBNull.Value);
        yield return new DataTypeSample("Nullable(String)", typeof(string), "CAST(NULL as Nullable(String))", DBNull.Value);
        yield return new DataTypeSample("Nullable(DateTime)", typeof(int?), "CAST(NULL AS Nullable(DateTime))", DBNull.Value);

        yield return new DataTypeSample("LowCardinality(Nullable(String))", typeof(string), "CAST(NULL AS LowCardinality(Nullable(String)))", DBNull.Value);
        yield return new DataTypeSample("LowCardinality(String)", typeof(string), "toLowCardinality('lowcardinality')", "lowcardinality");

        yield return new DataTypeSample("Tuple(Int8, String, Nullable(Int8))", typeof(Tuple<int, string, int?>), "tuple(1, 'a', 8)", Tuple.Create<int, string, int?>(1, "a", 8));
        yield return new DataTypeSample("Tuple(Int32, Tuple(UInt8, String, Nullable(Int32)))", typeof(Tuple<int, Tuple<byte, string, int?>>), "tuple(123, tuple(5, 'a', 7))", Tuple.Create(123, Tuple.Create((byte)5, "a", 7)));
        yield return new DataTypeSample("Tuple(a Int32, b Int32)", typeof(Tuple<int, ushort>), "tuple(123, 456)", Tuple.Create(123, 456));

        yield return new DataTypeSample("Date", typeof(DateTime), "toDateOrNull('1999-11-12')", new DateTime(1999, 11, 12, 0, 0, 0, DateTimeKind.Unspecified));
        yield return new DataTypeSample("DateTime('UTC')", typeof(DateTime), "toDateTime('1988-08-28 11:22:33', 'UTC')", new DateTime(1988, 08, 28, 11, 22, 33, DateTimeKind.Unspecified));
        yield return new DataTypeSample("DateTime('Pacific/Fiji')", typeof(DateTime), "toDateTime('1999-01-01 13:00:00', 'Pacific/Fiji')", new DateTime(1999, 01, 01, 13, 00, 00, DateTimeKind.Unspecified));

        yield return new DataTypeSample("DateTime64(4, 'UTC')", typeof(DateTime), "toDateTime64('2043-03-01 18:34:04.4444', 9, 'UTC')", new DateTime(644444444444444000, DateTimeKind.Utc));
        yield return new DataTypeSample("DateTime64(7, 'UTC')", typeof(DateTime), "toDateTime64('2043-03-01 18:34:04.4444444', 9, 'UTC')", new DateTime(644444444444444444, DateTimeKind.Utc));
        yield return new DataTypeSample("DateTime64(7, 'Pacific/Fiji')", typeof(DateTime), "toDateTime64('2043-03-01 18:34:04.4444444', 9, 'Pacific/Fiji')", new DateTime(644444444444444444, DateTimeKind.Unspecified));

        yield return new DataTypeSample("Decimal32(3)", typeof(ClickHouseDecimal), "toDecimal32(123.45, 3)", new ClickHouseDecimal(123.450m));
        yield return new DataTypeSample("Decimal32(3)", typeof(ClickHouseDecimal), "toDecimal32(-123.45, 3)", new ClickHouseDecimal(-123.450m));

        yield return new DataTypeSample("Decimal64(7)", typeof(ClickHouseDecimal), "toDecimal64(1.2345, 7)", new ClickHouseDecimal(1.2345000m));
        yield return new DataTypeSample("Decimal64(7)", typeof(ClickHouseDecimal), "toDecimal64(-1.2345, 7)", new ClickHouseDecimal(-1.2345000m));

        yield return new DataTypeSample("Decimal128(9)", typeof(ClickHouseDecimal), "toDecimal128(12.34, 9)", new ClickHouseDecimal(12.340000000m));
        yield return new DataTypeSample("Decimal128(9)", typeof(ClickHouseDecimal), "toDecimal128(-12.34, 9)", new ClickHouseDecimal(-12.340000000m));

        yield return new DataTypeSample("Decimal128(25)", typeof(ClickHouseDecimal), "toDecimal128(1e-24, 25)", new ClickHouseDecimal(10e-25m));
        yield return new DataTypeSample("Decimal128(0)", typeof(ClickHouseDecimal), "toDecimal128(repeat('1', 30), 0)", ClickHouseDecimal.Parse(new string('1', 30)));

        yield return new DataTypeSample("Decimal128(30)", typeof(ClickHouseDecimal), "toDecimal128(1, 30)", new ClickHouseDecimal(BigInteger.Pow(10, 30), 30));

        yield return new DataTypeSample("Nested(Id int, Comment String)", typeof(Tuple<int, string>[]), "CAST([(1, 'a')], 'Nested(Id int, Comment String)')", new[] { Tuple.Create(1, "a") });

        if (SupportedFeatures.HasFlag(Feature.WideTypes))
        {
            yield return new DataTypeSample("Decimal256(25)", typeof(ClickHouseDecimal), "toDecimal256(1e-24, 25)", new ClickHouseDecimal(10e-25m));
            yield return new DataTypeSample("Decimal256(0)", typeof(ClickHouseDecimal),"toDecimal256(repeat('1', 50), 0)", ClickHouseDecimal.Parse(new string('1', 50)));
            yield return new DataTypeSample("DateTime32('UTC')", typeof(DateTime), "toDateTime('1988-08-28 11:22:33', 'UTC')", new DateTime(1988, 08, 28, 11, 22, 33, DateTimeKind.Unspecified));
        }

        yield return new DataTypeSample("IPv6", typeof(IPAddress), "toIPv6('2001:0db8:85a3:0000:0000:8a2e:0370:7334')", IPAddress.Parse("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));

        if (SupportedFeatures.HasFlag(Feature.Map))
        {
            yield return new DataTypeSample("Map(String, UInt8)", typeof(Dictionary<string, byte>), "map('A',1,'B',2)", new Dictionary<string, byte> { { "A", 1 }, { "B", 2 } });
            yield return new DataTypeSample("Map(UInt8, String)", typeof(Dictionary<byte, string>), "map(1,'A',2,'B')", new Dictionary<byte, string> { { 1, "A" }, { 2, "B" } });

            yield return new DataTypeSample("Map(String, Nullable(UInt8))",
                typeof(Dictionary<string, byte?>),
                "map('five', 5, 'null', NULL)",
                new Dictionary<string, byte?> {
                    { "five", 5 },
                    { "null", null },
                });
        }

        if (SupportedFeatures.HasFlag(Feature.Bool))
        {
            yield return new DataTypeSample("Bool", typeof(bool), "CAST(1, 'Bool')", true);
        }

        if (SupportedFeatures.HasFlag(Feature.Date32))
        {
            yield return new DataTypeSample("Date32", typeof(DateTime), "toDate32('2001-02-03')", new DateTime(2001, 02, 03));
            yield return new DataTypeSample("Date32", typeof(DateTime), "toDate32('1925-01-02')", new DateTime(1925, 01, 02));
        }

        if (SupportedFeatures.HasFlag(Feature.WideTypes))
        {
            yield return new DataTypeSample("Int128", typeof(BigInteger), "toInt128(concat('-1', repeat('0', 30)))", -BigInteger.Pow(new BigInteger(10), 30));
            yield return new DataTypeSample("Int128", typeof(BigInteger), "toInt128('170141183460469231731687303715884105727')", BigInteger.Parse("170141183460469231731687303715884105727"));
            yield return new DataTypeSample("Int128", typeof(BigInteger), "toInt128('-170141183460469231731687303715884105728')", BigInteger.Parse("-170141183460469231731687303715884105728"));

            yield return new DataTypeSample("UInt128", typeof(BigInteger), "toInt128(concat('1', repeat('0', 30)))", BigInteger.Pow(new BigInteger(10), 30));
            yield return new DataTypeSample("UInt128", typeof(BigInteger), "toUInt128('340282366920938463463374607431768211455')", BigInteger.Parse("340282366920938463463374607431768211455"));

            yield return new DataTypeSample("Int256", typeof(BigInteger), "toInt256(concat('-1', repeat('0', 50)))", -BigInteger.Pow(new BigInteger(10), 50));
            yield return new DataTypeSample("UInt256", typeof(BigInteger), "toInt256(concat('1', repeat('0', 50)))", BigInteger.Pow(new BigInteger(10), 50));
        }

        yield return new DataTypeSample("Point", typeof(Tuple<double, double>), "(10,20)", Tuple.Create(10.0, 20.0));
        yield return new DataTypeSample("Ring", typeof(Tuple<double, double>[]), "[(0.1,0.2), (0.2,0.3), (0.3,0.4)]", new[] {
            Tuple.Create(.1, .2),
            Tuple.Create(.2, .3),
            Tuple.Create(.3, .4)
        });

        if (SupportedFeatures.HasFlag(Feature.Variant))
        {
            yield return new DataTypeSample("Variant(UInt64, String, Array(UInt64))", typeof(string), "'Hello, World!'::Variant(UInt64, String, Array(UInt64))", "Hello, World!");
        }

        if (SupportedFeatures.HasFlag(Feature.Json))
        {
            // TODO: properly test nulls as ClickHouse eats them
            var jsonExamples = new[]
            {
                "{}",
                //"{\"val\": null}",
                "{\"val\": \"string\"}",
                "{\"val\": 1}",
                "{\"val\": 1.5}",
                "{\"val\": [1,2]}",
                "{ \"nested\": { \"double\": 1.25, \"int\": 123456, \"string\": \"stringValue\", \"int2\": 54321 } }",
                "{ \"nestedArray\": [{\"val\": 1}, {\"val\": 2}] }",
            };

            foreach (var example in jsonExamples)
                yield return new DataTypeSample("Json", typeof(string), $"'{example}'::Json", (JsonObject)JsonNode.Parse(example));
        }

        if (SupportedFeatures.HasFlag(Feature.Time))
        {
            yield return new DataTypeSample("Time", typeof(TimeSpan), "'5:25:05'::Time", new TimeSpan(5, 25, 5));
            yield return new DataTypeSample("Time", typeof(TimeSpan), "'-5:25:05'::Time", new TimeSpan(5, 25, 5).Negate());
            yield return new DataTypeSample("Time", typeof(TimeSpan), "'55:25:05'::Time", new TimeSpan(55, 25, 5));

            yield return new DataTypeSample("Time64(1)", typeof(TimeSpan), "'5:25:05.0'::Time64(1)", new TimeSpan(5, 25, 5));
            yield return new DataTypeSample("Time64(3)", typeof(TimeSpan), "'55:25:05.123'::Time64(3)", new TimeSpan(55, 25, 5).Add(TimeSpan.FromMilliseconds(123)));
            yield return new DataTypeSample("Time64(6)", typeof(TimeSpan), "'5:25:05.123456'::Time64(6)", new TimeSpan(5, 25, 5).Add(TimeSpan.FromMilliseconds(123.456)));
            yield return new DataTypeSample("Time64(6)", typeof(TimeSpan), "'-5:25:05.123456'::Time64(6)", (new TimeSpan(5, 25, 5).Add(TimeSpan.FromMilliseconds(123.456)).Negate()));
        }

        // Generate composite type tests for ALL base types that FromByteCode supports
        // This ensures that all type decoders work correctly in composite contexts (Array, Nullable, Tuple, Map, Variant)
        var baseTypesToTest = new List<DataTypeSample>
        {
            // 0x01: UInt8
            new DataTypeSample("UInt8", typeof(byte), "toUInt8(42)", (byte)42),
            // 0x02: UInt16
            new DataTypeSample("UInt16", typeof(ushort), "toUInt16(1234)", (ushort)1234),
            // 0x03: UInt32
            new DataTypeSample("UInt32", typeof(uint), "toUInt32(12345)", (uint)12345),
            // 0x04: UInt64
            new DataTypeSample("UInt64", typeof(ulong), "toUInt64(123456)", (ulong)123456),
            // 0x07: Int8
            new DataTypeSample("Int8", typeof(sbyte), "toInt8(-42)", (sbyte)-42),
            // 0x08: Int16
            new DataTypeSample("Int16", typeof(short), "toInt16(-1234)", (short)-1234),
            // 0x09: Int32
            new DataTypeSample("Int32", typeof(int), "toInt32(-12345)", -12345),
            // 0x0A: Int64
            new DataTypeSample("Int64", typeof(long), "toInt64(-123456)", (long)-123456),
            // 0x0D: Float32
            new DataTypeSample("Float32", typeof(float), "toFloat32(3.14)", 3.14f),
            // 0x0E: Float64
            new DataTypeSample("Float64", typeof(double), "toFloat64(3.14159)", 3.14159),
            // 0x31: BFloat16
            new DataTypeSample("BFloat16", typeof(float), "toBFloat16(1.25)", 1.25f),
            // 0x0F: Date
            // 0x10: Date32
            // 0x11: DateTime (UTC)
            //new DataTypeSample("Date", typeof(DateTime), "toDate('2024-01-15')", new DateTime(2024, 1, 15)),
            //new DataTypeSample("DateTime('UTC')", typeof(DateTime), "toDateTime('2024-01-15 10:30:00', 'UTC')", new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Unspecified)),
            //baseTypesToTest.Add(new DataTypeSample("Date32", typeof(DateTime), "toDate32('2024-01-15')", new DateTime(2024, 1, 15)));
            // No dates or datetimes here because automatic type mapping tests are problematic
            // 0x15: String
            new DataTypeSample("String", typeof(string), "'test'", "test"),
            // 0x16: FixedString
            new DataTypeSample("FixedString(4)", typeof(string), "toFixedString('test', 4)", "test"),
            // 0x17: Enum8
            new DataTypeSample("Enum8('a' = 1, 'b' = 2)", typeof(string), "CAST('a', 'Enum8(\\'a\\' = 1, \\'b\\' = 2)')", "a"),
            // 0x18: Enum16
            new DataTypeSample("Enum16('x' = 100, 'y' = 200)", typeof(string), "CAST('x', 'Enum16(\\'x\\' = 100, \\'y\\' = 200)')", "x"),
            // 0x19: Decimal32
            new DataTypeSample("Decimal32(3)", typeof(ClickHouseDecimal), "toDecimal32(123.45, 3)", new ClickHouseDecimal(123.450m)),
            // 0x1A: Decimal64
            new DataTypeSample("Decimal64(5)", typeof(ClickHouseDecimal), "toDecimal64(12.345, 5)", new ClickHouseDecimal(12.34500m)),
            // 0x1B: Decimal128
            new DataTypeSample("Decimal128(5)", typeof(ClickHouseDecimal), "toDecimal128(12.34, 5)", new ClickHouseDecimal(12.34000m)),
            // 0x1D: UUID
            new DataTypeSample("UUID", typeof(Guid), "toUUID('12345678-1234-1234-1234-123456789abc')", Guid.Parse("12345678-1234-1234-1234-123456789abc")),
            // 0x28: IPv4
            new DataTypeSample("IPv4", typeof(IPAddress), "toIPv4('192.168.1.1')", IPAddress.Parse("192.168.1.1")),
            // 0x29: IPv6
            new DataTypeSample("IPv6", typeof(IPAddress), "toIPv6('::1')", IPAddress.Parse("::1")),
            
            // After https://github.com/ClickHouse/ClickHouse/pull/90430 is released, change to Int32,Int32. Issue is parameter inferred to be wrong type.
            // Unnamed tuple
            new DataTypeSample("Tuple(Int32, Int32)", typeof(Tuple<int,int>), "tuple(35455,35456)::Tuple(Int32, Int32)", new Tuple<int, int>(35455, 35456)),
            // Named tuple - the type inference for this as a parameter doesn't work, breaks a lot of tests
            //new DataTypeSample("Tuple(a Int32, b Int32)", typeof(Tuple<int,int>), "tuple(35455,35456)::Tuple(a Int32, b Int32)", new Tuple<int, int>(35455, 35456)),
        };

        // Feature-gated types
        if (SupportedFeatures.HasFlag(Feature.Bool))
        {
            // 0x2D: Bool
            baseTypesToTest.Add(new DataTypeSample("Bool", typeof(bool), "CAST(1, 'Bool')", true));
        }


        if (SupportedFeatures.HasFlag(Feature.WideTypes))
        {
            // 0x05: UInt128
            baseTypesToTest.Add(new DataTypeSample("UInt128", typeof(BigInteger), "toUInt128(123456)", BigInteger.Parse("123456")));
            // 0x06: UInt256
            baseTypesToTest.Add(new DataTypeSample("UInt256", typeof(BigInteger), "toUInt256(42)", new BigInteger(42)));
            // 0x0B: Int128
            baseTypesToTest.Add(new DataTypeSample("Int128", typeof(BigInteger), "toInt128(123456)", BigInteger.Parse("123456")));
            // 0x0C: Int256
            baseTypesToTest.Add(new DataTypeSample("Int256", typeof(BigInteger), "toInt256(42)", new BigInteger(42)));
        }

        if (SupportedFeatures.HasFlag(Feature.Time))
        {
            // Similarly to Date items above, the parsing is problematic on the server side for these atm
            //baseTypesToTest.Add(new DataTypeSample("Time", typeof(TimeSpan), "'5:25:05'::Time", new TimeSpan(5, 25, 5)));
            //baseTypesToTest.Add(new DataTypeSample("Time64(3)", typeof(TimeSpan), "'5:25:05.123'::Time64(3)", new TimeSpan(5, 25, 5).Add(TimeSpan.FromMilliseconds(123))));
        }

        foreach (var baseType in baseTypesToTest)
        {
            foreach (var composite in GenerateCompositeTypeSamples(baseType))
            {
                yield return composite;
            }
        }
    }

    public static object[] GetEnsureSingleRow(this DbDataReader reader)
    {
        ClassicAssert.IsTrue(reader.HasRows, "Reader expected to have rows");
        ClassicAssert.IsTrue(reader.Read(), "Failed to read first row");

        var data = reader.GetFieldValues();

        ClassicAssert.IsFalse(reader.Read(), "Unexpected extra row: " + string.Join(",", reader.GetFieldValues()));

        return data;
    }

    public static Type[] GetFieldTypes(this DbDataReader reader) => Enumerable.Range(0, reader.FieldCount).Select(reader.GetFieldType).ToArray();

    public static string[] GetFieldNames(this DbDataReader reader) => Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();

    public static object[] GetFieldValues(this DbDataReader reader) => Enumerable.Range(0, reader.FieldCount).Select(reader.GetValue).ToArray();

    public static void AssertHasFieldCount(this DbDataReader reader, int expectedCount) => Assert.That(reader.FieldCount, Is.EqualTo(expectedCount));
}
