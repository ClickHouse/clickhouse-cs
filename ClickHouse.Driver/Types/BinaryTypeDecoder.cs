using System;
using System.Collections.Generic;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Decodes ClickHouse type definitions from binary encoding.
/// See: https://clickhouse.com/docs/en/sql-reference/data-types/data-types-binary-encoding
/// </summary>
internal static class BinaryTypeDecoder
{
    // Many ClickHouse types carry no per-instance state (all integer/float/date/ip/bool types).
    // On the Dynamic read path FromByteCode is invoked once per value, so allocating a fresh
    // instance every call is pure GC pressure. These types are immutable, so a single shared
    // instance can be reused safely across every decode. Types that carry state (String's
    // ReadAsByteArray, timezones, scales, precision, composites) are still constructed per call.
    private static readonly NothingType NothingSingleton = new();
    private static readonly UInt8Type UInt8Singleton = new();
    private static readonly UInt16Type UInt16Singleton = new();
    private static readonly UInt32Type UInt32Singleton = new();
    private static readonly UInt64Type UInt64Singleton = new();
    private static readonly UInt128Type UInt128Singleton = new();
    private static readonly UInt256Type UInt256Singleton = new();
    private static readonly Int8Type Int8Singleton = new();
    private static readonly Int16Type Int16Singleton = new();
    private static readonly Int32Type Int32Singleton = new();
    private static readonly Int64Type Int64Singleton = new();
    private static readonly Int128Type Int128Singleton = new();
    private static readonly Int256Type Int256Singleton = new();
    private static readonly Float32Type Float32Singleton = new();
    private static readonly Float64Type Float64Singleton = new();
    private static readonly BFloat16Type BFloat16Singleton = new();
    private static readonly DateType DateSingleton = new();
    private static readonly Date32Type Date32Singleton = new();
    private static readonly UuidType UuidSingleton = new();
    private static readonly IPv4Type IPv4Singleton = new();
    private static readonly IPv6Type IPv6Singleton = new();
    private static readonly BooleanType BooleanSingleton = new();
    private static readonly TimeType TimeSingleton = new();

    // String is stateless apart from the ReadAsByteArray flag, which has exactly two values;
    // cache one shared instance per variant rather than allocating per value.
    private static readonly StringType StringSingleton = new() { ReadAsByteArray = false };
    private static readonly StringType StringAsByteArraySingleton = new() { ReadAsByteArray = true };

    internal static ClickHouseType FromByteCode(ExtendedBinaryReader reader, TypeSettings typeSettings)
    {
        var value = reader.ReadByte();
        switch (value)
        {
            case BinaryTypeIndex.Nothing:
                return NothingSingleton;

            case BinaryTypeIndex.UInt8:
                return UInt8Singleton;
            case BinaryTypeIndex.UInt16:
                return UInt16Singleton;
            case BinaryTypeIndex.UInt32:
                return UInt32Singleton;
            case BinaryTypeIndex.UInt64:
                return UInt64Singleton;
            case BinaryTypeIndex.UInt128:
                return UInt128Singleton;
            case BinaryTypeIndex.UInt256:
                return UInt256Singleton;

            case BinaryTypeIndex.Int8:
                return Int8Singleton;
            case BinaryTypeIndex.Int16:
                return Int16Singleton;
            case BinaryTypeIndex.Int32:
                return Int32Singleton;
            case BinaryTypeIndex.Int64:
                return Int64Singleton;
            case BinaryTypeIndex.Int128:
                return Int128Singleton;
            case BinaryTypeIndex.Int256:
                return Int256Singleton;

            case BinaryTypeIndex.Float32:
                return Float32Singleton;
            case BinaryTypeIndex.Float64:
                return Float64Singleton;
            case BinaryTypeIndex.BFloat16:
                return BFloat16Singleton;

            case BinaryTypeIndex.Date:
                return DateSingleton;
            case BinaryTypeIndex.Date32:
                return Date32Singleton;
            case BinaryTypeIndex.DateTimeUTC:
                return new DateTimeType();
            case BinaryTypeIndex.DateTimeWithTimezone:
                return new DateTimeType { TimeZone = AbstractDateTimeType.ResolveTimezone(reader.ReadString()) };
            case BinaryTypeIndex.DateTime64UTC:
                return new DateTime64Type() { Scale = reader.ReadByte() };
            case BinaryTypeIndex.DateTime64WithTimezone:
                return new DateTime64Type() { Scale = reader.ReadByte(), TimeZone = AbstractDateTimeType.ResolveTimezone(reader.ReadString()) };

            case BinaryTypeIndex.String:
                return typeSettings.readStringsAsByteArrays ? StringAsByteArraySingleton : StringSingleton;
            case BinaryTypeIndex.FixedString:
                return new FixedStringType() { Length = reader.Read7BitEncodedInt(), ReadAsByteArray = typeSettings.readStringsAsByteArrays };

            case BinaryTypeIndex.Enum8:
                return DecodeEnum8(reader);
            case BinaryTypeIndex.Enum16:
                return DecodeEnum16(reader);

            case BinaryTypeIndex.Decimal32:
                return DecodeDecimal32(reader, typeSettings);
            case BinaryTypeIndex.Decimal64:
                return DecodeDecimal64(reader, typeSettings);
            case BinaryTypeIndex.Decimal128:
                return DecodeDecimal128(reader, typeSettings);
            case BinaryTypeIndex.Decimal256:
                return DecodeDecimal256(reader, typeSettings);

            case BinaryTypeIndex.UUID:
                return UuidSingleton;

            case BinaryTypeIndex.Array:
                return new ArrayType() { UnderlyingType = FromByteCode(reader, typeSettings) };

            case BinaryTypeIndex.UnnamedTuple:
                return DecodeUnnamedTuple(reader, typeSettings);
            case BinaryTypeIndex.NamedTuple:
                return DecodeNamedTuple(reader, typeSettings);

            case BinaryTypeIndex.Set:
                throw new NotSupportedException("Set type cannot be decoded.");

            case BinaryTypeIndex.Interval:
                // Interval is stored as Int64 with a kind indicator
                // TODO: following interval implementation
                break;

            case BinaryTypeIndex.Nullable:
                return new NullableType() { UnderlyingType = FromByteCode(reader, typeSettings) };

            case BinaryTypeIndex.Function:
                return DecodeFunction(reader, typeSettings);

            case BinaryTypeIndex.AggregateFunction:
                return DecodeAggregateFunction(reader);

            case BinaryTypeIndex.LowCardinality:
                return new LowCardinalityType() { UnderlyingType = FromByteCode(reader, typeSettings) };

            case BinaryTypeIndex.Map:
                return new MapType() { UnderlyingTypes = Tuple.Create(FromByteCode(reader, typeSettings), FromByteCode(reader, typeSettings)) };

            case BinaryTypeIndex.IPv4:
                return IPv4Singleton;
            case BinaryTypeIndex.IPv6:
                return IPv6Singleton;

            case BinaryTypeIndex.Variant:
                return DecodeVariant(reader, typeSettings);

            case BinaryTypeIndex.Dynamic:
                reader.ReadByte(); // max_dynamic_types, ignored
                return new DynamicType
                {
                    TypeSettings = typeSettings,
                };

            case BinaryTypeIndex.Custom:
                return DecodeCustomType(reader); // "Ring, Polygon, etc"

            case BinaryTypeIndex.Bool:
                return BooleanSingleton;

            case BinaryTypeIndex.SimpleAggregateFunction:
                return DecodeSimpleAggregateFunction(reader);

            case BinaryTypeIndex.Nested:
                return DecodeNested(reader, typeSettings);

            case BinaryTypeIndex.Json:
                return DecodeJson(reader, typeSettings);

            case BinaryTypeIndex.Time:
                return TimeSingleton;

            case BinaryTypeIndex.Time64:
                return new Time64Type
                {
                    Scale = reader.Read7BitEncodedInt(),
                };

            case BinaryTypeIndex.QBit:
                return DecodeQBit(reader, typeSettings);

            default:
                break;
        }

#pragma warning disable CA2208
        throw new ArgumentOutOfRangeException(nameof(value), $"Unknown type code: {value}");
#pragma warning restore CA2208
    }

    private static Enum8Type DecodeEnum8(ExtendedBinaryReader reader)
    {
        var size = reader.Read7BitEncodedInt();
        var values = new Dictionary<string, int>(size);
        for (int i = 0; i < size; i++)
        {
            var name = reader.ReadString();
            var enumValue = reader.ReadSByte();
            values[name] = enumValue;
        }
        return new Enum8Type(values);
    }

    private static Enum16Type DecodeEnum16(ExtendedBinaryReader reader)
    {
        var size = reader.Read7BitEncodedInt();
        var values = new Dictionary<string, int>(size);
        for (int i = 0; i < size; i++)
        {
            var name = reader.ReadString();
            var enumValue = reader.ReadInt16();
            values[name] = enumValue;
        }
        return new Enum16Type(values);
    }

    private static Decimal32Type DecodeDecimal32(ExtendedBinaryReader reader, TypeSettings typeSettings)
    {
        var precision = reader.ReadByte();
        var scale = reader.ReadByte();
        return new Decimal32Type { Precision = precision, Scale = scale, UseBigDecimal = typeSettings.useBigDecimal };
    }

    private static Decimal64Type DecodeDecimal64(ExtendedBinaryReader reader, TypeSettings typeSettings)
    {
        var precision = reader.ReadByte();
        var scale = reader.ReadByte();
        return new Decimal64Type { Precision = precision, Scale = scale, UseBigDecimal = typeSettings.useBigDecimal };
    }

    private static Decimal128Type DecodeDecimal128(ExtendedBinaryReader reader, TypeSettings typeSettings)
    {
        var precision = reader.ReadByte();
        var scale = reader.ReadByte();
        return new Decimal128Type { Precision = precision, Scale = scale, UseBigDecimal = typeSettings.useBigDecimal };
    }

    private static Decimal256Type DecodeDecimal256(ExtendedBinaryReader reader, TypeSettings typeSettings)
    {
        var precision = reader.ReadByte();
        var scale = reader.ReadByte();
        return new Decimal256Type { Precision = precision, Scale = scale, UseBigDecimal = typeSettings.useBigDecimal };
    }

    private static TupleType DecodeUnnamedTuple(ExtendedBinaryReader reader, TypeSettings typeSettings)
    {
        var size = reader.Read7BitEncodedInt();
        var types = new ClickHouseType[size];
        for (int i = 0; i < size; i++)
        {
            types[i] = FromByteCode(reader, typeSettings);
        }
        return new TupleType { UnderlyingTypes = types };
    }

    private static TupleType DecodeNamedTuple(ExtendedBinaryReader reader, TypeSettings typeSettings)
    {
        var size = reader.Read7BitEncodedInt();
        var types = new ClickHouseType[size];
        for (int i = 0; i < size; i++)
        {
            string name = reader.ReadString();
            types[i] = FromByteCode(reader, typeSettings);
        }
        return new TupleType { UnderlyingTypes = types };
    }

    private static NothingType DecodeFunction(ExtendedBinaryReader reader, TypeSettings typeSettings)
    {
        var argumentsSize = reader.Read7BitEncodedInt();
        for (int i = 0; i < argumentsSize; i++)
        {
            FromByteCode(reader, typeSettings); // Skip argument types
        }
        FromByteCode(reader, typeSettings); // Skip return type

        // Function types are not directly queryable, return a placeholder
        return new NothingType();
    }

    private static AggregateFunctionType DecodeAggregateFunction(ExtendedBinaryReader reader)
    {
        throw new NotImplementedException("AggregateFunction decoding not implemented.");
    }

    private static SimpleAggregateFunctionType DecodeSimpleAggregateFunction(ExtendedBinaryReader reader)
    {
        throw new NotImplementedException("SimpleAggregateFunction decoding not implemented.");
    }

    private static VariantType DecodeVariant(ExtendedBinaryReader reader, TypeSettings typeSettings)
    {
        var size = reader.Read7BitEncodedInt();
        var types = new ClickHouseType[size];
        for (int i = 0; i < size; i++)
        {
            types[i] = FromByteCode(reader, typeSettings);
        }
        return new VariantType { UnderlyingTypes = types };
    }

    private static NestedType DecodeNested(ExtendedBinaryReader reader, TypeSettings typeSettings)
    {
        var size = reader.Read7BitEncodedInt();
        var types = new ClickHouseType[size];
        for (int i = 0; i < size; i++)
        {
            var name = reader.ReadString(); // Skip field name
            types[i] = FromByteCode(reader, typeSettings);
        }
        return new NestedType { UnderlyingTypes = types };
    }

    private static ClickHouseType DecodeCustomType(ExtendedBinaryReader reader)
    {
        var typeName = reader.ReadString();
        // Try to parse custom type name through the type converter
        try
        {
            return TypeConverter.ParseClickHouseType(typeName, TypeSettings.Default);
        }
        catch
        {
            // If parsing fails, return a string type as fallback
            return new StringType();
        }
    }

    private static QBitType DecodeQBit(ExtendedBinaryReader reader, TypeSettings typeSettings)
    {
        var elementType = FromByteCode(reader, typeSettings);
        var dimension = reader.Read7BitEncodedInt();
        return new QBitType { ElementType = elementType, Dimension = dimension };
    }

    private static JsonType DecodeJson(ExtendedBinaryReader reader, TypeSettings typeSettings)
    {
        var serializationVersion = reader.ReadByte();
        var maxDynamicPaths = reader.Read7BitEncodedInt();
        var maxDynamicTypes = reader.ReadByte();

        // Read typed paths
        var typedPathsSize = reader.Read7BitEncodedInt();
        var typedPaths = new Dictionary<string, ClickHouseType>(typedPathsSize);
        for (int i = 0; i < typedPathsSize; i++)
        {
            var path = reader.ReadString();
            var pathType = FromByteCode(reader, typeSettings);
            typedPaths[path] = pathType;
        }

        // Skip paths to skip
        var pathsToSkipSize = reader.Read7BitEncodedInt();
        for (int i = 0; i < pathsToSkipSize; i++)
        {
            reader.ReadString();
        }

        // Skip path regexps to skip
        var pathRegexpsToSkipSize = reader.Read7BitEncodedInt();
        for (int i = 0; i < pathRegexpsToSkipSize; i++)
        {
            reader.ReadString();
        }

        return new JsonType(typedPaths) { TypeSettings = typeSettings };
    }
}
