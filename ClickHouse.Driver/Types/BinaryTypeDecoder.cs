using System;
using System.Collections.Generic;
using ClickHouse.Driver.Formats;
using NodaTime;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Decodes ClickHouse type definitions from binary encoding.
/// See: https://clickhouse.com/docs/en/sql-reference/data-types/data-types-binary-encoding
/// </summary>
internal static class BinaryTypeDecoder
{
    internal static ClickHouseType FromByteCode(ExtendedBinaryReader reader, TypeSettings typeSettings)
    {
        var value = reader.ReadByte();
        switch (value)
        {
            case BinaryTypeIndex.Nothing:
                return new NothingType();

            case BinaryTypeIndex.UInt8:
                return new UInt8Type();
            case BinaryTypeIndex.UInt16:
                return new UInt16Type();
            case BinaryTypeIndex.UInt32:
                return new UInt32Type();
            case BinaryTypeIndex.UInt64:
                return new UInt64Type();
            case BinaryTypeIndex.UInt128:
                return new UInt128Type();
            case BinaryTypeIndex.UInt256:
                return new UInt256Type();

            case BinaryTypeIndex.Int8:
                return new Int8Type();
            case BinaryTypeIndex.Int16:
                return new Int16Type();
            case BinaryTypeIndex.Int32:
                return new Int32Type();
            case BinaryTypeIndex.Int64:
                return new Int64Type();
            case BinaryTypeIndex.Int128:
                return new Int128Type();
            case BinaryTypeIndex.Int256:
                return new Int256Type();

            case BinaryTypeIndex.Float32:
                return new Float32Type();
            case BinaryTypeIndex.Float64:
                return new Float64Type();
            case BinaryTypeIndex.BFloat16:
                return new BFloat16Type();

            case BinaryTypeIndex.Date:
                return new DateType();
            case BinaryTypeIndex.Date32:
                return new Date32Type();
            case BinaryTypeIndex.DateTimeUTC:
                return new DateTimeType();
            case BinaryTypeIndex.DateTimeWithTimezone:
                return new DateTimeType { TimeZone = DateTimeZoneProviders.Tzdb.GetZoneOrNull(reader.ReadString()) };
            case BinaryTypeIndex.DateTime64UTC:
                return new DateTime64Type() { Scale = reader.ReadByte() };
            case BinaryTypeIndex.DateTime64WithTimezone:
                return new DateTime64Type() { Scale = reader.ReadByte(), TimeZone = DateTimeZoneProviders.Tzdb.GetZoneOrNull(reader.ReadString()) };

            case BinaryTypeIndex.String:
                return new StringType();
            case BinaryTypeIndex.FixedString:
                return new FixedStringType() { Length = reader.Read7BitEncodedInt() };

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
                return new UuidType();

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
                return new IPv4Type();
            case BinaryTypeIndex.IPv6:
                return new IPv6Type();

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
                return new BooleanType();

            case BinaryTypeIndex.SimpleAggregateFunction:
                return DecodeSimpleAggregateFunction(reader);

            case BinaryTypeIndex.Nested:
                return DecodeNested(reader, typeSettings);

            case BinaryTypeIndex.Json:
                return DecodeJson(reader, typeSettings);

            case BinaryTypeIndex.Time:
                return new TimeType();

            case BinaryTypeIndex.Time64:
                return new Time64Type
                {
                    Scale = reader.Read7BitEncodedInt(),
                };

            default:
                break;
        }

        throw new ArgumentOutOfRangeException(nameof(value), $"Unknown type code: {value}");
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

    private static ClickHouseType DecodeFunction(ExtendedBinaryReader reader, TypeSettings typeSettings)
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
