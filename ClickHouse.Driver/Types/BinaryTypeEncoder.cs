using System;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Encodes ClickHouse type definitions to binary encoding.
/// This is the inverse of <see cref="BinaryTypeDecoder"/>.
/// See: https://clickhouse.com/docs/en/sql-reference/data-types/data-types-binary-encoding
/// </summary>
internal static class BinaryTypeEncoder
{
    /// <summary>
    /// Writes the binary type header for a ClickHouseType.
    /// </summary>
    internal static void WriteTypeHeader(ExtendedBinaryWriter writer, ClickHouseType type)
    {
        switch (type)
        {
            // Nothing/Null
            case NothingType:
                writer.Write(BinaryTypeIndex.Nothing);
                break;

            // Unsigned integers
            case UInt8Type:
                writer.Write(BinaryTypeIndex.UInt8);
                break;
            case UInt16Type:
                writer.Write(BinaryTypeIndex.UInt16);
                break;
            case UInt32Type:
                writer.Write(BinaryTypeIndex.UInt32);
                break;
            case UInt64Type:
                writer.Write(BinaryTypeIndex.UInt64);
                break;
            case UInt128Type:
                writer.Write(BinaryTypeIndex.UInt128);
                break;
            case UInt256Type:
                writer.Write(BinaryTypeIndex.UInt256);
                break;

            // Signed integers
            case Int8Type:
                writer.Write(BinaryTypeIndex.Int8);
                break;
            case Int16Type:
                writer.Write(BinaryTypeIndex.Int16);
                break;
            case Int32Type:
                writer.Write(BinaryTypeIndex.Int32);
                break;
            case Int64Type:
                writer.Write(BinaryTypeIndex.Int64);
                break;
            case Int128Type:
                writer.Write(BinaryTypeIndex.Int128);
                break;
            case Int256Type:
                writer.Write(BinaryTypeIndex.Int256);
                break;

            // Floating point
            case Float32Type:
                writer.Write(BinaryTypeIndex.Float32);
                break;
            case Float64Type:
                writer.Write(BinaryTypeIndex.Float64);
                break;
            case BFloat16Type:
                writer.Write(BinaryTypeIndex.BFloat16);
                break;

            // Boolean
            case BooleanType:
                writer.Write(BinaryTypeIndex.Bool);
                break;

            // Date/Time types
            case Date32Type:
                writer.Write(BinaryTypeIndex.Date32);
                break;
            case DateType:
                writer.Write(BinaryTypeIndex.Date);
                break;
            case DateTimeType dt:
                if (dt.TimeZone == null)
                {
                    writer.Write(BinaryTypeIndex.DateTimeUTC);
                }
                else
                {
                    writer.Write(BinaryTypeIndex.DateTimeWithTimezone);
                    writer.Write(dt.TimeZone.Id);
                }
                break;
            case DateTime64Type dt64:
                if (dt64.TimeZone == null)
                {
                    writer.Write(BinaryTypeIndex.DateTime64UTC);
                    writer.Write((byte)dt64.Scale);
                }
                else
                {
                    writer.Write(BinaryTypeIndex.DateTime64WithTimezone);
                    writer.Write((byte)dt64.Scale);
                    writer.Write(dt64.TimeZone.Id);
                }
                break;

            // String types
            case StringType:
                writer.Write(BinaryTypeIndex.String);
                break;
            case FixedStringType fs:
                writer.Write(BinaryTypeIndex.FixedString);
                writer.Write7BitEncodedInt(fs.Length);
                break;

            // Enum types
            case Enum8Type e8:
                writer.Write(BinaryTypeIndex.Enum8);
                writer.Write7BitEncodedInt(e8.Values.Count);
                foreach (var kvp in e8.Values)
                {
                    writer.Write(kvp.Key);
                    writer.Write((sbyte)kvp.Value);
                }
                break;
            case Enum16Type e16:
                writer.Write(BinaryTypeIndex.Enum16);
                writer.Write7BitEncodedInt(e16.Values.Count);
                foreach (var kvp in e16.Values)
                {
                    writer.Write(kvp.Key);
                    writer.Write((short)kvp.Value);
                }
                break;

            // Decimal types
            case Decimal32Type d32:
                writer.Write(BinaryTypeIndex.Decimal32);
                writer.Write((byte)d32.Precision);
                writer.Write((byte)d32.Scale);
                break;
            case Decimal64Type d64:
                writer.Write(BinaryTypeIndex.Decimal64);
                writer.Write((byte)d64.Precision);
                writer.Write((byte)d64.Scale);
                break;
            case Decimal128Type d128:
                writer.Write(BinaryTypeIndex.Decimal128);
                writer.Write((byte)d128.Precision);
                writer.Write((byte)d128.Scale);
                break;
            case Decimal256Type d256:
                writer.Write(BinaryTypeIndex.Decimal256);
                writer.Write((byte)d256.Precision);
                writer.Write((byte)d256.Scale);
                break;

            // UUID
            case UuidType:
                writer.Write(BinaryTypeIndex.UUID);
                break;

            // IP addresses
            case IPv4Type:
                writer.Write(BinaryTypeIndex.IPv4);
                break;
            case IPv6Type:
                writer.Write(BinaryTypeIndex.IPv6);
                break;

            // Parameterized types (recursive)
            // Note: Order matters - derived types must come before base types

            // Geometry types use Custom encoding (0x2C) - must come before ArrayType/TupleType
            case PointType:
                writer.Write(BinaryTypeIndex.Custom);
                writer.Write("Point");
                break;
            case RingType:
                writer.Write(BinaryTypeIndex.Custom);
                writer.Write("Ring");
                break;
            case LineStringType:
                writer.Write(BinaryTypeIndex.Custom);
                writer.Write("LineString");
                break;
            case PolygonType:
                writer.Write(BinaryTypeIndex.Custom);
                writer.Write("Polygon");
                break;
            case MultiLineStringType:
                writer.Write(BinaryTypeIndex.Custom);
                writer.Write("MultiLineString");
                break;
            case MultiPolygonType:
                writer.Write(BinaryTypeIndex.Custom);
                writer.Write("MultiPolygon");
                break;

            // Geometry is a Variant of all geometry types
            case GeometryType:
                writer.Write(BinaryTypeIndex.Custom);
                writer.Write("Geometry");
                break;

            case ArrayType at:
                writer.Write(BinaryTypeIndex.Array);
                WriteTypeHeader(writer, at.UnderlyingType);
                break;

            case NullableType nt:
                writer.Write(BinaryTypeIndex.Nullable);
                WriteTypeHeader(writer, nt.UnderlyingType);
                break;

            case LowCardinalityType lc:
                writer.Write(BinaryTypeIndex.LowCardinality);
                WriteTypeHeader(writer, lc.UnderlyingType);
                break;

            case MapType mt:
                writer.Write(BinaryTypeIndex.Map);
                WriteTypeHeader(writer, mt.KeyType);
                WriteTypeHeader(writer, mt.ValueType);
                break;

            case TupleType tt:
                writer.Write(BinaryTypeIndex.UnnamedTuple);
                writer.Write7BitEncodedInt(tt.UnderlyingTypes.Length);
                foreach (var underlyingType in tt.UnderlyingTypes)
                {
                    WriteTypeHeader(writer, underlyingType);
                }
                break;

            case VariantType vt:
                writer.Write(BinaryTypeIndex.Variant);
                writer.Write7BitEncodedInt(vt.UnderlyingTypes.Length);
                foreach (var underlyingType in vt.UnderlyingTypes)
                {
                    WriteTypeHeader(writer, underlyingType);
                }
                break;

            // JSON type
            case JsonType jt:
                writer.Write(BinaryTypeIndex.Json);
                writer.Write((byte)0); // serialization version
                writer.Write7BitEncodedInt(1024); // max_dynamic_paths (default)
                writer.Write((byte)32); // max_dynamic_types (default)
                // Write typed paths
                writer.Write7BitEncodedInt(jt.HintedTypes.Count);
                foreach (var kvp in jt.HintedTypes)
                {
                    writer.Write(kvp.Key);
                    WriteTypeHeader(writer, kvp.Value);
                }
                // Write skip paths (none)
                writer.Write7BitEncodedInt(0);
                // Write skip path regexps (none)
                writer.Write7BitEncodedInt(0);
                break;

            // SimpleAggregateFunction
            case SimpleAggregateFunctionType saf:
                writer.Write(BinaryTypeIndex.SimpleAggregateFunction);
                writer.Write(saf.AggregateFunction); // string length is prefixed automatically
                writer.Write7BitEncodedInt(0); // number of parameters (none supported currently)
                writer.Write7BitEncodedInt(1); // number of arguments
                WriteTypeHeader(writer, saf.UnderlyingType);
                break;

            // AggregateFunction - not supported for writing
            case AggregateFunctionType aft:
                throw new NotSupportedException($"Cannot write binary type header for AggregateFunction({aft.Function}).");

            // Dynamic type
            case DynamicType:
                writer.Write(BinaryTypeIndex.Dynamic);
                writer.Write((byte)32); // max_types (default)
                break;

            // Time types
            case TimeType:
                writer.Write(BinaryTypeIndex.Time);
                break;

            case Time64Type t64:
                writer.Write(BinaryTypeIndex.Time64);
                writer.Write((byte)t64.Scale);
                break;

            // QBit type
            case QBitType qb:
                writer.Write(BinaryTypeIndex.QBit);
                WriteTypeHeader(writer, qb.ElementType);
                writer.Write7BitEncodedInt(qb.Dimension);
                break;

            default:
                throw new NotSupportedException($"Cannot write binary type header for {type.GetType().Name}: {type}");
        }
    }
}
