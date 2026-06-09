using System;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Native;

/// <summary>
/// Decodes a Native-format <see cref="NativeBlock"/> from the wire. Native format is columnar:
/// every value of a column is laid out contiguously, with composite types (Nullable, Array)
/// using separate prefix streams (null-map, offsets) ahead of the values stream. This differs
/// fundamentally from the row-oriented RowBinary layout used over HTTP, so composite columns are
/// read here rather than via the per-value <see cref="ClickHouseType.Read"/> path. Scalar and
/// String columns <em>do</em> read identically per value, so those reuse <see cref="ClickHouseType.Read"/>.
/// </summary>
internal static class NativeBlockReader
{
    public static NativeBlock ReadBlock(ExtendedBinaryReader reader, int negotiatedRevision, TypeSettings typeSettings)
    {
        if (negotiatedRevision >= NativeConstants.MinRevisionWithBlockInfo)
            SkipBlockInfo(reader);

        var numColumns = checked((int)NativeWire.ReadVarUInt(reader));
        var numRows = checked((int)NativeWire.ReadVarUInt(reader));

        if (numColumns == 0)
            return new NativeBlock { Names = Array.Empty<string>(), Types = Array.Empty<ClickHouseType>(), RowCount = numRows, Columns = Array.Empty<object[]>() };

        var names = new string[numColumns];
        var types = new ClickHouseType[numColumns];
        var columns = new object[numColumns][];

        for (var c = 0; c < numColumns; c++)
        {
            names[c] = NativeWire.ReadString(reader);
            var typeName = NativeWire.ReadString(reader);
            types[c] = TypeConverter.ParseClickHouseType(typeName, typeSettings);

            if (negotiatedRevision >= NativeConstants.MinRevisionWithCustomSerialization)
            {
                var hasCustom = reader.ReadByte();
                if (hasCustom != 0)
                    throw new NotSupportedException(
                        $"Column '{names[c]}' ({typeName}) uses custom serialization, which the Native protocol MVP does not support yet. Use the HTTP protocol for this query.");
            }

            columns[c] = ReadColumn(reader, types[c], numRows);
        }

        return new NativeBlock { Names = names, Types = types, RowCount = numRows, Columns = columns };
    }

    /// <summary>
    /// Reads one column's worth of <paramref name="rows"/> values in Native columnar layout and
    /// returns them as a boxed <c>object[]</c> (DBNull for nulls).
    /// </summary>
    private static object[] ReadColumn(ExtendedBinaryReader reader, ClickHouseType type, int rows)
    {
        switch (type)
        {
            case NullableType nullable:
            {
                // Null-map stream (one byte per row, 0 = present) precedes the inner values stream,
                // which carries placeholder bytes at null positions.
                var nullMap = NativeWire.ReadFully(reader, rows);
                var inner = ReadColumn(reader, nullable.UnderlyingType, rows);
                var result = new object[rows];
                for (var i = 0; i < rows; i++)
                    result[i] = nullMap[i] != 0 ? DBNull.Value : inner[i];
                return result;
            }

            case ArrayType array:
            {
                // Offsets stream: cumulative UInt64 end positions, one per row.
                var offsets = new long[rows];
                for (var i = 0; i < rows; i++)
                    offsets[i] = checked((long)reader.ReadUInt64());

                var total = rows == 0 ? 0 : (int)offsets[rows - 1];
                var flat = ReadColumn(reader, array.UnderlyingType, total);

                var elementType = array.UnderlyingType.FrameworkType;
                var result = new object[rows];
                var start = 0;
                for (var i = 0; i < rows; i++)
                {
                    var end = (int)offsets[i];
                    var length = end - start;
                    var arr = System.Array.CreateInstance(elementType, length);
                    for (var j = 0; j < length; j++)
                    {
                        var value = flat[start + j];
                        arr.SetValue(value is DBNull ? null : value, j);
                    }

                    result[i] = arr;
                    start = end;
                }

                return result;
            }

            default:
            {
                // Scalar / String / FixedString / Date* / Decimal / UUID / Enum / IP / Bool:
                // the per-value encoding is identical to RowBinary, so reuse the type's reader.
                var result = new object[rows];
                for (var i = 0; i < rows; i++)
                    result[i] = type.Read(reader);
                return result;
            }
        }
    }

    private static void SkipBlockInfo(ExtendedBinaryReader reader)
    {
        // Field-tagged: VarUInt field id, payload; id 0 terminates.
        while (true)
        {
            var fieldId = NativeWire.ReadVarUInt(reader);
            switch (fieldId)
            {
                case 0:
                    return;
                case 1: // is_overflows (UInt8)
                    reader.ReadByte();
                    break;
                case 2: // bucket_number (Int32)
                    reader.ReadInt32();
                    break;
                default:
                    throw new NotSupportedException($"Unsupported BlockInfo field id {fieldId}");
            }
        }
    }
}
