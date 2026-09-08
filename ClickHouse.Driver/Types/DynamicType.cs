using System;
using System.Collections.Concurrent;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Numerics;

namespace ClickHouse.Driver.Types;

internal class DynamicType : ClickHouseType
{
    /// <summary>
    /// Cache for inferred ClickHouse types from .NET types.
    /// </summary>
    private static readonly ConcurrentDictionary<Type, ClickHouseType> InferredTypeCache = new();

    public override Type FrameworkType => typeof(object);

    public TypeSettings TypeSettings { get; init; }

    public override string ToString() => "Dynamic";

    public override object Read(ExtendedBinaryReader reader) =>
        BinaryTypeDecoder.
            FromByteCode(reader, TypeSettings).
            Read(reader);

    /// <summary>
    /// Writes a value with its type header for dynamic type encoding.
    /// The type is inferred from the value's .NET type and cached, except for decimals, whose
    /// ClickHouse scale/width depend on the value itself and so are inferred per value.
    /// </summary>
    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        if (value is null || value is DBNull)
        {
            writer.Write(BinaryTypeIndex.Nothing);
            return;
        }

        // Decimals must be inferred from the value, not just its .NET type: the ClickHouse scale is
        // derived from the value's own scale, so the per-Type cache (which cannot vary by value)
        // would truncate any value whose scale exceeds the cached type's fixed scale (issue #466).
        ClickHouseType inferredType;
        if (value is ClickHouseDecimal chd)
            inferredType = TypeConverter.InferDecimalType(chd);
        else if (value is decimal dec)
            inferredType = TypeConverter.InferDecimalType(dec); // implicit decimal -> ClickHouseDecimal
        else
            inferredType = GetCachedInferredType(value.GetType());

        BinaryTypeDescriptionWriter.WriteTypeHeader(writer, inferredType);
        inferredType.Write(writer, value);
    }

    /// <summary>
    /// Use the type converter to infer the ClickHouse type from the .NET type, and cache the results.
    /// </summary>
    private static ClickHouseType GetCachedInferredType(Type type)
        => InferredTypeCache.GetOrAdd(type, TypeConverter.ToClickHouseType);
}
