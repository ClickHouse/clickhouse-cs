using System;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types.Grammar;
using NodaTime;

namespace ClickHouse.Driver.Types;

internal class NullableType : ParameterizedType, IInstantReader
{
    public ClickHouseType UnderlyingType { get; set; }

    public override Type FrameworkType
    {
        get
        {
            var underlyingFrameworkType = UnderlyingType.FrameworkType;
            return underlyingFrameworkType.IsValueType ? typeof(Nullable<>).MakeGenericType(underlyingFrameworkType) : underlyingFrameworkType;
        }
    }

    public override string Name => "Nullable";

    public override ParameterizedType Parse(SyntaxTreeNode node, Func<SyntaxTreeNode, ClickHouseType> parseClickHouseTypeFunc, TypeSettings settings)
    {
        return new NullableType
        {
            UnderlyingType = parseClickHouseTypeFunc(node.SingleChild),
        };
    }

    public override object Read(ExtendedBinaryReader reader) => reader.ReadByte() > 0 ? DBNull.Value : UnderlyingType.Read(reader);

    bool IInstantReader.ReportsInstant => UnderlyingType is IInstantReader { ReportsInstant: true };

    object IInstantReader.ReadWithInstant(ExtendedBinaryReader reader, out Instant? instant)
    {
        if (reader.ReadByte() > 0)
        {
            instant = null;
            return DBNull.Value;
        }

        if (UnderlyingType is IInstantReader underlying)
            return underlying.ReadWithInstant(reader, out instant);

        instant = null;
        return UnderlyingType.Read(reader);
    }

    public override string ToString() => $"{Name}({UnderlyingType})";

    internal override string CacheSignature =>
        ComposeCacheSignature(children => $"{Name}({children[0]})", UnderlyingType);

    public override void Write(ExtendedBinaryWriter writer, object value)
    {
        if (value == null || value is DBNull)
        {
            writer.Write((byte)1);
        }
        else
        {
            writer.Write((byte)0);
            UnderlyingType.Write(writer, value);
        }
    }
}
