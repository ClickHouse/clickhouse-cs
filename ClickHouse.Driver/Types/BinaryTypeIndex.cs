namespace ClickHouse.Driver.Types;

/// <summary>
/// Binary type index constants for ClickHouse data types binary encoding.
/// See https://clickhouse.com/docs/en/sql-reference/data-types/data-types-binary-encoding
/// </summary>
internal static class BinaryTypeIndex
{
    public const byte Nothing = 0x00;
    public const byte UInt8 = 0x01;
    public const byte UInt16 = 0x02;
    public const byte UInt32 = 0x03;
    public const byte UInt64 = 0x04;
    public const byte UInt128 = 0x05;
    public const byte UInt256 = 0x06;
    public const byte Int8 = 0x07;
    public const byte Int16 = 0x08;
    public const byte Int32 = 0x09;
    public const byte Int64 = 0x0A;
    public const byte Int128 = 0x0B;
    public const byte Int256 = 0x0C;
    public const byte Float32 = 0x0D;
    public const byte Float64 = 0x0E;
    public const byte Date = 0x0F;
    public const byte Date32 = 0x10;
    public const byte DateTimeUTC = 0x11;
    public const byte DateTimeWithTimezone = 0x12;
    public const byte DateTime64UTC = 0x13;
    public const byte DateTime64WithTimezone = 0x14;
    public const byte String = 0x15;
    public const byte FixedString = 0x16;
    public const byte Enum8 = 0x17;
    public const byte Enum16 = 0x18;
    public const byte Decimal32 = 0x19;
    public const byte Decimal64 = 0x1A;
    public const byte Decimal128 = 0x1B;
    public const byte Decimal256 = 0x1C;
    public const byte UUID = 0x1D;
    public const byte Array = 0x1E;
    public const byte UnnamedTuple = 0x1F;
    public const byte NamedTuple = 0x20;
    public const byte Set = 0x21;
    public const byte Interval = 0x22;
    public const byte Nullable = 0x23;
    public const byte Function = 0x24;
    public const byte AggregateFunction = 0x25;
    public const byte LowCardinality = 0x26;
    public const byte Map = 0x27;
    public const byte IPv4 = 0x28;
    public const byte IPv6 = 0x29;
    public const byte Variant = 0x2A;
    public const byte Dynamic = 0x2B;
    public const byte Custom = 0x2C;
    public const byte Bool = 0x2D;
    public const byte SimpleAggregateFunction = 0x2E;
    public const byte Nested = 0x2F;
    public const byte Json = 0x30;
    public const byte BFloat16 = 0x31;
    public const byte Time = 0x32;
    public const byte Time64 = 0x34;
    public const byte QBit = 0x36;
}
