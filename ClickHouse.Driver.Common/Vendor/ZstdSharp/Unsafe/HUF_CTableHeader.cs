namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    internal unsafe struct HUF_CTableHeader
    {
        public byte tableLog;
        public byte maxSymbolValue;
        public fixed byte unused[6];
    }
}