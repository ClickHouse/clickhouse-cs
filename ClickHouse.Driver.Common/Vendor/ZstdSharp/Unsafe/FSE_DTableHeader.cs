namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    /* ======    Decompression    ====== */
    internal struct FSE_DTableHeader
    {
        public ushort tableLog;
        public ushort fastMode;
    }
}