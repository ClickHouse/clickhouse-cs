namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    /*-*******************************************************
     *  Decompression types
     *********************************************************/
    internal struct ZSTD_seqSymbol_header
    {
        public uint fastMode;
        public uint tableLog;
    }
}