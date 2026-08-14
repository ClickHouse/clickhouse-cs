namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    /*********************************
     *  Compression internals structs *
     *********************************/
    internal struct ZSTD_match_t
    {
        /* Offset sumtype code for the match, using ZSTD_storeSeq() format */
        public uint off;
        /* Raw length of match */
        public uint len;
    }
}