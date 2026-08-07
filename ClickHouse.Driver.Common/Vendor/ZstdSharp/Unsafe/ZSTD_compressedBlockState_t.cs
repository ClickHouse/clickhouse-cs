namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    internal unsafe struct ZSTD_compressedBlockState_t
    {
        public ZSTD_entropyCTables_t entropy;
        public fixed uint rep[3];
    }
}