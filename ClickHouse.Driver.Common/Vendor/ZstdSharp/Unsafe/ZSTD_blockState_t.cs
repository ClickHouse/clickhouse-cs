namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    internal unsafe struct ZSTD_blockState_t
    {
        public ZSTD_compressedBlockState_t* prevCBlock;
        public ZSTD_compressedBlockState_t* nextCBlock;
        public ZSTD_MatchState_t matchState;
    }
}