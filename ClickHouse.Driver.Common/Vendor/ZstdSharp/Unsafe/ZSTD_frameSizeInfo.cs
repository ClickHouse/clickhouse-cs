namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    /**
     * Contains the compressed frame size and an upper-bound for the decompressed frame size.
     * Note: before using `compressedSize`, check for errors using ZSTD_isError().
     *       similarly, before using `decompressedBound`, check for errors using:
     *          `decompressedBound != ZSTD_CONTENTSIZE_ERROR`
     */
    internal struct ZSTD_frameSizeInfo
    {
        public nuint nbBlocks;
        public nuint compressedSize;
        public ulong decompressedBound;
    }
}