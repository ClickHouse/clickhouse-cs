namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    /**
     * Struct used for the dictionary selection function.
     */
    internal unsafe struct COVER_dictSelection
    {
        public byte* dictContent;
        public nuint dictSize;
        public nuint totalCompressedSize;
    }
}