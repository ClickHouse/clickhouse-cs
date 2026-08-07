namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    /*-*******************************************************
     *  Types
     *********************************************************/
    internal unsafe struct ZSTD_DDict_s
    {
        public void* dictBuffer;
        public void* dictContent;
        public nuint dictSize;
        public ZSTD_entropyDTables_t entropy;
        public uint dictID;
        public uint entropyPresent;
        public ZSTD_customMem cMem;
    }
}