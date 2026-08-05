namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    /* *****************************************
     *  FSE symbol decompression API
     *******************************************/
    internal unsafe struct FSE_DState_t
    {
        public nuint state;
        /* precise table may vary, depending on U16 */
        public void* table;
    }
}