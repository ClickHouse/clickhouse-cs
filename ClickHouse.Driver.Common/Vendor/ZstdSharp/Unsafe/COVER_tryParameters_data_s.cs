namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    /**
     * Parameters for COVER_tryParameters().
     */
    internal unsafe struct COVER_tryParameters_data_s
    {
        public COVER_ctx_t* ctx;
        public COVER_best_s* best;
        public nuint dictBufferCapacity;
        public ZDICT_cover_params_t parameters;
    }
}