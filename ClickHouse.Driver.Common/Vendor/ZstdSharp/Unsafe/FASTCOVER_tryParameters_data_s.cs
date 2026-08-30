namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    /**
     * Parameters for FASTCOVER_tryParameters().
     */
    internal unsafe struct FASTCOVER_tryParameters_data_s
    {
        public FASTCOVER_ctx_t* ctx;
        public COVER_best_s* best;
        public nuint dictBufferCapacity;
        public ZDICT_cover_params_t parameters;
    }
}