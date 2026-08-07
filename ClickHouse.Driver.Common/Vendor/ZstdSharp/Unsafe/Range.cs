namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    /* ====   Serial State   ==== */
    internal unsafe struct Range
    {
        public void* start;
        public nuint size;
        public Range(void* start, nuint size)
        {
            this.start = start;
            this.size = size;
        }
    }
}