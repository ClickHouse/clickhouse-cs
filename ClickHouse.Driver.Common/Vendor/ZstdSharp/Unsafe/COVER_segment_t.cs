namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    /**
     * A segment is a range in the source as well as the score of the segment.
     */
    internal struct COVER_segment_t
    {
        public uint begin;
        public uint end;
        public uint score;
    }
}