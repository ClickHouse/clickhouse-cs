namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    /* Struct to keep track of where we are in our recursive calls. */
    internal unsafe struct seqStoreSplits
    {
        /* Array of split indices */
        public uint* splitLocations;
        /* The current index within splitLocations being worked on */
        public nuint idx;
    }
}