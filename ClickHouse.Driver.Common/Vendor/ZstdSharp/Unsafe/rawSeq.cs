namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    internal struct rawSeq
    {
        /* Offset of sequence */
        public uint offset;
        /* Length of literals prior to match */
        public uint litLength;
        /* Raw length of match */
        public uint matchLength;
    }
}