namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    internal unsafe struct SeqCollector
    {
        public int collectSequences;
        public ZSTD_Sequence* seqStart;
        public nuint seqIndex;
        public nuint maxSequences;
    }
}