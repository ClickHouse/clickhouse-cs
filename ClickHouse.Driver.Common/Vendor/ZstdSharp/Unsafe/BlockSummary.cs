namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    internal struct BlockSummary
    {
        public nuint nbSequences;
        public nuint blockSize;
        public nuint litSize;
    }
}