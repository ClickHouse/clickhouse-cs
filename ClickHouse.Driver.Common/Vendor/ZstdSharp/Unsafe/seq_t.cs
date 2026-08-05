namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    internal struct seq_t
    {
        public nuint litLength;
        public nuint matchLength;
        public nuint offset;
    }
}