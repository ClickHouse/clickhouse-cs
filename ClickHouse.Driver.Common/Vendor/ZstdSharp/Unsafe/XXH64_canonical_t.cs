namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    /*!
     * @brief Canonical (big endian) representation of @ref XXH64_hash_t.
     */
    internal unsafe struct XXH64_canonical_t
    {
        public fixed byte digest[8];
    }
}