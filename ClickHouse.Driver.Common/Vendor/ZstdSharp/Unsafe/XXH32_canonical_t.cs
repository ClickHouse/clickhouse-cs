namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    /*!
     * @brief Canonical (big endian) representation of @ref XXH32_hash_t.
     */
    internal unsafe struct XXH32_canonical_t
    {
        /*!< Hash bytes, big endian */
        public fixed byte digest[4];
    }
}