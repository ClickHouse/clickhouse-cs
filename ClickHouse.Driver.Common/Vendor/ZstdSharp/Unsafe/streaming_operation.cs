namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    /* Streaming state is used to inform allocation of the literal buffer */
    internal enum streaming_operation
    {
        not_streaming = 0,
        is_streaming = 1
    }
}