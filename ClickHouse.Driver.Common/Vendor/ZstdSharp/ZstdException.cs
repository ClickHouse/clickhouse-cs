using System;
using ClickHouse.Driver.Vendor.ZstdSharp.Unsafe;

namespace ClickHouse.Driver.Vendor.ZstdSharp
{
    internal class ZstdException : Exception
    {
        public ZstdException(ZSTD_ErrorCode code, string message) : base(message)
            => Code = code;

        public ZSTD_ErrorCode Code { get; }
    }
}
