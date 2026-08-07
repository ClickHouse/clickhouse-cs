using System.Runtime.InteropServices;

namespace ClickHouse.Driver.Vendor.ZstdSharp.Unsafe
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal unsafe delegate uint ZSTD_getAllMatchesFn(ZSTD_match_t* param0, ZSTD_MatchState_t* param1, uint* param2, byte* param3, byte* param4, uint* rep, uint ll0, uint lengthToBeat);
}