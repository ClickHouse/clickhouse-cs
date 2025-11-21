using System;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

internal class IntervalNanosecondType : IntervalType
{
#if NET7_0_OR_GREATER
    public const long NanosecondsPerTick = TimeSpan.NanosecondsPerTick;
#else
    public const long NanosecondsPerTick = 100;
#endif

    public override Type FrameworkType => typeof(TimeSpan);

    // Anything less than 100 nanoseconds will be truncated to 0 as TimeSpan's smallest unit is 1 tick (100 nanoseconds)
    public override object Read(ExtendedBinaryReader reader) => TimeSpan.FromTicks(reader.ReadInt64() / NanosecondsPerTick);

    public override string ToString() => "IntervalNanosecond";

    public override void Write(ExtendedBinaryWriter writer, object value) => writer.Write(((TimeSpan)value).Ticks * NanosecondsPerTick);
}
