using System;
using ClickHouse.Driver.Tcp.Tests.Utilities;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// The columnar write tier (<c>InsertAsync</c> with the caller's columns) and <c>ClickHouseTcpTypes.CanWrite</c> as they
/// were before they moved onto the converter derivation: the old dispatch, copied here as the reference of the
/// differential tests (SPEC invariant 11). The old members that it calls (<see cref="IColumnCodec.CanWrite"/>, the
/// codecs' write shapes, <see cref="IColumnCodec.CanWriteElementType"/>) stay in production until the old path is removed.
/// </summary>
internal static class LegacyColumnarWrite
{
    /// <summary>The old <c>ClickHouseTcpTypes.CanWrite</c>.</summary>
    public static bool CanWrite(string clickHouseType, Type elementType)
    {
        ArgumentNullException.ThrowIfNull(elementType);
        ArgumentNullException.ThrowIfNull(clickHouseType);
        return ColumnCodecRegistry.Default.Resolve(clickHouseType, ResolveContext.ForWrite).CanWriteElementType(elementType);
    }

    /// <summary>
    /// The old write of one insert column: the insert plan asked the codec's <c>CanWrite</c>, then the block writer ran
    /// the codec's <c>BeginWrite</c>, state prefix and body for the rows of the block.
    /// </summary>
    internal sealed class WriteArm : Differential.WriteArm
    {
        public WriteArm(string name)
            : base(name)
        {
        }

        public override SliceWriter Bind<T>(IColumn<T> column, string columnType, ResolveContext context)
        {
            IColumnCodec codec = ColumnCodecRegistry.Default.Resolve(columnType, context);
            if (!codec.CanWrite(column))
            {
                throw new ArmRefusal($"The codec of '{columnType}' refuses a column of {TypeNames.Of(typeof(T))} ({column.GetType().Name}).");
            }

            return (writer, start, length) =>
            {
                IColumnWriteState state = codec.BeginWrite(column, start, length);
                try
                {
                    codec.WriteStatePrefix(writer, column, start, length, state);
                    codec.WriteColumn(writer, column, start, length, state);
                }
                finally
                {
                    state?.Dispose();
                }
            };
        }
    }
}
