using System;
using System.Collections.Generic;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// The columnar read tier (<c>Block.ReadAs&lt;T&gt;</c>) and <c>ClickHouseTcpTypes.CanRead</c> as they were before they
/// moved onto the converter derivation: the old dispatch, copied here as the reference of the differential tests (SPEC
/// invariant 11). The old members that it calls (<see cref="LegacyColumnProjection"/>, the codecs'
/// <c>TryProjectColumnRead</c> and <c>TryProjectRead</c>, <c>ReadableElementTypes</c>) stay in production until the old
/// path is removed.
/// </summary>
internal static class LegacyColumnarRead
{
    /// <summary>The old <c>ColumnReadProjections.ReadAs</c>, without its cache of projections.</summary>
    public static IColumn<T> ReadAs<T>(IColumn column, in ResolveContext context)
    {
        if (column is IColumn<T> already)
        {
            return already;
        }

        if (column.TypeName is null)
        {
            throw new InvalidCastException(
                $"Column '{column.Name}' carries no ClickHouse type (it was built by a caller, not decoded), so it offers no reading other than {column.ElementType}.");
        }

        ColumnReadProjection projection = LegacyColumnProjection.For(ColumnCodecRegistry.Default.Resolve(column.TypeName, in context), typeof(T));
        if (projection is null)
        {
            IReadOnlyList<Type> readable = ColumnCodecRegistry.Default.Resolve(column.TypeName, in context).ReadableElementTypes;
            throw new InvalidCastException(
                $"Column '{column.Name}' has type '{column.TypeName}', whose values cannot be read as {typeof(T)}. It reads as: {string.Join(", ", readable)}.");
        }

        return (IColumn<T>)projection(column);
    }

    /// <summary>The old <c>ClickHouseTcpTypes.CanRead</c>.</summary>
    public static bool CanRead(string clickHouseType, Type elementType)
    {
        ArgumentNullException.ThrowIfNull(elementType);
        ArgumentNullException.ThrowIfNull(clickHouseType);
        return LegacyColumnProjection.Offers(ColumnCodecRegistry.Default.Resolve(clickHouseType, ResolveContext.ForWrite), elementType);
    }

    /// <summary>The reference arm of the ReadAs tier: <see cref="ReadAs{T}"/>, read as the client arm reads.</summary>
    internal sealed class ReadAsArm : ClientArms.ReadAsArm
    {
        public ReadAsArm(string name)
            : base(name)
        {
        }

        protected override IColumn<T> View<T>(Block block) => LegacyColumnarRead.ReadAs<T>(block[0], block.Context);
    }
}
