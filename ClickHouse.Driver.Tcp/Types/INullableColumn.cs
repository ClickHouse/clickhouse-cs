using System;

namespace ClickHouse.Driver.Tcp;

/// <summary>
/// Exposes a decoded <c>Nullable(T)</c> as a dense inner column and null map. Row <c>i</c> is NULL when
/// <c>NullMap[i] != 0</c>; the corresponding inner value is then only a placeholder. The wrapper does not
/// implement the inner type's specialized interfaces. Both members borrow storage from the owning block. This
/// identifies the wire <c>Nullable(T)</c> layout, not every column whose values may be null.
/// </summary>
public interface INullableColumn : IColumn
{
    /// <summary>
    /// The dense inner column: one decoded value per row, with an arbitrary placeholder at the rows the null-map
    /// marks null (the wire carries a value there too, so its content is meaningless — always consult
    /// <see cref="NullMap"/> before reading a row). A borrowed view valid only while the owning block is alive —
    /// it is the block's to dispose, never the caller's.
    /// </summary>
    IColumn Inner { get; }

    /// <summary>
    /// The per-row null-map: a non-zero byte marks the row null. One entry per row, aligned with
    /// <see cref="Inner"/>. A borrowed span valid only while the owning block is alive.
    /// </summary>
    ReadOnlySpan<byte> NullMap { get; }
}

/// <summary>
/// Adds typed access to the inner column. <typeparamref name="T"/> is the inner type, not its nullable form; for
/// example, <c>Nullable(Int32)</c> implements <c>INullableColumn&lt;int&gt;</c>. Use the non-generic interface when
/// the inner type is not known.
/// </summary>
/// <typeparam name="T">The inner (non-nullable) element type; <see cref="Inner"/> is a column of these.</typeparam>
public interface INullableColumn<T> : INullableColumn
{
    /// <summary>The dense inner column, typed. See <see cref="INullableColumn.Inner"/> for the layout and the borrowing rule.</summary>
    new IColumn<T> Inner { get; }

    /// <inheritdoc/>
    IColumn INullableColumn.Inner => Inner;
}
