using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace ClickHouse.Driver.Tcp.Types;

/// <summary>
/// An <c>Enum8</c>/<c>Enum16</c> column: the decoded ordinals, plus the type's declared members so the labels are
/// reachable without re-parsing the type string. The values surface as the raw ordinal, exactly as the underlying
/// fixed-width column decoded them — the wrapper adds <see cref="IEnumColumn"/> and forwards everything else,
/// including the contiguous span the fixed-width writer blits from.
/// </summary>
/// <typeparam name="T">The underlying signed integer (<see cref="sbyte"/> for <c>Enum8</c>, <see cref="short"/> for <c>Enum16</c>).</typeparam>
internal sealed class EnumColumn<T> : IColumn<T>, ISpanColumn<T>, IStoredValuesColumn, IEnumColumn
    where T : unmanaged
{
    private readonly IColumn<T> ordinals;
    private readonly EnumMemberTable members;

    /// <summary>Initializes a column over the decoded ordinals, which it takes ownership of.</summary>
    /// <param name="ordinals">The decoded ordinal column.</param>
    /// <param name="members">The enum type's declared members.</param>
    public EnumColumn(IColumn<T> ordinals, EnumMemberTable members)
    {
        this.ordinals = ordinals ?? throw new ArgumentNullException(nameof(ordinals));
        this.members = members ?? throw new ArgumentNullException(nameof(members));
    }

    /// <inheritdoc/>
    public string Name => ordinals.Name;

    /// <inheritdoc/>
    public string TypeName => ordinals.TypeName;

    /// <inheritdoc/>
    public int RowCount => ordinals.RowCount;

    /// <inheritdoc/>
    public ReadOnlySpan<T> Values => ordinals.Values;

    /// <inheritdoc/>
    ReadOnlySpan<T> ISpanColumn<T>.Span => ordinals.Values;

    /// <inheritdoc/>
    public IReadOnlyList<KeyValuePair<string, long>> Members => members.Members;

    /// <inheritdoc/>
    public T this[int row] => ordinals[row];

    /// <inheritdoc/>
    public object GetValue(int row) => ordinals.GetValue(row);

    /// <inheritdoc/>
    public string GetLabel(int row) => members.Label(ToInt64(ordinals[row]));

    /// <inheritdoc/>
    public bool TryGetLabel(long ordinal, out string label) => members.TryGetLabel(ordinal, out label);

    /// <inheritdoc/>
    public bool TryGetOrdinal(string label, out long ordinal) => members.TryGetOrdinal(label, out ordinal);

    /// <inheritdoc/>
    public void Dispose() => ordinals.Dispose();

    // Widen the storage type to long. The typeof comparisons fold at JIT time, so this is a sign extension.
    private static long ToInt64(T value)
    {
        if (typeof(T) == typeof(sbyte))
        {
            return Unsafe.As<T, sbyte>(ref value);
        }

        if (typeof(T) == typeof(short))
        {
            return Unsafe.As<T, short>(ref value);
        }

        throw new NotSupportedException($"An enum column cannot be stored as {typeof(T)}.");
    }
}
