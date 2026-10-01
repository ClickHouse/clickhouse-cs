using System;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.ADO.Readers;

/// <summary>
/// Per-column storage for the reader's current row, replacing the shared <c>object[]</c> row buffer that boxed
/// every value-type cell of every row whether the caller asked for it or not. One instance per wire column,
/// allocated once per <see cref="ClickHouseDataReader"/> and overwritten in place on every
/// <see cref="ClickHouseDataReader.Read"/>; the box now happens only in <see cref="GetBoxed"/>.
///
/// <para>Every slot must be observationally identical to the boxed path it replaces: <see cref="Read"/> has to
/// consume exactly the bytes <see cref="ClickHouseType.Read(ExtendedBinaryReader)"/> would, and
/// <see cref="GetBoxed"/> has to return exactly what it would have returned — same CLR type, and
/// <see cref="DBNull.Value"/> for a SQL NULL.</para>
/// </summary>
internal abstract class ColumnSlot
{
    /// <summary>Decodes this column's bytes from the row stream into typed storage.</summary>
    public abstract void Read(ExtendedBinaryReader reader);

    /// <summary>
    /// Boxes the current value on demand for the untyped path. Returns <see cref="DBNull.Value"/> for a SQL
    /// NULL, matching <see cref="NullableType.Read(ExtendedBinaryReader)"/>.
    /// </summary>
    public abstract object GetBoxed();

    /// <summary>Null check that neither materializes nor boxes the value.</summary>
    public abstract bool IsNull { get; }
}

/// <summary>
/// A non-nullable column whose type can decode straight into <typeparamref name="T"/>, which is always the
/// column's <see cref="ClickHouseType.FrameworkType"/> — so the stored value is exactly what the boxed
/// <see cref="ClickHouseType.Read(ExtendedBinaryReader)"/> produces.
/// </summary>
internal sealed class ValueSlot<T> : ColumnSlot
{
    // typeof(T) is a JIT-time constant per closed generic, so this folds to a constant load; for a value-typed
    // T the IsNull check below then folds to `false` and never reaches its boxing conversion.
    private static readonly bool CanBeNull = !typeof(T).IsValueType;

    private readonly ITypedReader<T> typedReader;

    public T Value;

    public ValueSlot(ITypedReader<T> typedReader) => this.typedReader = typedReader;

    public override void Read(ExtendedBinaryReader reader) => Value = typedReader.ReadValue(reader);

    public override object GetBoxed() => Value;

    // A non-nullable column has no wire null marker, so this can only be null if a reference-typed reader hands
    // one back. None currently do; the check keeps IsNull agreeing with GetBoxed if one ever did.
    public override bool IsNull => CanBeNull && (object)Value is null;
}

/// <summary>
/// A <c>Nullable(T)</c> column: the decoded value plus a presence flag, so a non-null cell no longer boxes
/// (a NULL never allocated — the boxed path returned the <see cref="DBNull.Value"/> singleton).
/// </summary>
internal sealed class NullableSlot<T> : ColumnSlot
{
    private readonly ITypedReader<T> typedReader;

    public T Value;

    public bool HasValue;

    public NullableSlot(ITypedReader<T> typedReader) => this.typedReader = typedReader;

    public override void Read(ExtendedBinaryReader reader)
    {
        // Byte-identical to NullableType.Read: marker > 0 means NULL, and the underlying type then wrote
        // nothing, so nothing more is consumed.
        if (reader.ReadByte() > 0)
        {
            HasValue = false;
            Value = default; // don't pin the previous row's value (a string/byte[] would stay reachable)
        }
        else
        {
            Value = typedReader.ReadValue(reader);
            HasValue = true;
        }
    }

    // Boxes the *underlying* value, never a Nullable<T> — NullableType.Read did the same, and
    // GetFieldValue<long> on a Nullable(Int64) column depends on finding a boxed long here.
    public override object GetBoxed() => HasValue ? (object)Value : DBNull.Value;

    public override bool IsNull => !HasValue;
}

/// <summary>
/// Fallback for any column with no <see cref="ITypedReader{T}"/> for its own
/// <see cref="ClickHouseType.FrameworkType"/> — composites, the polymorphic types, geo, and so on. Byte-for-byte
/// and value-for-value the pre-slot behaviour.
/// </summary>
internal sealed class BoxedSlot : ColumnSlot
{
    private readonly ClickHouseType type;

    public object Value;

    public BoxedSlot(ClickHouseType type) => this.type = type;

    public override void Read(ExtendedBinaryReader reader) => Value = type.Read(reader);

    public override object GetBoxed() => Value;

    public override bool IsNull => Value is null || Value is DBNull;
}
