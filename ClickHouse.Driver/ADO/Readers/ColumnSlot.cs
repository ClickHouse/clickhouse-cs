using System;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.ADO.Readers;

/// <summary>
/// Per-column storage for the reader's current row. One instance per wire column, allocated once per
/// <see cref="ClickHouseDataReader"/> and overwritten in place on every <see cref="ClickHouseDataReader.Read"/>.
///
/// <para>This replaces the shared <c>object[]</c> row buffer, whose every value-type cell was boxed once per
/// value per row by <see cref="ClickHouseType.Read(ExtendedBinaryReader)"/> — whether or not the caller ever
/// asked for that column. A slot decodes into strongly-typed storage instead, so the box happens only when
/// someone actually calls an untyped accessor (<see cref="GetBoxed"/>), and never at all for the typed ones.</para>
///
/// <para>Every slot must be observationally identical to the boxed path it replaces: <see cref="GetBoxed"/>
/// has to return exactly what <see cref="ClickHouseType.Read(ExtendedBinaryReader)"/> would have returned for
/// the same bytes, including <see cref="DBNull.Value"/> for a SQL NULL, and <see cref="Read"/> has to consume
/// exactly the same bytes.</para>
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
/// A non-nullable column whose type can decode straight into <typeparamref name="T"/>.
/// <typeparamref name="T"/> is always the column type's <see cref="ClickHouseType.FrameworkType"/>, so the
/// stored value is exactly what the boxed <see cref="ClickHouseType.Read(ExtendedBinaryReader)"/> produces.
/// </summary>
internal sealed class ValueSlot<T> : ColumnSlot
{
    // typeof(T) is a JIT-time constant per closed generic, so this static folds to a constant load (the same
    // trick ClickHouseDataReader.FieldValueDispatcher<T> uses). For a value-typed T the IsNull check below
    // then folds to `false` outright and the boxing conversion in it is never reached.
    private static readonly bool CanBeNull = !typeof(T).IsValueType;

    private readonly ITypedReader<T> typedReader;

    public T Value;

    public ValueSlot(ITypedReader<T> typedReader) => this.typedReader = typedReader;

    public override void Read(ExtendedBinaryReader reader) => Value = typedReader.ReadValue(reader);

    public override object GetBoxed() => Value;

    // A non-nullable column has no null marker on the wire, so the only way this can be null is a
    // reference-typed reader handing one back — none currently do, but the check keeps GetBoxed and IsNull
    // agreeing with the boxed path if one ever did.
    public override bool IsNull => CanBeNull && (object)Value is null;
}

/// <summary>
/// A <c>Nullable(T)</c> column: the decoded value plus a presence flag, so a NULL costs no object at all
/// (the boxed path allocated nothing for it either — it returned the <see cref="DBNull.Value"/> singleton —
/// but it did box every non-null cell).
/// </summary>
internal sealed class NullableSlot<T> : ColumnSlot
{
    private readonly ITypedReader<T> typedReader;

    public T Value;

    public bool HasValue;

    public NullableSlot(ITypedReader<T> typedReader) => this.typedReader = typedReader;

    public override void Read(ExtendedBinaryReader reader)
    {
        // Byte-identical to NullableType.Read: a marker > 0 means NULL, and in that case the underlying type
        // wrote nothing, so nothing more is consumed.
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
/// <see cref="ClickHouseType.FrameworkType"/> — composites (Array, Tuple, Map, Nested), the polymorphic types
/// (Variant, Dynamic, JSON), geo types, and so on. Byte-for-byte and value-for-value the pre-slot behaviour.
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
