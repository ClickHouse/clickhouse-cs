using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Implemented by a <see cref="ClickHouseType"/> that can serialize a value of the CLR type
/// <typeparamref name="T"/> without boxing, for the POCO binary-insert fast path.
///
/// The type's boxed <see cref="ClickHouseType.Write(ExtendedBinaryWriter, object)"/> override must
/// delegate to <see cref="WriteValue"/> after unboxing/coercing, so the two paths are byte-identical
/// by construction and <see cref="WriteValue"/> is the single source of the serialization logic.
/// </summary>
/// <typeparam name="T">The exact CLR type this type can write without boxing (e.g. <see cref="System.Guid"/>).</typeparam>
internal interface ITypedWriter<T>
{
    void WriteValue(ExtendedBinaryWriter writer, T value);
}
