using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Implemented by a <see cref="ClickHouseType"/> that can serialize a value of the CLR type
/// <typeparamref name="T"/> without boxing, for the POCO binary-insert fast path (issue #505).
///
/// The type's boxed <see cref="ClickHouseType.Write(ExtendedBinaryWriter, object)"/> override must
/// delegate to <see cref="WriteValue"/> after unboxing/coercing, so the two paths are byte-identical
/// by construction and <see cref="WriteValue"/> is the single source of the serialization logic.
///
/// The interface is intentionally <b>invariant</b> in <typeparamref name="T"/>: the fast path fires
/// only when a POCO property's CLR type is exactly <typeparamref name="T"/>
/// (see <see cref="ClickHouse.Driver.Poco.PocoWriteExpressionFactory"/>), which is what guarantees the emitted bytes match the
/// boxed path's coercion rules.
/// </summary>
/// <typeparam name="T">The exact CLR type this type can write without boxing (e.g. <see cref="System.Guid"/>).</typeparam>
internal interface ITypedWriter<T>
{
    void WriteValue(ExtendedBinaryWriter writer, T value);
}
