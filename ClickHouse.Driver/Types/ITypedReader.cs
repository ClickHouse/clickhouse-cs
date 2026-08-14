using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Implemented by a <see cref="ClickHouseType"/> that can deserialize a value of the CLR type
/// <typeparamref name="T"/> without boxing, for the POCO read (materialization) fast path.
///
/// A type may implement this for more than one <typeparamref name="T"/> when it can produce several CLR
/// representations of the same column (e.g. a DateTime column as <see cref="System.DateTime"/>,
/// <see cref="System.DateTimeOffset"/> or <see cref="System.DateOnly"/>). Such implementations are explicit,
/// since they differ only by return type. The boxed
/// <see cref="ClickHouseType.Read(ExtendedBinaryReader)"/> reads the same bytes and returns the canonical
/// representation, so every <see cref="ReadValue"/> is byte-identical to it by construction.
/// </summary>
/// <typeparam name="T">The exact CLR type this type can read without boxing (e.g. <see cref="System.Guid"/>).</typeparam>
internal interface ITypedReader<T>
{
    T ReadValue(ExtendedBinaryReader reader);
}
