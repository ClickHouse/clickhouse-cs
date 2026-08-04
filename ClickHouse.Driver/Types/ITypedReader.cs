using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Types;

/// <summary>
/// Implemented by a <see cref="ClickHouseType"/> that can deserialize <typeparamref name="T"/> without
/// boxing, for the box-free read paths (POCO materialization and the reader's column slots).
///
/// A type may implement this for several <typeparamref name="T"/> when one column has more than one CLR
/// representation — a DateTime column as <see cref="System.DateTime"/>/<see cref="System.DateTimeOffset"/>/
/// <see cref="System.DateOnly"/>, a String column as <see cref="string"/>/<c>byte[]</c>. Every
/// <see cref="ReadValue"/> must consume exactly the bytes
/// <see cref="ClickHouseType.Read(ExtendedBinaryReader)"/> would.
/// </summary>
/// <typeparam name="T">The exact CLR type read without boxing.</typeparam>
internal interface ITypedReader<T> : ITypedReader
{
    T ReadValue(ExtendedBinaryReader reader);
}

/// <summary>
/// Non-generic base, so "can this type read box-free at all?" is a plain type test rather than an
/// interface-list walk. <see cref="ClickHouse.Driver.ADO.Readers.ColumnSlotFactory"/> asks that for every
/// column, and must ask it before touching <see cref="ClickHouseType.FrameworkType"/>.
/// </summary>
internal interface ITypedReader
{
}
