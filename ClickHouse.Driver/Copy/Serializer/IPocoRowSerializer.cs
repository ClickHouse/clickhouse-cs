using System;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Copy.Serializer;

/// <summary>
/// Serializes a single POCO row into ClickHouse binary format.
/// Mirrors <see cref="IRowSerializer"/> but reads values from a typed instance via compiled getters
/// instead of an <c>object[]</c>.
/// </summary>
internal interface IPocoRowSerializer
{
    /// <summary>
    /// Writes one row to the binary stream.
    /// </summary>
    /// <typeparam name="T">The POCO type.</typeparam>
    /// <param name="row">The POCO instance to serialize.</param>
    /// <param name="getters">Compiled property accessors, ordered to match <paramref name="types"/>.</param>
    /// <param name="types">The ClickHouse column types for each property.</param>
    /// <param name="writer">The binary writer to write to.</param>
    void Serialize<T>(T row, Func<T, object>[] getters, ClickHouseType[] types, ExtendedBinaryWriter writer);
}
