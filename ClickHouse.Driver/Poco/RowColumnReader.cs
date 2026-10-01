using ClickHouse.Driver.ADO.Readers;
using ClickHouse.Driver.Formats;

namespace ClickHouse.Driver.Poco;

/// <summary>
/// Reads one wire column of the current row and assigns it to <paramref name="instance"/>.
///
/// The converter and the raw column type names are parameters rather than captured constants because the
/// delegates are cached on the client-wide <see cref="PocoTypeRegistry"/>, while a converter can be supplied
/// per query (<c>QueryOptions.ReadValueConverter</c>) and the raw type names are the reader's own strings.
/// Delegates built for a converter-free reader ignore both.
/// </summary>
/// <param name="reader">The row stream, positioned at this column.</param>
/// <param name="instance">The row being materialized.</param>
/// <param name="converter">The reader's value converter; null when the reader has none.</param>
/// <param name="columnTypeNames">Raw server-sent type strings, in wire order; null when there is no converter.</param>
internal delegate void RowColumnReader<in T>(
    ExtendedBinaryReader reader,
    T instance,
    IReadValueConverter converter,
    string[] columnTypeNames)
    where T : class;
