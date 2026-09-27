using System.IO;
using ClickHouse.Driver.Compression;

namespace ClickHouse.Driver.Copy.Serializer;

internal interface IBatchSerializer
{
    /// <summary>
    /// Writes the batch into <paramref name="stream"/>. The <c>INSERT</c> statement is written ahead of
    /// the rows only when <paramref name="queryPlacement"/> is <see cref="InsertQueryPlacement.Body"/>;
    /// with <see cref="InsertQueryPlacement.Url"/> the caller puts it in the URL and the body carries
    /// rows alone.
    /// </summary>
    void Serialize(Batch batch, Stream stream, IClickHouseCompressor compressor, InsertQueryPlacement queryPlacement);
}
