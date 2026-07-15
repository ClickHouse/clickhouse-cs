using System.IO;
using ClickHouse.Driver.Compression;

namespace ClickHouse.Driver.Copy.Serializer;

internal interface IBatchSerializer
{
    void Serialize(Batch batch, Stream stream, IClickHouseCompressor compressor);
}
