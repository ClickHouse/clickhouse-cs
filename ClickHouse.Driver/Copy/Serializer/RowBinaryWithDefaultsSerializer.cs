using ClickHouse.Driver.Constraints;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Copy.Serializer;

// https://clickhouse.com/docs/reference/formats/RowBinary/RowBinaryWithDefaults
internal class RowBinaryWithDefaultsSerializer : IRowSerializer
{
    public void Serialize(object[] row, ClickHouseType[] types, ExtendedBinaryWriter writer)
    {
        for (int col = 0; col < row.Length; col++)
        {
            if (row[col] is DBDefault)
            {
                writer.Write((byte)1);
            }
            else
            {
                writer.Write((byte)0);
                types[col].Write(writer, row[col]);
            }
        }
    }
}
