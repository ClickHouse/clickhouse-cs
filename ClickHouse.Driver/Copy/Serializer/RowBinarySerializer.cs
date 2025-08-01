using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Copy.Serializer;

internal class RowBinarySerializer : IRowSerializer
{
    public void Serialize(object[] row, ClickHouseType[] types, ExtendedBinaryWriter writer)
    {
        for (int col = 0; col < row.Length; col++)
        {
            types[col].Write(writer, row[col]);
        }
    }
}
