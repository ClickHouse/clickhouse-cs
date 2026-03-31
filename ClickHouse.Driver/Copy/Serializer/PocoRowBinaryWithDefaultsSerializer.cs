using System;
using ClickHouse.Driver.Constraints;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Copy.Serializer;

// https://clickhouse.com/docs/en/interfaces/formats#rowbinarywithdefaults
internal class PocoRowBinaryWithDefaultsSerializer : IPocoRowSerializer
{
    public void Serialize<T>(T row, Func<T, object>[] getters, ClickHouseType[] types, ExtendedBinaryWriter writer)
    {
        for (int col = 0; col < getters.Length; col++)
        {
            var value = getters[col](row);

            if (value is DBDefault)
            {
                writer.Write((byte)1);
            }
            else
            {
                writer.Write((byte)0);
                types[col].Write(writer, value);
            }
        }
    }
}
