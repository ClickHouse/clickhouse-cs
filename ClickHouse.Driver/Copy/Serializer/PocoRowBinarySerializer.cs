using System;
using ClickHouse.Driver.Formats;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Copy.Serializer;

internal class PocoRowBinarySerializer : IPocoRowSerializer
{
    public void Serialize<T>(T row, Func<T, object>[] getters, ClickHouseType[] types, ExtendedBinaryWriter writer)
    {
        for (int col = 0; col < getters.Length; col++)
        {
            types[col].Write(writer, getters[col](row));
        }
    }
}
