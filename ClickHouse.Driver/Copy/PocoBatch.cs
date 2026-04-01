using System;
using System.Buffers;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Copy;

internal struct PocoBatch<T> : IDisposable
{
    public T[] Rows;
    public int Size;
    public string Query;
    public ClickHouseType[] Types;

    public void Dispose()
    {
        if (Rows != null)
        {
            ArrayPool<T>.Shared.Return(Rows, true);
            Rows = default;
        }
    }
}
