using System.Runtime.CompilerServices;

namespace ClickHouse.Driver.Utility;

internal class LargeTuple : ITuple
{
    private readonly object[] items;

    public LargeTuple(params object[] items)
    {
        this.items = items;
    }

    public object this[int index] => items[index];

    public int Length => items.Length;
}
