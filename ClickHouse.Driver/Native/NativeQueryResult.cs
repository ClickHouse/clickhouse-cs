using System.Collections.Generic;
using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Native;

/// <summary>
/// The fully-buffered result of a Native-protocol query: the column schema plus all rows in
/// row-major order. Buffering the whole result is an MVP simplification; streaming can be added later.
/// </summary>
internal sealed class NativeQueryResult
{
    public NativeQueryResult(string[] names, ClickHouseType[] types, IReadOnlyList<object[]> rows)
    {
        Names = names;
        Types = types;
        Rows = rows;
    }

    public string[] Names { get; }

    public ClickHouseType[] Types { get; }

    public IReadOnlyList<object[]> Rows { get; }
}
