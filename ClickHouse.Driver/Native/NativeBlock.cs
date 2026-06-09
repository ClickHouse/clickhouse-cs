using ClickHouse.Driver.Types;

namespace ClickHouse.Driver.Native;

/// <summary>
/// A decoded Native-format block: the column schema (names + types) plus the column data in
/// column-major order. A header block has <see cref="RowCount"/> == 0 but a non-empty
/// <see cref="Names"/>/<see cref="Types"/>; an end-of-data marker block has zero columns.
/// </summary>
internal sealed class NativeBlock
{
    public string[] Names { get; init; }

    public ClickHouseType[] Types { get; init; }

    public int RowCount { get; init; }

    /// <summary>Column-major data: <c>Columns[col][row]</c>. Length equals <see cref="Names"/>.Length.</summary>
    public object[][] Columns { get; init; }

    public bool IsEmptyMarker => Names is null || Names.Length == 0;
}
