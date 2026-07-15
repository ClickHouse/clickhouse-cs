namespace ClickHouse.Driver.Compression;

/// <summary>
/// LZ4 compression level for <see cref="Lz4Compressor"/>. The numeric values match the levels of the
/// underlying LZ4 codec.
/// <para>
/// <see cref="Fast"/> is recommended for almost all inserts: the higher (<c>High*</c>/<c>Optimal*</c>)
/// levels trade markedly more CPU for very small reductions in payload size, and <see cref="Max"/> is
/// CPU-pathological. Prefer <see cref="Lz4Compressor.Default"/> unless you have measured a benefit.
/// </para>
/// </summary>
public enum Lz4Level
{
    /// <summary>Fast compression (level 0). The recommended default.</summary>
    Fast = 0,

    /// <summary>High compression, level 3.</summary>
    High3 = 3,

    /// <summary>High compression, level 4.</summary>
    High4 = 4,

    /// <summary>High compression, level 5.</summary>
    High5 = 5,

    /// <summary>High compression, level 6.</summary>
    High6 = 6,

    /// <summary>High compression, level 7.</summary>
    High7 = 7,

    /// <summary>High compression, level 8.</summary>
    High8 = 8,

    /// <summary>High compression, level 9.</summary>
    High9 = 9,

    /// <summary>Optimal compression, level 10.</summary>
    Optimal10 = 10,

    /// <summary>Optimal compression, level 11.</summary>
    Optimal11 = 11,

    /// <summary>Maximum compression, level 12. CPU-pathological; avoid for inserts.</summary>
    Max = 12,
}
