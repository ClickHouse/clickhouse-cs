using System;
using System.Collections.Generic;
using ClickHouse.Driver.Copy;

namespace ClickHouse.Driver;

/// <summary>
/// Options for binary insert operations that can override client-level defaults.
/// </summary>
public sealed class InsertOptions : QueryOptions
{
    /// <summary>
    /// Gets or sets the number of rows per batch. Default is 100,000.
    /// </summary>
    public int BatchSize { get; init; } = 100_000;

    /// <summary>
    /// Gets or sets the maximum number of parallel batch insert operations. Default is 1.
    /// </summary>
    public int MaxDegreeOfParallelism { get; init; } = 1;

    /// <summary>
    /// Gets or sets the row binary format to use. Default is RowBinary.
    /// </summary>
    public RowBinaryFormat Format { get; init; } = RowBinaryFormat.RowBinary;
}
