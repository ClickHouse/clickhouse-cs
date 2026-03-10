namespace ClickHouse.Driver.ADO;

#pragma warning disable SA1313 // Parameter names should begin with lower-case letter

/// <summary>
/// Contains query execution statistics returned by ClickHouse in the X-ClickHouse-Summary header.
/// </summary>
/// <param name="ReadRows">Number of rows read during query execution.</param>
/// <param name="ReadBytes">Number of bytes read during query execution.</param>
/// <param name="WrittenRows">Number of rows written.</param>
/// <param name="WrittenBytes">Number of bytes written.</param>
/// <param name="TotalRowsToRead">Total number of rows to be read.</param>
/// <param name="ResultRows">Number of rows in the final result set.</param>
/// <param name="ResultBytes">Number of bytes in the final result.</param>
/// <param name="ElapsedNs">Query execution time in nanoseconds.</param>
public record QueryStats(
    long ReadRows,
    long ReadBytes,
    long WrittenRows,
    long WrittenBytes,
    long TotalRowsToRead,
    long ResultRows,
    long ResultBytes,
    long ElapsedNs);

#pragma warning restore SA1313 // Parameter names should begin with lower-case letter
