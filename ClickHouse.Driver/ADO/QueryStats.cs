﻿namespace ClickHouse.Driver.ADO;

#pragma warning disable SA1313 // Parameter names should begin with lower-case letter

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
