* Added `Tuple` columns of 8 to 256 elements to the TCP client, for reads and writes. Such a tuple reads as the type
  that C# gives a tuple literal of that length, `ValueTuple<T1, ..., T7, TRest>`, so
  `(int, int, int, int, int, int, int, string, string)` reads a `Tuple` of nine elements.
