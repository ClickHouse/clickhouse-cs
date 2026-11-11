* Added support for trimming and native AOT to the HTTP client on .NET 8.0 and later. Under native AOT, most
  Map and Tuple columns read as `Dictionary<object, object>` and `ITuple`; see the Native AOT section of the
  HTTP client docs.
