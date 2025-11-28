v0.8.2
---

**Breaking Changes:**
 * Removed obsolete MySQL compatibility mapping TIME -> Int64.
 * Json serialization of bool arrays now uses the Boolean type instead of UInt8 (it is now consistent with how bool values outside arrays were handled).

**New Features/Improvements:**
 * Added support for BFloat16. It is converted to and from a 32-bit float.
 * Added support for Time and Time64, which are converted to and from TimeSpan. The types are available since ClickHouse 25.6 and using them requires the enable_time_time64_type flag to be set.
 * The Dynamic type now offers full support for all underlying types.
 * Json support has been improved in many ways:
   * Now supports parsing Json that includes Maps; they are read into JsonObjects.
   * Added support for decoding BigInteger types, UUID, IPv4, IPv6, and ClickHouseDecimal types (they are handled as strings).
   * Expanded binary parsing to cover all types.
   * Improved handling of numeric types when writing Json using BulkCopy: now properly detects and preserves Int32/In64 in addition to double (previously all numeric types were handled as double).
   * Parsing null values in arrays is now handled properly.
 * ClickHouseConnection.ConnectionString can now be set after creating the connection, to support cases where passing the connection string to the constructor is not possible.
 * ClickHouseConnection.CreateCommand() now has an optional argument for the command text.

**Bug Fixes:**
 * Fixed a bug where serializing to json with an array of bools with both true and false elements would fail.