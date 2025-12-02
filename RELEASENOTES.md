v0.9.0
---

**Breaking Changes:**
 * FixedString is now returned as byte[] rather than String. FixedStrings are not necessarily valid UTF-8 strings, and the string transformation caused loss of information in some cases. Use Encoding.UTF8.GetString() on the resulting byte[] array to emulate the old behavior. String can still be used as a parameter or when inserting using BulkCopy into a FixedString column. When part of a json object, FixedString is still returned as a string.
 * Removed obsolete MySQL compatibility mapping TIME -> Int64.
 * Json serialization of bool arrays now uses the Boolean type instead of UInt8 (it is now consistent with how bool values outside arrays were handled).
 * GEOMETRY is no longer an alias for String.

**New Features/Improvements:**
 * Added support for BFloat16. It is converted to and from a 32-bit float.
 * Added support for Time and Time64, which are converted to and from TimeSpan. The types are available since ClickHouse 25.6 and using them requires the enable_time_time64_type flag to be set.
 * The Dynamic type now offers full support for all underlying types.
 * Added support for LineString and MultiLineString geo types.
 * Added support for the Geometry type, which can hold any geo subtype (Point, Ring, LineString, Polygon, MultiLineString, MultiPolygon). Available since ClickHouse 25.11. Requires allow_suspicious_variant_types to be set to 1.
 * Json support has been improved in many ways:
   * Now supports parsing Json that includes Maps; they are read into JsonObjects.
   * Added support for decoding BigInteger types, UUID, IPv4, IPv6, and ClickHouseDecimal types (they are handled as strings).
   * Expanded binary parsing to cover all types.
   * Improved handling of numeric types when writing Json using BulkCopy: now properly detects and preserves Int32/In64 in addition to double (previously all numeric types were handled as double).
   * Parsing null values in arrays is now handled properly.
 * ClickHouseConnection.ConnectionString can now be set after creating the connection, to support cases where passing the connection string to the constructor is not possible.
 * ClickHouseConnection.CreateCommand() now has an optional argument for the command text.
 * Fixed a NullReferenceException when adding a parameter with null value and no provided type. The driver now simply sends '\N' (null value special character) when encountering this scenario. 

**Bug Fixes:**
 * Fixed a bug where serializing to json with an array of bools with both true and false elements would fail.
