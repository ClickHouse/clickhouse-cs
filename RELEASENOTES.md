v0.8.2
---

**Breaking Changes:**
 * Removed obsolete MySQL compatibility mapping TIME -> Int64.

**New Features/Improvements:**
 * Added support for BFloat16. It is converted to and from a 32-bit float.
 * Added support for Time and Time64, which are converted to and from TimeSpan. The types are available since ClickHouse 25.6 and using them requires the enable_time_time64_type flag to be set.