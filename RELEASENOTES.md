v0.8.0

Breaking Changes:

New Features:
 * Optimized response header parsing.
 * Added list type conversion, so List<T> can now be passed to the library (converts to Array() in ClickHouse). Thanks to @jorgeparavicini.

Bug fixes:
 * Fixed a crash when processing a tuple with an enum in it.
