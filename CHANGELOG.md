Unreleased
---

**Bug Fixes:**
* **Breaking Change**: Fixed a `JSON` column with overlapping paths silently losing data. ClickHouse accepts a column which declares a path both as a value and as the parent of another path — for example `JSON(a Int64, a.b Int64)` — and sends both paths in every row, so the row renders as a document with a duplicate key (`{"a":0,"a":{"b":7}}`). A `JsonObject` cannot hold two values for one key, so the driver silently kept whichever path the server sent last: reading `{"a":{"b":7}}` back out of that column returned `{"a":0}` and the `7` was gone. The same loss hit a `Map` path overlapped by a deeper path (`JSON(a Map(String, Int64))` holding `{"a":{"b":1},"a.b":7}` returned `{"a":{"b":7}}`, dropping the map's own entry), except that there the two values were merged into one object rather than one replacing the other. A row where both sides of such an overlap hold a value now throws a `SerializationException` naming the two paths. With non-`Nullable` paths the driver cannot tell a stored `0` from an absent path, so no rule can recover the real value — the choice is between a wrong answer and a clear failure. A side which holds nothing is not a collision: a null, an empty object, and an all-null subtree all give way to the side which has the data, so `Nullable` overlaps continue to read as before. Use `JsonReadMode.String` to read such a column as the server's JSON text, duplicate key included.

v1.4.0
---

**Breaking Changes:**
* **Query responses are now compressed with zstd instead of gzip by default.** The driver advertises `zstd, lz4, gzip, deflate`, and ClickHouse's fixed preference order resolves that to zstd. Bodies are decoded transparently as before, so no code change is needed.
* **Binary inserts are now compressed with zstd instead of gzip by default.** `InsertOptions.Compressor` defaults to `ZstdCompressor.Default` (level 3) rather than `GZipCompressor.Default` (gzip, fastest). You can control this via `InsertOptions`.

* **If you set `AcceptEncoding` and then read a raw result, you now get compressed bytes.** `ExecuteRawResultAsync`, `PostStreamAsync` and `InsertRawStreamAsync` hand back the response body exactly as the server sent it. Call `ClickHouseRawResult.ReadDecompressedStreamAsync()` to have the driver decompress it, or check `ContentEncoding` and decompress it yourself. Raw exports with no `AcceptEncoding` set are unaffected: plaintext before, plaintext now.
* **Reading a column value from `ClickHouseDataReader` with no current row now throws `InvalidOperationException`** — that is, before the first `Read()` or after `Read()` has returned `false`.

**New Features:**
* Pluggable binary-insert compression via `InsertOptions.Compressor` (`IClickHouseCompressor`). The presence of a compressor is the on/off switch: set it to `null` to send the payload uncompressed (useful over fast/local links where the compression CPU outweighs the bandwidth savings). GZip, Brotli, LZ4, and ZStd compressors are available.
  - **Tuning guidance:** the best codec depends on where the server is. Over a fast/local link, compression is pure overhead, so `Compressor = null` is worth trying. Over a remote/cloud connection the payload reduction dominates, depending on bandwidth.
* Built-in LZ4 codec for binary inserts (`Lz4Compressor`; HTTP `Content-Encoding: lz4` plus the native-protocol block path). LZ4 now ships **in the core driver with no third-party runtime dependency**.
* Built-in ZSTD codec (`ZstdCompressor`; HTTP `Content-Encoding: zstd` plus the native-protocol block path).
* You can now choose how ClickHouse compresses query responses, via `AcceptEncoding` — client-wide (`ClickHouseClientSettings.AcceptEncoding`), in the connection string (`AcceptEncoding=lz4`), or for one query (`QueryOptions.AcceptEncoding`). `lz4`, `gzip`, `deflate` and `br` are all decompressed for you; `identity` turns response compression off. Previously only `gzip` and `deflate` worked: asking for `lz4` or `br` returned unreadable data, and `identity` did not actually switch compression off. To decompress a raw export, use the new `ClickHouseRawResult.ReadDecompressedStreamAsync()`.
* zstd responses are now decoded transparently, so `AcceptEncoding = "zstd"` works with every read API instead of failing as an unsupported codec.
* Added `MapReadMode` (connection string `MapReadMode=KeyValuePairs`, or `ClickHouseClientSettings.MapReadMode`), which reads `Map(K, V)` columns as `List<KeyValuePair<K, V>>` instead of `Dictionary<K, V>`. A ClickHouse map may repeat a key and a dictionary cannot, so the default representation silently drops pairs. Also accepted on the write path.

**Improvements:**
* A large number of performance improvements have been made in this version, significantly cutting heap allocations, reducing GC pressure, and improving speed. You will see the best results by using the POCO read and write methods, as they avoid boxing values.
* **A custom `HttpClient` no longer needs `AutomaticDecompression`.** The driver decompresses responses itself, so supplying your own `HttpClient` (or `IHttpClientFactory`) no longer risks unreadable data. If you do set `AutomaticDecompression`, prefer leaving it at its default of `None`.
* **Clearer failures around compression.** A response compressed with a codec the driver cannot read (e.g. `snappy`) now raises an error naming that codec and how to fix it, instead of failing as a confusing type-parse error. Server error messages are now readable when the response is compressed, rather than surfacing as binary.
* Lower GC pressure on binary inserts: the built-in compressors (`GZipCompressor`, `BrotliCompressor`, `Lz4Compressor`) no longer allocate a fresh 256 KiB `BufferedStream` buffer per batch on the large object heap. The buffering now rents its backing buffer from `ArrayPool<byte>`.
* Binary inserts (`InsertBinaryAsync`) now stream the serialized batch directly into the HTTP request body instead of buffering the whole payload. Requests are now sent with chunked transfer-encoding (no `Content-Length`).
* Removed a 4KiB/batch allocation in the binary-insert serializers (for writing the SQL query). Pooled buffers are now used instead.
* Faster multidimensional array binary inserts: rectangular multidimensional CLR arrays (`int[,]`, `double[,,]`, …) of fixed-width primitive leaves (`Int8/16/32/64`, `UInt8/16/32/64`, `Float32/64`, `Bool`) are now serialized by blitting each contiguous inner row in a single write.
* Reduced allocations on the binary write path. Writing an `Int128`/`UInt128`/`Int256`/`UInt256` value no longer allocates two temporary arrays per value, and a rectangular multidimensional array whose leaf is a fixed-width primitive behind a wire-transparent wrapper (`LowCardinality(Int32)`, `SimpleAggregateFunction(any, Int32)`, …) now takes the same blit fast path as a bare leaf instead of boxing every element. `Nullable(...)` leaves keep the per-element path. Output bytes are unchanged.
* Reduced allocations when writing `Decimal`/`Decimal128`/`Decimal256` values (binary inserts and parameters): the write path no longer allocates a `BigInteger.ToByteArray()` array plus a separate destination buffer per value.
* Mostly eliminated allocations in POCO binary inserts: value-type properties are now written through box-free delegate instead of boxing each value into `object` and unboxing it inside the type's `Write`. Note that if you use `RowBinaryWithDefaults`, the path is unchanged and still incurs the boxing cost. The change only applies to `RowBinary` (the default).
* Faster `Variant` write-type resolution: matching a value to its variant subtype now uses an O(1) hash lookup by runtime type instead of an O(n) linear scan for variants with 3 or more underlying types.
* Lower GC pressure when reading query responses: the reader no longer allocates a fresh `BufferedStream` buffer per query. The response read buffering now rents its backing buffer from `ArrayPool<byte>` and returns it when the reader is disposed. **The default `ReadBufferSize` has been raised from 8 KiB to 64 KiB** now that pooling removes the per-query allocation that motivated the small default (see v1.3.0), reducing read refills on large responses.
* Faster scalar reads from query responses: the response stream wrappers now implement `Stream.Read(Span<byte>)` directly instead of falling through to the base implementation, which rents and copies through a pooled array on every call, and mid-stream exception detection now records from below the read buffer so it observes one read per buffer refill rather than one per value decoded.
* Reduced per-query request-URI allocations: `ClickHouseUriBuilder.ToString()` now composes the URI into a thread-reused `StringBuilder` instead of allocating a `UriBuilder`, two dictionaries, and a LINQ-projected string per parameter. Cuts URI-assembly allocations ~65% on the common parameter-free path. The produced URI string and behavior are unchanged.
* Reduced per-row allocations when reading fixed-size binary columns. `Int128`/`Int256`/`UInt128`/`UInt256`, `Decimal128`/`Decimal256`, `IPv4`, and `IPv6` no longer allocate a temporary `byte[]` per value during deserialization.
* Reading an `Int128` or `UInt128` column into a native `Int128`/`UInt128` property no longer allocates a 16-byte array per value (40 bytes per value on 64-bit).
* Reduced the per-value allocations on the read and write paths for `UUID`, `Array(...)` and `Decimal` columns.
* Lower GC pressure when reading `Dynamic` columns: every value in a `Dynamic` column carries its own binary type header, so the driver decoded (and allocated) a fresh type descriptor per row. Stateless, parameterless types now decode to a shared immutable singleton instead, eliminating the per-row type allocation.
* Faster reading of `Tuple(...)` columns with up to 7 elements: `TupleType.Read` now constructs the `System.Tuple<...>` directly through a cached compiled factory instead of allocating two intermediate `object[]` buffers and invoking the constructor reflectively via `Activator.CreateInstance`.
* Faster reading of `Array` columns with common leaf element types (integers, floats, `Bool`, `Decimal`, date/time types, `UUID`, `String`, `Enum`, `FixedString`, and their `Nullable` forms): `ArrayType.Read` now fills a strongly-typed `T[]` through the array indexer instead of allocating via `Array.CreateInstance` and storing each element with reflective `Array.SetValue`.
* Reading `Map` columns now pre-sizes the result `Dictionary` to the known entry count.
* Reduced allocations in POCO reads (`client.QueryAsync<T>(...)`): scalar, `String` and `FixedString` columns are now materialized straight into the target property instead of through a boxed `object[]` row buffer, removing one box and one unbox per value-type property per row.
* `SimpleAggregateFunction(f, T)` columns now take the box-free fast path in `QueryAsync<T>`, as `LowCardinality(T)` already did. Previously they fell back to the slower boxed read.
* `QueryAsync<T>` can now bind one column to more than one CLR type, chosen by the property: `DateTime`/`DateTime64`/`Date` into `DateTime`, `DateTimeOffset` or `DateOnly`; `String`/`FixedString` into `string` or `byte[]`; `Decimal` into `decimal` or `ClickHouseDecimal`; `Enum8`/`Enum16` into `string` (the label) or `int` (the stored numeric value); and, on .NET 8+, `Int128`/`UInt128` into the native `System.Int128`/`System.UInt128` as well as `BigInteger`.
* `client.QueryAsync<T>(...)` can now read a `Map(K, V)` column into a `List<KeyValuePair<K, V>>` or `KeyValuePair<K, V>[]` POCO property, in addition to `Dictionary<K, V>`.
* With an `IReadValueConverter` configured, `client.QueryAsync<T>(...)` applies it per column on the overload that matches how the column was read: `ConvertValue<T>`, with `T` the property type, for a column read without boxing, and the boxed `ConvertValue` for a composite one.
* Reduced per-row allocations for `ClickHouseDataReader` typed accessors (`GetInt64`, `GetDouble`, `GetDateTime`, `GetGuid`, …), `GetFieldValue<T>`, `IsDBNull` and ORMs such as linq2db by decoding scalar columns into reusable typed slots instead of eagerly boxing every value-type cell. Allocations drop by roughly two thirds on a typical multi-column typed read and to zero per row for all-numeric typed reads.
* With an `IReadValueConverter` configured, `ClickHouseDataReader`'s typed accessors (`GetInt64`, `GetDouble`, `GetDecimal`, `GetString`, …) keep their reduced allocations: each reads its typed slot and converts through `ConvertValue<T>`, the overload `GetFieldValue<T>` uses. These accessors previously routed through `GetValue` and so through the boxed `ConvertValue`.
* Added `ClickHouseDataReader.TryGetEnumOrdinal(int ordinal, out int value)` for reading the underlying integer ordinal of an `Enum8`/`Enum16` (or `Nullable(Enum...)`) column.
* You can now pass `UseFormDataParameters` through the connection string. Added `UseFormDataParameters` property to `ClickHouseConnectionStringBuilder`.
* Reading `Map` columns now builds the result `Dictionary` through a cached compiled factory instead of a reflection invoke, removing the per-row constructor resolution that the pre-sizing change added.

**Deprecations:**
* `ClickHouseClient.MemoryStreamManager` is now `[Obsolete]`. Since binary inserts stream directly into the request body (see above), this property is no longer used and has no effect; it will be removed in a future version.

**Bug Fixes:**
* Fixed uncompressed binary inserts (`InsertOptions.Compressor = null`) inflating the HTTP request body. The request body is sent chunked, and the row serializer wrote straight to it, so every field write became its own HTTP chunk.
* Fixed binary inserts flushing the compression stream once per value written by copying a stream — binary-mode `JSON` POCOs, and `String`/`FixedString` values given as a `Stream`.
* Fixed `InsertOptions.WithColumnTypes()` and `InsertOptions.WithQueryId()` silently dropping some caller-set options (such as `AcceptEncoding`) when copying.
* Fixed POCO binary inserts writing `JSON` columns against the wrong typed-path hints when the same POCO type is inserted into two tables whose `JSON` columns declare different hints. The second insert reused the first table's cached write delegates, so values were written silently mis-typed.
* Fixed `ClickHouseServerException` carrying a blank `Message` and an `ErrorCode` of `-1` when the server — or an upstream component such as a load balancer or the ClickHouse Cloud edge — returned a non-2xx HTTP response with an empty (or whitespace-only) body. The exception now reports the HTTP status code and reason phrase, and uses the `X-ClickHouse-Exception-Code` response header as the error code when the server sets it. Non-empty error bodies are unaffected.
* Fixed `ClickHouseClient` leaking the query's `HttpResponseMessage` on the paths that fully consume it (`ExecuteNonQueryAsync`, `InsertBinaryAsync`) and on any request that fails. Paths that hand the response to the caller (`ExecuteReaderAsync`/`ExecuteRawResultAsync`/`InsertRawStreamAsync`/`PostStreamAsync`) are unchanged.
* Fixed `ClickHouseClient.InsertRawStreamAsync` disposing the supplied `Stream` twice when the request failed: the request message already disposes the content it carries, so the extra `Dispose()` in the failure path was redundant. The stream-taking write paths now document that the supplied stream is disposed once the request completes.
* Fixed `PingAsync` ignoring the `Path` connection setting: the ping request now targets `<Path>/ping`, so clients behind a reverse-proxy prefix no longer report a healthy server as unreachable.
* Fixed mid-stream server exceptions never surfacing on the streaming read path (`ExecuteReader`/`ExecuteReaderAsync`). A query that fails after the HTTP response is committed (for example a `throwIf` partway through a large result) now raises a `ClickHouseServerException` with the real server error, instead of a bare `HttpIOException` or `EndOfStreamException`.
* Fixed `ClickHouseCommand` returning wrong results (or a syntax error) for `CommandBehavior.SchemaOnly` and `CommandBehavior.SingleRow` when `CommandText` ended with a single-line comment (`--` / `#`) or a statement-terminating `;`.
* Fixed `ClickHouseDataReader.GetSchemaTable()` leaving `NumericScale` unset (`DBNull`) for `DateTime64(N)` and `Time64(N)` columns (including their `Nullable(...)` variants). The schema table now reports the fractional-seconds precision `N` in `NumericScale`, matching how `Decimal` columns are already reported.
* Fixed `DbConnection.GetSchema("Columns", ...)` not disposing the command it creates internally, which delayed the release of that command's cancellation-token source until garbage collection.
* Fixed `ClickHouseConnection.GetSchema("Columns", ...)` building invalid SQL when the table restriction is supplied without the database restriction (for example `[null, "functions"]`). The `WHERE` clause is now composed from the restrictions that are actually set, so filtering by table alone no longer fails with a server syntax error.
* Fixed `ClickHouseConnection.GetSchema("Columns", ...)` silently ignoring restriction values beyond the supported `database` and `table` positions.
* Fixed JSON typed paths whose names start with `max_dynamic_paths` or `max_dynamic_types` being mistaken for JSON settings and decoded as dynamic values.
* Fixed `JSON` columns being unreadable when a typed path name requires backtick quoting — for example ``JSON(`a b` Int64)`` or ``JSON(`a,b` Int64)``. Such a path made the whole query fail with `SerializationException: Unsupported path in JSON hint`, because the type parser split each hint on every space and did not treat backticks as quotes. Quoted path names (including ones containing spaces, commas, parentheses and escaped characters) are now parsed and unescaped correctly.
* Fixed reading a `JSON` column where a typed path holds `NULL`. The path was dropped from the returned `JsonObject` entirely, so `{"x": null}` came back as `{}` and callers could not tell "path not present in this row" from "path present but null"; for a nested typed path such as `JSON(a.b Nullable(Int64))` the whole parent subtree disappeared. Typed paths are now materialized with an explicit JSON null, matching the server's own JSON rendering. Dynamic (unhinted) paths are unchanged and stay absent, as the server also omits them.
* Fixed string values inside a `JSON` column being returned as base64 when `ReadStringsAsByteArrays = true`, and `Map(String, ...)` keys throwing `InvalidCastException`. String leaves inside a `JSON` column are now always decoded as UTF-8 text regardless of the setting — remove any base64 workaround; the setting is unchanged for ordinary `String`/`FixedString` columns.
* Fixed named `Tuple` and `Nested` columns being unreadable when an element name requires backtick quoting and contains a space — for example ``Tuple(`p q` Int64, r String)`` or ``Nested(`a b` Decimal(10, 2), c String)``. Reading such a column failed with `ArgumentException: Unknown type`, because the type parser split the element declaration on its first space and so cut the quoted name in half. The element name is now skipped as a whole before the name/type separator is located.
* Fixed the type parser building a self-referential node for an empty parameter list, such as the `Tuple()` column type that ClickHouse accepts and reports back in `system.columns`.
* Fixed enum type names rendering as invalid ClickHouse syntax. Enum labels are now quoted and escaped, and the declaration includes its closing parenthesis.
* Fixed HTTP query parameters mangling a `byte[]`/`ReadOnlyMemory<byte>` bound to `String`/`FixedString` (a `byte[]` was sent as the literal text `System.Byte[]`) and rejecting a `TimeOnly` bound to `Time`/`Time64`. Byte payloads are now escaped byte-for-byte, so data that is not valid UTF-8 round-trips losslessly; `TimeOnly` binds on both the HTTP and binary write paths, and infers as `Time64(7)` when no type hint is given.
* Fixed `{name:Type}` parameter type hints being mis-detected in queries containing `//` comments, nested block comments, backtick/double-quoted identifiers, backslash escapes or `$tag$` heredocs. A bare `#` no longer starts a comment (only `# ` and `#!` do).
* Fixed `{name:Type}` parameter type hints being dropped, or a hint being invented for a parameter that does not exist, when the query contains another `{` that is not a type hint — for example a `SETTINGS` map value such as `additional_table_filters = {'t': 'a > 0'}`. A dropped hint fell back to CLR-type inference, losing precision.
* Fixed ADO-style `@name` parameter placeholders being rewritten inside string literals, quoted identifiers, heredocs and comments, which corrupted values such as `'user@id'` into `'user{id:Int32}'`. Placeholders are now only replaced in code positions.
* Fixed a `$` inside an unquoted identifier being mistaken for the start of a `$tag$` heredoc. Everything up to the next occurrence of the same `$...$` text was skipped as heredoc body, silently dropping any `{name:Type}` type hint (and any ADO-style `@name` placeholder) in between — for example in `WITH 1 AS b$c$ SELECT {d:Date} AS v, b$c$ AS x`. A heredoc is now only recognized where a token starts, matching the server lexer.
* Fixed ADO-style `@name` placeholders not working when the parameter name contains a `$`, which ClickHouse accepts in a query parameter name: `@id$x` could not be bound at all (the name was interpolated into a regex, where `$` is an end-of-input anchor), and a shorter name won over a longer one, so with only `id` defined `SELECT @id$x` was silently rewritten into a different, still valid query that aliased the value as `$x` instead of being left for the server to reject. A `$` is now part of the placeholder name, matching the server lexer.

v1.3.0
---

**New Features:**
* **POCO reads**: stream query results directly into your own classes.
    - `ClickHouseClient.QueryAsync<T>(...)` returns `IAsyncEnumerable<T>`; rows are materialized lazily and the underlying reader is disposed when enumeration completes, faults, or stops early.
    - `ClickHouseDataReader.MapTo<T>()` materializes the current row into a registered POCO without advancing the reader.
    - **Registration**: use `RegisterPocoType<T>()`, it sets up both the insert and read mappings, validating both up front. `RegisterBinaryInsertType<T>()` is unchanged and remains insert-only for backwards compatibility.
    - **Type requirements**: a public parameterless constructor and at least one public property with a public non-init setter. `required` properties are supported.
    - **Column matching** is case-sensitive (`StringComparer.Ordinal`); missing result columns leave properties at their default value, extra result columns are ignored.
    - **No automatic conversions**: type mismatches throw `InvalidOperationException` with the POCO type, property, column, and returned CLR type. Static mismatches fail fast at first `MapTo<T>()` call (or first iteration of `QueryAsync<T>()`) before any rows are materialized.
    - **Registration diagnostics**: when a `LoggerFactory` is configured, `RegisterPocoType<T>()` / `RegisterBinaryInsertType<T>()` emit a `Debug`-level log (category `ClickHouse.Driver.Client`) listing which properties mapped to which columns and which were skipped and why.
* **Nested array parameters and multidimensional arrays** (issue #320): `Array(Array(T))` and deeper nestings are now supported end-to-end via parameterized queries, binary inserts, and bulk inserts. Both jagged CLR shapes (`T[][]`, `List<List<T>>`) and rectangular multidimensional CLR shapes (`T[,]`, `T[,,]`, …) are accepted on the write path. Reads return jagged `T[][]` via `GetValue`; callers who know their data is rectangular can use `reader.GetFieldValue<T[,]>(ordinal)` and the driver materialises the column as that CLR shape (throws `InvalidOperationException` on ragged data).
* **ValueTuple support on write path**: `System.ValueTuple` values (C# tuple literals like `(1, "hello")`) are now supported in binary inserts, HTTP parameterized queries, and automatic type inference. Tuples with more than 7 elements are correctly flattened from the compiler-generated rest-nesting structure. Note: if you need exactly 7 scalar elements followed by a nested tuple as the 8th element, wrap the inner tuple in an extra layer (e.g., `Tuple.Create(1,...,7, Tuple.Create(Tuple.Create("a","b")))`) so the driver can distinguish it from TRest nesting.
* **Configurable parameter value formatting**: new `IParameterFormatter` interface allowing configuration of how parameter values are serialized for HTTP transport (sibling to `IParameterTypeResolver`, which governs type resolution). Set `ParameterFormatter` on `ClickHouseClientSettings` to override the built-in serialization logic for any CLR type (e.g., custom `DateTime` precision, decimal culture, string escaping). Includes one implementation, `DictionaryParameterFormatter`, for simple CLR-type → format-function mappings. Return `null` from the formatter to fall through to the built-in formatter. Can also be set per-query via `QueryOptions.ParameterFormatter`. The formatter is also invoked for every element inside composite values (Array, Tuple, Map, Nested); see docs for quoting caveats when formatting string-like types inside composites.
* **Per-query `Accept-Encoding` override**: new `QueryOptions.AcceptEncoding` (mirrored on `ClickHouseCommand.AcceptEncoding`) replaces the default `gzip, deflate` Accept-Encoding header for a single request. Supports multiple algorithms with quality weights (e.g. `"zstd, gzip;q=0.5"`) and forces `enable_http_compression=1` on the URL so ClickHouse honours the header. For codecs the BCL cannot decode (zstd, lz4) the underlying `HttpClient` must be configured with `AutomaticDecompression = None` and the body consumed via `ExecuteRawResultAsync`.
* **`ClickHouseRawResult.ContentEncoding`**: exposes the response body's `Content-Encoding` for callers using `ExecuteRawResultAsync` to decode it themselves; `identity` is normalized to `null`.
* **Customizable read value conversion**: new `IReadValueConverter` interface allows transformation of values returned by the data reader after deserialization (e.g., `DateTime.SpecifyKind` to set UTC kind, string trimming/normalization). Set `ReadValueConverter` on `ClickHouseClientSettings` to apply transformations globally, or override per-query via `QueryOptions.ReadValueConverter`. The converter intercepts `GetValue()` and `GetFieldValue<T>()` calls, and is also applied during POCO materialization (`MapTo<T>()` / `QueryAsync<T>()`). When no converter is set, there is zero performance overhead. Includes one implementation, `DictionaryReadValueConverter`, with a fluent `.For<T>(value => …)` registration for dispatch based on CLR type.
* Added a `ClickHouseDataReader.GetFieldValue<T>(string name)` overload that resolves the column by name, complementing the existing ordinal-based overload.
* **Application-identity tagging in User-Agent**: new `ClickHouseClientSettings.ApplicationInfo` property: an `IReadOnlyDictionary<string, string>` of free-form tags (e.g. `app`, `ver`, `env`) appended to the HTTP `User-Agent` header as a comment token (e.g. `(app:MyApp; ver:2.3.1; env:prod)`).
* **Server-side `Identifier` query-parameter type**: `{name:Identifier}` parameters (and explicit `ClickHouseDbParameter.ClickHouseType = "Identifier"`) are now supported, letting you safely bind a database/table/column name instead of a quoted string literal — e.g. `CREATE DATABASE {name:Identifier}` or `SELECT {col:Identifier} FROM t`. The value is sent verbatim and the server substitutes it as a bare SQL identifier, applying its own backtick quoting/escaping, so identifiers containing special characters (including backticks) round-trip safely with no client-side escaping. Previously these threw `ArgumentException: Unknown type: Identifier` (surfaced from clickhouse-go#1635).
* **Configurable response read buffer size**: new `ClickHouseClientSettings.ReadBufferSize` (and connection-string key `ReadBufferSize`) controls the size of the buffer used when reading HTTP query responses. **Behavioral change:** the default has been lowered from 512 KiB to 8 KiB. The previous 512KiB size exceeded the large object heap limit and could cause substantial performance issues due to GC. Workloads with large responses that prefer fewer buffer refills can set a larger value.

**Improvements:**
* HTTP parameter mismatch errors now include the parameter name, the full ClickHouse type, and the value's runtime CLR type. The previous message (`"Cannot convert 219 to Array(UInt8)"`) collapsed the outer type, omitted which parameter failed, and didn't say what the value actually was.
* `GetFieldValue<T[,]>` errors are now categorised by cause. `InvalidCastException` covers type-structure mismatches: the column is null/DBNull, the value isn't a collection, the source's structural depth differs from the target rank (shallower or deeper), or a leaf is the wrong scalar type or `null` into a non-nullable value-type target. Messages include the column ordinal, target CLR type, and offending indices where applicable. `InvalidOperationException` is reserved for shape-validation failures — the value's structure matches `T` but rows are ragged or an intermediate row is null. Previously, structural-depth mismatches and a `null` leaf into e.g. `int[,]` either threw `InvalidOperationException` or were silently coerced to `default(int)`.

**Bug Fixes:**
* **Breaking Change**: Fixed timezone shift for `@`-style `DateTime`/`DateTimeOffset` parameters on non-UTC servers (issue #350). When a parameter's ClickHouse type was inferred (no explicit `{name:Type}` hint in SQL and no `parameter.ClickHouseType` set), the driver previously emitted a bare `DateTime` hint and the server parsed the wire wall-clock in `session_timezone`/server tz — silently shifting instant-bearing values by the server's offset. Inferred types for `DateTime { Kind: Utc or Local }` and `DateTimeOffset` are now sent as `DateTime('UTC')`, preserving the instant across any server timezone. Explicit hints (`{name:DateTime}` in SQL or `parameter.ClickHouseType`) are untouched, users who specify the type continue to own timezone correctness.
* Fixed silent 32-bit wraparound on the binary write path for `DateTime`/`DateTime32` values outside ClickHouse's supported range (1970-01-01 .. 2106-02-07 06:28:15 UTC). Out-of-range values, e.g. `new DateTime(1900, 1, 1)`, were being cast through a 32-bit signed int and reinterpreted as `UInt32` by the server, producing real-but-wrong timestamps (e.g. `2036-02-07`). The same audit fixed analogous silent out-of-range writes for `Date32` (server would clamp to `[1900-01-01, 2299-12-31]`) and tightened the existing `Date` `OverflowException` into a descriptive `ArgumentOutOfRangeException`. All four types now throw `ArgumentOutOfRangeException` at `Write` time naming the column type and supported range.
* Fixed silent wall-clock shift for `DateTime` and `DateTime64` columns declared with ClickHouse's synthetic `Fixed/UTC±HH:MM:SS` timezone names (e.g. `DateTime('Fixed/UTC+05:30:00')`). These names are not in the IANA TZDB, so `GetZoneOrNull` returned `null`, causing the driver to interpret the stored instant as UTC and return a value shifted by the column's offset. The driver now recognises the `Fixed/UTC` pattern and constructs a correct fixed-offset `DateTimeZone` from it (issue #370).
* Fixed HTTP parameter serialization for `Date`, `DateTime`, and `DateTime64` values inside composite types such as `Array`, `Tuple`, `Map`, and `Variant`. These values are now quoted correctly when sent over HTTP.
* Fixed type inference for `System.Tuple` with more than 7 elements. The TRest nesting was not being flattened, causing the 8th+ elements to be inferred as nested tuple types instead of their actual flat types. This could lead to incorrect ClickHouse type inference and serialization errors.
* Fixed parsing of enum labels containing escaped quotes, parentheses, and `=` characters. This fixes cases like `variantType()` on `Variant(String, DateTime('UTC'))`, which could previously round-trip through the driver as an empty string.
* Fixed `ClickHouseServerException.Message` returning compressed binary bytes when the server returned a non-2xx response with a `Content-Encoding` header. The driver now decompresses gzip / deflate / brotli error bodies before attaching them to the exception. Unknown codecs (zstd, lz4) yield a placeholder pointing at `system.query_log`.

v1.2.0
---

**New Features:**
* **POCO binary inserts**: new `InsertBinaryAsync<T>` overload on `ClickHouseClient` accepts `IEnumerable<T>` directly, mapping public properties to columns automatically. Register types upfront with `RegisterBinaryInsertType<T>()`. Customize column names and ClickHouse types with `[ClickHouseColumn(Name = "...", Type = "...")]`, or exclude properties with `[ClickHouseNotMapped]`. When all properties specify explicit types via the attribute, the schema probe is skipped entirely.
* **Configurable parameter type resolution**: new `IParameterTypeResolver` interface allowing configuration of type mapping for `@`-style parameterized queries. Set `ParameterTypeResolver` on `ClickHouseClientSettings` to override how .NET types are mapped to ClickHouse types (e.g., `DateTime` → `DateTime64(3)`, `decimal` → `Decimal64(4)`). Includes one implementation, `DictionaryParameterTypeResolver` for simple type→type mappings, and supports custom implementations for value-aware or name-based resolution. Can also be set per-query via `QueryOptions.ParameterTypeResolver`.

**Improvements:**
* Type inference now inspects `IPAddress.AddressFamily` to correctly distinguish between IPv4 and IPv6 types. Previously, all `IPAddress` values were inferred as IPv4. This also works for collections, tuples, and maps containing `IPAddress` values.

**Internal Improvements:**
* Centralized parameter type resolution into `ParameterTypeResolution`, replacing previously scattered logic in `ClickHouseDbParameter.QueryForm` and `HttpParameterFormatter`. Each parameter's type is now resolved exactly once per request, ensuring consistency between SQL placeholder generation and HTTP value formatting.

**Bug Fixes:**
* `JsonReadMode` and `JsonWriteMode` will now correcly set the corresponding settings when set to `Binary` mode.

v1.1.0
---

**New Features:**
* `InsertOptions.ColumnTypes`: provide a dictionary of column name → ClickHouse type string to skip the schema probe query (`SELECT ... WHERE 1=0`) entirely. Ideal when the table schema is known at compile time.
* `InsertOptions.UseSchemaCache`: when `true`, the full table schema is cached per (database, table) for the lifetime of the `ClickHouseClient` instance. Subsequent inserts to the same table reuse the cached schema regardless of which columns are selected, eliminating redundant round-trips.

**Breaking Changes:**
* `InsertBinaryAsync` now throws `InvalidOperationException` when sessions are enabled and `MaxDegreeOfParallelism > 1`. ClickHouse only allows one concurrent query per session, so parallel batch inserts would cause `SESSION_IS_LOCKED` errors and partial writes. This also affects the deprecated `ClickHouseBulkCopy`, which defaults to `MaxDegreeOfParallelism = 4`. To fix, set `MaxDegreeOfParallelism` to 1, or disable sessions for the insert via `InsertOptions.UseSession = false`.

**Bug Fixes:**
* Fixed `IndexOutOfRangeException` when reading NULL values from `Variant` columns. The Variant `None` discriminator (used for NULLs) was not handled, causing an out-of-bounds array access instead of returning `DBNull.Value`.
* Fixed writing NULL values to `Variant` columns. Writing null/DBNull now correctly emits the `None` discriminator (`0xFF`) for binary writes, and null marker `\N` when using HTTP parameters. Note: null Variant HTTP parameter parsing is broken in server versions prior to 26.3.

v1.0.2
---

**Bug Fixes:**
* Fixed `QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING` errors when using `InsertBinaryAsync` with a `QueryId`. The schema probe and all batch inserts were sharing the same query ID. The schema probe now uses the base query ID, and each batch insert receives a unique suffixed ID (`{queryId}-1`, `{queryId}-2`, etc.).

v1.0.1
---

 * Marked ClickHouseConnection.ServerVersion property as Obsolete.

v1.0.0
---

**Documentation and Usage Examples:**
Coinciding with the 1.0.0 release of the driver, we have greatly expanded the documentation and usage examples.
* Documentation: https://clickhouse.com/docs/integrations/csharp
* Usage examples: https://github.com/ClickHouse/clickhouse-cs/tree/main/examples

---

**New: ClickHouseClient - Simplified Primary API**

`ClickHouseClient` is the new recommended way to interact with ClickHouse. Thread-safe, singleton-friendly, and simpler than ADO.NET classes.

```csharp
using var client = new ClickHouseClient("Host=localhost");
```

| Method | Description                                                  |
|--------|--------------------------------------------------------------|
| `ExecuteNonQueryAsync` | Execute DDL/DML (CREATE, INSERT, ALTER, DROP)                |
| `ExecuteScalarAsync` | Return first column of first row                             |
| `ExecuteReaderAsync` | Stream results via `ClickHouseDataReader`                    |
| `InsertBinaryAsync` | High-performance bulk insert (replaces `ClickHouseBulkCopy`) |
| `ExecuteRawResultAsync` | Get raw result stream bypassing the parser                   |
| `InsertRawStreamAsync` | Insert from stream (CSV, JSON, Parquet, etc.)                |
| `PingAsync` | Check server connectivity                                    |
| `CreateConnection()` | Get `ClickHouseConnection` for ORM compatibility             |

**Per-query configuration** via `QueryOptions`.

**Parameters** via `ClickHouseParameterCollection`:
```csharp
var parameters = new ClickHouseParameterCollection();
parameters.Add("id", 42UL);
await client.ExecuteReaderAsync("SELECT * FROM t WHERE id = {id:UInt64}", parameters);
```

**Deprecation:** `ClickHouseBulkCopy` is deprecated. Use `client.InsertBinaryAsync(table, columns, rows)` instead.

---

**Breaking Changes:**
* **Dropped support for .NET Framework and .NET Standard.** The library now targets only `net6.0`, `net8.0`, `net9.0`, and `net10.0`. Removed support for `net462`, `net48`, and `netstandard2.1`. If you are using .NET Framework, you will need to stay on the previous version or migrate to .NET 6.0+.

* **Removed feature discovery query from `OpenAsync`.** The connection's `OpenAsync()` method no longer executes `SELECT version()` to discover server capabilities. This makes connection opening instantaneous (no network round-trip) but removes the `SupportedFeatures` property from `ClickHouseConnection`. The `ServerVersion` property now throws `InvalidOperationException`.

  **Migration guidance:** If you need to check the server version:
  ```csharp
  using var reader = await connection.ExecuteReaderAsync("SELECT version()");
  reader.Read();
  var version = reader.GetString(0);
  ```

* **DateTime reading behavior changed for columns without explicit timezone.** Previously, `DateTime` columns without a timezone (e.g., `DateTime` vs `DateTime('Europe/Amsterdam')`) would use the server timezone (with `UseServerTimezone=true`) or client timezone to interpret the stored value. Now, these columns return `DateTime` with `Kind=Unspecified`, preserving the wall-clock time exactly as stored without making assumptions about timezone.

  | Column Type | Old Behavior | New Behavior |
  |-------------|--------------|--------------|
  | `DateTime` (no timezone) | Returned with server/client timezone applied | `DateTime` with `Kind=Unspecified` |
  | `DateTime('UTC')` | `DateTime` with `Kind=Utc` | `DateTime` with `Kind=Utc` (unchanged) |
  | `DateTime('Europe/Amsterdam')` | `DateTime` with `Kind=Unspecified` | `DateTime` with `Kind=Unspecified` (unchanged). Reading as DateTimeOffset has correct offset applied. |

  **Migration guidance:** If you need timezone-aware behavior, either:
    1. Use explicit timezones in your column definitions: `DateTime('UTC')` or `DateTime('Europe/Amsterdam')`
    2. Apply the timezone yourself after reading.

* **DateTime writing now respects `DateTime.Kind` property.** Previously, all `DateTime` values were treated as wall-clock time in the target column's timezone regardless of their `Kind` property. The new behavior:

  | DateTime.Kind | Old Behavior | New Behavior |
  |---------------|--------------|--------------|
  | `Utc` | Treated as wall-clock time in column timezone | Preserved as-is (instant is maintained) |
  | `Local` | Treated as wall-clock time in column timezone | Instant is maintained (inserted as UTC timestamp) |
  | `Unspecified` | Treated as wall-clock time in column timezone | Treated as wall-clock time in column timezone (unchanged) |

  Migration guidance: If you were relying on the old behavior where UTC `DateTime` values were reinterpreted in the column timezone, you should change these to `DateTimeKind.Unspecified`:
  ```csharp
  // Old code (worked by accident):
  var utcTime = DateTime.UtcNow;  // Would be reinterpreted in column timezone

  // New code (explicit intent):
  var wallClockTime = DateTime.SpecifyKind(myTime, DateTimeKind.Unspecified);
  ```

  **Important:** When using parameters, you must specify the timezone in the parameter type hint to have string values interpreted in the column timezone:
  ```csharp
  command.AddParameter("dt", myDateTime);
  
  // Correct: timezone in type hint ensures proper interpretation
  command.CommandText = "INSERT INTO table (dt_column) VALUES ({dt:DateTime('Europe/Amsterdam')})";

  // Gotcha: without timezone hint, UTC is used for interpretation
  command.CommandText = "INSERT INTO table (dt_column) VALUES ({dt:DateTime})";
  // ^ String value interpreted in UTC, not column timezone!
  ```

  This differs from bulk copy operations where the column timezone is known and used automatically.

* **Removed `UseServerTimezone` setting.** This setting has been removed from the connection string, `ClickHouseClientSettings`, and `ClickHouseConnectionStringBuilder`. It no longer has any effect since columns without timezones now return `Unspecified` DateTime values without any timezone changes applied to what is returned from the server.
* **Moved `ServerTimezone` property from `ClickHouseConnection` to `ClickHouseCommand`.** The server timezone is now available on `ClickHouseCommand.ServerTimezone` after any query execution (the timezone is now extracted from the `X-ClickHouse-Timezone` response header instead of requiring a separate query).
* **Helper and extension methods made internal:** DateTimeConversions, DataReaderExtensions, DictionaryExtensions, EnumerableExtensions, MathUtils, StringExtensions.

* **JSON writing default behavior changed.** The default `JsonWriteMode` has changed from `Binary` to `String`. This affects how JSON data is written to ClickHouse:

  | Input Type | Old Default (Binary) | New Default (String) |
  |------------|---------------------|----------------------|
  | `JsonObject` / `JsonNode` | Binary encoding | Serialized via `JsonSerializer.Serialize()` |
  | `string` | Binary encoding (parsed client-side) | Passed through directly |
  | POCO (registered) | Binary encoding with type hints | Serialized via `JsonSerializer.Serialize()` |
  | POCO (unregistered) | Exception | Serialized via `JsonSerializer.Serialize()` |

  **Impact if you don't modify your code:**
    - JSON writing will still work, but uses string serialization instead of binary encoding; the JSON string will be parsed on the server instead of the client. This could lead to subtle changes in paths without type hints, e.g., values previously parsed as ints may be parsed as longs.
    - `ClickHouseJsonPath` and `ClickHouseJsonIgnore` attributes are ignored in String mode (they only work in Binary mode). Serialization happens via `System.Text.Json`, so you can use those attributes instead.
    - Server setting `input_format_binary_read_json_as_string=1` is automatically set when using String write mode

**New Features/Improvements:**

* **Automatic parameter type extraction from SQL.** Types specified in the SQL query using `{name:Type}` syntax are now automatically used for parameter formatting, eliminating the need to specify the type twice:
  ```csharp
  // Before: type specified twice
  command.CommandText = "SELECT {dt:DateTime('Europe/Amsterdam')}";
  command.AddParameter("dt", "DateTime('Europe/Amsterdam')", value);

  // After: type extracted from SQL automatically
  command.CommandText = "SELECT {dt:DateTime('Europe/Amsterdam')}";
  command.AddParameter("dt", value);
  ```
  The `AddParameter(name, type, value)` overload is now marked obsolete. Use `AddParameterWithTypeOverride()` if you need to explicitly override the SQL type hint.

* **POCO serialization support for JSON columns.** When writing POCOs to JSON columns with typed hints (e.g., `JSON(id Int64, name String)`), the driver serializes properties using the hinted types for full type fidelity. Properties without a corresponding hinted path will have their ClickHouse types inferred automatically. Two attributes are available: `[ClickHouseJsonPath("path")]` for custom JSON paths and `[ClickHouseJsonIgnore]` to exclude properties. Property name matching to hint paths is case-sensitive (matching ClickHouse behavior which allows paths like `userName` and `UserName` to coexist). Register types via `client.RegisterJsonSerializationType<T>()`.

* **`JsonReadMode` and `JsonWriteMode` connection string settings** for configurable JSON handling:
    - `JsonReadMode.Binary` (default): Returns `System.Text.Json.Nodes.JsonObject`
    - `JsonReadMode.String`: Returns raw JSON string. Sets server setting `output_format_binary_write_json_as_string=1`.
    - `JsonWriteMode.String` (default): Accepts `JsonObject`, `JsonNode`, strings, and any object (serialized via `System.Text.Json.JsonSerializer`). Sets server setting `input_format_binary_read_json_as_string=1`.
    - `JsonWriteMode.Binary`: Only accepts registered POCO types with full type hint support and custom path attributes. Writing `string` or `JsonNode` values with `JsonWriteMode.Binary` throws an exception.

* **QBit data type support.** QBit is a transposed vector column, designed to allow the user to choose a desired quantization level at runtime, speeding up approximate similarity searches. See the GitHub repo for usage examples.

* **Dynamic type binary writing support** via `InsertBinaryAsync`. Values are automatically type-inferred from their .NET types and serialized with the appropriate binary type header. Supports all common types including integers, floating point, strings, booleans, DateTime, Guid, decimal, arrays, lists, and dictionaries.

* **Binary data in String/FixedString columns.** Write `byte[]`, `ReadOnlyMemory<byte>`, or `Stream` values to String and FixedString columns via `InsertBinaryAsync`. Read binary data back using the `ReadStringsAsByteArrays` connection string setting, which returns String columns as `byte[]` instead of `string`. Useful for storing binary data that may not be valid UTF-8.

* **First-class support for roles**, with query-level override.

* **Custom HTTP headers** at the connection level for proxy/infrastructure integration.

* **Support for JWT authentication**, with query-level override.

* **Mid-stream exception detection** via `X-ClickHouse-Exception-Tag` header (ClickHouse 25.11+). When `http_write_exception_in_output_format` is set to 0 on the server, exceptions that occur while streaming results are now properly detected and thrown as `ClickHouseServerException` (which includes the exception message) instead of `EndOfStreamException`.

* **Query ID auto-generation.** When the query ID has not been set, it will now be automatically generated by the client.

* **`AddParameter()` convenience method** for `ClickHouseParameterCollection`, simplifying parameter creation.

**Bug Fixes:**
* Fixed a crash when reading a Map with duplicate keys. The current behavior is to return only the last value for a given key.


v0.9.0
---

**Breaking Changes:**
 * FixedString is now returned as byte[] rather than String. FixedStrings are not necessarily valid UTF-8 strings, and the string transformation caused loss of information in some cases. Use Encoding.UTF8.GetString() on the resulting byte[] array to emulate the old behavior. String can still be used as a parameter or when inserting using BulkCopy into a FixedString column. When part of a json object, FixedString is still returned as a string.
 * Removed obsolete MySQL compatibility mapping TIME -> Int64.
 * Json serialization of bool arrays now uses the Boolean type instead of UInt8 (it is now consistent with how bool values outside arrays were handled).
 * GEOMETRY is no longer an alias for String.

**New Features/Improvements:**
 * Sessions can now be used with custom HttpClient or HttpClientFactory. Previously this combination was not allowed. Note that when sessions are enabled, ClickHouseConnection will allow only one request at a time, and responses are fully buffered before returning to ensure proper request serialization.
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


v0.8.1
---

**Improvements:**
 * Fixed NuGet readme file.

v0.8.0
---

**Breaking Changes:**
 * Trying to set ClickHouseConnection.ConnectionString will now throw a NotSupportedException. Create a new connection with the desired settings instead.
 * When a default database is not provided, the client no longer uses "default" (now uses empty string). This allows default user database settings to function as expected.
 * ClickHouseDataSource.Logger (ILogger) property changed to LoggerFactory (ILoggerFactory).
 * Removed support for loading configuration from environment variables (CLICKHOUSE_DB, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD). Use connection strings or ClickHouseClientSettings instead.
 * The default PooledConnectionIdleTimeout has been changed to 5 seconds, to prevent issues with half-open connections when using ClickHouse Cloud (where the default server-side idle timetout is 10s).

**New Features:**
 * Added .NET 10 as a target.
 * The NuGet package is now signed.
 * Enabled strong naming for the library.
 * Added a new way to configure ClickHouseConnection: the ClickHouseClientSettings class. You can initialize it from a connection string by calling ClickHouseClientSettings.FromConnectionString(), or simply by setting its properties.
 * Added settings validation to prevent incorrect configurations.
 * Added logging in the library, enable it by passing a LoggerFactory through the settings. Logging level configuration is configured through the factory. For more info, see the documentation: https://clickhouse.com/docs/integrations/csharp#logging-and-diagnostics
 * Added EnableDebugMode setting to ClickHouseClientSettings for low-level .NET network tracing (.NET 5+). When enabled, traces System.Net events (HTTP, Sockets, DNS, TLS) to help diagnose network issues. Requires ILoggerFactory with Trace-level logging enabled. WARNING: Significant performance impact - not recommended for production use.
 * AddClickHouseDataSource now automatically injects ILoggerFactory from the service provider when not explicitly provided.
 * Improvements to ActivitySource for tracing: stopped adding tags when it was not necessary, and made it configurable through ClickHouseDiagnosticsOptions.
 * Added new AddClickHouseDataSource extension methods that accept ClickHouseClientSettings for strongly-typed configuration in DI scenarios.
 * Added new AddClickHouseDataSource extension method that accepts IHttpClientFactory for better DI integration.
 * Optimized response header parsing.
 * Added list type conversion, so List<T> can now be passed to the library (converts to Array() in ClickHouse). Thanks to @jorgeparavicini.
 * Optimized EnumType value lookups.
 * Avoid unnecessarily parsing the X-ClickHouse-Summary headers twice. Thanks to @verdie-g.
 * Added the ability to pass a query id to ClickHouseConnection.PostStreamAsync(). Thanks to @dorki.
 * The user agent string now also contains information on the host operating system, .NET version, and processor architecture.

**Bug fixes:**
 * Fixed a crash when processing a tuple with an enum in it.
 * Fixed a potential sync-over-async issue in the connection. Thanks to @verdie-g.
 * Fixed a bug with parsing table definitions with parametrized json fields. Thanks to @dorki.
