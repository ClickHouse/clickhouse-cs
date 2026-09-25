* Fixed reading a `JSON` path whose value is composite, which threw
  `InvalidOperationException: The element cannot be an object or array` and made the whole row
  unreadable ([#626](https://github.com/ClickHouse/clickhouse-cs/issues/626)). A `Tuple` and a
  `Nested` path now render the way the server renders them — a named tuple as an object, an unnamed
  one as an array — and a `Dynamic` or `Variant` path renders by the type of the value it holds
  ([#530](https://github.com/ClickHouse/clickhouse-cs/issues/530)).
  - A string under a `Dynamic` path is no longer base64 under `ReadStringsAsByteArrays`, and an
    array under a `Variant` or `SimpleAggregateFunction` path is no longer base64 either.
* Fixed `Dynamic(max_types = N)` being rejected as an unknown type
  ([#626](https://github.com/ClickHouse/clickhouse-cs/issues/626)). The argument bounds only the
  server's tracked type set, so it is accepted and ignored.
