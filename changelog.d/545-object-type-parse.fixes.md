* Fixed `Object('json')` columns being reported as `SimpleAggregateFunction(, Json)`. The type is now
  parsed as an `Object` type, so `GetSchema("Columns")` reports `Object(Json)` and the `ObjectType`
  members are reachable (#545).
