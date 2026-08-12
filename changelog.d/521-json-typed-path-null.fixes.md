* Fixed reading a `JSON` column where a typed path holds `NULL` (issue #521). The path was dropped
  from the returned `JsonObject` entirely, so `{"x": null}` came back as `{}` and callers could not
  tell "path not present in this row" from "path present but null"; for a nested typed path such as
  `JSON(a.b Nullable(Int64))` the whole parent subtree disappeared. Typed paths are now materialized
  with an explicit JSON null, matching the server's own JSON rendering. Dynamic (unhinted) paths are
  unchanged and stay absent, as the server also omits them.
