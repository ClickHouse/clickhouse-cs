* Changed an empty `QueryOptions.BearerToken`, `InsertOptions.BearerToken` or `ClickHouseCommand.BearerToken`
  to count as not set: the request now uses the next credentials of the client (`CredentialsProvider`, then
  `BearerToken`). Before, it used `Username` and `Password`, also when the client had a `BearerToken`.
