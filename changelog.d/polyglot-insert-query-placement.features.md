* Added `InsertOptions.QueryPlacement`. Set it to `InsertQueryPlacement.Url` to send the `INSERT`
  statement of a binary insert as the `query` URL parameter, where proxies and access logs can read
  it, instead of inside the request body, which stays the default.
