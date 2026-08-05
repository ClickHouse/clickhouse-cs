* Fixed `DbConnection.GetSchema("Columns", ...)` not disposing the command it creates internally, which delayed the release of that command's cancellation-token source until garbage collection.
