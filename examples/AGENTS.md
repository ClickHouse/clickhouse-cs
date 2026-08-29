# Examples — contributor guide

Each example is a self-contained, runnable demonstration of one topic. `dotnet run` in this directory
runs them all against a live server, and CI does the same on every pull request that touches
`examples/` or the driver, so an example that throws fails the build.

Examples are grouped by transport: `Http/` uses `ClickHouseClient` / `ClickHouseConnection` over
HTTP, `Tcp/` uses `ClickHouseTcpClient` over the native protocol. See `Tcp/README.md` for what is
specific to the native client.

## Adding an example

Five steps. Skip any one of them and the example does not run.

1. **Put the file** in the transport and category it belongs to:
   `Http/<Category>/<Category>_0NN_<Topic>.cs` or `Tcp/<Category>/Tcp_0NN_<Topic>.cs`. Take the next
   free number in that category.

2. **Declare `namespace ClickHouse.Driver.Examples;`** — not a namespace derived from the folder.
   `ExampleRunner` matches that namespace exactly and will not find a class in any other one.

3. **Make it a `public static class` with a `public static Task Run()`.** Discovery requires both.
   The class name is what `--list` prints and what `--filter` matches, so name it after the topic
   (`BasicUsage`), not after the file (`Core_001_BasicUsage`).

   **A native-protocol example's class name must start with `Tcp`** (`TcpBasicUsage`). Every example
   shares one namespace, so it could not reuse an HTTP example's name anyway, and that prefix is how
   `ExampleInfo.Transport` knows which endpoint to check before running it — and what `--http` and
   `--tcp` select on.

4. **Add it to `RunAllExamples` in `Program.cs`**, under its category banner, in file-number order.
   That list is hand-maintained so the run order and the banners stay meaningful. An example missing
   from it still compiles, still appears in `--list`, and still runs under `--filter` — it just never
   runs in CI, so nothing tells you when it breaks.

5. **Add it to the index in `README.md`**, in the matching section, with a one-line description.

Then run it (`dotnet run -- --filter <topic>`) and read the output. An example whose output does not
teach the topic is not finished.

## Never hard-code a connection string

Take the server from `ExampleConfig`, which resolves environment variables over localhost defaults:

- `ExampleConfig.CreateHttpClient()` / `CreateHttpConnection()` for the common case.
- `ExampleConfig.HttpConnectionString` where a constructor takes the string itself.
- `$"{ExampleConfig.HttpConnectionString};SomeKey=value"` to add a key the assembled string does not
  already set. It sets `Host`, `Port`, `Username`, `Password` and `Database`, so appending any of
  those would produce a duplicate.
- `ExampleConfig.HttpBuilder()` to *change* one of those five. It returns a fresh builder each call.

Four examples are exempt, because configuration is their subject or they start their own server:
`Core_002_ConnectionStringConfiguration`, `Core_003_DependencyInjection`,
`Testing_001_Testcontainers`, `Tcp_030_Testcontainers`. A literal connection string inside a comment,
shown to teach the reader what one looks like, is also fine.

Ask `ExampleConfig` for the endpoint with `HttpEndpoint` or `TcpEndpoint` when an example has to name
or dial it itself. There are no `Host`/`Port` properties: a whole-string override would not reach
them, so an example reading one could print an endpoint it did not connect to.

## Examples deliberately left out of `RunAllExamples`

Three need infrastructure the CI server does not have, so they are registered nowhere and run only
by explicit filter:

- `Tables_002_CreateTableCluster` — needs a ClickHouse cluster.
- `Tables_003_CreateTableCloud` — needs ClickHouse Cloud credentials.
- `Auth_001_JwtAuthentication` — needs a JWT.

Their class names are also in `ExampleRunner._optIn`, which is what keeps `--http` and `--tcp` from
running them: those select by reflection, so leaving an example out of `RunAllExamples` does not on
its own keep a transport run from picking it up.

If you add an example of that kind, leave it out of `RunAllExamples`, add its class name to
`_optIn`, and list it here. Otherwise the omission is indistinguishable from having forgotten step 4.

## Style

- Console output is the teaching surface. Print what you did and what came back, not just "OK".
- Show one thing well rather than covering an API exhaustively. A reader who wants the full surface
  reads the docs.
- Drop any table you create, in a `finally`, and `DROP TABLE IF EXISTS` it before the `CREATE` as well. The
  second one is what lets a run survive an earlier run that was interrupted before its `finally`.
- Name a table `example_<topic>`, fixed, not unique per run. This project is not the test suites: it assumes
  one suite run at a time against a server, so it does not need `CreateTableName`. Two things do need a
  per-run `Guid`: a name the example needs the server *not* to have (a deliberately missing table), and a
  marker an example counts in `system.processes` or `system.query_log`. A second run would otherwise make
  the first one's measurement or expected failure wrong.
- Comments explain why the server or the driver behaves as it does, not what the line does.
