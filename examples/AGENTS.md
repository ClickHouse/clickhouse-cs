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

4. **Add it to `RunAllExamples` in `Program.cs`**, under its category banner, in file-number order.
   That list is hand-maintained so the run order and the banners stay meaningful. An example missing
   from it still compiles, still appears in `--list`, and still runs under `--filter` — it just never
   runs in CI, so nothing tells you when it breaks.

5. **Add it to the index in `README.md`**, in the matching section, with a one-line description.

Then run it (`dotnet run -- --filter <topic>`) and read the output. An example whose output does not
teach the topic is not finished.

## Examples deliberately left out of `RunAllExamples`

Three need infrastructure the CI server does not have, so they are registered nowhere and run only
by explicit filter:

- `Tables_002_CreateTableCluster` — needs a ClickHouse cluster.
- `Tables_003_CreateTableCloud` — needs ClickHouse Cloud credentials.
- `Auth_001_JwtAuthentication` — needs a JWT.

If you add an example of that kind, leave it out of `RunAllExamples` and list it here. Otherwise the
omission is indistinguishable from having forgotten step 4.

## Style

- Console output is the teaching surface. Print what you did and what came back, not just "OK".
- Show one thing well rather than covering an API exhaustively. A reader who wants the full surface
  reads the docs.
- Drop any table you create.
- Comments explain why the server or the driver behaves as it does, not what the line does.
