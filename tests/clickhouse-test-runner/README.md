# clickhouse-test-runner

This harness runs the ClickHouse core SQL tests
([`tests/queries/0_stateless`](https://github.com/ClickHouse/ClickHouse/tree/master/tests/queries/0_stateless))
through the two clients of this repository: the HTTP client (`ClickHouse.Driver`) and the native TCP client
(`ClickHouse.Driver.Tcp`).

The upstream test runner
([`tests/clickhouse-test`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/clickhouse-test))
runs each `.sql` test through `clickhouse-client` and compares the output with the `.reference` file of the
test. This harness is a small .NET program (the shim, `ClickHouse.Driver.TestRunner`) that imitates
`clickhouse-client` well enough for the runner to drive it without changes: the same flags, the same stdin
and `--query` behavior, and the `extract-from-config` shortcut.

The shim sends every query through one of the two clients, with the client's public API. It reads each value
with the client's normal type mapping, and writes the values as `TabSeparated` text itself. Thus a test
passes only when the client decodes every value correctly. `CLICKHOUSE_TEST_RUNNER_PROTOCOL` selects the
protocol:

| Protocol | Client | API |
|---|---|---|
| `http` (default) | `ClickHouse.Driver` | ADO.NET: `ClickHouseConnection`, `ClickHouseCommand`, `ClickHouseDataReader.GetValue` (RowBinary over HTTP) |
| `tcp` | `ClickHouse.Driver.Tcp` | `ClickHouseTcpClient.OpenSessionAsync`, `IClickHouseTcpSession.StreamAsync`, `IColumn.GetValue` (Native) |

The shim is a port of the
[Go harness](https://github.com/ClickHouse/clickhouse-go/pull/2141). It is not in
`ClickHouse.Driver.sln` and not in the NuGet package.

## Run the tests locally

You need the .NET 10 SDK, Python 3 with `jinja2`, and Docker. The TCP client and the shim read the tz
database of the system, and some tests use legacy zone names (`US/Samoa`): on Ubuntu, install `tzdata-legacy`
too. Run the commands from the root of the repository.

1. Start the pinned ClickHouse server. The version is in [`clickhouse-version`](clickhouse-version):

   ```bash
   docker run -d --name clickhouse -p 8123:8123 -p 9000:9000 --ulimit nofile=262144:262144 \
     --hostname "$(hostname)" -e CLICKHOUSE_SKIP_USER_SETUP=1 \
     "clickhouse/clickhouse-server:$(tests/clickhouse-test-runner/scripts/pinned-version.sh)"
   ```

   `CLICKHOUSE_SKIP_USER_SETUP=1` keeps the `default` user of the image: without it, the user without a
   password can connect only from inside the container. `tests/clickhouse-test` replaces its own host name
   with `localhost` in the output of each test, and some `.reference` files have `localhost` for
   `hostName()`, so the server gets the host name of your machine.

2. Get the upstream tests of the same version:

   ```bash
   tests/clickhouse-test-runner/scripts/fetch-upstream.sh
   ```

   The script checks out the release tag of the pinned version (for example `v26.10.4.207-stable`) into
   `.upstream/ClickHouse`, sparse and shallow. The same server build made the `.reference` files of that tag.
   The script also generates
   [`ClickHouse.Driver.TestRunner/ErrorCodes.g.cs`](ClickHouse.Driver.TestRunner/ErrorCodes.g.cs) from the
   same checkout. To try another version, give it as the argument (`fetch-upstream.sh 26.11.1.10`), and
   start the server of that version.

   The run script stops when the server and the tests are of different versions, because then the
   `.reference` files do not match. Set `ALLOW_VERSION_MISMATCH=1` to run anyway.

3. Run the allowlisted tests over each protocol:

   ```bash
   pip install jinja2
   tests/clickhouse-test-runner/scripts/run-upstream-tests.sh -j 8
   CLICKHOUSE_TEST_RUNNER_PROTOCOL=tcp tests/clickhouse-test-runner/scripts/run-upstream-tests.sh -j 8
   ```

The run script builds the shim with [`scripts/build.sh`](scripts/build.sh) (`dotnet publish`, Release,
ReadyToRun) into `.bin/publish`, writes the `.bin/clickhouse` command and its `clickhouse-client` symlink,
puts `.bin` first in `PATH` for the runner only, and starts `tests/clickhouse-test`. It gives the runner
`--no-stateful --no-random-settings --no-random-merge-tree-settings --max-failures-chain 1000000`; other
arguments go to the runner after these. `--max-failures-chain` turns off a limit of the runner: by default it
stops after 20 failures in a row. To run any one test, also one that is not in the allowlist:

```bash
UPSTREAM_TEST_LIST=<(echo 00001_select_1) tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
```

The runner writes the diff of a failed test to its own output. The output of the test is next to the test,
in `.upstream/ClickHouse/tests/queries/0_stateless/<test>.<pid>.stdout` and `.stderr`. A test that runs alone
(the `no-parallel` tag, or all tests without `-j`) writes `<test>.stdout` and `<test>.stderr`.

The unit tests of the shim need no server. The live tests compare the output of the shim with the server's
own `TabSeparated` output, over both protocols, and need the server of step 1:

```bash
CLICKHOUSE_TEST_RUNNER_LIVE=1 dotnet test \
  tests/clickhouse-test-runner/ClickHouse.Driver.TestRunner.Tests/ClickHouse.Driver.TestRunner.Tests.csproj
```

To run one statement through the shim, build it once and call it as `clickhouse-client`:

```bash
tests/clickhouse-test-runner/scripts/build.sh
tests/clickhouse-test-runner/.bin/clickhouse-client --query "SELECT [1, 2], map('a', 1)"
CLICKHOUSE_TEST_RUNNER_PROTOCOL=tcp tests/clickhouse-test-runner/.bin/clickhouse-client --query "SELECT 1"
```

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `CLICKHOUSE_TEST_RUNNER_PROTOCOL` | `http` | The client: `http` (`ClickHouse.Driver`) or `tcp` (`ClickHouse.Driver.Tcp`). |
| `UPSTREAM_CLICKHOUSE_DIR` | `.upstream/ClickHouse` | The checkout of `ClickHouse/ClickHouse`. |
| `UPSTREAM_REPO_URL` | the `ClickHouse/ClickHouse` repository on GitHub | The repository that `fetch-upstream.sh` checks out. |
| `UPSTREAM_TEST_LIST` | `allowlist.txt` | The file with the tests to run. |
| `SHARD_INDEX`, `SHARD_TOTAL` | `1`, `1` | Run only shard `SHARD_INDEX` of `SHARD_TOTAL` round-robin shards of the list. |
| `KNOWN_FAILURES` | (none) | Run the known failures of these groups instead of the allowlist: comma-separated IDs, or `all`. |
| `RETRY_FAILED` | `0` | Run the failed tests again, at most this many times. See [CI](#ci). |
| `SKIP_BUILD` | `0` | `1`: do not build; use the shim in `.bin` as it is. |
| `ALLOW_VERSION_MISMATCH` | `0` | `1`: run also when the server and the upstream tests are of different versions. |
| `CLICKHOUSE_CLIENT_CLI_LOG` | (none) | A file. Each invocation of the shim adds its arguments (the `--password` value hidden), statements and errors to it. |
| `CLICKHOUSE_HOST` | `localhost` | The server host, for the runner, the version check of the run script and the live tests. |
| `CLICKHOUSE_PORT_TCP`, `CLICKHOUSE_PORT_HTTP` | `9000`, `8123` | The ports of the server. When `CLICKHOUSE_PORT_TCP` is set, the runner gives `--port=$CLICKHOUSE_PORT_TCP`; with `http`, the shim uses `CLICKHOUSE_PORT_HTTP` for it. With `--secure`, the shim maps `CLICKHOUSE_PORT_TCP_SECURE` (`9440`) to `CLICKHOUSE_PORT_HTTPS` (`8443`) in the same way. |
| `CLICKHOUSE_TEST_RUNNER_LIVE` | (none) | `1`: run the live tests of `ClickHouse.Driver.TestRunner.Tests` (they read `CLICKHOUSE_HOST` and the two ports). |
| `CLICKHOUSE_CONTAINER` | `clickhouse` | The server container, for `run-with-real-client.sh`. |

Do not set `CLICKHOUSE_DATABASE`: `tests/clickhouse-test` then gives the client `--database` two times. The
real `clickhouse-client` (`run-with-real-client.sh`) refuses this, and every test fails.

## The allowlist

[`allowlist.txt`](allowlist.txt) lists the tests that pass reliably, one test name (the file name without
`.sql`) per line. `#` starts a comment. The script gives the runner anchored regular expressions, so each
name selects exactly one test. (The runner itself matches each name as a substring, so `00396_uuid` would
also select `00396_uuid_v7`.)

The file has four kinds of lines:

| Line | Meaning |
|---|---|
| `00001_select_1` | The test runs over both protocols. |
| `00015_totals_having_constants  # only=tcp: <reason>` | The test runs over that protocol only, because it cannot pass over the other one for an environment cause (for example, the server sends no `WITH TOTALS` row in the HTTP `RowBinary` output). |
| `01523_interval_operator_support_string_literal  # known=http: [http-type-interval]` | A **known failure over one protocol**: a bug of that client or a gap of the shim. The test runs over the other protocol. |
| `# 02243_make_date32  # [date32-year-zero]` | A **known failure over both protocols**: the test does not run. |

The ID in brackets names the group of the known failure. The known failures are at the end of the file, one
section per group. The header of the group describes the cause, names the protocols and gives the number of
tests per protocol. The header of a client group also gives a small reproduction and has an `Issue:` line; a
shim group has `shim:` in its first line. A `known=<protocol>` line is in the section of its group.

The run script reads only one group per line. So a test that fails for two causes is in the section of one
group, and the header of the other group names it ("Also:"). A test can fail over one protocol for an
environment cause and over the other for a client bug. Then its line is `# <test>  # [<id>]`, and its group
header gives the environment cause.

To run the known failures of one or more groups, for example to check a fix:

```bash
KNOWN_FAILURES=http-type-interval tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
KNOWN_FAILURES=all CLICKHOUSE_TEST_RUNNER_PROTOCOL=tcp tests/clickhouse-test-runner/scripts/run-upstream-tests.sh --timeout 120
```

Over a protocol, `KNOWN_FAILURES` selects the `# <test>  # [<id>]` lines and the `known=<protocol>` lines of
that protocol. Some known failures over `tcp` hang until the timeout of the runner (600 s; see the group
`[tcp-insert-needs-client-data]`), so give `--timeout 120` for a run of all groups.

When a fix makes the tests of a group pass, change their lines: `# <test>  # [<id>]` becomes `<test>`, and
`<test>  # known=<protocol>: [<id>]` becomes `<test>`. When all tests of a group pass, remove the group
header.

### Grow the allowlist

1. Run the candidate tests through the harness over **both** protocols. To try all `.sql` tests:

   ```bash
   ls tests/clickhouse-test-runner/.upstream/ClickHouse/tests/queries/0_stateless \
     | sed -nE 's/\.sql(\.j2)?$//p' | sort -u > /tmp/candidates.txt
   for p in http tcp; do
     CLICKHOUSE_TEST_RUNNER_PROTOCOL=$p UPSTREAM_TEST_LIST=/tmp/candidates.txt \
       tests/clickhouse-test-runner/scripts/run-upstream-tests.sh -j 6 --no-long --timeout 120 \
         | tee /tmp/run-$p.log
   done
   ```

   Run the protocols one after the other, not at the same time: the memory pressure on the server causes
   `MEMORY_LIMIT_EXCEEDED` failures that are not real.

2. Keep the tests that report `[ OK ]` over both protocols:

   ```bash
   comm -12 \
     <(sed -nE 's/^.*\] ([^ :]+): +\[ OK \].*/\1/p' /tmp/run-http.log | sort -u) \
     <(sed -nE 's/^.*\] ([^ :]+): +\[ OK \].*/\1/p' /tmp/run-tcp.log | sort -u)
   ```

   Find the cause of each test that fails over one protocol or both (see [`AGENTS.md`](AGENTS.md)), and add
   it as an `only=`, `known=` or `# <test>  # [<id>]` line.

3. Run the new allowlist three times on a fresh server, with `-j` higher than CI uses, over each protocol.
   Find the cause of each failure before you remove a test.

**Rule:** add only tests that pass reliably. Before you remove a test or decide not to add it, find out
whether it fails because of a bug in the client. The harness exists to find those bugs. See
[`AGENTS.md`](AGENTS.md) for the steps.

To compare with the real `clickhouse-client`, run the same tests through it, in the server container:

```bash
UPSTREAM_TEST_LIST=<(echo 01523_interval_operator_support_string_literal) \
  tests/clickhouse-test-runner/scripts/run-with-real-client.sh
```

A test that passes there and fails through the shim shows a bug in the client or in the shim.

## How the shim imitates clickhouse-client

The shim sends nothing that `clickhouse-client` does not send, because some tests read their own queries from
`system.query_log`.

- **Statements.** The shim splits the input into statements as `clickhouse-client` does: at a top-level `;`,
  with the same-line trailing comment kept with its statement. It sends each statement with its `;`. The
  inline data of `INSERT ... FORMAT <name>` (other than `Values`) ends at an empty line. The `-- Tags:` header
  is not part of a statement. Every statement runs, with or without `--multiquery`.
- **Test hints.** `-- { serverError X }`, `-- { clientError X }` and `-- { error X }` (codes or names) make
  the error the expected result. A hint in a leading comment is ignored, as upstream. Through a client, the
  server finds all errors, so a server error can satisfy a `clientError` hint. `-- { echo }`,
  `-- { echoOn }` and `-- { echoOff }` print the statements, but not a statement with a syntax error
  (`clickhouse-client` parses before it echoes). `-- { retry ... }` is not supported.
- **Sessions.** `SET`, `USE` and temporary tables last for the whole invocation.
  - `http`: all requests of an invocation have the same session ID (`UseSession`, `SessionId`). The client
    sends the database of the connection with each request, so a `USE` calls
    `ClickHouseConnection.ChangeDatabase`.
  - `tcp`: the session is one connection (`OpenSessionAsync`, `MaxPoolSize=1`). The TCP client closes the
    session after a server exception, also after one that a hint expects
    (`[tcp-session-lost-on-error]` in the allowlist). The shim then opens a new session and runs the `USE` and
    `SET` statements of the invocation again. Other session state, such as a temporary table, is lost.
- **Statement types.** Statements that start with `SELECT`, `WITH`, `FROM`, `SHOW`, `DESC`, `DESCRIBE`,
  `EXISTS`, `EXPLAIN`, `CHECK` or `KILL` return rows: `ExecuteReaderAsync` (`http`) or `StreamAsync`
  (`tcp`). All other statements run with `ExecuteNonQueryAsync` (`http`) or `ExecuteAsync` (`tcp`). After
  `SET dialect` to another language (KQL, PRQL, ...), every statement other than `SET` returns rows. Over
  `http`, the shim removes the trailing `;` of such a statement. After `SET implicit_select = 1`, an
  expression alone (`1 + 2`) also returns rows.
- **Values.** The shim renders the value that the client gives (`ClickHouseDataReader.GetValue` or
  `IColumn.GetValue`), directed by the column type of the result (`GetDataTypeName` or `IColumn.TypeName`),
  as the server writes `TabSeparated`. Over `http`, the connection uses `MapReadMode=KeyValuePairs`, the
  option of the client that keeps the server order of the map entries and repeated keys. The renderer also
  follows the output settings of the session and the query: `date_time_output_format`,
  `format_tsv_null_representation`, `output_format_decimal_trailing_zeros` and
  `date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands`.
- **Output formats.** The clients read results as `RowBinaryWithNamesAndTypes` (`http`) or `Native` (`tcp`),
  and the shim writes them in the `TabSeparated` family (also `TSVRaw` and `...WithNames[AndTypes]`) or
  `Null`. A statement with any other output format fails. Over `tcp`, the shim sends the statement as it is
  (the server ignores the `FORMAT` clause). Over `http`, it removes the clause, except for `FORMAT Null`.
  Where the SQL parser expects an operand, `format` is a name and not the clause (`SELECT * FROM format f`,
  `SELECT format TSV`), as for the server.
- **Totals and extremes.** Over `tcp`, the totals and the extremes come from `ClickHouseTcpQueryCallbacks`,
  and the shim writes an empty line before each block, as the server does. The HTTP `RowBinary` output has
  no totals and no extremes, so such tests run over `tcp` only.
- **Settings.** Of the settings on the command line, only `log_comment`, `max_insert_threads`, `max_threads`
  and `send_logs_level` go to the server, with each query (`ClickHouseCommand.CustomSettings` or
  `ClickHouseTcpQueryOptions.Settings`), as in `clickhouse-client`. The runner's feature-tag overrides
  `async_insert` and `enable_parallel_replicas` go too, when their value is `0`. A `SET` of the same setting
  in the test replaces the command-line value. `--param_<name>=<value>` gives a query parameter to the
  statements that use it.
- **Errors.** An unexpected error stops the shim. It writes the error with its code and name to stderr, and
  the exit code is the error code modulo 256. Otherwise the shim writes nothing to stderr.
- **Error names.** [`ErrorCodes.g.cs`](ClickHouse.Driver.TestRunner/ErrorCodes.g.cs) maps the error names
  of test hints to codes. [`scripts/fetch-upstream.sh`](scripts/fetch-upstream.sh) generates it from
  `src/Common/ErrorCodes.cpp` of the upstream checkout, so it always matches the tests. Commit the file when
  it changes.
- **Config.** `clickhouse extract-from-config --key listen_host` prints `127.0.0.1`; other keys print
  nothing.

## Known failures

The end of [`allowlist.txt`](allowlist.txt) lists the tests that fail because of a bug in one of the clients
or a gap in the shim, in groups. Each group header describes the cause and names the protocols; a client group
also gives a small reproduction. A test that cannot pass for an environment cause (a cluster, ZooKeeper, server
configuration, features of `clickhouse-client` itself) is not listed; see
[`AGENTS.md`](AGENTS.md#environment-causes). The shim does not try `.sh` tests.

One known failure can pass without a fix: `03024_total_rows_approx_is_set_for_system_zeros_and_generate_random`
uses `generateRandom()` without a structure, and fails over `tcp` only when the random structure has a tuple
of more than 7 elements (`[tcp-tuple-over-7-elements]`).

## CI

[`upstream-sql-tests.yml`](../../.github/workflows/upstream-sql-tests.yml) runs the allowlist nightly,
manually, and on pushes and pull requests that change this directory or the workflow. A pull request that
changes only the clients does not start it; the nightly run tests the clients of `main`.

- The `build` job builds the shim once (`scripts/build.sh`), and the test jobs use it. The `harness-tests` job
  runs the unit and live tests of the shim.
- The matrix of the test jobs is `{protocol: http, tcp} × {shard: 1..8}`, at `-j 4` on 4-vCPU runners.
- CI does not stop a shard after 20 failures in a row (`--max-failures-chain`, see above), so a failed run
  shows all the failures of its shard.
- CI does not run the failed tests again: no known client bug makes random tests fail, so a failed test fails
  the job. If such a bug appears, `RETRY_FAILED=<n>` runs the failed tests again, at most `n` times: a test
  that passes on a retry shows as a warning, and a test that fails every time fails the job. Do not set
  `RETRY_FAILED` when you grow or check the allowlist: a test that needs a retry is not reliable.
- The server version and the upstream tests are pinned together in [`clickhouse-version`](clickhouse-version).
  A new ClickHouse release changes nothing in CI, so a pull request never fails because of a release that it
  does not touch. A bump is a pull request of its own; see [`AGENTS.md`](AGENTS.md#bump-the-server-version).
- The `clickhouse_version` input of a manual run tries another version without a commit, for example before a
  bump. The version must have a `clickhouse/clickhouse-server:<version>` image and a `v<version>-stable` tag
  upstream. An LTS release has a `v<version>-lts` tag instead, so the input does not take it.
- When an allowlisted test is not in the checkout (upstream renamed or removed it), the run shows a warning
  with the names.
- A failed job uploads the output of the runner (with the diffs), the log of the shim
  (`CLICKHOUSE_CLIENT_CLI_LOG`), the error log of the server, and the `.stdout` and `.stderr` files of the
  failed tests.
- If a shard takes much more than a few minutes, increase the number of shards: change the `shard` list and
  `SHARD_TOTAL` together.
- A pull request that changes only Markdown files in this directory does not start the workflow.

## Bump the server version

A bump of [`clickhouse-version`](clickhouse-version) is a pull request of its own. Try the new version with a
manual run first, find the cause of each new failure, and commit the new `ErrorCodes.g.cs` with the pin. The
steps are in [`AGENTS.md`](AGENTS.md#bump-the-server-version).
