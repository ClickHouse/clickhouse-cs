# Instructions for agents: upstream SQL test harness

This harness runs the ClickHouse core SQL tests through the two clients of this repository: `http`
(`ClickHouse.Driver`) and `tcp` (`ClickHouse.Driver.Tcp`). See [`README.md`](README.md) for how it works and
how to run it. The root [`AGENTS.md`](../../AGENTS.md) also applies.

## The goal

The goal is to find bugs in the clients. To pass more tests is not the goal. A failed test is information.
Do not hide it.

## Rules

1. **Find the cause of a failure before you do anything with the test.** A test fails for one of three
   causes:
   - **The client** (`ClickHouse.Driver` or `ClickHouse.Driver.Tcp`). This is what the harness is for.
   - **The shim** (the program in this directory). It does not do what `clickhouse-client` does.
   - **The environment.** The test cannot pass through any client on this server (see
     [Environment causes](#environment-causes)).
2. **The default for a client bug is a fix in the client:** a pull request that fixes the client, with a
   test in its test project (read the `AGENTS.md` of the client first, as the root `AGENTS.md` says). Do not
   fix a client bug in a pull request that changes the harness: one change per pull request. Some logic
   exists in both clients (see the root `AGENTS.md`), so check the other client too. When someone files an
   issue for a group, put it on the `Issue:` line of the group.
3. **The default for a shim bug is a fix in the shim.** Make the shim copy `clickhouse-client` more exactly.
   Do not change the shim to hide a client bug, for example by another read method, a type-name workaround
   or a correction of a wrong value. The shim must use each client as a user does: `http` reads each value
   with `ClickHouseDataReader.GetValue(i)`, and `tcp` with `IColumn.GetValue(row)`.
4. **Remove a test from the allowlist only when it cannot pass for an environment cause** (see rules 6 and
   7). Write the reason in the commit message.
5. **A test that fails because of a client bug or a shim gap stays visible.** Do not delete it. Put it in
   [`allowlist.txt`](allowlist.txt) as a known failure in the group of its cause: `# <test>  # [<id>]` when
   it fails over both protocols, `<test>  # known=<protocol>: [<id>]` when it fails over one protocol only
   and passes over the other. Each group has a header that describes the cause, names the protocols and
   gives the number of tests per protocol. The header of a client group also gives a small reproduction
   (public API only) and has an `Issue:` line. Make a new group when no group fits; mark a shim group
   `shim:` in its header. See the README for the format and for how to
   run one group (`KNOWN_FAILURES=<id>`).
6. **A test that cannot pass over one protocol** for an environment cause (for example `WITH TOTALS` over
   `http`) gets `# only=<protocol>: <reason>`. Do not use this for a client bug: use a known failure.
7. **Do not list a test that cannot pass for an environment cause** over both protocols.

## How to find the cause

Do these steps in this order. Build the shim once (`scripts/build.sh`); the commands below use
`SKIP_BUILD=1`.

1. **Run the test over both protocols:**

   ```bash
   UPSTREAM_TEST_LIST=<(echo <test>) SKIP_BUILD=1 tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
   UPSTREAM_TEST_LIST=<(echo <test>) SKIP_BUILD=1 CLICKHOUSE_TEST_RUNNER_PROTOCOL=tcp tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
   ```

   The two protocols are two clients with their own type parsers and readers. A test that passes over one
   protocol and fails over the other points to the client of the failing protocol, unless the cause is in
   the list below.
2. **Run the test through the real `clickhouse-client`** in the server container, on the same server:

   ```bash
   UPSTREAM_TEST_LIST=<(echo <test>) tests/clickhouse-test-runner/scripts/run-with-real-client.sh
   ```

   If it also fails here, the cause is the environment. If it passes here, the cause is the client or the
   shim.
3. **Find the statement that fails.** Set `CLICKHOUSE_CLIENT_CLI_LOG` to a file; the log shows each statement
   and the error. Read the diff in the output of the runner, and the `.stdout` and `.stderr` files next to the
   test.
4. **Compare the shim with the server on that one statement.** Run it through the shim
   (`tests/clickhouse-test-runner/.bin/clickhouse-client --query "<SQL>"`, with
   `CLICKHOUSE_TEST_RUNNER_PROTOCOL` for each protocol) and through `curl` with `FORMAT TSV`. Then make the
   statement smaller until it is as small as possible.
5. **Decide between the client and the shim with a small program** that uses only the public API of the
   client: a console project that references the client projects, as `examples/` does. Read the value as
   the shim does (rule 3) and print it. If the program also gives a wrong value or an error, the bug is in
   the client. If the program gives the correct value, the bug is in the shim. Try the client of each
   protocol that fails.

### Environment causes

A test cannot pass through any client on the CI server when it needs one of these:

- A cluster, shards or replicas (`CLUSTER_DOESNT_EXIST`, `test_shard_localhost`).
- ZooKeeper or Keeper (`NO_ZOOKEEPER`, `Replicated*` engines).
- External services: S3, Azure, HDFS, Kafka, MySQL, PostgreSQL, MongoDB, URL sources.
- Server configuration of the upstream CI: named collections, dictionaries, storage policies, disks, users
  and grants, macros (`NAMED_COLLECTION_DOESNT_EXIST`, `NO_ELEMENTS_IN_CONFIG`, `UNKNOWN_POLICY`,
  `UNKNOWN_DISK`, `ACCESS_DENIED`).
- Features that the server build or platform does not have (`SUPPORT_IS_DISABLED`).
- Stateful datasets (`--no-stateful`) or long tests (`--no-long`).
- Behavior of the HTTP interface itself (`only=tcp`). The server sends no `WITH TOTALS` and no `extremes`
  rows in the HTTP `RowBinary` output, and an HTTP request fails when the current database of the session
  was dropped.
- Behavior of the native protocol itself (`only=http`), when the real `clickhouse-client` gives the same
  result as the `tcp` protocol.
- Features of `clickhouse-client` itself that are not part of a client library: `INTO OUTFILE`,
  `FROM INFILE`, client-side parsing of `INSERT` data, and client-only settings.
- Results that depend on the threads that the server gives under parallel load: `EXPLAIN PIPELINE` and
  `EXPLAIN ANALYZE` in a test without the `no-parallel` tag, and tests that set
  `max_streams_to_max_threads_ratio`, `max_streams_multiplier_for_merge_tables` or
  `max_streams_for_merge_tree_reading`. Such a test passes alone and fails now and then in a full run.
- Tests that conflict with other tests of the set: a fixed table UUID, or server state that a test leaves
  behind (failpoints that a failed test did not disable, row policies). Such a test passes alone on a fresh
  server and fails in a full run. A failure message with "Injected failure" comes from a failpoint of another
  test.
- Memory limits of the server, when the test also fails through the real `clickhouse-client`.

These are **not** environment causes. Find the bug:

- A value that is different from the server's value. This includes a time zone offset: the HTTP client uses
  its own tz database (NodaTime TZDB), and its rules for some zones before 1940 are not the rules of the
  server (`[http-tz-database-differs]`).
- A type that the client does not parse, or a read error.
- A failure over one protocol only, when the cause is not in the list above.
- A test that hangs over one protocol only.

## Conditions for the allowlist

- Add a test only when it passes over **both** protocols, or over one protocol with an `only=` reason
  (rule 6) or a `known=` line (rule 5).
- Run new tests three times, also with a higher `-j` than CI (CI uses `-j 4`), on a **fresh** server (remove
  the server container and start a new one). Tests change the state of the server (row policies, settings
  profiles, users). A test that passes only after other tests ran must not go in.
- The TCP client and the shim read the tz database of the system (`TimeZoneInfo`). Some tests use legacy
  zone names (`US/Samoa`, `Asia/Calcutta`), which current Ubuntu versions have only in `tzdata-legacy`. CI
  installs it; install it locally too, or such tests fail for a cause that is not in the client.
- Do not run two sweeps at the same time on one server. The memory pressure causes `MEMORY_LIMIT_EXCEEDED`
  failures that are not real.
- CI does not run failed tests again. When a test in the allowlist starts to fail intermittently, find the
  cause first (rule 1). Remove it only when the cause is in the environment or in the test itself, and write
  the reason. A client bug can make random tests fail. Put the tests that it makes fail in its group. When
  the set changes from run to run, set `RETRY_FAILED` in the workflow instead, and document the group in the
  README.

## Bump the server version

The server version and the upstream tests are pinned together in [`clickhouse-version`](clickhouse-version).
A new ClickHouse release changes nothing in CI until a pull request bumps the pin, so other pull requests do
not fail because of a release. Bump the pin in a pull request of its own, and make that pull request pass
before it merges:

1. Select the version: a full version with a server image tag and a `v<version>-stable` tag upstream,
   normally the version of the `latest` image.
2. Try it: start a manual run with the `clickhouse_version` input, or run the allowlist locally on that
   version (`fetch-upstream.sh <version>`, and a fresh server of that version).
3. For each allowlisted test that now fails, find the cause (rule 1). A release changes the tests and the
   server output, so many tests can fail at the same time with no change in the clients. To see whether a
   client is the cause, run the same test through the real client (`run-with-real-client.sh`) on the same
   server.
4. Run the known failures (`KNOWN_FAILURES=all`, over each protocol). Enable the tests that pass now.
5. Fix the names of allowlisted tests that the run reports as not in the checkout (upstream renamed or
   removed them).
6. Optionally, try the new tests of the release (see "Grow the allowlist" in the README).
7. Change `clickhouse-version`, run `fetch-upstream.sh`, and commit `ClickHouse.Driver.TestRunner/ErrorCodes.g.cs`
   with it. CI fails when the committed table does not match the pinned version.

## Keep the documents in sync

When you change the behavior of the shim (flags, statement handling, output formats, settings), update
[`README.md`](README.md), this file and the workflow in the same change. The shim is not in
`ClickHouse.Driver.sln`, so the other workflows do not build it: build it and run its tests
(`ClickHouse.Driver.TestRunner.Tests`) also when you change a public API of a client that it uses.
