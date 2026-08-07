# ClickHouse.Driver.Benchmark.Blog

Long-running macro benchmarks for the performance write-up: sustained, dataset-backed measurements of
the driver as a whole, with server CPU, GC pause distributions and bytes on the wire.

This is **not** `ClickHouse.Driver.Benchmark`. That project holds the per-PR micro-benchmarks, runs on
every push to `main` via `benchmark.yml` (`--filter "*"`), and finishes in minutes. Adding a 45-second
sustained burst to it would make that CI job unusable, and its harnesses need a staged `bench.hits`
that CI does not have. Hence a separate project, invisible to `benchmark.yml`, in the solution so it
still builds.

`ComparisonConfig` is likewise untouched: `/benchmark-compare` depends on it, and changing it would
move already-published PR numbers.

---

## Quick start

```bash
# 1. Dedicated server on 8124. Deliberately not 8123 — see "Why a separate server".
docker compose -f ClickHouse.Driver.Benchmark.Blog/env/docker-compose.yml up -d

# 2. Datasets: 1M-row bench.hits, 200k-row bench.types_wide, both insert sinks.
./ClickHouse.Driver.Benchmark.Blog/scripts/stage-datasets.sh

# 3. Prove the wiring end to end. Minutes, not hours. Numbers are NOT publishable.
./ClickHouse.Driver.Benchmark.Blog/scripts/run-suite.sh --smoke

# 4. A real pass of one family.
BENCH_ENV_LABEL=L-ec2-4c ./ClickHouse.Driver.Benchmark.Blog/scripts/run-suite.sh --filter '*Overhead*'
```

Everything lands in `results/<timestamp>-<label>/`, which is the unit of publication: it carries its
own `environment.txt` provenance, so any chart built from it can be traced back to the configuration
that produced it.

---

## The two families

Every claim belongs to exactly one of them. They have different harnesses, different metrics and
different headline numbers.

| | Family O — overhead-bound | Family C — content-bound |
|---|---|---|
| Workload | Many small queries, concurrency 1/8/32/64 | One big scan / one big insert |
| Cost model | Per **query** fixed cost | Per **row** and per **byte** |
| Harness | `SmallQueryOverheadBenchmark` | `HitsScanBenchmark`, `HitsInsertBenchmark`, `AdoReaderShapeBenchmark` |
| Metrics | queries/sec, p50/p95/p99, GC pause distribution under load, connection-pool health, LOH growth | rows/sec, MB/s, total allocated, gen counts, peak working set, wire bytes, server CPU |

Compression appears in both, **with opposite sign**: a bandwidth win in C, a latency tax in O. Saying
that plainly is more credible than one "compression is faster" claim — and
`SmallQueryOverheadBenchmark.SelectOneUncompressed` exists specifically to let the tax be published.

| Harness | What it is for |
|---|---|
| `SmallQueryOverheadBenchmark` | Family O. Sustained concurrent bursts; pool + fragmentation effects. |
| `HitsScanBenchmark` | Family C read. 105-column and 10-column scans of `bench.hits`. |
| `HitsInsertBenchmark` | Family C write. 105-column POCO vs `object[]`, two batch sizes, Null + MergeTree sinks. |
| `AdoReaderShapeBenchmark` | The four reader access patterns, with `GetValue` as a self-validating control. |
| `CodecReadBenchmark` | Codec matrix, read direction. Server compresses; level sweep 1/3/9. |
| `CodecInsertBenchmark` | Codec matrix, insert direction. Server decompresses; client level sweep. |
| `TypesWideBenchmark` | The composite-type long tail, one arm per type, with a control arm. |
| `ColdStartBenchmark` | First-query cost: what a short-lived process pays and never amortises. |

---

## Three BenchmarkDotNet defaults this project overrides

All three verified against 0.15.8 by reflection, not from documentation.

| Default | Value | Why it is wrong here |
|---|---|---|
| `GcMode.Force` | **`true`** | BDN calls `GC.Collect()` between every iteration, resetting exactly the LOH fragmentation and promotion state a steady-state run exists to observe. Set to `false`. |
| `GcMode.Server` | **`false`** | Workstation GC — the wrong mode for a server-side driver, and the mode **every existing GC number in every PR body in this release was measured under**. Both modes now run as paired arms. |
| `EventPipeProfiler(profile)` | `performExtraBenchmarksRun: true` | The attribute form re-runs the whole benchmark a second time just to collect the trace. That doubles a 45-second workload, and the summary table then comes from a *different process* than the trace beside it. Set to `false`, and tracing is opt-in so headline runs stay unperturbed. |

`GcMode.Concurrent` stays `true`: background gen2 on is correct, and is set explicitly so it reads as
a decision rather than an oversight.

### Server GC is an arm, not an environment detail

Server GC gives each logical core its own heap with its own GC thread, and a gen0 budget of tens of MB
per heap instead of something L2-cache sized. Almost every PR in this release quotes *Gen0 collections
per 1k ops*, which is a direct function of that budget. The same real allocation win therefore reads
as a very different-sounding claim in the two modes — a "Gen2: 2000 → 0" may become "30 → 0". Both
modes run; if they tell different stories that is a finding, not a footnote.

Because Server GC scales its heap count with core count, **core count is a published parameter**. This
box has 4 logical cores, which is low enough that a bigger benchmark host will behave visibly
differently.

---

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `CLICKHOUSE_CONNECTION` | `Host=localhost;Port=8124` | DSN. Port 8124 is the dedicated container. |
| `BENCH_PROFILE` | `full` | `smoke` shrinks every axis at once. |
| `BENCH_ENV_LABEL` | `unlabelled` | Environment tag recorded in results, e.g. `L-ec2-4c`, `C-aws-us-east-1`. |
| `BENCH_DATASET_DB` / `BENCH_SINK_DB` | `bench` | Where the datasets and insert sinks live. |
| `BENCH_BURST_SECONDS` | `45` (smoke: 3) | Length of one Family O iteration. |
| `BENCH_CONTENT_ROWS` | `1000000` (smoke: 20000) | Rows for Family C. |
| `BENCH_TYPES_WIDE_ROWS` | `200000` (smoke: 10000) | Rows for the long-tail scan. |
| `BENCH_WARMUP` / `BENCH_ITERATIONS` / `BENCH_LAUNCHES` | `1` / `5` / `3` | Per-axis overrides; win over the profile. |
| `BENCH_VERSIONS` | `source` | Comma-separated version arms, e.g. `1.0.0,1.2.0,1.3.0,1.4.0-local`. First is the baseline. |
| `NUGET_SOURCE` | — | Extra restore source for a locally packed `.nupkg`. |
| `BENCH_GC_MODES` | `both` | `server`, `workstation`, or `both`. |
| `BENCH_GC_TRACE` | off | `1` attaches `EventPipeProfiler`, writing a `.nettrace` per benchmark. |
| `BENCH_GC_TRACE_PROFILE` | `verbose` | `collect` is the lighter variant when GcVerbose traces get unwieldy. |
| `BENCH_METRICS_CSV` | `results/side-metrics.csv` | Side-channel metrics destination. |
| `BENCH_RUN_ID` | `adhoc` | Groups every child process of one pass. `run-suite.sh` sets it. |

---

## Where the numbers come from

Three outputs, because no single one carries everything.

**1. BenchmarkDotNet's own reports** (`artifacts/results/*.csv`, `*-report-github.md`) — wall clock,
allocated bytes/op, Gen0/1/2 per 1k ops. Also `*-measurements.csv`, one row per iteration, which is
what a distribution chart should be built from rather than the summary table's mean.

**2. `side-metrics.csv`** — everything BDN cannot carry, in long format
(`run_id, ts, env_label, profile, gc_mode, driver_version, benchmark, args, iteration, metric, value`):

- `server_cpu_us` — `ProfileEvents['OSCPUVirtualTimeMicroseconds']`. Transport-independent, so it
  cannot be confounded by network conditions, and on a managed service it is what the user pays for.
- `server_cpu_wait_ratio` — a **validity** metric, not a cost one. Above ~0.25 the server was contended
  and any wall clock from that pass describes a busy box. `run-suite.sh` warns.
- `server_net_send_bytes` / `server_net_recv_bytes` — bytes on the wire, counted by the server, i.e.
  post-compression. The codec matrix's byte column.
- `client_cpu_us` — process CPU across the measured region. Process-wide, so it includes runtime
  threads; that is the honest number for a codec comparison and is not the method's exclusive CPU.
- `latency_p50_us` / `p95` / `p99` / `max` — from an allocation-free bucketed histogram, so recording
  them does not pollute the allocation figures next to them.
- `pool_peak_http11_connections_current_total` and friends — from the runtime's own event counters.
  This is the direct evidence for the leaked-response claim: latency alone cannot distinguish a
  degraded pool from a slow server, but connection count can.

Long format on purpose: harnesses keep adding metrics, and a wide schema would either churn or grow a
forest of empty columns. Pivot in the analysis step.

**3. `gc/gc-events.csv` and `gc/gc-summary.csv`** — from `gc-report`, when `--gc-trace` was used. One
row per collection (pause, generation, per-generation sizes, LOH size and fragmentation, compacting)
and one summary row per trace (total pause, % time in GC, p50/p95/p99/max pause, LOH peak).

> `MemoryDiagnoser` reports **only** allocated bytes/op and Gen0/1/2 counts per 1k ops. No pause
> durations, no LOH size, no fragmentation, no % time in GC. Those come exclusively from the trace.

**Check `has_detailed_gc_info` before plotting anything fragmentation-shaped.** Fragmentation needs
per-heap-history events; when a trace lacks them the columns are written **empty rather than zero**,
because a zero would read as a perfectly compacted heap. `run-suite.sh` warns when any trace is
missing them.

---

## Why a separate server

The only other local ClickHouse is shared with `clickhouse-client-benchmarks`, whose test suite runs
against it. A benchmark run and somebody else's suite on one server contend for CPU, and the
contention arrives as wall-clock variance that looks exactly like a driver regression.

`env/docker-compose.yml` therefore starts a **pinned** image on **8124/9010**, with `query_log` on
(the codec matrix reads server CPU out of it), `http_zlib_compression_level` pinned to 3, and the
trace/metric logs that only cost CPU switched off.

Two things that cost real time to discover:

- **The volumes mount single files, not directories.** Mounting a directory onto
  `/etc/clickhouse-server/config.d` replaces the image's own contents — including
  `docker_related_config.xml`, the file that sets `listen_host`. Without it ClickHouse listens on
  loopback inside the container only: the healthcheck still passes, the published port still accepts a
  TCP connection, and every request from the host is reset **with nothing written to the server log**.
- **`CLICKHOUSE_SKIP_USER_SETUP=1` is required.** The entrypoint otherwise writes
  `users.d/default-user.xml` on boot to disable network access for `default`, which both fails against
  a read-only mount and is the opposite of what is wanted.

The image tag is pinned rather than floating because §8 of the plan publishes the server version as a
confounder, and `:latest` would silently re-baseline every number between two runs. **Note:** the plan
document names `26.6.1.1193`; this container pins `25.10.7.6-alpine`, which is what is actually
available locally. Reconcile that before publishing — `environment.txt` records whatever really ran.

---

## Datasets

### `bench.hits` — 1,000,000 rows, 105 columns

The canonical ClickBench `hits` schema, column for column: 48 `Int16`, 28 `String`, 19 `Int32`, 6
`Int64`, 3 `DateTime`, 1 `Date`.

**Caption it as 1M rows, not 100M.** Anyone who knows ClickBench will assume otherwise. The 1M figure
is exactly the first `athena_partitioned` partition (`hits_0.parquet`), so "the first 1M-row partition
of ClickBench hits" is both true and checkable.

**The plan's open question about the type mix is resolved: it is not an issue.** The concern was that
this copy is Int-heavy where upstream ClickBench is UInt-heavy. Verified with `DESCRIBE` against
`hits_0.parquet`: the upstream `hits_compatible` parquet is itself Int-typed, so this schema and
upstream's agree and no caveat is needed. The UInt-heavy schema people remember is the older
Yandex.Metrica `hits_v1`, a different dataset. `stage-datasets.sh` prints the type histogram on every
run as the evidence behind the caption.

`--source local` copies from an existing `hits` table over Native format (fast, offline);
`--source clickbench` streams the canonical parquet (~122 MB). Both produce the same schema.

### `bench.types_wide` — 200,000 rows

One column per long-tail optimisation, because `hits` covers **none** of them: `Nullable`, `Decimal`,
`Array`, `Array(Nullable)`, `Map`, `Tuple`, `UUID`, `LowCardinality`, `Enum8`, `Dynamic`, `Variant`,
`Int128`, `IPv6`, `FixedString`, `JSON`, `DateTime64`. Generated deterministically from
`system.numbers` — no `rand()`, so two stagings produce byte-identical data and a decode number stays
comparable across machines and across time.

`Dynamic` cycles three distinct shapes per three rows so its per-value dispatch is actually exercised;
a single-type `Dynamic` column would measure only the fast path. `JSON` carries a numeric typed path, a
string path, a genuinely nested object, a bool, and a null leaf every fifth row.

`TypesWideBenchmark` includes an `id` **control arm**: subtract it to remove query, transport and
row-iteration overhead, none of which is what the long-tail chart is comparing.

### Insert sinks

`hits_sink_null` (`ENGINE = Null`) and `hits_sink_mt` (MergeTree), both created with
`CREATE TABLE ... AS hits` so the 105-column list has exactly one definition and cannot drift.

The Null sink isolates client cost — serialization, compression, transport — and **any number from it
must say so**, because it is not what an insert into a real table costs. The MergeTree arm is the
credibility check, truncated between iterations so part accumulation does not make later iterations
look slower than earlier ones.

---

## Version arms

`BENCH_VERSIONS` turns each entry into its own BDN job, built against that published package, so the
release chart gets four bars mapping onto the three phases of the year. `scripts/pack-local.sh` packs
the working tree as `1.4.0-local` and then verifies every arm both restores **and compiles**.

Comparison mode compiles one source tree against every package, so a harness spanning v1.0.0 can only
touch v1.0.0 API. The `CH_API_*` define constants in the csproj are how each harness opts in — see
[`docs/API-CORRIDOR.md`](docs/API-CORRIDOR.md) for the verified surface, and for the trap that a
`0.0.0-pr.N` version compares below every real release.

---

## Cloud (environment C)

Every harness is endpoint-agnostic, and `stage-datasets.sh` and `run-suite.sh` both work against a
cloud DSN. What is **not** provisioned here: the VM and the service.

```bash
export CLICKHOUSE_CONNECTION='Host=<host>;Protocol=https;Port=8443;Username=<u>;Password=<p>'
export BENCH_ENV_LABEL=C-aws-us-east-1
./scripts/stage-datasets.sh --source clickbench    # the local copy is not reachable from a VM
./scripts/run-suite.sh --filter '*'
```

Run the client on a VM **in the service's own region**. Not a laptop over a WAN: that flatters
compression in a way most deployments do not reproduce.

The local arm is on EC2 (4-core Xeon 8375C, kernel 6.17-aws), so the plan's "move off WSL2" item is
already satisfied — no number produced here comes from WSL2.

`log_queries` must be on at the endpoint or every server-CPU arm fails loudly. `stage-datasets.sh`
warns if it is off.

---

## Traps

- **Don't claim wall-clock wins on loopback.** Allocation is the local signal; wall clock is the cloud
  signal.
- **`hits` here is 1M rows, not 100M.** Caption it.
- **The zstd default is breaking.** Pair the compression charts with the regimes where lz4 is still the
  better ask, or the post is selling something.
- **The `1.2.0 → new` read number is partly an LOH fix that already shipped in 1.3.0.** Label it; that
  is why the chart has a 1.3.0 bar.
- **Fragmentation needs a long iteration and `GcForce=false`.** A three-second run with forced
  collection between iterations shows none of it.
- **Existing PR numbers are Workstation-GC numbers**, taken on different machines on different dates.
  Don't quote them beside freshly-measured Server-GC numbers without re-running.
- **Don't build a table lining up per-change percentages as if they were commensurable.** Separate
  sections with their own numbers are fine; one table with a total row is not.
- **`smoke` numbers are not measurements.** They are in every results header for a reason.
