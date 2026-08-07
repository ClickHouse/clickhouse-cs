# The API corridor

Comparison mode compiles **one source tree against several driver packages**. A harness that must
produce a v1.0.0 bar can therefore only touch API that v1.0.0 shipped — and the failure mode is a
compile error in the middle of a long run, not a wrong number.

The `CH_API_*` define constants in the csproj are how each harness opts into the API level it needs.

## How the level is resolved

```
BenchmarkComparisonMode != true     →  apiLevel = latest        (ProjectReference: the working tree)
BenchmarkComparisonMode == true     →  apiLevel = ClickHouseDriverVersion
  ...unless that version is < 1.0.0 →  apiLevel = latest        (see the 0.0.0 trap below)
```

Every build prints the resolved corridor, so a silently-wrong level cannot be mistaken for a harness
that simply has fewer arms:

```
[blog-bench] driver=1.2.0 apiLevel=1.2.0 1.2=True 1.3=False codecs=False
```

| Constant | Gate | Guards |
|---|---|---|
| `CH_API_1_2` | driver >= 1.2.0 | `RegisterBinaryInsertType<T>`, `InsertBinaryAsync<T>` — the POCO binary insert |
| `CH_API_1_3` | driver >= 1.3.0 | `ReadBufferSize`, `RegisterPocoType<T>`, `QueryAsync<T>`, `MapTo<T>`, `ReadValueConverter` |
| `CH_API_1_4` | driver >= 1.4.0 (unreleased) | `InsertOptions.Compressor` (#427), `Lz4Compressor`/`Lz4Level` (#431), `ZstdCompressor` (#523/#526), **and `ClickHouseClientSettings.AcceptEncoding`** (#490/#526) |

## ⚠ The 0.0.0 trap

`/benchmark-compare` builds the PR package as `0.0.0-pr.N` and main as `0.0.0-main`. MSBuild's
`VersionGreaterThanOrEquals` puts both **below** every real release, so a naive gate would compile the
modern API *out* of the arm that is supposed to have all of it — and the arm would still build, still
run, and still report numbers. Silently wrong, in the direction that flatters the baseline.

Hence: any version below `1.0.0` resolves to `latest`, which is what such a package actually is. Set
`/p:ClickHouseDriverApiLevel=<version>` to override this explicitly.

Note also that `VersionGreaterThanOrEquals` **ignores prerelease suffixes**: `1.3.0-rc1 >= 1.3.0` is
true, and `1.4.0-local >= 1.3.0` is true. That is what makes `1.4.0-local` work as the "new" arm
without special-casing, and it is why an rc is treated as having its release's surface.

## Verified arms

`scripts/pack-local.sh` packs the working tree as `1.4.0-local` and then **builds the harness against
every arm**, which is the only check that actually proves the corridor. Restoring is not enough: a
package can restore and still fail to compile the harness.

Verified on 2026-08-07 against BenchmarkDotNet 0.15.8 / .NET SDK 10.0.302:

| Arm | Restores | Harness compiles | `CH_API_1_2` | `CH_API_1_3` | `CH_API_1_4` |
|---|:--:|:--:|:--:|:--:|:--:|
| `1.0.0` | ✅ | ✅ | — | — | — |
| `1.2.0` | ✅ | ✅ | ✅ | — | — |
| `1.3.0` | ✅ | ✅ | ✅ | ✅ | — |
| `1.4.0-local` (packed working tree) | ✅ | ✅ | ✅ | ✅ | ✅ |
| source (`ProjectReference`) | n/a | ✅ | ✅ | ✅ | ✅ |

### ⚠ Two corrections to the benchmark plan's corridor table

Both found by running the check, not by reading tags:

1. **`AcceptEncoding` is not in v1.3.0.** The plan lists it as shipped there (alongside
   `ReadValueConverter`, #293/#321). It is not: `git show 1.3.0:ClickHouse.Driver/ADO/ClickHouseClientSettings.cs`
   contains no such property. It arrived unreleased with #490/#526. Consequence: the read-direction
   codec matrix is **source/1.4.0-only**, one release later than planned, and the cross-version
   compression story is default-vs-default for every published arm — including 1.3.0.
2. **A `using` needs gating too, not just the arms that use it.** `using ClickHouse.Driver.Compression;`
   sitting unguarded at the top of a file fails against every published package no matter how carefully
   the benchmark methods inside are `#if`-ed.

## What each arm can measure

| Harness | 1.0.0 | 1.2.0 | 1.3.0 | new | Notes |
|---|:--:|:--:|:--:|:--:|---|
| `HitsScanBenchmark` | ✅ | ✅ | ✅ | ✅ | Reader scan and typed accessors are in the corridor throughout — so the headline read chart gets all four bars. |
| `AdoReaderShapeBenchmark` | ✅ | ✅ | ✅ | ✅ | Same. This is the four-bar deep-dive figure. |
| `HitsInsertBenchmark` `ObjectArray*` | ✅ | ✅ | ✅ | ✅ | |
| `HitsInsertBenchmark.PocoNull` | — | ✅ | ✅ | ✅ | Needs `RegisterBinaryInsertType<T>`. Three bars, not four. |
| `SmallQueryOverheadBenchmark` | ✅ | ✅ | ✅ | ✅ | `ExecuteScalarAsync` only. All of #457/#451/#492 sit on it. |
| `TypesWideBenchmark` | ✅ | ✅ | ✅ | ✅ | `GetValue` only, deliberately. |
| `ColdStartBenchmark` | ✅ | ✅ | ✅ | ✅ | |
| `CodecReadBenchmark` | ⚠ | ⚠ | ⚠ | ✅ | `AcceptEncoding` is unreleased (see the corrections above), so on **every** published arm the codec cannot be selected and each compressed cell collapses onto the client default. Read those as **default-vs-default**. |
| `CodecInsertBenchmark` | ⚠ | ⚠ | ⚠ | ✅ | `InsertOptions.Compressor` is unreleased, so a package arm contributes a single `client-default` cell. Codec-by-codec insert comparison is source-mode only. |

**Both families' headline harnesses fit inside the v1.0.0 corridor.** That is the useful outcome: the
work that most wants a long baseline — #499, #472, #451, #383, #434, #442, #492 — is all reachable
through `ExecuteReaderAsync` / `ExecuteScalarAsync` / `InsertBinaryAsync`, which have been stable since
1.0.0.

## Adding a harness that needs newer API

1. Wrap the arm in the narrowest gate that covers it.
2. Run `scripts/pack-local.sh`. It builds every arm and fails loudly if the gate is wrong.
3. If a *whole* harness needs unreleased API, gate the class and say so in its XML docs — a harness
   that silently contributes zero arms to three of four bars is worse than one documented as 1.4.0-only.

**Gate on the version, not on `BenchmarkComparisonMode`.** A "source mode only" gate looks equivalent
and is not: it compiles the unreleased surface out of a locally packed `1.4.0-local`, which is the one
arm meant to have all of it — and that arm still builds and still reports numbers, just with silently
fewer cells. The `[blog-bench]` build line exists to make that visible; this was a real bug caught by
reading it.

## Related: re-running the existing micro-benchmarks under Server GC

Every GC number in every PR body in this release was measured under **Workstation** GC, because that is
BenchmarkDotNet's default and `ComparisonConfig` does not override it. Before quoting any of them beside
a number from this project, re-run them.

`ComparisonConfig` is deliberately not modified — `/benchmark-compare` depends on it. BenchmarkDotNet's
CLI offers `--noForcedGCs` (which covers the `GcForce` half) but **no** switch for Server GC, so the
existing project cannot be flipped to Server GC from the command line. Options, in order of preference:

1. Re-measure the same claim with the equivalent harness in **this** project, which runs both GC modes
   as paired arms by construction.
2. Add a second config class to `ClickHouse.Driver.Benchmark` alongside `ComparisonConfig` and select it
   per-run, leaving `ComparisonConfig` untouched.

Do not set `DOTNET_gcServer=1` and assume it took effect: BenchmarkDotNet writes the job's resolved GC
settings into the generated child's `runtimeconfig.json`, which wins over the environment variable. If
you go that route, verify it — this project's `env` verb prints the GC mode as the process actually
resolved it.
