#!/usr/bin/env bash
#
# Runs a labelled, reproducible pass of the macro benchmark suite into a timestamped results
# directory, with the confounder set stamped into it before anything is measured.
#
#   ./run-suite.sh --smoke                        # ~minutes: proves the wiring, numbers unpublishable
#   ./run-suite.sh --filter '*Overhead*'          # one family, full size
#   ./run-suite.sh --gc-trace --filter '*Hits*'   # + .nettrace capture and gc-report
#   ./run-suite.sh --versions 1.0.0,1.2.0,1.3.0,1.4.0-local --nuget-source "$PWD/artifacts/nupkg"
#
# Everything lands in results/<timestamp>-<label>/:
#   environment.txt    the confounder set (§8), captured before the run
#   run.log            full BenchmarkDotNet output
#   side-metrics.csv   server CPU, client CPU, wire bytes, latency percentiles, pool counters
#   artifacts/         BenchmarkDotNet's own reports, per-iteration measurements, .nettrace files
#   gc/                gc-events.csv + gc-summary.csv, when --gc-trace was used
#
# The results directory is the unit of publication: it contains its own provenance, so a chart built
# from it can always be traced back to the exact configuration that produced it.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PROJECT_DIR/.." && pwd)"
PROJECT="$PROJECT_DIR/ClickHouse.Driver.Benchmark.Blog.csproj"

FILTER='*'
LABEL=""
GC_TRACE=0
DOTNET="${DOTNET:-dotnet}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --smoke) export BENCH_PROFILE=smoke; shift ;;
    --filter) FILTER="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    --gc-trace) GC_TRACE=1; export BENCH_GC_TRACE=1; shift ;;
    --versions) export BENCH_VERSIONS="$2"; shift 2 ;;
    --nuget-source) export NUGET_SOURCE="$2"; shift 2 ;;
    --gc-modes) export BENCH_GC_MODES="$2"; shift 2 ;;
    -h|--help) sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

: "${CLICKHOUSE_CONNECTION:=Host=localhost;Port=8124}"
: "${BENCH_ENV_LABEL:=unlabelled}"
export CLICKHOUSE_CONNECTION BENCH_ENV_LABEL

# One run id shared by every child process, so the side-channel CSV can be grouped by run.
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${STAMP}-${LABEL:-$BENCH_ENV_LABEL}"
OUT="$REPO_ROOT/results/$RUN_ID"
mkdir -p "$OUT/artifacts"

export BENCH_RUN_ID="$RUN_ID"
export BENCH_METRICS_CSV="$OUT/side-metrics.csv"

echo "==> Results: $OUT"

# ---------------------------------------------------------------------------------------------------
# Provenance first. Capturing the environment AFTER a run that turned out to be interesting is how
# unreproducible numbers happen.
# ---------------------------------------------------------------------------------------------------
{
  echo "# run_id=$RUN_ID"
  echo "# filter=$FILTER"
  echo "# gc_trace=$GC_TRACE"
  echo "# bench_versions=${BENCH_VERSIONS:-source}"
  echo "# bench_gc_modes=${BENCH_GC_MODES:-both}"
  echo "# git_commit=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "# git_dirty=$(test -n "$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null)" && echo yes || echo no)"
  echo "# host_kernel=$(uname -r)"
  echo "# host_cpu=$(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | xargs || echo unknown)"
  echo "# host_mem_kb=$(awk '/MemTotal/{print $2}' /proc/meminfo 2>/dev/null || echo unknown)"
  # kernel.perf_event_paranoid gates perf counters; recorded because its value explains why some
  # profiling options silently produce nothing on this box.
  echo "# perf_event_paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo unknown)"
} > "$OUT/environment.txt"

# The `env` verb resolves the rest from inside a real client process: server version, GC mode,
# dataset row counts, the compression level the server will actually use.
"$DOTNET" run -c Release --project "$PROJECT" -- env >> "$OUT/environment.txt" 2>&1 || {
  echo "!! Environment probe failed. Is the server up and are the datasets staged?" >&2
  cat "$OUT/environment.txt" >&2
  exit 1
}

if grep -q 'server_probe_error' "$OUT/environment.txt"; then
  echo "!! Server unreachable — see $OUT/environment.txt" >&2
  grep 'server_probe_error' "$OUT/environment.txt" >&2
  exit 1
fi

if grep -qE '^bench\..*\.rows=absent' "$OUT/environment.txt"; then
  echo "!! A dataset is missing. Run scripts/stage-datasets.sh first." >&2
  grep -E '^bench\..*\.rows=' "$OUT/environment.txt" >&2
  exit 1
fi

if [[ "${BENCH_PROFILE:-}" == "smoke" ]]; then
  echo "==> BENCH_PROFILE=smoke: this pass proves the wiring. Its numbers are NOT publishable."
fi

grep -E '^(env_label|server_version|logical_cores|gc_mode|http_zlib|bench_profile|driver_version)' \
  "$OUT/environment.txt" | sed 's/^/    /'

# ---------------------------------------------------------------------------------------------------
# The run.
# ---------------------------------------------------------------------------------------------------
echo "==> Running: --filter '$FILTER'"
set +e
"$DOTNET" run -c Release --project "$PROJECT" -- \
  --filter "$FILTER" --artifacts "$OUT/artifacts" 2>&1 | tee "$OUT/run.log"
STATUS=${PIPESTATUS[0]}
set -e

# ---------------------------------------------------------------------------------------------------
# Post-processing.
# ---------------------------------------------------------------------------------------------------
if [[ "$GC_TRACE" == "1" ]]; then
  echo "==> Converting .nettrace files"
  mkdir -p "$OUT/gc"
  "$DOTNET" run -c Release --project "$PROJECT" -- gc-report "$OUT/artifacts" --out "$OUT/gc" \
    | tee -a "$OUT/run.log" || echo "!! gc-report failed; traces are still in $OUT/artifacts" >&2

  # has_detailed_gc_info=False means every fragmentation column in the CSV is empty. Saying so here
  # is cheaper than discovering it while building the LOH chart.
  if [[ -f "$OUT/gc/gc-summary.csv" ]] && grep -q ',False$' "$OUT/gc/gc-summary.csv"; then
    echo "!! Some traces lack detailed GC info: per-generation size and fragmentation are unavailable" >&2
    echo "   for those. Check the has_detailed_gc_info column before plotting anything LOH-shaped." >&2
  fi
fi

# A benchmark that reported no server rows produced no server-CPU number; flag it rather than let a
# chart quietly average over the gap.
if [[ -f "$OUT/side-metrics.csv" ]] && grep -q 'server_lookup_failed' "$OUT/side-metrics.csv"; then
  echo "!! Some server-cost lookups failed. Affected arms:" >&2
  awk -F, '/server_lookup_failed/{print "   " $7 " " $8}' "$OUT/side-metrics.csv" | sort -u >&2
fi

# Server CPU wait is the contention tell: a high ratio means something else was using the box and the
# wall-clock numbers from this pass describe a busy machine, not the driver.
if [[ -f "$OUT/side-metrics.csv" ]]; then
  awk -F, '$10 == "server_cpu_wait_ratio" && $11 > 0.25 { n++ } END {
    if (n > 0) print "!! " n " sample(s) had server CPU wait > 25% of CPU time: the server was contended."
  }' "$OUT/side-metrics.csv" >&2
fi

echo
echo "==> Done (benchmark exit $STATUS)"
echo "    $OUT"
[[ -f "$OUT/side-metrics.csv" ]] && echo "    side-metrics rows: $(( $(wc -l < "$OUT/side-metrics.csv") - 1 ))"
exit "$STATUS"
