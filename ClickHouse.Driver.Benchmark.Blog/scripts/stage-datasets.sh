#!/usr/bin/env bash
#
# Stages bench.hits and bench.types_wide on the benchmark server, then prints what it built.
#
#   ./stage-datasets.sh                      # 1M-row hits + 200k-row types_wide + sinks
#   ./stage-datasets.sh --source clickbench  # force the canonical download
#   ./stage-datasets.sh --rows 50000         # small, for a wiring check
#   ./stage-datasets.sh --force              # re-load even if the tables already look right
#
# Environment:
#   BENCH_HTTP_URL          Target server. Default http://localhost:8124 (the dedicated container).
#   BENCH_DATASET_DB        Database. Default bench.
#   BENCH_HITS_ROWS         Rows in hits. Default 1000000 (one canonical ClickBench partition).
#   BENCH_TYPES_WIDE_ROWS   Rows in types_wide. Default 200000.
#   BENCH_HITS_SOURCE_URL   For --source local: server holding a hits copy. Default http://localhost:8123.
#   BENCH_HITS_SOURCE_TABLE For --source local: qualified table name. Default datasets.hits.
#
# Sources for hits, both producing the SAME schema (verified column-for-column):
#   clickbench  Streams hits_0.parquet from datasets.clickhouse.com — exactly 1,000,000 rows, the
#               canonical first partition. ~122 MB of download.
#   local       Copies from an existing hits table on another endpoint over Native format. Fast and
#               offline, but only as trustworthy as that table.
#   auto        local if its source table exists and has enough rows, else clickbench. Default.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DDL_DIR="$SCRIPT_DIR/../datasets"

URL="${BENCH_HTTP_URL:-http://localhost:8124}"
DB="${BENCH_DATASET_DB:-bench}"
HITS_ROWS="${BENCH_HITS_ROWS:-1000000}"
TYPES_ROWS="${BENCH_TYPES_WIDE_ROWS:-200000}"
SRC_URL="${BENCH_HITS_SOURCE_URL:-http://localhost:8123}"
SRC_TABLE="${BENCH_HITS_SOURCE_TABLE:-datasets.hits}"
CLICKBENCH_PARQUET="https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet"

SOURCE="auto"
FORCE=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --source) SOURCE="$2"; shift 2 ;;
    --rows) HITS_ROWS="$2"; TYPES_ROWS="$2"; shift 2 ;;
    --hits-rows) HITS_ROWS="$2"; shift 2 ;;
    --types-rows) TYPES_ROWS="$2"; shift 2 ;;
    --force) FORCE=1; shift ;;
    -h|--help) sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

# --- helpers ---------------------------------------------------------------------------------------

# POST a statement. Every request is POST: GET implies readonly, so DDL fails with a message that
# says nothing about the actual problem.
ch() {
  local target="$1"; shift
  curl -sS --fail-with-body --max-time 3600 -X POST --data-binary @- "$target" "$@"
}

# Runs one statement against the benchmark DB. Extra query-string params may follow.
q() {
  local sql="$1"; shift
  local qs="database=$DB"
  for kv in "$@"; do qs="$qs&$kv"; done
  printf '%s' "$sql" | ch "$URL/?$qs"
}

# Runs a .sql file. One statement per file, by construction (see datasets/00-database.sql).
qfile() {
  local file="$1"; shift
  local qs="database=$DB"
  for kv in "$@"; do qs="$qs&$kv"; done
  ch "$URL/?$qs" < "$DDL_DIR/$file"
}

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

# --- preflight -------------------------------------------------------------------------------------

say "Target"
SERVER_VERSION="$(printf 'SELECT version()' | ch "$URL/" || true)"
if [[ -z "$SERVER_VERSION" ]]; then
  echo "Cannot reach a ClickHouse server at $URL." >&2
  echo "Start the dedicated one with:" >&2
  echo "  docker compose -f $SCRIPT_DIR/../env/docker-compose.yml up -d" >&2
  exit 1
fi
echo "  $URL  ClickHouse $SERVER_VERSION  database=$DB"

# query_log is not optional: the codec matrix reads server CPU out of it, and ServerMetrics throws
# rather than under-report when the rows are missing. Better to fail here than 40 minutes in.
LOG_QUERIES="$(printf "SELECT value FROM system.settings WHERE name = 'log_queries'" | ch "$URL/")"
if [[ "$LOG_QUERIES" != "1" ]]; then
  echo "  WARNING: log_queries=$LOG_QUERIES on this server. Server-CPU arms will fail." >&2
fi

# --- schema ----------------------------------------------------------------------------------------

say "Schema"
# 00 creates the database, so it is the one file that cannot use database=<db> itself.
ch "$URL/?param_db=$DB" < "$DDL_DIR/00-database.sql"
for f in 01-hits.sql 02-types-wide.sql; do qfile "$f"; echo "  $f"; done
for f in 04-sink-null.sql 05-sink-mergetree.sql; do qfile "$f"; echo "  $f"; done

# --- hits ------------------------------------------------------------------------------------------

existing_rows() {
  q "SELECT count() FROM $1" 2>/dev/null || echo 0
}

HITS_HAVE="$(existing_rows hits)"
if [[ "$FORCE" == "0" && "$HITS_HAVE" -ge "$HITS_ROWS" ]]; then
  say "hits: already $HITS_HAVE rows (>= $HITS_ROWS), skipping. Use --force to reload."
else
  # Decide the source.
  if [[ "$SOURCE" == "auto" ]]; then
    SRC_COUNT="$(printf 'SELECT count() FROM %s' "$SRC_TABLE" | ch "$SRC_URL/" 2>/dev/null || echo 0)"
    if [[ "${SRC_COUNT:-0}" -ge "$HITS_ROWS" ]]; then SOURCE="local"; else SOURCE="clickbench"; fi
    echo "  auto-selected source: $SOURCE (local candidate $SRC_TABLE has ${SRC_COUNT:-0} rows)"
  fi

  say "hits: loading $HITS_ROWS rows from $SOURCE"
  q "TRUNCATE TABLE hits"

  case "$SOURCE" in
    clickbench)
      # schema_inference_make_columns_nullable=0 keeps the parquet columns non-Nullable so they cast
      # positionally into the DateTime/Date columns; input_format_null_as_default handles the rest.
      q "INSERT INTO hits SELECT * FROM url('$CLICKBENCH_PARQUET', Parquet) LIMIT $HITS_ROWS" \
        "input_format_null_as_default=1" "schema_inference_make_columns_nullable=0" "max_execution_time=3600"
      ;;
    local)
      # Native format: no text parsing, no type inference, no loss. Both endpoints must run the same
      # major version, which is true when the source is another container from this repo.
      printf 'SELECT * FROM %s LIMIT %s FORMAT Native' "$SRC_TABLE" "$HITS_ROWS" \
        | ch "$SRC_URL/" \
        | curl -sS --fail-with-body --max-time 3600 -X POST --data-binary @- \
            "$URL/?database=$DB&query=INSERT%20INTO%20hits%20FORMAT%20Native"
      ;;
    *) echo "unknown --source: $SOURCE (expected auto, clickbench or local)" >&2; exit 2 ;;
  esac
  q "OPTIMIZE TABLE hits FINAL" "max_execution_time=3600" || true
fi

# --- types_wide ------------------------------------------------------------------------------------

TYPES_HAVE="$(existing_rows types_wide)"
if [[ "$FORCE" == "0" && "$TYPES_HAVE" -ge "$TYPES_ROWS" ]]; then
  say "types_wide: already $TYPES_HAVE rows (>= $TYPES_ROWS), skipping. Use --force to reload."
else
  say "types_wide: generating $TYPES_ROWS rows"
  q "TRUNCATE TABLE types_wide"
  qfile 03-types-wide-fill.sql "param_rows=$TYPES_ROWS"
  q "OPTIMIZE TABLE types_wide FINAL" || true
fi

# --- report ----------------------------------------------------------------------------------------

say "Staged"
q "SELECT name AS table, engine, formatReadableQuantity(total_rows) AS rows, formatReadableSize(total_bytes) AS size
   FROM system.tables WHERE database = currentDatabase() ORDER BY name FORMAT PrettyCompactMonoBlock"

# The type histogram is printed every time on purpose: it is the evidence behind captioning this
# "ClickBench hits", and it costs nothing to keep honest.
say "hits column types (the caption's evidence)"
q "SELECT type, count() AS columns FROM system.columns
   WHERE database = currentDatabase() AND table = 'hits'
   GROUP BY type ORDER BY columns DESC FORMAT PrettyCompactMonoBlock"

say "Done"
echo "  Sanity-check the harness view of this server with:"
echo "    CLICKHOUSE_CONNECTION='Host=localhost;Port=${URL##*:}' \\"
echo "      dotnet run -c Release --project ClickHouse.Driver.Benchmark.Blog -- env"
