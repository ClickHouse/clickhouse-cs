#!/usr/bin/env bash
#
# Packs the working tree as ClickHouse.Driver 1.4.0-local into artifacts/nupkg, then verifies that the
# published baseline versions the four-bar chart needs can actually be restored.
#
#   ./pack-local.sh
#   ./run-suite.sh --versions 1.0.0,1.2.0,1.3.0,1.4.0-local --nuget-source "$PWD/artifacts/nupkg"
#
# Why pack at all, when source mode already measures the working tree: comparison mode compiles ONE
# source tree against several packages, so the "new" arm has to be a package too. Otherwise the new arm
# would be built through a ProjectReference while the baselines come from NuGet, and the two would
# differ in more than the driver version.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT="$REPO_ROOT/artifacts/nupkg"
VERSION="${BENCH_LOCAL_VERSION:-1.4.0-local}"
DOTNET="${DOTNET:-dotnet}"

# Baselines from §7 of the plan: the four bars map onto the three phases of the year.
BASELINES=("${@:-1.0.0 1.2.0 1.3.0}")
read -r -a BASELINES <<< "${BASELINES[*]}"

mkdir -p "$OUT"

# Build every target framework first. Packing straight after a build of this benchmark project fails
# with NU5026 ("the file ... net6.0/ClickHouse.Driver.dll to be packed was not found"): building a
# net10.0 consumer builds only the driver's matching net10.0 output, and the multi-target pack then has
# nothing to pack for net6.0/net8.0/net9.0.
#
# Not --no-build on the pack itself: the bundled ClickHouse.Driver.Common project is built as part of
# packing, and NoBuild then fails with NETSDK1085. The explicit build above is what fixes NU5026; the
# pack's own build pass is then incremental and nearly free.
echo "==> Building ClickHouse.Driver for all target frameworks"
"$DOTNET" build "$REPO_ROOT/ClickHouse.Driver/ClickHouse.Driver.csproj" \
  -c Release "/p:Version=$VERSION" | tail -2

echo "==> Packing $VERSION into $OUT"
"$DOTNET" pack "$REPO_ROOT/ClickHouse.Driver/ClickHouse.Driver.csproj" \
  -c Release -o "$OUT" "/p:Version=$VERSION" "/p:ContinuousIntegrationBuild=true" | tail -3

ls -1 "$OUT"/*.nupkg | sed 's/^/    /'

# ---------------------------------------------------------------------------------------------------
# Verify every arm restores AND compiles. A baseline that restores but fails to compile the harness is
# the failure mode the API corridor exists to prevent, and it is much cheaper to find here than at
# hour three of an overnight run.
# ---------------------------------------------------------------------------------------------------
echo
echo "==> Verifying each arm restores and compiles"
FAILED=()
for version in "$VERSION" "${BASELINES[@]}"; do
  printf '    %-14s ' "$version"
  if log=$("$DOTNET" build "$SCRIPT_DIR/../ClickHouse.Driver.Benchmark.Blog.csproj" \
        -c Release \
        /p:BenchmarkComparisonMode=true \
        "/p:ClickHouseDriverVersion=$version" \
        "/p:RestoreAdditionalProjectSources=$OUT" 2>&1); then
    corridor=$(grep -o '\[blog-bench\][^]]*' <<< "$log" | tail -1)
    echo "ok    ${corridor#\[blog-bench\] }"
  else
    echo "FAILED"
    grep -E 'error' <<< "$log" | head -4 | sed 's/^/        /'
    FAILED+=("$version")
  fi
done

# Restore the working tree's own build outputs: the last loop left obj/ configured for whichever
# version ran last, and a following source-mode run would otherwise restore again anyway.
"$DOTNET" build "$SCRIPT_DIR/../ClickHouse.Driver.Benchmark.Blog.csproj" -c Release >/dev/null

echo
if [[ ${#FAILED[@]} -gt 0 ]]; then
  echo "!! These arms do not build: ${FAILED[*]}" >&2
  echo "   Either the harness touches API they lack — add a corridor gate in the csproj — or the" >&2
  echo "   package genuinely is not available. See docs/API-CORRIDOR.md." >&2
  exit 1
fi

echo "==> All arms build. Run the four-bar comparison with:"
echo "    ./run-suite.sh --versions $(IFS=,; echo "${BASELINES[*]}"),$VERSION --nuget-source $OUT"
