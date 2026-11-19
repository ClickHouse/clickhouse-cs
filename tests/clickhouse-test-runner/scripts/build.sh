#!/usr/bin/env bash
# Builds the shim into ../.bin: publishes ClickHouse.Driver.TestRunner (Release, ReadyToRun) to .bin/publish,
# and writes the `clickhouse` and `clickhouse-client` commands that tests/clickhouse-test calls. Both names
# go to the same program. The program starts once per test, so ReadyToRun code makes each test faster.
#
# Usage: scripts/build.sh
#
# run-upstream-tests.sh runs this script unless SKIP_BUILD=1. The commands find the program relative to their
# own path, so a copy of .bin works in another checkout of the same commit (CI builds once and copies it).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BIN_DIR="${RUNNER_DIR}/.bin"

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) rid=linux-x64 ;;
  Linux-aarch64 | Linux-arm64) rid=linux-arm64 ;;
  Darwin-arm64) rid=osx-arm64 ;;
  Darwin-x86_64) rid=osx-x64 ;;
  *) echo "Error: unknown platform $(uname -s)-$(uname -m)." >&2; exit 1 ;;
esac

dotnet publish "${RUNNER_DIR}/ClickHouse.Driver.TestRunner/ClickHouse.Driver.TestRunner.csproj" \
  --configuration Release --runtime "${rid}" --self-contained false -p:PublishReadyToRun=true \
  --output "${BIN_DIR}/publish" --nologo --verbosity quiet >&2

cat > "${BIN_DIR}/clickhouse" <<'WRAPPER'
#!/usr/bin/env bash
exec "$(dirname "${BASH_SOURCE[0]}")/publish/clickhouse-test-runner" "$@"
WRAPPER
chmod +x "${BIN_DIR}/clickhouse"
ln -sf clickhouse "${BIN_DIR}/clickhouse-client"
echo "Built ${BIN_DIR}/clickhouse (${rid})" >&2
