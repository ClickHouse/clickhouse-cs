#!/usr/bin/env bash
#
# Re-derives the vendored ZstdSharp copy in this directory from upstream.
#
# Every transform applied to the upstream source lives here, so the vendored tree is
# reproducible: check out the tag below, run this script, and `git diff` must be empty.
# Never hand-edit the vendored codec — change this script instead.
#
# Usage:  ./vendor.sh [tag]
#
set -euo pipefail

TAG="${1:-0.8.8}"
UPSTREAM="https://github.com/oleg-st/ZstdSharp.git"

DEST="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # …/Vendor/ZstdSharp
PROJECT_DIR="$(cd "$DEST/../.." && pwd)"                  # …/ClickHouse.Driver.Common

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$TAG" "$UPSTREAM" "$tmp/upstream"
src="$tmp/upstream/src/ZstdSharp"

# (1) Copy the vendored subset: src/ZstdSharp only. The rest of the upstream repo
#     (Sandbox/, Zstd.Extern/, ZstdSharp.Benchmark/, ZstdSharp.Test/, the .sln) is build
#     tooling and tests we do not ship.
rm -rf "$DEST/Unsafe"
find "$DEST" -maxdepth 1 -name '*.cs' -delete
cp "$src"/*.cs "$DEST/"
cp -r "$src/Unsafe" "$DEST/Unsafe"
cp "$tmp/upstream/LICENSE" "$DEST/LICENSE"

# (2) Prune the pieces that are dead or unwanted at net6.0+:
#       Polyfills/            — every file is inside #if !NETCOREAPP3_0_OR_GREATER / #if !NET5_0_OR_GREATER
#       key.snk               — upstream strong-naming key (the driver signs with its own)
#       ZstdSharp.csproj      — replaced by ClickHouse.Driver.Common.csproj
rm -f "$DEST/Polyfills"/*.cs
rmdir "$DEST/Polyfills" 2>/dev/null || true

# (3) Fody weaver configuration belongs next to the project file that consumes it.
cp "$src/FodyWeavers.xml" "$PROJECT_DIR/FodyWeavers.xml"
cp "$src/FodyWeavers.xsd" "$PROJECT_DIR/FodyWeavers.xsd"

# (4) Namespace rewrite: ZstdSharp.* -> ClickHouse.Driver.Vendor.ZstdSharp.*
#     (5) Internalization: every top-level type declaration -> internal. Upstream uses
#     block-scoped namespaces, so a type declaration is exactly a `public` at four spaces
#     of indentation; members sit deeper and keep their upstream visibility (changing it
#     would break implicit interface implementations).
find "$DEST" -name '*.cs' -print0 | xargs -0 sed -i \
  -e 's|ZstdSharp\.|ClickHouse.Driver.Vendor.ZstdSharp.|g' \
  -e 's|namespace ZstdSharp$|namespace ClickHouse.Driver.Vendor.ZstdSharp|' \
  -e 's|^    public |    internal |'

echo "Vendored ZstdSharp $TAG into $DEST"
echo "  $(find "$DEST" -name '*.cs' | wc -l) .cs files"
