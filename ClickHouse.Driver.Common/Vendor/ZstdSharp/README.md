# Vendored ZstdSharp

This directory contains a **vendored copy** of Oleg Stepanischev's MIT-licensed `ZstdSharp` — a
pure-managed C# port of Meta's [zstd](https://github.com/facebook/zstd) — bundled directly into
`ClickHouse.Driver` so the driver can offer ZSTD compression (HTTP `Content-Encoding: zstd` and the
native-protocol block path) **without taking a third-party runtime dependency**.

The port is fully managed: it contains no P/Invoke and ships no native binary, so vendoring it does
not reintroduce the native dependency an `libzstd` binding would.

## Sources

| Sub-folder            | Upstream project              | Version | Repository |
|-----------------------|-------------------------------|---------|------------|
| `.` (top-level `*.cs`)| `ZstdSharp` (`src/ZstdSharp`) | 0.8.8   | https://github.com/oleg-st/ZstdSharp |
| `Unsafe/`             | `ZstdSharp` (`src/ZstdSharp/Unsafe`) | 0.8.8 | https://github.com/oleg-st/ZstdSharp |

NuGet publishes this project as **`ZstdSharp.Port`** (not `ZstdSharp`) — the id used by the
test-only differential oracle described below.

## License

MIT — see [`LICENSE`](LICENSE), copied verbatim. Copyright (c) 2021 Oleg Stepanischev. Upstream
carries no per-file copyright headers in this tree, so there are none to preserve; the vendoring
adds none either.

This directory contains only Oleg Stepanischev's MIT-licensed C# port, no zstd source. The port
derives from Meta's zstd v1.5.7 (dual BSD-3-Clause / GPL-2.0), including the copy of xxHash that
library bundles. Upstream's transpilation carries the zstd code comments across but not its
copyright headers, and upstream records no attribution to Meta, so the driver reproduces zstd's
BSD-3-Clause notice itself, in the root `THIRD-PARTY-NOTICES.txt` that ships in the NuGet package.

## Modifications from upstream

The source is copied verbatim except for the following mechanical changes, applied so the code
compiles cleanly as an internal component of `ClickHouse.Driver.Common`:

1. **Namespace prefix** — every `ZstdSharp.*` namespace is rewritten to
   `ClickHouse.Driver.Vendor.ZstdSharp.*` to avoid any collision with the real `ZstdSharp.Port`
   package a consumer might also reference.
2. **Internalized types** — all top-level `public` type declarations are changed to `internal`
   so the vendored codec is not part of the driver's public API surface. Member visibility is
   left untouched (to preserve implicit interface implementations).
3. **Only `src/ZstdSharp` is vendored** — the upstream `Sandbox/`, `Zstd.Extern/`,
   `ZstdSharp.Benchmark/` and `ZstdSharp.Test/` projects and the solution file are build tooling
   and tests, and are not shipped.
4. **Pruned `Polyfills/`** — both files (`BitOperations`, `SkipLocalsInitAttribute`) are entirely
   inside `#if !NETCOREAPP3_0_OR_GREATER` / `#if !NET5_0_OR_GREATER`, so they are dead code for a
   net6.0+ build.
5. **Dropped `key.snk`** — the upstream strong-naming key, which only applied to upstream's own
   assembly (`ClickHouse.Driver.Common` does not set `SignAssembly`).
6. Targeting net6.0+ only, so the upstream old-TFM runtime package references
   (`System.Memory`, `System.Threading.Tasks.Extensions`,
   `System.Runtime.CompilerServices.Unsafe`) are unnecessary and omitted, as is
   `Microsoft.SourceLink.GitHub`. The `#if` blocks guarding pre-net6 code paths are left in place
   rather than hand-pruned, to keep the copy re-derivable.
7. **`FodyWeavers.xml` / `FodyWeavers.xsd` moved** to the `ClickHouse.Driver.Common` project
   directory, where Fody looks for them. `Fody` and `InlineMethod.Fody` are referenced with
   `PrivateAssets="all"` (build-time IL weaving only, no runtime dependency), so the
   `[InlineMethod.Inline]` attributes the port relies on for its hot paths still take effect.
8. Analyzer, code-style and the port's own compiler warnings are silenced for this tree via
   `Vendor/.editorconfig` (`generated_code = true`), and `AllowUnsafeBlocks` is enabled on the
   project — no source change.

Nothing else is touched: **no codec logic is hand-edited**.

The correctness of this copy is guarded by `ZstdCompressorTests`, which uses the **real
`ZstdSharp.Port` 0.8.8** package (a test-only dependency) as a differential oracle: what the
vendored codec encodes must decode with upstream ZstdSharp and vice versa, on both the HTTP frame
path and the native block path.

## Updating

Modifications 1–7 above are applied by [`vendor.sh`](vendor.sh), which is the definition of this
copy:

```bash
./ClickHouse.Driver.Common/Vendor/ZstdSharp/vendor.sh 0.8.8   # re-derives this directory
git diff --stat                                               # must be empty
```

To upgrade, run it with the new tag, bump the version in this README and the
`ZstdSharp.Port` `PackageVersion` in `Directory.Packages.props` (the test oracle must match the
vendored version), and re-run `ZstdCompressorTests`. Do not hand-edit the codec logic — change
`vendor.sh` instead, so the copy stays clean and re-derivable.
