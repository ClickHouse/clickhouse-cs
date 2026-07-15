# Vendored K4os LZ4 / xxHash

This directory contains a **vendored copy** of Milosz Krajewski's MIT-licensed LZ4 and xxHash
libraries, bundled directly into `ClickHouse.Driver` so the driver can offer LZ4 compression
(HTTP `Content-Encoding: lz4` and the native-protocol block path) **without taking a third-party
runtime dependency**.

## Sources

| Sub-folder    | Upstream project                | Version | Repository |
|---------------|---------------------------------|---------|------------|
| `LZ4/`        | `K4os.Compression.LZ4`          | 1.3.8   | https://github.com/MiloszKrajewski/K4os.Compression.LZ4 |
| `LZ4.Streams/`| `K4os.Compression.LZ4.Streams`  | 1.3.8   | https://github.com/MiloszKrajewski/K4os.Compression.LZ4 |
| `xxHash/`     | `K4os.Hash.xxHash`              | 1.0.8   | https://github.com/MiloszKrajewski/K4os.Hash.xxHash |

## License

MIT — see [`LICENSE`](LICENSE). Copyright (c) 2017 Milosz Krajewski. All copyright headers are
preserved. Both libraries ship under the same MIT license text.

## Modifications from upstream

The source is copied verbatim except for the following mechanical changes, applied so the code
compiles cleanly as an internal component of `ClickHouse.Driver.Common`:

1. **Namespace prefix** — every `K4os.*` namespace is rewritten to
   `ClickHouse.Driver.Vendor.K4os.*` to avoid any collision with the real K4os packages a
   consumer might also reference.
2. **Internalized types** — all top-level `public` type declarations are changed to `internal`
   so the vendored codec is not part of the driver's public API surface. Member visibility is
   left untouched (to preserve implicit interface implementations).
3. **Pruned `System.IO.Pipelines` support** — the `PipeReader`/`PipeWriter` adapters and the
   `Pipe`-based `LZ4Frame`/`LZ4FrameReader`/`LZ4FrameWriter` overloads are removed. The driver
   only uses the `Stream`-based and raw block APIs, and dropping the Pipe path keeps the vendored
   code dependency-free (no `System.IO.Pipelines` package).
4. Only the three projects above are vendored (not the `Legacy`/`vPrev` variants), targeting
   net6.0+ only, so the upstream old-TFM polyfill packages (System.Memory,
   System.Runtime.CompilerServices.Unsafe, PolySharp) are unnecessary and omitted.

The correctness of this copy is guarded by `Lz4CompressorTests`, which uses the **real** K4os
package (a test-only dependency) as a differential oracle: what the vendored codec encodes must
decode with upstream K4os and vice versa.

## Updating

To upgrade, re-run the same mechanical transforms against a fresh checkout of the upstream tags
above. Do not hand-edit the codec logic — keep this a clean, re-derivable vendoring.
