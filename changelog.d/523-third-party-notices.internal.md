* The NuGet package now ships `LICENSE` and a new `THIRD-PARTY-NOTICES.txt` at its root. The
  notices file reproduces the license and copyright of the third-party code vendored into
  `ClickHouse.Driver.Common.dll` — K4os (LZ4/xxHash) and ZstdSharp, both MIT — along with the
  BSD-3-Clause notice of Meta's zstd, which the ZstdSharp port derives from. Previously the package
  carried no license file at all, only the `MIT` SPDX expression, which names no third party.
