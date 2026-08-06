* zstd responses are now decoded transparently, so `AcceptEncoding = "zstd"` works with every read API instead of failing as an unsupported codec.
