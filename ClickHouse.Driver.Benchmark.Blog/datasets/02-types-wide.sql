-- bench.types_wide — the composite/long-tail type dataset.
--
-- `hits` is 105 columns of Int16/Int32/Int64/String/DateTime/Date and nothing else. It therefore
-- exercises none of the per-value decode and encode work in this release: Nullable, Decimal, Array,
-- Map, Tuple, UUID, LowCardinality, Enum, Dynamic, Variant, JSON, Int128, IPv6, FixedString.
--
-- One column per optimisation, so the normalized strip chart (chart 8 in the plan) comes out of a
-- single scan rather than eleven separate runs. Deliberately narrow-ish and short (~200k rows): the
-- point is per-value decode cost, and a wide row at 1M rows would spend all its time on the wire.
--
-- Types verified against ClickHouse 25.10.7.6: Dynamic, Variant and JSON all work with no
-- experimental setting on this version. Do not add allow_experimental_* here — a setting that is a
-- no-op today becomes a lie in the results header tomorrow.

CREATE TABLE IF NOT EXISTS types_wide
(
    -- Key, and the value every generated column is derived from.
    `id`            UInt64,

    -- Nullable, with a real null every 7th row so the null branch is actually taken.
    `n_int32`       Nullable(Int32),
    `n_string`      Nullable(String),

    -- Arrays: fixed-width element, nullable element, and string element.
    `arr_int32`     Array(Int32),
    `arr_nullable`  Array(Nullable(Int32)),
    `arr_string`    Array(String),

    `map_str_int`   Map(String, Int32),
    `tup`           Tuple(Int64, String, Float64),

    -- Decimal write/read allocations.
    `dec128`        Decimal128(10),

    -- Per-value type dispatch: Dynamic resolves its type per value, Variant per discriminator.
    `dyn`           Dynamic,
    `var`           Variant(Int64, String, Float64),

    -- Fixed-size reads that used to allocate a scratch buffer per value.
    `i128`          Int128,
    `uuid`          UUID,
    `ipv6`          IPv6,
    `fs16`          FixedString(16),

    -- Transparent wrappers on the RowBinary wire, and the Enum read path.
    `lc_str`        LowCardinality(String),
    `enum8`         Enum8('alpha' = 1, 'beta' = 2, 'gamma' = 3),

    `json`          JSON,
    `dt64`          DateTime64(3)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;
