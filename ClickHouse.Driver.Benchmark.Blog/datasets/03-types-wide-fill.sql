-- Fills bench.types_wide deterministically from system.numbers.
--
-- Deterministic on purpose: no rand(), so two stagings produce byte-identical data and a decode
-- number is comparable across machines and across time. Every column varies with `number` so no
-- column is a constant the server could optimise into nothing.
--
-- Parameters: {rows:UInt64}; the target database comes from the HTTP `database=` parameter.

INSERT INTO types_wide
SELECT
    number                                                       AS id,

    -- Null every 7th row: often enough that the null branch is measured, rare enough that the
    -- value branch still dominates.
    if(number % 7 = 0, NULL, toInt32(number % 100000))           AS n_int32,
    if(number % 7 = 3, NULL, concat('s', toString(number % 997))) AS n_string,

    -- Length varies 0..4 so the length prefix is exercised, not just the elements.
    range(toUInt32(number % 5))                                  AS arr_int32,
    arrayMap(x -> if(x % 3 = 0, NULL, toInt32(x)), range(toUInt32(number % 5))) AS arr_nullable,
    arrayMap(x -> concat('e', toString(x)), range(toUInt32(number % 4)))        AS arr_string,

    map('k' || toString(number % 13), toInt32(number % 251),
        'j' || toString(number % 17), toInt32(number % 331))      AS map_str_int,

    tuple(toInt64(number), concat('t', toString(number % 89)), toFloat64(number) * 0.25) AS tup,

    toDecimal128(number % 1000000, 10) / 7                       AS dec128,

    -- Dynamic cycles through three CLR-distinct shapes, which is what makes its per-value dispatch
    -- visible; a single-type Dynamic column would measure the fast path only.
    multiIf(number % 3 = 0, toInt64(number)::Dynamic,
            number % 3 = 1, concat('d', toString(number % 71))::Dynamic,
            (toFloat64(number) * 1.5)::Dynamic)                  AS dyn,

    multiIf(number % 3 = 0, toInt64(number)::Variant(Int64, String, Float64),
            number % 3 = 1, concat('v', toString(number % 61))::Variant(Int64, String, Float64),
            (toFloat64(number) * 0.5)::Variant(Int64, String, Float64)) AS var,

    toInt128(number) * 1000000007                                AS i128,
    -- Deterministic UUID: generateUUIDv4() would make the data unreproducible.
    toUUID(concat('00000000-0000-4000-8000-', leftPad(toString(number % 1000000), 12, '0'))) AS uuid,
    toIPv6(concat('2001:db8::', hex(number % 65536)))            AS ipv6,
    toFixedString(leftPad(toString(number % 100000000), 16, '0'), 16) AS fs16,

    concat('lc-', toString(number % 25))                         AS lc_str,
    CAST((number % 3) + 1 AS Enum8('alpha' = 1, 'beta' = 2, 'gamma' = 3)) AS enum8,

    -- A numeric typed path, a string path, a genuinely nested object, a bool, and a null leaf every
    -- 5th row — the last one being the shape #529 was about (a typed path whose value is NULL).
    -- Built with concat rather than toJSONString(map(...)): a map's values are all strings, so
    -- `nested` would come back as a string *containing* JSON and the nested-object decode path
    -- would never be exercised.
    CAST(concat(
        '{"n":', toString(number % 1000),
        ',"label":"j', toString(number % 41), '"',
        ',"nested":{"deep":', toString(number % 7), ',"flag":', if(number % 2 = 0, 'true', 'false'), '}',
        ',"maybe":', if(number % 5 = 0, 'null', toString(number % 9)),
        '}') AS JSON)                                            AS json,

    toDateTime64(1700000000 + (number % 2592000) + (number % 1000) / 1000, 3, 'UTC') AS dt64
FROM numbers({rows:UInt64});
