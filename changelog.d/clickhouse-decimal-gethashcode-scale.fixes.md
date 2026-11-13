* Fixed `ClickHouseDecimal` hash codes so numerically equal values with different scales can be used consistently in `HashSet` and `Dictionary` lookups. (#639)
