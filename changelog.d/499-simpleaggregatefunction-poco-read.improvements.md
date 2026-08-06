* `SimpleAggregateFunction(f, T)` columns now take the box-free fast path in `QueryAsync<T>`, as `LowCardinality(T)` already did. Previously they fell back to the slower boxed read.
