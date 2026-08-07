// Namespaces every harness needs. Kept global so the per-file using blocks stay about what a given
// harness actually does.
//
// Note that ClickHouseClient itself needs no using: it lives in the root ClickHouse.Driver namespace,
// which encloses this project's ClickHouse.Driver.Benchmark.Blog namespace.
global using ClickHouse.Driver.ADO;
global using ClickHouse.Driver.Benchmark.Blog.Infrastructure;
