# ClickHouse.Driver.Dapper

Dapper and Dapper.Contrib support for the official ClickHouse .NET driver.

## What this adds

Calling `ClickHouseDapper.Register()` once at startup wires up:

- Type handlers so `ClickHouseDecimal`, `DateTimeOffset`, `IPAddress`, `ITuple`, and `BigInteger` round-trip correctly through Dapper without you registering anything manually.
- `DbType.DateTime2` mapping for `DateTime`, so Dapper-driven inserts target ClickHouse `DateTime`/`DateTime64` correctly.
- A `ClickHouseContribSqlAdapter` registered against `Dapper.Contrib`'s `AdapterDictionary`, so `Insert<T>` emits ClickHouse SQL instead of the default SQL Server dialect.

`WHERE id IN @ids` with an array value already works out of the box without this package — Dapper 2.x ships explicit support for `ClickHouseConnection` in its `FeatureSupport`, so the array is sent as one native `Array(T)` parameter rather than being expanded client-side into N parameters.

## Usage

```csharp
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Dapper;
using Dapper;

ClickHouseDapper.Register(); // idempotent; do once at startup

using var connection = new ClickHouseConnection("Host=localhost");
var rows = await connection.QueryAsync<MyRow>(
    "SELECT id, name FROM things WHERE id = @id",
    new { id = 42 });
```

### Dapper.Contrib

```csharp
[Table("things")]
public record Thing(int Id, string Name);

await connection.InsertAsync(new Thing(1, "alice"));
await connection.UpdateAsync(new Thing(1, "alice-renamed"));
await connection.DeleteAsync(new Thing(1, ""));
```

`Insert<T>` always returns `0` — ClickHouse has no auto-increment, so the caller is responsible for the key.

### Native `Array(T)` for `WHERE … IN @ids`

```csharp
var rows = await connection.QueryAsync<Thing>(
    "SELECT id, name FROM things WHERE id IN @ids",
    new { ids = new[] { 1, 2, 3 } });
```

This sends a single `Array(Int32)` parameter to the server, not three individual ones. Works without any explicit setup — Dapper recognises `ClickHouseConnection` and skips its default client-side IN-expansion.

## What's not supported

- `QueryMultiple` multi-result-sets — ClickHouse returns a single result set per query.
- `CommandType.StoredProcedure` — ClickHouse has no stored procedures.
- Output parameters / return values — ClickHouse has no concept of them.
- `Insert<T>` identity retrieval — see above; returns `0`.
- `Update<T>` / `Delete<T>` via Dapper.Contrib — these emit standard `UPDATE … SET …` / `DELETE FROM …` which ClickHouse does not stably support. Use `ALTER TABLE … UPDATE/DELETE` directly via `connection.ExecuteAsync(...)`.
