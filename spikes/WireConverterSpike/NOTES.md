# Spike: wire layer plus converter combinators (ClickHouse/integrations#801)

A quick, partial prototype. It is not production code and it is not for merge.

## What the spike implements

- **Leaf converters.** `DateTime` to `DateTimeOffset` and `uint`; `String` to `string` and `byte[]`.
  Writes: `DateTime` from `DateTimeOffset`; `String` and `FixedString(N)` from `string` and `byte[]`.
  Each leaf conversion is one entry in a table (`ReadDerivation.Leaves`, `WriteDerivation.Leaves`).
- **Combinators.** Read: `LiftRef`, `LiftValue` (Nullable), `Each` (Array), `DictionaryReader`
  (LowCardinality, including `LowCardinality(Nullable(X))` for reference types). Write: `LiftValueWriter`,
  `EachWriter`, `DictionaryWriter`.
- **One derivation** (`ReadDerivation.Derive(type, context, T)`), cached by type string, timezone, and `T`.
  The columnar tier and a small POCO tier (`ConverterPocoPlan`) both use it.
- **No new wire layer.** The current decoders already produce the dense layout: `DateTimeColumn` holds raw
  `uint` seconds, `StringColumn` holds a blob and offsets, LowCardinality holds a dictionary and keys,
  Nullable holds a null map and an inner column, Array holds offsets and a flat child. The spike reads those
  through the existing surfaces (`IStringColumn`, `INullableColumn`, `IArrayColumn`, `ILowCardinalityColumn`).

The only change outside `spikes/` is one `InternalsVisibleTo` line in `ClickHouse.Driver.Tcp/AssemblyInfo.cs`.

## Correctness

`dotnet run -c Release -- verify` compares every result with the current client, on 10,000 rows:

- Every columnar read gives the same values as `Block.ReadAs<T>`.
- The POCO read of a 6-column mixed row gives the same rows as `PocoReadPlan<T>`.
- Every write gives **the same bytes** as `IColumnCodec.WriteFull`, including the LowCardinality dictionary
  order and the reserved slots.
- `LowCardinality(FixedString(N))` from `string`: the current client refuses it (`CanWrite` is false,
  because `string` has no key writer for FixedString). The candidate writes it, and the bytes are the same as
  the current write from zero-padded `byte[]`. The canonical value gives this case with no extra code.

## Measurements

100,000 rows, in memory (no server), .NET 10. **The box was shared and loaded** (load average 15 to 26 on
4 cores), so BenchmarkDotNet means had errors larger than the means. The numbers below come from
`dotnet run -c Release -- quick 61`: it runs the two arms alternately and reports the minimum and the
median. Ratio < 1 means the candidate is faster. Two passes gave the same direction for every case.

| Case | min ratio | median ratio |
|---|---:|---:|
| Columnar read, `Array(String)` as `byte[][]` | 0.54 to 0.82 | 0.66 to 0.70 |
| Columnar read, `LowCardinality(String)` as `byte[]` | 0.28 to 0.46 | 0.06 to 0.14 |
| Columnar read, `Nullable(DateTime)` as `DateTimeOffset?` | 0.57 to 0.61 | 0.50 to 0.55 |
| Columnar read, wide mixed row (6 columns) | 1.03 to 1.05 | 1.08 to 1.10 |
| POCO read, `Array(String)` as `byte[][]` | 1.15 to 1.43 | 0.90 to 0.92 |
| POCO read, `LowCardinality(String)` as `byte[]` | 0.98 to 1.07 | 0.95 |
| POCO read, `Nullable(DateTime)` as `DateTimeOffset?` | 1.17 to 1.19 | 1.26 to 1.44 |
| POCO read, wide mixed row | 1.22 to 1.29 | 1.16 to 1.26 |
| Write `LowCardinality(String)` from `string`, 100 distinct | 1.33 to 1.35 | 1.33 to 1.39 |
| Write `LowCardinality(String)` from `string`, all distinct | 0.78 to 0.93 | 0.82 to 0.84 |
| Write `LowCardinality(FixedString(16))` from `byte[]`, 100 distinct | 0.85 to 0.91 | 0.88 to 0.90 |
| Write `LowCardinality(FixedString(16))` from `byte[]`, all distinct | 0.66 to 0.70 | 0.55 to 0.65 |
| Write `Nullable(DateTime)` from `DateTimeOffset?` | 0.86 to 0.97 | 0.88 to 1.01 |
| Write `Array(String)` from `string[]` | 0.95 to 0.97 | 0.92 to 1.01 |

Not measured: `InsertRowsAsync<T>` (the POCO write tier), anything against a server, and reads in windows
smaller than a block.

## Answers to the questions in the issue

**Read performance.**

- Columnar: the candidate is as fast or faster. A bulk `Fill(start, Span<T>)` per combinator amortizes the
  virtual call over a run of values. LowCardinality gains the most, because the dictionary converts once
  when the reader binds to the column, and each row is then one array load.
- POCO: the candidate is slower at the minimum in 3 of 4 shapes (ratio 1.15 to 1.43), and the
  medians are mixed (0.90 to 1.44). The spike fills a pooled buffer with a bulk pass
  and then runs a second pass that assigns the properties. The current scatter puts the conversion inside
  the assignment loop, so it makes one pass with no buffer. The derived tree must be compiled into the
  scatter loop (issue item 5) to match. The spike does not do that. **This is the open question that
  decides the result.** The next step is a fused path, for example an `Expression` (or a struct `Get(row)`)
  for each leaf, spliced into the scatter loop. Combinators then only build that expression.

**Write performance.** Parity or better for Nullable, Array, and the FixedString dictionary.

**LowCardinality writes.** "Convert, then intern canonical values" is faster when values are mostly distinct,
and faster for `FixedString` from `byte[]`. The spike did not isolate the cause. It can be the canonical
key or only the open-addressing table (`ByteInterner`) against `Dictionary<TKey, int>`. It is **33% slower for `String` from `string` with many repeats**: the
canonical value of a `string` is its UTF-8 bytes, so each row is encoded before it is hashed, while the
current writer hashes the `string` directly. To fix this, the leaf can offer an optional "CLR key" fast
path when CLR equality agrees with canonical equality (`string` and `String`). That brings back one
comparer for each write type, but only as an optimization, not as a requirement for correctness.

**Public API.** No change is necessary. `StringColumn` already stores bytes plus offsets and decodes text
when asked. Text stays the default read type.

**Migration.** Reads can change in steps. The converter layer sits on the existing column surfaces, so a
derived reader can go behind `ColumnProjection.For` one composite at a time, with no change to the codecs.
Writes are harder: the codecs' `WriteColumn` takes an `IColumn` and the write shapes, so the write
combinators need their own entry point next to `IColumnCodec`.

**Members that go away.**

- `TryProjectColumnRead`: the combinator chooses bulk or per value, so there is one hook.
- `NullPlaceholderAs`: one canonical placeholder for each leaf (`LeafWriter.WritePlaceholders`).
- `LowCardinalityKeyWriter` and the comparers: the dictionary interns canonical bytes (`ByteInterner`).
  Fixed-width leaves need a second interner over `TCanon`, which the spike does not have.
- The Nullable, LowCardinality, and Array write shapes: replaced by three write combinators.
- `ClaimsValue` stays, for Variant.

**Ahead-of-time compilation.**

- Leaves work: the table has closed generic types over struct converters, written in source.
- Combinators do not: `Derive` builds `LiftValue<T>`, `Each<T>`, and `DictionaryReader<T>` with
  `MakeGenericType` from a runtime `Type`. A generic `Derive<T>()` entry point does not help, because the
  recursion must take `T[]` apart into `T`, which needs reflection. For value-type arguments this needs
  dynamic code. Options: a source generator for the POCO types, or an interpreted fallback over `object`.

## Problems found

1. **The value and reference doubling stays in Nullable.** A `Span<T?>` cannot be passed where a `Span<T>`
   is expected. So `LiftValue<T>` fills a pooled scratch buffer and copies, and `LiftRef<T>` fills in place.
   On write, `LiftValueWriter<T>` makes one virtual call into the leaf for each value. The doubling is now
   in one place and not in each family of shapes, but it does not go away.
2. **A read converter runs on the placeholder rows too.** `LiftRef` and `LiftValue` convert every row and
   then overwrite the null rows. So a leaf converter must accept the placeholder value. A converter that
   rejects some canonical values (for example a narrowing one) would need a masked fill.
3. **A composite writes each stream across all rows.** `Array(Nullable(X))` writes all the offsets, then
   the whole null map, then all values. So `EachWriter` cannot call its child once for each row. The spike
   passes the rows' arrays to the child as a list of segments, with no flat copy, but each Array level
   allocates (pools) one segment list.
4. **LowCardinality still looks at Nullable.** The dictionary of `LowCardinality(Nullable(X))` is a plain `X`
   column with NULL in slot 0. So the derivation for LowCardinality must unwrap a Nullable child, and a
   value-type target needs a lifting dictionary. This is a small special case, but it is a special case.
5. **Identity reads.** The current `ReadAs<T>` returns the column itself when it is already an `IColumn<T>`.
   The candidate always converts. A real version needs the same identity shortcut. This can explain part of
   the gap in the wide columnar case (the `String` and `LowCardinality(Nullable(String))` columns as
   `string`), but the spike did not measure that.
6. **Shared arrays.** LowCardinality read as `byte[]` gives the same array instance to every row that has the
   same key. The current client does the same.

## Decision proposed by the spike

**Adopt in part.** Adopt it for the columnar read tier and for LowCardinality and Nullable writes now. The
results are faster or equal, the code is simpler (one table entry for each leaf conversion), and it adds a
missing write (`LowCardinality(FixedString)` from `string`). Before the POCO tier moves, prove that a fused,
compiled scatter over the derived tree is as fast as the current one. That is the next experiment.

## How to run

```
cd spikes/WireConverterSpike
dotnet run -c Release -- verify
dotnet run -c Release -- quick 61
dotnet run -c Release -- bench --filter '*' --inProcess   # BenchmarkDotNet; needs a quiet box
```
