---
name: issue-priority
description: >
  Decide the priority of a `ClickHouse/clickhouse-cs` issue: `Urgent`, `High`,
  `Medium`, or `Low`, for the org issue field **Priority**. Use this skill
  when the task is to "prioritize", "set the priority of", "rate", or "triage"
  an issue, or to explain why an issue has a given priority. It defines the
  reach tiers of the clients and their features, the severity rows, the base-
  level matrix, the modifiers, and the confidence.
---

# Issue priority

This skill is the single source of truth for the **Priority** of
`ClickHouse/clickhouse-cs` issues. People and automated triage use the same
rules.

The value goes into the org issue field **Priority** (`Urgent`, `High`,
`Medium`, `Low`). The field is visible to org members only. An automated tool
sets the value once, never changes a value that is already set, and keeps the
level and its reason out of public comments.

## When to set no priority

Set no priority when one of these is true:

- A maintainer can close the issue with no further action: it is already fixed
  on `main`, a duplicate, works as intended, already supported, answered, or
  obsolete.
- Only a deprecated public API reaches the defect: an API with an
  `[Obsolete]` attribute, for example `ClickHouseBulkCopy` (use
  `ClickHouseClient.InsertBinaryAsync`), `ClickHouseConnection.ServerVersion`,
  or the `AddParameter` overload that takes a type. Fixes to deprecated code
  are not released.
- Only a ClickHouse server older than the support floor shows the defect. The
  floor is the lowest entry of the `version` matrix in
  `.github/workflows/tests.yml` (HTTP) and `.github/workflows/tests-tcp.yml`
  (TCP).
- The issue is a question, or it is about CI, dependencies, the release
  process, or a discussion.

## Vulnerabilities that are not public yet

A security issue must not stay public with a description of how to reproduce
it. This section applies when a reproduction or code inspection confirms a
vulnerability that is not public yet. The classification skill of this
repository defines it
([`../issue-classification/SKILL.md`](../issue-classification/SKILL.md), "A
vulnerability that is not public yet"): memory corruption is one also when only
a hostile server or network causes it, and a hang, or a crash with no memory
corruption, that only such a server or network causes is not one.

- Report it in a private GitHub vulnerability report for this repository
  (**Security** → **Report a vulnerability**), never in a public issue,
  comment, or pull request. If a public issue reports it, copy the title, the
  text, and the comments of the issue into the report first.
- Set no Priority on the public issue. Close it with a comment that gives no
  details, and ask a repository administrator to delete it: closing does not
  hide the text of the issue.
- Rate the vulnerability with the rules below, and put the level and its reason
  into the private report as the proposed severity.

A vulnerability that is already public (a published CVE or GHSA, for example in
a dependency), and an authentication method that does not work or is missing,
get a Priority on the issue as usual.

## 1. Reach

The reach is the tier of the most-used public path **and condition** that
reaches the defect, not the tier of the file that contains it. A defect that
shows only with a rare type, a non-default setting, or an unusual server setup
is Narrow, also when it goes through `ClickHouseClient` or the ADO.NET API.

| Tier      | Public paths and conditions |
| --------- | --------------------------- |
| Core      | The HTTP client with default settings: `ClickHouseClient` (`ExecuteNonQueryAsync`, `ExecuteScalarAsync`, `ExecuteReaderAsync`, `InsertBinaryAsync`, also with POCOs); the ADO.NET API that Dapper, EF Core and linq2db use (`ClickHouseDataSource`, `ClickHouseConnection`, `ClickHouseCommand`, `ClickHouseDataReader`, `ClickHouseDbParameter`); connection-string and `ClickHouseClientSettings` parsing; the HTTP transport (`Http/`: connection pool, `HttpClient` factories, timeouts, cancellation, gzip compression, which is on by default); auth and TLS; error handling (`ClickHouseServerException`); query parameters (`{name:Type}` and `@name`, `HttpParameterFormatter`); the read and write paths of common types: integers, floats, `Decimal` (also as `ClickHouseDecimal`, the default), `String`, `FixedString`, `Bool`, `UUID`, `IPv4`, `IPv6`, `Date`, `Date32`, `DateTime` and `DateTime64` with a named time zone, `Enum`, `Nullable`, `LowCardinality`, `Array`, `Map`, `Tuple` |
| Common    | Raw streams (`InsertRawStreamAsync`, `ExecuteRawResultAsync`, `ClickHouseRawResult`) with CSV, TSV, JSONEachRow or Parquet; the `JSON` type with typed paths; sessions (`UseSession`) and roles; custom HTTP headers and bearer tokens; `QueryOptions`/`InsertOptions` (query ID, per-query settings, per-query database); other compression (`AcceptEncoding`, ZSTD); `ClickHouseDataAdapter`; OpenTelemetry (`ActivitySource`) and `ILogger` logging; the dependency-injection extensions; `ReadValueConverter` |
| Narrow    | The native TCP client (`ClickHouse.Driver.Tcp`, `ClickHouseTcpClient`): its API is experimental (`CHTCP0001`); rare types (`Dynamic`, `Variant`, the Geo types such as `Point` and `Polygon`, `Nested`, `Object('json')`, `AggregateFunction`, `SimpleAggregateFunction`, `Time`, `Time64`, `Int256`/`UInt256`, `QBit`); `JSON` paths with a `Dynamic` or `Variant` hint; fixed-offset or DST-ambiguous time zones (`Fixed/UTC+05:30:00`); non-default settings that few applications use (`ReadStringsAsByteArrays`, `UseFormDataParameters`, `MapReadMode`, `send_progress_in_http_headers`); `ClickHouseDecimal` arithmetic and equality (`Floor`, `GetHashCode`); the metadata APIs `GetSchema` and `GetSchemaTable`; a specific server setup (a `readonly` user, a proxy or load balancer, a cluster) |
| Satellite | Code that is not in the `ClickHouse.Driver` package: `examples/`, `docs/`, `ClickHouse.Driver.Benchmark/`, the test projects, `scripts/`, the agent files (`.claude/`, `AGENTS.md`); also a defect that needs two or more Narrow conditions together (for example a `Dynamic`-hinted `JSON` path and `ReadStringsAsByteArrays=true`) |

Integrations that live in other repositories (`ClickHouse.EntityFrameworkCore`,
`Serilog.Sinks.ClickHouse`, `ClickHouse.Aspire`, `ClickHouse.SemanticKernel`)
are not in this repository. Rate a defect of this package that they show by
the public path that they call.

## 2. Severity

First name the **impacts** of the issue with the classification skill of this
repository
([`../issue-classification/SKILL.md`](../issue-classification/SKILL.md)): none,
one, or more of Security (part vulnerability or part authentication), Data
integrity, Stability, and Performance. Most issues have none.

Then use the first severity row that fits. With more than one impact, the
highest row applies:

| Severity                | Use when |
| ----------------------- | -------- |
| Wrong data or security  | impact **Data integrity**: the client returns or stores wrong or incomplete data with no error, or reports success for a query or insert that failed; or impact **Security** with a vulnerability: a security defect |
| Broken, no workaround   | the feature fails (an exception, a hang, a crash, a stack overflow, a leak that ends in a crash or in exhausted connections), and the caller has no reasonable workaround; also a wrong or incomplete error with no workaround: the failure is reported, but its type, code, or message is wrong or lost. The impact **Stability** (a hang, a crash, such a leak) uses this row when the caller has no reasonable workaround |
| Workaround exists       | the feature fails, but the caller can avoid the failure with a small change in their own code, for example a cast in SQL, another overload, or another setting (name the workaround in the reason); also impact **Performance**: a result that is correct but slow or uses too much memory |
| Cosmetic, docs, feature | wrong log or message text, documentation, a feature request, a missing conversion that a cast avoids |

An issue that has the impact Security with no vulnerability (an authentication
method that does not work or is missing) does not use the first row. Rate an
authentication method that does not work by its failure ("Broken, no
workaround" or "Workaround exists"), and a request for a missing method as a
feature request.

## 3. Base level

| Severity ↓ / Reach →    | Core   | Common | Narrow | Satellite |
| ----------------------- | ------ | ------ | ------ | --------- |
| Wrong data or security  | Urgent | Urgent | High   | Medium    |
| Broken, no workaround   | High   | High   | Medium | Low       |
| Workaround exists       | Medium | Medium | Low    | Low       |
| Cosmetic, docs, feature | Low    | Low    | Low    | Low       |

## 4. Modifiers

Apply them in this order:

1. **Security with a shown path from untrusted input.** The issue or a
   reproduction shows how a value that an application takes from its users
   reaches the defect: `Urgent` at any reach. Skip the other modifiers. A
   defect that puts a caller-supplied value into SQL without quoting or
   escaping (a parameter value, a table or column name, an identifier helper)
   is a candidate: look for such a path. If none is shown, rate the defect by
   the failure that it shows. A vulnerability that is not public yet goes to a
   private vulnerability report, not to a public issue (see "Vulnerabilities
   that are not public yet").
2. **Regression in the latest published release:** up one level. This can
   give `Urgent`. It is a regression only when the latest release shows the
   problem and the release before it does not. Tags are `X.Y.Z` with no `v`
   (for example `1.5.0`); pre-releases are `X.Y.Z-rcN`, and they do not count
   as the latest release. The native TCP client is in the package from
   `1.5.0`: a TCP defect is not a regression before a second release has the
   TCP client.
3. **Customer request in Linear.** The issue links (`linear.app`) a Linear
   issue for a customer request: up one level, but never to `Urgent` (`High`
   stays `High`).

## 5. Confidence

A missing reproduction lowers the confidence, not the level. Record the
confidence with the value when the tool that sets it supports it:

| Confidence | Use when |
| ---------- | -------- |
| `HIGH`     | a run shows the problem (or, for a feature, code inspection confirms its absence on `main`) |
| `MEDIUM`   | code inspection shows a defect, but no reproduction was run (for example, it needs ClickHouse Cloud, Windows, or a cluster) |
| `LOW`      | the report lacks what a reproduction needs, or a run did not show the problem |

## 6. Reason

Write one line with the value:

`<tier> (<path>) · <impacts, or "no impact"> · <severity> · <modifiers, or "no modifier"> → <level>.`

Name the condition that makes the reach Narrow or Satellite. Name the
workaround when the severity is "Workaround exists". When the confidence is
`LOW`, say what is missing.

## 7. Both clients

The HTTP client and the TCP client do not share a type tree, so some logic is
written twice (see "Logic that exists in both clients" in `AGENTS.md`). When
an issue reports a defect in one copy and the other copy has it too, rate the
issue by the client with the higher reach (the HTTP client), and say in the
reason that the fix needs both copies.
