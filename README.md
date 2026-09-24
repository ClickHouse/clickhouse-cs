<p align="center">
<img src=".static/logo.svg" width="200px" align="center">
<h1 align="center">ClickHouse C# client</h1>
</p>
<br/>
<p align="center">
<a href="https://www.nuget.org/packages/ClickHouse.Driver">
<img alt="NuGet Version" src="https://img.shields.io/nuget/v/ClickHouse.Driver">
</a>

<a href="https://www.nuget.org/packages/ClickHouse.Driver">
<img alt="NuGet Downloads" src="https://img.shields.io/nuget/dt/ClickHouse.Driver">
</a>

<a href="https://github.com/ClickHouse/clickhouse-cs/actions/workflows/tests.yml">
<img src="https://github.com/ClickHouse/clickhouse-cs/actions/workflows/tests.yml/badge.svg?branch=main">
</a>

<a href="https://codecov.io/gh/ClickHouse/clickhouse-cs">
<img src="https://codecov.io/gh/ClickHouse/clickhouse-cs/graph/badge.svg">
</a>

</p>

## About

Official C# client for [ClickHouse](https://clickhouse.com/). The `ClickHouse.Driver` NuGet package contains two clients:

* **HTTP client**: `ClickHouseClient` and an ADO.NET provider for ORMs. It sends data in the RowBinary format over HTTP(S). Use it for most applications.
* **Native TCP client** (experimental): `ClickHouseTcpClient`. It uses the ClickHouse native protocol and sends data in columnar blocks.

## Choosing a client

| | HTTP | Native TCP |
|---|---|---|
| Status | Stable | Experimental. The API can change in a future release. |
| Main API | `ClickHouseClient`, ADO.NET (`ClickHouseConnection`) | `ClickHouseTcpClient` |
| .NET versions | 6.0 and newer | 8.0 and newer |
| Default ports | 8123 (HTTP), 8443 (HTTPS) | 9000, 9440 (TLS) |
| Performance | Good | Better, especially when you process data in columns |
| ADO.NET and ORMs (Dapper, EF Core, linq2db) | Yes | No |
| CSV, JSONEachRow, Parquet, and raw streams | Yes | No |
| `JSON` performance | Better: binary transfer | Text transfer only |
| Bearer authentication and custom HTTP headers | Yes | No |
| Columnar block reads | No | Yes |
| Progress, profile, and server log callbacks | No | Yes |

The [overview](https://clickhouse.com/docs/integrations/language-clients/csharp/overview) gives more information about when to use each client.

## Documentation

Full documentation is on the ClickHouse website:

* [Overview](https://clickhouse.com/docs/integrations/language-clients/csharp/overview)
* [HTTP client](https://clickhouse.com/docs/integrations/language-clients/csharp/http)
* [Native TCP client](https://clickhouse.com/docs/integrations/language-clients/csharp/tcp)

## Usage examples

We have a wide range of [examples](./examples), aiming to cover typical scenarios of client usage. They are grouped by client: [HTTP](./examples/Http) and [native TCP](./examples/Tcp).

## ClickHouse versions

Both clients support the last 3 releases plus the last 2 LTS releases of the ClickHouse server.

## Contact us

If you have any questions or need help, feel free to reach out to us in the [Community Slack](https://clickhouse.com/slack) or via [GitHub issues](https://github.com/ClickHouse/clickhouse-cs/issues).

## Contributing

Contributions are welcome and highly appreciated! Check out our [contributing guide](./CONTRIBUTING.md).

## Official integrations

* **Serilog**: [Serilog.Sinks.ClickHouse](https://github.com/ClickHouse/Serilog.Sinks.ClickHouse)
* **Entity Framework Core**: [ClickHouse.EntityFrameworkCore](https://github.com/ClickHouse/ClickHouse.EntityFrameworkCore)
* **Aspire**: [ClickHouse.Aspire](https://github.com/ClickHouse/ClickHouse.Aspire/)
* **Semantic Kernel**: [ClickHouse.SemanticKernel](https://github.com/ClickHouse/ClickHouse.SemanticKernel/)

## Acknowledgements
Originally created by [Oleg V. Kozlyuk](https://github.com/DarkWanderer)
