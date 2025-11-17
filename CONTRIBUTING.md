# Contributing to ClickHouse.Driver

ClickHouse.Driver is an open-source project, and we welcome any contributions from the community.
Please share your ideas, contribute to the codebase, and help us maintain up-to-date documentation.

## Getting started

### Prerequisites

You will need:

- **.NET SDK 9.0+** (the project also targets .NET 6.0, 8.0, .NET Standard 2.1, and .NET Framework 4.6.2/4.8)

You can verify your .NET installation:

```bash
dotnet --version
```

### Create a fork of the repository and clone it

```bash
git clone https://github.com/ClickHouse/clickhouse-cs
cd clickhouse-cs
```

### Restore dependencies

### Build the solution

```bash
dotnet build ClickHouse.Driver.sln -c Release
```


## Style Guide

The project enforces code style automatically via .editorconfig and ```<EnforceCodeStyleInBuild>true</EnforceCodeStyleInBuild>```.


## Testing

Whenever you add a new feature or fix a bug, we strongly encourage you to add appropriate tests
to ensure everyone in the community can safely benefit from your contribution.

### Test structure

The solution includes three test projects:

- **ClickHouse.Driver.Tests** - Unit and functional tests
- **ClickHouse.Driver.IntegrationTests** - Integration tests (Liq2db)
- **ClickHouse.Driver.Benchmark** - Performance benchmarks (BenchmarkDotNet)

### Running unit tests

Tests require a running ClickHouse server. The easiest way is to use Docker. Running the tests requires setting up an environment variable with a valid connection string, eg

```bash
# Windows (PowerShell)
$env:CLICKHOUSE_CONNECTION="Host=localhost;Port=8123;Username=default"

# Linux/macOS (Bash)
export CLICKHOUSE_CONNECTION="Host=localhost;Port=8123;Username=default"
```

#### Start ClickHouse in Docker

```bash
docker run --rm -d \
  -p 8123:8123 \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  --name clickhouse-dev clickhouse/clickhouse-server:latest
```


#### Run the tests

Run all tests on .NET 9.0:

```bash
dotnet test ClickHouse.Driver.Tests/ClickHouse.Driver.Tests.csproj --framework net9.0 -c Release
```


#### Stop the ClickHouse container

```bash
docker stop clickhouse-dev
```

### Running integration tests

Integration tests use the same ClickHouse instance but are in a separate project:

```bash
dotnet test ClickHouse.Driver.IntegrationTests/ClickHouse.Driver.IntegrationTests.csproj --framework net9.0 -c Release
```

### Running tests with code coverage

The project uses Coverlet for code coverage:

```bash
dotnet test ClickHouse.Driver.Tests/ClickHouse.Driver.Tests.csproj \
  --framework net9.0 \
  -c Release \
  /p:CollectCoverage=true \
  /p:CoverletOutputFormat=opencover \
  /p:SkipAutoProps=true
```

Coverage reports are generated in the test project directory:

```
ClickHouse.Driver.Tests/coverage.net9.0.opencover.xml
```

### Running benchmarks

Benchmarks use BenchmarkDotNet and require a running ClickHouse instance:

```bash
dotnet run --project ClickHouse.Driver.Benchmark/ClickHouse.Driver.Benchmark.csproj \
  --framework net9.0 \
  --configuration Release \
  -- --join --filter "*" --artifacts ./results --job Short
```

Results will be saved in the `results/` directory.

### Testing against different ClickHouse versions

To test against a specific ClickHouse version:

```bash
docker run --rm -d \
  -p 8123:8123 \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  --name clickhouse-dev clickhouse/clickhouse-server:25.3

# Set the version environment variable
export CLICKHOUSE_VERSION="25.3"

# Run tests
dotnet test ClickHouse.Driver.Tests/ClickHouse.Driver.Tests.csproj --framework net9.0 -c Release
```

## CI/CD

### Expected CI behavior

- **All tests must pass** on all target frameworks and ClickHouse versions
- **Code coverage** is reported to Codecov (must not decrease significantly)
- **Benchmarks** run but don't fail the build (used for performance regression detection)
- **CodeQL** must not find security issues
- **Public API analyzer** will fail if API surface changes aren't documented


## Release process

Releases are managed by maintainers through the GitHub Actions release workflow.

### Version updates

Before release, update version in:

- `ClickHouse.Driver/ClickHouse.Driver.csproj` - `<Version>` property
- Ensure `PublicAPI.Shipped.txt` is updated with new APIs
- Move entries from `PublicAPI.Unshipped.txt` to `PublicAPI.Shipped.txt`

### Release workflow

The release is triggered manually via GitHub Actions:

1. Go to Actions → Release workflow
2. Click "Run workflow"
3. The workflow will:
   - Build the solution in Release mode
   - Pack the NuGet package
   - Publish to NuGet.org
   - Create a GitHub release

## Getting help

- **Issues**: Open a GitHub issue for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions
- **Pull Requests**: Submit PRs with clear descriptions and tests

## Code of Conduct

Please be respectful and constructive in all interactions. We aim to foster an inclusive and welcoming community.

## License

By contributing to ClickHouse.Driver, you agree that your contributions will be licensed under the MIT License.
