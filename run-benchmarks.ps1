#!/usr/bin/env pwsh

# Run ArrayPool benchmark for ClickHouseDataReader

param(
    [switch]$SkipBuild,
    [switch]$Detailed,
    [string]$Filter = "*RecyclableMemoryStreamBenchmark*",
    [int]$WarmupCount = 3,
    [int]$IterationCount = 5
)

if (-not $SkipBuild) {
    Write-Host "Building ClickHouse.Driver.Benchmark in Release mode..." -ForegroundColor Green
    dotnet build ClickHouse.Driver.Benchmark/ClickHouse.Driver.Benchmark.csproj --configuration Release

    if ($LASTEXITCODE -ne 0) {
        Write-Host "Build failed!" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "Skipping build (using existing binaries)..." -ForegroundColor Yellow
}

Write-Host "`nRunning benchmark with filter: $Filter" -ForegroundColor Green
Write-Host "WarmupCount: $WarmupCount, IterationCount: $IterationCount" -ForegroundColor Cyan

if ($Detailed) {
    # Run with detailed memory diagnostics
    Write-Host "Running with detailed diagnostics..." -ForegroundColor Yellow
    dotnet run --project ClickHouse.Driver.Benchmark/ClickHouse.Driver.Benchmark.csproj `
        --configuration Release `
        --no-build `
        -- --filter "$Filter" `
        --warmupCount $WarmupCount `
        --iterationCount $IterationCount `
        --memory `
        --disasm `
        --profiler ETW
} else {
    # Standard run with memory diagnostics
    dotnet run --project ClickHouse.Driver.Benchmark/ClickHouse.Driver.Benchmark.csproj `
        --configuration Release `
        --no-build `
        -- --filter "$Filter" `
        --warmupCount $WarmupCount `
        --iterationCount $IterationCount `
        --memory
}

Write-Host "`nBenchmark complete! Results are in BenchmarkDotNet.Artifacts folder" -ForegroundColor Green
Write-Host "You can find detailed results in: BenchmarkDotNet.Artifacts\results\" -ForegroundColor Cyan