# ClickHouse .NET Driver Test Runner Script
# This script configures the environment and runs tests against a Docker ClickHouse instance

param(
    [string]$Filter = "",
    [string]$Framework = "",
    [switch]$IntegrationTests = $false,
    [switch]$Verbose = $false,
    [switch]$SkipDockerSetup = $false
)

Write-Host "ClickHouse .NET Driver Test Runner" -ForegroundColor Cyan
Write-Host "===================================" -ForegroundColor Cyan

# Function to check if a command exists
function Test-CommandExists {
    param($Command)
    try {
        if (Get-Command $Command -ErrorAction Stop) {
            return $true
        }
    } catch {
        return $false
    }
}

# Function to wait for ClickHouse to be ready
function Wait-ForClickHouse {
    param(
        [string]$ContainerName = "clickhouse-test",
        [int]$MaxAttempts = 30
    )
    
    Write-Host "Waiting for ClickHouse to be ready..." -ForegroundColor Yellow
    
    for ($i = 1; $i -le $MaxAttempts; $i++) {
        try {
            # Try to execute a simple query
            $result = docker exec $ContainerName clickhouse-client --query "SELECT 1" 2>&1
            if ($result -eq "1") {
                Write-Host "ClickHouse is ready!" -ForegroundColor Green
                return $true
            }
        } catch {
            # Ignore errors during startup
        }
        
        Write-Host "Attempt $i/$MaxAttempts - ClickHouse not ready yet..." -ForegroundColor Gray
        Start-Sleep -Seconds 1
    }
    
    return $false
}

if (-not $SkipDockerSetup) {
    # Check if Docker is installed
    Write-Host "`nChecking Docker installation..." -ForegroundColor Yellow
    if (-not (Test-CommandExists "docker")) {
        Write-Host "Docker is not installed!" -ForegroundColor Red
        Write-Host "`nDocker Desktop can be downloaded from: https://www.docker.com/products/docker-desktop" -ForegroundColor Yellow
        Write-Host "After installing Docker, please restart this script." -ForegroundColor Yellow
        exit 1
    }

    # Check if Docker is running
    Write-Host "Checking if Docker is running..." -ForegroundColor Yellow
    try {
        docker version | Out-Null
        Write-Host "Docker is installed and running." -ForegroundColor Green
    } catch {
        Write-Host "Docker is installed but not running!" -ForegroundColor Red
        Write-Host "Please start Docker Desktop and try again." -ForegroundColor Yellow
        exit 1
    }

    # Define expected container configuration
    $expectedContainerName = "clickhouse-test"
    $expectedUsername = "default"
    $expectedPassword = "test123"
    $expectedHttpPort = 8123
    $expectedNativePort = 9000

    # Check for any running ClickHouse container
    Write-Host "`nChecking for ClickHouse containers..." -ForegroundColor Yellow
    $runningContainers = docker ps --format "table {{.Names}}\t{{.Ports}}" | Select-Object -Skip 1 | Where-Object { $_ -match "clickhouse" }
    
    $needNewContainer = $true
    
    if ($runningContainers) {
        Write-Host "Found running ClickHouse container(s):" -ForegroundColor Yellow
        $runningContainers | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
        
        # Check if our expected container is running with correct ports
        $expectedContainer = docker ps --filter "name=$expectedContainerName" --format "json" | ConvertFrom-Json
        if ($expectedContainer) {
            $ports = docker port $expectedContainerName 2>$null
            if ($ports -match "${expectedHttpPort}/tcp" -and $ports -match "${expectedNativePort}/tcp") {
                Write-Host "Container '$expectedContainerName' is running with correct ports." -ForegroundColor Green
                $needNewContainer = $false
            } else {
                Write-Host "Container '$expectedContainerName' exists but has incorrect port mappings." -ForegroundColor Yellow
            }
        }
    }

    if ($needNewContainer) {
        Write-Host "`nSetting up ClickHouse container..." -ForegroundColor Yellow
        
        # Stop and remove existing test container if it exists
        $existingContainer = docker ps -a --filter "name=$expectedContainerName" --format "json" | ConvertFrom-Json
        if ($existingContainer) {
            Write-Host "Removing existing container '$expectedContainerName'..." -ForegroundColor Yellow
            docker stop $expectedContainerName 2>$null | Out-Null
            docker rm $expectedContainerName 2>$null | Out-Null
        }
        
        # Pull latest ClickHouse image
        Write-Host "Pulling latest ClickHouse image..." -ForegroundColor Yellow
        docker pull clickhouse/clickhouse-server:latest
        
        # Start new container with proper configuration
        Write-Host "Starting new ClickHouse container..." -ForegroundColor Yellow
        $containerId = docker run -d `
            --name $expectedContainerName `
            -p ${expectedHttpPort}:8123 `
            -p ${expectedNativePort}:9000 `
            -e CLICKHOUSE_DB=default `
            -e CLICKHOUSE_USER=$expectedUsername `
            -e CLICKHOUSE_PASSWORD=$expectedPassword `
            clickhouse/clickhouse-server:latest
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Container started successfully (ID: $($containerId.Substring(0, 12)))" -ForegroundColor Green
            
            # Wait for ClickHouse to be ready
            if (-not (Wait-ForClickHouse -ContainerName $expectedContainerName)) {
                Write-Host "ClickHouse failed to start properly!" -ForegroundColor Red
                Write-Host "`nContainer logs:" -ForegroundColor Yellow
                docker logs $expectedContainerName --tail 50
                exit 1
            }
        } else {
            Write-Host "Failed to start ClickHouse container!" -ForegroundColor Red
            exit 1
        }
    }
    
    # Verify connection
    Write-Host "`nVerifying ClickHouse connection..." -ForegroundColor Yellow
    try {
        $testUrl = "http://localhost:${expectedHttpPort}/ping"
        $response = Invoke-WebRequest -Uri $testUrl -Method Get -Headers @{
            "X-ClickHouse-User" = $expectedUsername
            "X-ClickHouse-Key" = $expectedPassword
        } -UseBasicParsing -ErrorAction Stop
        
        if ($response.StatusCode -eq 200) {
            Write-Host "Successfully connected to ClickHouse!" -ForegroundColor Green
        }
    } catch {
        Write-Host "Warning: Could not verify HTTP connection. Tests may fail." -ForegroundColor Yellow
        Write-Host "Error: $_" -ForegroundColor Gray
    }
}

# Set environment variable for tests
$env:CLICKHOUSE_CONNECTION = "Host=localhost;Port=8123;Username=default;Password=test123;Database=default"
Write-Host "`nConnection string set: $env:CLICKHOUSE_CONNECTION" -ForegroundColor Green

# Build the test command
$testProject = if ($IntegrationTests) { 
    "ClickHouse.Driver.IntegrationTests" 
} else { 
    "ClickHouse.Driver.Tests" 
}

$testCommand = "dotnet test $testProject/"

# Add framework if specified
if ($Framework) {
    $testCommand += " --framework $Framework"
}

# Add filter if specified
if ($Filter) {
    $testCommand += " --filter `"$Filter`""
}

# Add verbosity
if ($Verbose) {
    $testCommand += " -v normal"
} else {
    $testCommand += " -v minimal"
}

# Display test configuration
Write-Host "`nTest Configuration:" -ForegroundColor Cyan
Write-Host "  Project: $testProject" -ForegroundColor White
if ($Framework) { Write-Host "  Framework: $Framework" -ForegroundColor White }
if ($Filter) { Write-Host "  Filter: $Filter" -ForegroundColor White }
Write-Host "  Verbosity: $(if ($Verbose) { 'normal' } else { 'minimal' })" -ForegroundColor White

Write-Host "`nRunning command: $testCommand" -ForegroundColor Yellow
Write-Host "`nStarting tests..." -ForegroundColor Green
Write-Host "=================" -ForegroundColor Green

# Run the tests
try {
    Invoke-Expression $testCommand
    $exitCode = $LASTEXITCODE
} catch {
    Write-Host "`nError running tests: $_" -ForegroundColor Red
    exit 1
}

# Check test results
if ($exitCode -eq 0) {
    Write-Host "`nTests completed successfully!" -ForegroundColor Green
} else {
    Write-Host "`nTests failed with exit code: $exitCode" -ForegroundColor Red
}

# Offer to view container logs if tests failed
if ($exitCode -ne 0) {
    $viewLogs = Read-Host "`nWould you like to view ClickHouse container logs? (y/n)"
    if ($viewLogs -eq 'y') {
        Write-Host "`nLast 50 lines of ClickHouse logs:" -ForegroundColor Yellow
        docker logs clickhouse-test --tail 50
    }
}

exit $exitCode