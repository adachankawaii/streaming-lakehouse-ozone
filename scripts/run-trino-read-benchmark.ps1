param(
    [Parameter(Mandatory = $true)][long]$SnapshotId,
    [int]$DurationSeconds = 60,
    [int]$WarmupQueries = 3,
    [string]$Endpoint = "http://localhost:8080",
    [string]$RunTag = "baseline"
)
$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RepoRoot
python .\benchmarks\read\trino_read_benchmark.py `
    --snapshot-id $SnapshotId `
    --duration-seconds $DurationSeconds `
    --warmup-queries $WarmupQueries `
    --endpoint $Endpoint `
    --run-tag $RunTag
if ($LASTEXITCODE -ne 0) {
    throw "Trino read benchmark failed with exit code $LASTEXITCODE"
}
