[CmdletBinding()]
param()
$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RepoRoot
$jobs = (Invoke-RestMethod "http://localhost:8081/jobs/overview" -TimeoutSec 10).jobs
$running = @($jobs | Where-Object { $_.state -eq "RUNNING" -and $_.name -like "*ami_meter_raw*" })
if ($running.Count -gt 0) {
    Write-Host "Bronze job already RUNNING; no duplicate submit." -ForegroundColor Green
    return
}
Write-Host "Submitting Bronze streaming SQL. This creates a long-running job." -ForegroundColor Yellow
docker compose exec -T flink-jobmanager /opt/flink/bin/sql-client.sh `
    -i /opt/flink/conf/init.sql `
    -f /opt/flink/jobs/bronze_ingestion/ami_meter.sql
if ($LASTEXITCODE -ne 0) { throw "Bronze job submit failed ($LASTEXITCODE)" }
