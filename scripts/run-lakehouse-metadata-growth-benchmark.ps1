param(
    [int]$Stages = 4,
    [int]$WarmupSeconds = 60,
    [int]$StageSeconds = 300,
    [double]$TargetRate = 100,
    [double]$SampleInterval = 5,
    [double]$StorageSampleInterval = 30,
    [int]$DrainSeconds = 120
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RepoRoot

Write-Host ""
Write-Host "============================================================"
Write-Host "LAKEHOUSE METADATA / FILE GROWTH BENCHMARK v5"
Write-Host "============================================================"
Write-Host "Stages                  : $Stages"
Write-Host "Warm-up                 : $WarmupSeconds s"
Write-Host "Stage duration          : $StageSeconds s"
Write-Host "Constant target rate    : $TargetRate events/s"
Write-Host "Metric sample interval  : $SampleInterval s"
Write-Host "Storage sample interval : $StorageSampleInterval s"
Write-Host "Drain                    : $DrainSeconds s"
Write-Host ""
Write-Host "Write path under test:"
Write-Host "AMI -> Kafka -> Flink -> Iceberg/Lakekeeper -> S3G -> Ozone"
Write-Host ""
Write-Host "Recommended for this laptop: keep Trino and Recon stopped."
Write-Host ""

python .\benchmarks\metadata\benchmark_lakehouse_metadata_growth.py `
    --stages $Stages `
    --warmup-seconds $WarmupSeconds `
    --stage-seconds $StageSeconds `
    --target-rate $TargetRate `
    --sample-interval $SampleInterval `
    --storage-sample-interval $StorageSampleInterval `
    --drain-seconds $DrainSeconds

if ($LASTEXITCODE -ne 0) {
    throw "Lakehouse metadata/file-growth benchmark failed with exit code $LASTEXITCODE"
}
