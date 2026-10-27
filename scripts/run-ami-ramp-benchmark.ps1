param(
    [int]$StageSeconds = 60,
    [double]$SampleInterval = 5,
    [double]$StorageSampleInterval = 30,
    [int]$DrainSeconds = 30
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RepoRoot

python .\benchmarks\ingestion\benchmark_ami_ramp.py `
    --stage-seconds $StageSeconds `
    --sample-interval $SampleInterval `
    --storage-sample-interval $StorageSampleInterval `
    --drain-seconds $DrainSeconds

if ($LASTEXITCODE -ne 0) {
    throw "AMI RAMP benchmark failed with exit code $LASTEXITCODE"
}
