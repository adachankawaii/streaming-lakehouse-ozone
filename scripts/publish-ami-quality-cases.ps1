$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$fixtureDir = Join-Path $repoRoot "tests\fixtures"

function Read-OneLinePayload {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (-not (Test-Path $Path)) {
        throw "Fixture not found: $Path"
    }

    return ((Get-Content $Path -Raw) -replace '\r?\n', '').Trim()
}

$valid = Read-OneLinePayload (Join-Path $fixtureDir "ami-reading-v1.json")
$malformed = Read-OneLinePayload (Join-Path $fixtureDir "ami-malformed.txt")
$partial = Read-OneLinePayload (Join-Path $fixtureDir "ami-partial-v1.json")
$unsupported = Read-OneLinePayload (Join-Path $fixtureDir "ami-unsupported-schema-v2.json")

$records = @(
    "M00128:$valid",
    "M_BAD_JSON:$malformed",
    "M00129:$partial",
    "M00130:$unsupported"
)

Write-Host "Publishing 4 AMI Bronze quality cases to Kafka..."
Write-Host "  M00128      -> expected OK"
Write-Host "  M_BAD_JSON  -> expected MALFORMED"
Write-Host "  M00129      -> expected PARTIAL"
Write-Host "  M00130      -> expected UNSUPPORTED_SCHEMA"

($records -join "`n") |
    docker compose exec -T kafka `
        /opt/kafka/bin/kafka-console-producer.sh `
        --bootstrap-server kafka:19092 `
        --topic ami.meter.events `
        --property parse.key=true `
        --property key.separator=:

if ($LASTEXITCODE -ne 0) {
    throw "Kafka producer exited with code $LASTEXITCODE"
}

Write-Host ""
Write-Host "Published successfully."
Write-Host "Wait for the next Flink checkpoint, then query bronze.ami_meter_raw."
