param(
    [ValidateSet("baseline", "ramp", "burst", "quality", "custom")]
    [string]$Mode = "baseline",
    [double]$TargetRate = 0,
    [double]$Duration = 0,
    [double]$StageSeconds = 60,
    [double]$QualityRate = 0,
    [double]$DuplicateRate = 0,
    [double]$LateRate = 0,
    [double]$SequenceGapRate = 0,
    [string]$EventTimeStart = ""
)

$ErrorActionPreference = "Stop"

$argsList = @(
    "--mode", $Mode,
    "--stage-seconds", "$StageSeconds",
    "--quality-rate", "$QualityRate",
    "--duplicate-rate", "$DuplicateRate",
    "--late-rate", "$LateRate",
    "--sequence-gap-rate", "$SequenceGapRate"
)

if ($Duration -gt 0) { $argsList += @("--duration", "$Duration") }

if ($Mode -eq "custom") {
    if ($TargetRate -le 0) { throw "Custom mode requires -TargetRate > 0" }
    $argsList += @("--target-rate", "$TargetRate")
}

if (-not [string]::IsNullOrWhiteSpace($EventTimeStart)) {
    $argsList += @("--event-time-start", $EventTimeStart)
}

docker compose --profile simulator run --rm ami-simulator @argsList

if ($LASTEXITCODE -ne 0) {
    throw "AMI simulator exited with code $LASTEXITCODE"
}
