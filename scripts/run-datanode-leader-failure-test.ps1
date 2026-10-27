param(
    [string]$TargetDataNode = "datanode2",
    [int]$TargetRate = 50,
    [int]$DurationSeconds = 240,
    [int]$FailureAfterSeconds = 30,
    [int]$FailureDurationSeconds = 60,
    [int]$SampleIntervalSeconds = 10,
    [int]$DrainSeconds = 60,
    [int]$PreTestSettleSeconds = 10,
    [string]$ConsumerGroup = "bronze-ami-meter-v1",
    [string]$Topic = "ami.meter.events"
)

$ErrorActionPreference = "Stop"
& (Join-Path $PSScriptRoot "..\benchmarks\recovery\run-datanode-leader-failure-test.ps1") @PSBoundParameters
if (-not $?) { throw "DataNode failure runner returned an error" }
