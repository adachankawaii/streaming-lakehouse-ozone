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
Set-StrictMode -Version Latest

# Run from <repo>\benchmarks\recovery\
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $RepoRoot

$RunId = "$(Get-Date -Format 'yyyyMMdd-HHmmss')-dn-leader-failure"
$RunDir = Join-Path $RepoRoot "benchmarks\results\$RunId"
New-Item -ItemType Directory -Force -Path $RunDir | Out-Null

$TimelinePath = Join-Path $RunDir "timeline.csv"
$SimulatorLogPath = Join-Path $RunDir "simulator.log"
$SimulatorStdoutPath = Join-Path $RunDir "simulator.stdout.log"
$SimulatorStderrPath = Join-Path $RunDir "simulator.stderr.log"
$ManifestPath = Join-Path $RunDir "benchmark_manifest.json"
$SummaryPath = Join-Path $RunDir "summary.csv"
$PipelineBeforePath = Join-Path $RunDir "pipeline_before.txt"
$PipelineDownPath = Join-Path $RunDir "pipeline_during_failure.txt"
$PipelineAfterPath = Join-Path $RunDir "pipeline_after_restart.txt"
$DnBeforePath = Join-Path $RunDir "datanodes_before.txt"
$DnDownPath = Join-Path $RunDir "datanodes_during_failure.txt"
$DnAfterPath = Join-Path $RunDir "datanodes_after_restart.txt"
$FinalBronzeCheckPath = Join-Path $RunDir "bronze_final_check.txt"

$timeline = [System.Collections.Generic.List[object]]::new()
$sw = [System.Diagnostics.Stopwatch]::new()
$simProcess = $null
$nodeStopped = $false
$failureInjected = $false
$restartCompleted = $false
$failureObserved = $false
$recoveryObserved = $false

function Now-Iso {
    [DateTimeOffset]::Now.ToString("o")
}


function Get-SimulatorState {
    if ($null -eq $simProcess) {
        return "NOT_STARTED"
    }

    try {
        $simProcess.Refresh()
        if (-not $simProcess.HasExited) {
            return "Running"
        }

        if ($simProcess.ExitCode -eq 0) {
            return "Completed"
        }

        return "Failed"
    }
    catch {
        return "UNKNOWN"
    }
}

function Save-SimulatorCombinedLog {
    $stdout = ""
    $stderr = ""

    if (Test-Path $SimulatorStdoutPath) {
        $stdout = Get-Content $SimulatorStdoutPath -Raw -ErrorAction SilentlyContinue
    }

    if (Test-Path $SimulatorStderrPath) {
        $stderr = Get-Content $SimulatorStderrPath -Raw -ErrorAction SilentlyContinue
    }

    @(
        "===== STDOUT =====",
        [string]$stdout,
        "",
        "===== STDERR =====",
        [string]$stderr
    ) | Out-File -FilePath $SimulatorLogPath -Encoding utf8
}

function Get-SimulatorErrorTail {
    if (-not (Test-Path $SimulatorStderrPath)) {
        return ""
    }

    $tail = Get-Content $SimulatorStderrPath -Tail 20 -ErrorAction SilentlyContinue
    return (($tail | ForEach-Object { [string]$_ }) -join "`n")
}

function Get-FlinkState {
    try {
        $jobs = @((Invoke-RestMethod "http://localhost:8081/jobs/overview" -TimeoutSec 5).jobs)
        $matching = @($jobs | Where-Object { $_.name -like "*ami_meter_raw*" })

        if ($matching.Count -eq 0) {
            return [pscustomobject]@{ state = "NO_JOB"; jid = $null }
        }

        # Prefer the currently RUNNING Bronze job. /jobs/overview can also return
        # historical FINISHED jobs with the same name, and their order is not
        # guaranteed. The old implementation simply selected the first match.
        $running = $matching |
            Where-Object { $_.state -eq "RUNNING" } |
            Select-Object -First 1

        if ($null -ne $running) {
            return [pscustomobject]@{ state = [string]$running.state; jid = [string]$running.jid }
        }

        # If the job is temporarily RESTARTING/FAILING/etc., report the newest
        # matching job instead of accidentally picking an old FINISHED one.
        $job = $matching |
            Sort-Object -Property @{ Expression = { [long]$_.'start-time' }; Descending = $true } |
            Select-Object -First 1

        return [pscustomobject]@{ state = [string]$job.state; jid = [string]$job.jid }
    }
    catch {
        return [pscustomobject]@{ state = "REST_ERROR"; jid = $null }
    }
}

function Get-CheckpointState([string]$JobId) {
    if ([string]::IsNullOrWhiteSpace($JobId)) {
        return [pscustomobject]@{ completed=$null; failed=$null; in_progress=$null; latest_duration_ms=$null }
    }
    try {
        $cp = Invoke-RestMethod "http://localhost:8081/jobs/$JobId/checkpoints" -TimeoutSec 5
        $latestDuration = $null
        if ($null -ne $cp.latest -and $null -ne $cp.latest.completed) {
            $latestDuration = $cp.latest.completed.end_to_end_duration
        }
        return [pscustomobject]@{
            completed=$cp.counts.completed
            failed=$cp.counts.failed
            in_progress=$cp.counts.in_progress
            latest_duration_ms=$latestDuration
        }
    }
    catch {
        return [pscustomobject]@{ completed=$null; failed=$null; in_progress=$null; latest_duration_ms=$null }
    }
}

function Get-KafkaState {
    # Important:
    # kafka-consumer-groups.sh prints the "no active members" informational line
    # on stderr. With PowerShell + ErrorActionPreference=Stop, merging native
    # stderr using 2>&1 can behave inconsistently. Suppress that informational
    # stderr INSIDE the Kafka container and parse stdout only.
    $text = ""
    try {
        $kafkaCmd = "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group '$ConsumerGroup' 2>/dev/null"

        $raw = @(& docker compose exec -T kafka bash -lc $kafkaCmd)
        $dockerExit = $LASTEXITCODE
        $text = (($raw | ForEach-Object { [string]$_ }) -join "`n")

        if ($dockerExit -ne 0) {
            return [pscustomobject]@{
                committed_total = $null
                log_end_total = $null
                lag_total = $null
                parse_error = "docker/kafka command exited with code $dockerExit"
                raw_text = $text
            }
        }

        $committed = [long]0
        $logEnd = [long]0
        $lag = [long]0
        $matchedRows = 0

        foreach ($lineObj in ($text -split "`r?`n")) {
            $line = ([string]$lineObj).Trim()
            if ([string]::IsNullOrWhiteSpace($line)) { continue }

            # Expected data row:
            # GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
            $m = [regex]::Match(
                $line,
                '^(?<group>\S+)\s+(?<topic>\S+)\s+(?<partition>\d+)\s+(?<current>\d+|-)\s+(?<end>\d+)\s+(?<lag>\d+|-)(?:\s+.*)?$'
            )

            if (-not $m.Success) { continue }
            if ($m.Groups['group'].Value -ne $ConsumerGroup) { continue }
            if ($m.Groups['topic'].Value -ne $Topic) { continue }

            $current = $m.Groups['current'].Value
            $endValue = $m.Groups['end'].Value
            $lagValue = $m.Groups['lag'].Value

            if ($current -eq '-' -or $lagValue -eq '-') {
                return [pscustomobject]@{
                    committed_total = $null
                    log_end_total = $null
                    lag_total = $null
                    parse_error = "Kafka returned '-' for committed offset or lag"
                    raw_text = $text
                }
            }

            $committed += [long]$current
            $logEnd += [long]$endValue
            $lag += [long]$lagValue
            $matchedRows++
        }

        if ($matchedRows -eq 0) {
            return [pscustomobject]@{
                committed_total = $null
                log_end_total = $null
                lag_total = $null
                parse_error = "No Kafka partition rows matched stdout"
                raw_text = $text
            }
        }

        return [pscustomobject]@{
            committed_total = $committed
            log_end_total = $logEnd
            lag_total = $lag
            parse_error = $null
            raw_text = $text
        }
    }
    catch {
        $message = "Unknown Kafka parser error"
        if ($null -ne $_ -and $null -ne $_.Exception -and -not [string]::IsNullOrWhiteSpace($_.Exception.Message)) {
            $message = $_.Exception.Message
        }

        return [pscustomobject]@{
            committed_total = $null
            log_end_total = $null
            lag_total = $null
            parse_error = $message
            raw_text = $text
        }
    }
}

function Get-ContainerState([string]$ServiceName) {
    try {
        $idRaw = & docker compose ps -a -q $ServiceName 2>$null
        $id = ([string]($idRaw | Select-Object -First 1)).Trim()
        if ([string]::IsNullOrWhiteSpace($id)) { return "NOT_FOUND" }
        $stateRaw = & docker inspect -f "{{.State.Status}}" $id 2>$null
        return ([string]($stateRaw | Select-Object -First 1)).Trim()
    }
    catch {
        return "INSPECT_ERROR"
    }
}

function Get-PipelineText {
    $raw = & docker compose exec -T scm ozone admin pipeline list 2>&1
    (($raw | ForEach-Object { [string]$_ }) -join [Environment]::NewLine)
}

function Get-DatanodeText {
    $raw = & docker compose exec -T scm ozone admin datanode list 2>&1
    (($raw | ForEach-Object { [string]$_ }) -join [Environment]::NewLine)
}

function Find-OpenRatisThreeLeaderPipeline([string]$PipelineText, [string]$ServiceName) {
    $nodePattern = "\{([0-9a-fA-F-]+)\(" + [regex]::Escape($ServiceName) + "/"

    foreach ($line in ($PipelineText -split "\r?\n")) {
        if ($line -notmatch "ReplicationConfig:\s*RATIS/THREE") { continue }
        if ($line -notmatch "State:\s*OPEN") { continue }
        if ($line -notmatch ("\(" + [regex]::Escape($ServiceName) + "/")) { continue }

        $nodeUuid = $null
        $leaderUuid = $null
        $pipelineId = $null

        if ($line -match $nodePattern) { $nodeUuid = $Matches[1] }
        if ($line -match "leaderId:([0-9a-fA-F-]+)") { $leaderUuid = $Matches[1] }
        if ($line -match "Pipeline\{\s*Id:\s*([0-9a-fA-F-]+)") { $pipelineId = $Matches[1] }

        if (-not [string]::IsNullOrWhiteSpace($nodeUuid) -and
            -not [string]::IsNullOrWhiteSpace($leaderUuid) -and
            $nodeUuid -eq $leaderUuid) {
            return [pscustomobject]@{
                pipeline_id=$pipelineId
                leader_uuid=$leaderUuid
                line=$line
            }
        }
    }
    return $null
}

function Get-BronzeCount {
    $sql = @"
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
USE CATALOG lakehouse;
USE bronze;
SELECT COUNT(*) AS total_rows
FROM ami_meter_raw;
"@
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($sql)
    $b64 = [Convert]::ToBase64String($bytes)

    # Windows PowerShell 5.1 may turn harmless native stderr from Flink
    # (e.g. "Unknown module: jdk.compiler") into NativeCommandError when
    # ErrorActionPreference is Stop. Temporarily allow native stderr and
    # judge success by the native exit code instead.
    $oldEap = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $raw = @(& docker compose exec -T flink-jobmanager bash -lc `
            "echo '$b64' | base64 -d > /tmp/leader_failure_count.sql && /opt/flink/bin/sql-client.sh -i /opt/flink/conf/init.sql -f /tmp/leader_failure_count.sql" 2>&1)
        $dockerExit = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $oldEap
    }

    $text = (($raw | ForEach-Object { [string]$_ }) -join "`n")

    if ($dockerExit -ne 0) {
        throw "Bronze COUNT(*) query failed with docker exit code $dockerExit.`n$text"
    }

    $matches = [regex]::Matches($text, "\|\s*([0-9]+)\s*\|")
    if ($matches.Count -lt 1) {
        throw "Could not parse Bronze COUNT(*).`n$text"
    }

    return [long]$matches[$matches.Count - 1].Groups[1].Value
}

function Write-FinalBronzeCheck {
    $sql = @"
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
USE CATALOG lakehouse;
USE bronze;

SELECT COUNT(*) AS total_rows
FROM ami_meter_raw;

SELECT
    kafka_partition,
    MIN(kafka_offset) AS min_offset,
    MAX(kafka_offset) AS max_offset,
    COUNT(*) AS row_count,
    COUNT(DISTINCT kafka_offset) AS distinct_offsets
FROM ami_meter_raw
GROUP BY kafka_partition
ORDER BY kafka_partition;
"@
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($sql)
    $b64 = [Convert]::ToBase64String($bytes)

    $oldEap = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $raw = @(& docker compose exec -T flink-jobmanager bash -lc `
            "echo '$b64' | base64 -d > /tmp/leader_failure_final.sql && /opt/flink/bin/sql-client.sh -i /opt/flink/conf/init.sql -f /tmp/leader_failure_final.sql" 2>&1)
        $dockerExit = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $oldEap
    }

    $raw | Out-File -FilePath $FinalBronzeCheckPath -Encoding utf8

    if ($dockerExit -ne 0) {
        throw "Final Bronze SQL check failed with docker exit code $dockerExit. See $FinalBronzeCheckPath"
    }
}

function Add-TimelineRow([string]$EventName) {
    $flink = Get-FlinkState
    $cp = Get-CheckpointState $flink.jid
    $kafka = Get-KafkaState
    $dnState = Get-ContainerState $TargetDataNode

    $simState = Get-SimulatorState

    $row = [pscustomobject]@{
        timestamp_local = Now-Iso
        elapsed_s = $(if ($sw.IsRunning) { [math]::Round($sw.Elapsed.TotalSeconds, 2) } else { 0 })
        event = $EventName
        simulator_state = $simState
        datanode_container_state = $dnState
        flink_state = $flink.state
        kafka_committed_total = $kafka.committed_total
        kafka_log_end_total = $kafka.log_end_total
        kafka_lag_total = $kafka.lag_total
        checkpoint_completed = $cp.completed
        checkpoint_failed = $cp.failed
        checkpoint_in_progress = $cp.in_progress
        latest_checkpoint_duration_ms = $cp.latest_duration_ms
    }

    $timeline.Add($row)
    $timeline | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $TimelinePath

    Write-Host (
        "[{0}] t={1,6}s {2,-24} sim={3,-9} dn={4,-8} flink={5,-10} logEnd={6} committed={7} lag={8} cpOK={9} cpFail={10}" -f `
        (Get-Date -Format "HH:mm:ss"), $row.elapsed_s, $EventName,
        $row.simulator_state, $row.datanode_container_state, $row.flink_state,
        $row.kafka_log_end_total, $row.kafka_committed_total, $row.kafka_lag_total,
        $row.checkpoint_completed, $row.checkpoint_failed
    )
}

Write-Host ""
Write-Host "============================================================"
Write-Host "OZONE RATIS LEADER FAILURE TEST"
Write-Host "Script version       : v7-simulator-process"
Write-Host "============================================================"
Write-Host "Run ID              : $RunId"
Write-Host "Target DataNode     : $TargetDataNode"
Write-Host "Simulator rate      : $TargetRate events/s"
Write-Host "Simulator duration  : $DurationSeconds s"
Write-Host "Failure starts at   : T+$FailureAfterSeconds s"
Write-Host "Failure duration    : $FailureDurationSeconds s"
Write-Host "Sample interval     : $SampleIntervalSeconds s"
Write-Host "Run directory       : $RunDir"
Write-Host ""

# ---------- PRECHECK ----------
$flinkPre = Get-FlinkState
if ($flinkPre.state -ne "RUNNING") {
    throw "Precheck failed: Bronze Flink job is not RUNNING (state=$($flinkPre.state))."
}

$kafkaPre = Get-KafkaState
$kafkaDebugPath = Join-Path $RunDir "kafka_precheck_raw.txt"
@(
    "script_version=v7-simulator-process",
    "parse_error=$($kafkaPre.parse_error)",
    "committed_total=$($kafkaPre.committed_total)",
    "log_end_total=$($kafkaPre.log_end_total)",
    "lag_total=$($kafkaPre.lag_total)",
    "",
    "----- raw kafka stdout -----",
    [string]$kafkaPre.raw_text
) | Out-File -FilePath $kafkaDebugPath -Encoding utf8

if ($null -eq $kafkaPre.lag_total) {
    throw "Precheck failed: could not parse Kafka consumer-group lag. Detail=$($kafkaPre.parse_error). Debug: $kafkaDebugPath"
}
if ([long]$kafkaPre.lag_total -ne 0) {
    throw "Precheck failed: Kafka lag must be 0, current lag=$($kafkaPre.lag_total). Debug: $kafkaDebugPath"
}

$dnContainerPre = Get-ContainerState $TargetDataNode
if ($dnContainerPre -ne "running") {
    throw "Precheck failed: $TargetDataNode container is not running (state=$dnContainerPre)."
}

$pipelineBefore = Get-PipelineText
$pipelineBefore | Out-File $PipelineBeforePath -Encoding utf8
(Get-DatanodeText) | Out-File $DnBeforePath -Encoding utf8

$leaderPipeline = Find-OpenRatisThreeLeaderPipeline $pipelineBefore $TargetDataNode
if ($null -eq $leaderPipeline) {
    throw "Precheck failed: $TargetDataNode is not leader of an OPEN RATIS/THREE pipeline. Choose the current leader from 'ozone admin pipeline list'."
}

Write-Host "Leader pipeline verified:"
Write-Host "  Pipeline ID : $($leaderPipeline.pipeline_id)"
Write-Host "  Leader UUID : $($leaderPipeline.leader_uuid)"
Write-Host ""

Write-Host "Reading Bronze baseline COUNT(*)..."
$baselineRows = Get-BronzeCount
Write-Host "Bronze baseline rows : $baselineRows"

if ($PreTestSettleSeconds -gt 0) {
    Write-Host "Settling for $PreTestSettleSeconds seconds before workload..."
    Start-Sleep -Seconds $PreTestSettleSeconds
}

try {
    # ---------- START SIMULATOR ----------
    # Run the simulator as a separate PowerShell process instead of Start-Job.
    # This behaves like the proven manual invocation and avoids PowerShell job
    # remoting/native-stderr behavior.
    $currentPowerShell = (Get-Process -Id $PID).Path
    $simulatorScript = Join-Path $RepoRoot "scripts\run-ami-simulator.ps1"

    if (-not (Test-Path $simulatorScript)) {
        throw "Simulator script not found: $simulatorScript"
    }

    $simArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $simulatorScript,
        "-Mode", "custom",
        "-TargetRate", [string]$TargetRate,
        "-Duration", [string]$DurationSeconds
    )

    $simProcess = Start-Process `
        -FilePath $currentPowerShell `
        -ArgumentList $simArgs `
        -WorkingDirectory $RepoRoot `
        -RedirectStandardOutput $SimulatorStdoutPath `
        -RedirectStandardError $SimulatorStderrPath `
        -PassThru

    $sw.Start()
    Add-TimelineRow "SIMULATOR_STARTED"

    $nextSampleAt = 0.0
    $failureAt = [double]$FailureAfterSeconds
    $restartAt = [double]($FailureAfterSeconds + $FailureDurationSeconds)
    $downSnapshotAt = [double]($FailureAfterSeconds + [math]::Min(30, [math]::Floor($FailureDurationSeconds / 2)))
    $recoverySnapshotAt = [double]($restartAt + 30)

    while ($true) {
        $elapsed = $sw.Elapsed.TotalSeconds
        $simStateNow = Get-SimulatorState

        if ($simStateNow -ne "Running" -and $elapsed -lt ($DurationSeconds - 5)) {
            Save-SimulatorCombinedLog
            Add-TimelineRow "SIMULATOR_ENDED_EARLY"
            $errorTail = Get-SimulatorErrorTail
            throw "Simulator ended before expected duration (state=$simStateNow, exitCode=$($simProcess.ExitCode)). Aborting experiment. See $SimulatorLogPath`n$errorTail"
        }

        if (-not $failureInjected -and $elapsed -ge $failureAt) {
            if ($simStateNow -ne "Running") {
                Save-SimulatorCombinedLog
                throw "Simulator is not RUNNING at failure injection time (state=$simStateNow). Refusing to stop leader. See $SimulatorLogPath"
            }

            Write-Host ""
            Write-Host ">>> Injecting leader DataNode failure at T+$([math]::Round($elapsed,1))s"
            & docker compose stop $TargetDataNode
            if ($LASTEXITCODE -ne 0) { throw "docker compose stop $TargetDataNode failed." }

            $nodeStopped = $true
            $failureInjected = $true
            Add-TimelineRow "LEADER_DN_STOPPED"
        }

        if ($nodeStopped -and -not $failureObserved -and $elapsed -ge $downSnapshotAt) {
            (Get-PipelineText) | Out-File $PipelineDownPath -Encoding utf8
            (Get-DatanodeText) | Out-File $DnDownPath -Encoding utf8
            Add-TimelineRow "DN_DOWN_OBSERVATION"
            $failureObserved = $true
        }

        if ($failureInjected -and -not $restartCompleted -and $nodeStopped -and $elapsed -ge $restartAt) {
            Write-Host ""
            Write-Host ">>> Restarting leader DataNode at T+$([math]::Round($elapsed,1))s"
            & docker compose up -d $TargetDataNode
            if ($LASTEXITCODE -ne 0) { throw "docker compose up -d $TargetDataNode failed." }

            $nodeStopped = $false
            $restartCompleted = $true
            Add-TimelineRow "LEADER_DN_RESTARTED"
        }

        if ($restartCompleted -and -not $nodeStopped -and -not $recoveryObserved -and $elapsed -ge $recoverySnapshotAt) {
            (Get-PipelineText) | Out-File $PipelineAfterPath -Encoding utf8
            (Get-DatanodeText) | Out-File $DnAfterPath -Encoding utf8
            Add-TimelineRow "POST_RESTART_OBSERVATION"
            $recoveryObserved = $true
        }

        if ($elapsed -ge $nextSampleAt) {
            Add-TimelineRow "SAMPLE"
            $nextSampleAt = $elapsed + $SampleIntervalSeconds
        }

        if ($simStateNow -in @("Completed","Failed","UNKNOWN")) { break }

        if ($elapsed -gt ($DurationSeconds + 60)) {
            Add-TimelineRow "SIMULATOR_TIMEOUT"
            throw "Simulator exceeded expected duration by >60s."
        }

        Start-Sleep -Seconds 1
    }

    if ($nodeStopped) {
        & docker compose up -d $TargetDataNode | Out-Null
        $nodeStopped = $false
        $restartCompleted = $true
        Add-TimelineRow "LEADER_DN_RESTARTED_AT_END"
    }

    if ($null -ne $simProcess -and -not $simProcess.HasExited) {
        $simProcess.WaitForExit()
    }

    Save-SimulatorCombinedLog
    Add-TimelineRow "SIMULATOR_FINISHED"

    $stdoutText = ""
    $stderrText = ""
    if (Test-Path $SimulatorStdoutPath) {
        $stdoutText = Get-Content $SimulatorStdoutPath -Raw -ErrorAction SilentlyContinue
    }
    if (Test-Path $SimulatorStderrPath) {
        $stderrText = Get-Content $SimulatorStderrPath -Raw -ErrorAction SilentlyContinue
    }
    $simText = ([string]$stdoutText) + "`n" + ([string]$stderrText)

    $attempted = $null
    $delivered = $null
    $simFailed = $null

    if ($simText -match "Final:\s*attempted=([\d,]+),\s*delivered=([\d,]+),\s*failed=([\d,]+)") {
        $attempted = [long](($Matches[1]) -replace ",","")
        $delivered = [long](($Matches[2]) -replace ",","")
        $simFailed = [long](($Matches[3]) -replace ",","")
    }

    Write-Host ""
    Write-Host "Simulator finished. Draining for $DrainSeconds seconds..."
    Start-Sleep -Seconds $DrainSeconds
    Add-TimelineRow "DRAIN_COMPLETE"

    Write-Host "Reading final Bronze state..."
    $finalRows = Get-BronzeCount
    Write-FinalBronzeCheck
    Add-TimelineRow "FINAL_CHECK"

    $rowDelta = $finalRows - $baselineRows
    $finalKafka = Get-KafkaState
    $finalFlink = Get-FlinkState
    $finalCp = Get-CheckpointState $finalFlink.jid
    $finalDnState = Get-ContainerState $TargetDataNode

    $correctDelta = $null
    if ($null -ne $delivered) { $correctDelta = ($rowDelta -eq $delivered) }

    $pass = (
        $finalFlink.state -eq "RUNNING" -and
        $finalKafka.lag_total -eq 0 -and
        $finalDnState -eq "running" -and
        ($null -eq $delivered -or $rowDelta -eq $delivered)
    )

    $summary = [pscustomobject]@{
        run_id = $RunId
        target_datanode = $TargetDataNode
        target_pipeline_id = $leaderPipeline.pipeline_id
        target_rate_eps = $TargetRate
        duration_s = $DurationSeconds
        failure_after_s = $FailureAfterSeconds
        failure_duration_s = $FailureDurationSeconds
        baseline_rows = $baselineRows
        simulator_attempted = $attempted
        simulator_delivered = $delivered
        simulator_failed = $simFailed
        final_rows = $finalRows
        row_delta = $rowDelta
        row_delta_matches_delivered = $correctDelta
        final_kafka_lag = $finalKafka.lag_total
        final_flink_state = $finalFlink.state
        final_checkpoint_completed = $finalCp.completed
        final_checkpoint_failed = $finalCp.failed
        final_datanode_container_state = $finalDnState
        pass = $pass
    }
    $summary | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $SummaryPath

    $manifest = [ordered]@{
        run_id = $RunId
        created_at = Now-Iso
        experiment = "Concurrent streaming write under RATIS/THREE leader DataNode interruption"
        methodology = [ordered]@{
            target_datanode = $TargetDataNode
            verified_open_ratis_three_leader_pipeline = $leaderPipeline.pipeline_id
            initial_leader_uuid = $leaderPipeline.leader_uuid
            target_rate_eps = $TargetRate
            simulator_duration_s = $DurationSeconds
            failure_after_s = $FailureAfterSeconds
            failure_duration_s = $FailureDurationSeconds
            sampling_interval_s = $SampleIntervalSeconds
            drain_s = $DrainSeconds
            failure_injection = "docker compose stop"
        }
        correctness = [ordered]@{
            baseline_rows = $baselineRows
            simulator_attempted = $attempted
            simulator_delivered = $delivered
            simulator_failed = $simFailed
            final_rows = $finalRows
            row_delta = $rowDelta
            row_delta_matches_delivered = $correctDelta
            final_kafka_lag = $finalKafka.lag_total
            final_flink_state = $finalFlink.state
            final_checkpoint_completed = $finalCp.completed
            final_checkpoint_failed = $finalCp.failed
            final_datanode_container_state = $finalDnState
        }
        pass = $pass
        notes = @(
            "The test verifies that the selected DataNode is leader of at least one OPEN RATIS/THREE pipeline before failure injection.",
            "A successful end-to-end result does not prove every written object used the affected pipeline; Ozone may allocate writes across other OPEN pipelines.",
            "SCM health and leader metadata can lag physical container failure because failure detection depends on heartbeats/timeouts.",
            "Use pipeline/datanode snapshots and timeline timestamps as evidence; do not infer immediate re-election solely from final state."
        )
        files = [ordered]@{
            timeline = "timeline.csv"
            simulator_log = "simulator.log"
            simulator_stdout = "simulator.stdout.log"
            simulator_stderr = "simulator.stderr.log"
            summary = "summary.csv"
            pipeline_before = "pipeline_before.txt"
            pipeline_during_failure = "pipeline_during_failure.txt"
            pipeline_after_restart = "pipeline_after_restart.txt"
            datanodes_before = "datanodes_before.txt"
            datanodes_during_failure = "datanodes_during_failure.txt"
            datanodes_after_restart = "datanodes_after_restart.txt"
            bronze_final_check = "bronze_final_check.txt"
        }
    }
    $manifest | ConvertTo-Json -Depth 8 | Out-File -FilePath $ManifestPath -Encoding utf8

    Write-Host ""
    Write-Host "============================================================"
    Write-Host "LEADER FAILURE TEST COMPLETE"
    Write-Host "============================================================"
    Write-Host "Baseline rows       : $baselineRows"
    Write-Host "Simulator delivered : $delivered"
    Write-Host "Final rows          : $finalRows"
    Write-Host "Row delta           : $rowDelta"
    Write-Host "Final Kafka lag     : $($finalKafka.lag_total)"
    Write-Host "Flink state         : $($finalFlink.state)"
    Write-Host "Checkpoint failed   : $($finalCp.failed)"
    Write-Host "Target DN state     : $finalDnState"
    Write-Host "PASS                 : $pass"
    Write-Host ""
    Write-Host "Run directory       : $RunDir"
    Write-Host "Timeline            : $TimelinePath"
    Write-Host "Summary             : $SummaryPath"
    Write-Host "Manifest            : $ManifestPath"
    Write-Host ""
}
finally {
    if ($nodeStopped) {
        Write-Warning "$TargetDataNode is still stopped. Starting it again for safety..."
        try { & docker compose up -d $TargetDataNode | Out-Null } catch {}
    }

    if ($null -ne $simProcess) {
        try {
            $simProcess.Refresh()
            if (-not $simProcess.HasExited) {
                Write-Warning "Stopping remaining simulator process..."
                Stop-Process -Id $simProcess.Id -Force -ErrorAction SilentlyContinue
            }
            Save-SimulatorCombinedLog
        }
        catch {}
    }
}
