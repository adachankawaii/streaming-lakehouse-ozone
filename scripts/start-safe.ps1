[CmdletBinding()]
param(
    [int]$ServiceTimeoutSeconds = 90,
    [int]$DataNodeTimeoutSeconds = 120,
    [int]$StabilizeSeconds = 60,
    [int]$RecentLogWindowSeconds = 60
)

$ErrorActionPreference = "Stop"
$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptRoot "..")).Path

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host $Message -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Cyan
}

function Invoke-Compose {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$ComposeArgs
    )

    & docker compose @ComposeArgs
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose $($ComposeArgs -join ' ') failed with exit code $LASTEXITCODE."
    }
}

function Invoke-ComposeProbe {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$ComposeArgs
    )

    $oldErrorActionPreference = $ErrorActionPreference

    try {
        # Windows PowerShell can surface native stderr as NativeCommandError
        # records when $ErrorActionPreference is Stop. Readiness probes expect
        # transient stderr, so capture it and let the caller decide by exit code.
        $ErrorActionPreference = "Continue"
        $output = & docker compose @ComposeArgs 2>&1 | ForEach-Object { $_.ToString() }
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $oldErrorActionPreference
    }

    [pscustomobject]@{
        ExitCode = $exitCode
        Output = ($output -join [Environment]::NewLine)
    }
}

function ConvertTo-Base64Utf8 {
    param([Parameter(Mandatory = $true)][string]$Text)

    [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Text))
}

function Wait-ContainerRunning {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Service,
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    do {
        $cid = (& docker compose ps -q $Service 2>$null | Out-String).Trim()

        if ($cid) {
            $state = (& docker inspect -f '{{.State.Status}}' $cid 2>$null | Out-String).Trim()
            if ($state -eq "running") {
                Write-Host "  [OK] $Service is running." -ForegroundColor Green
                return
            }
        }

        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)

    throw "$Service did not reach RUNNING within $TimeoutSeconds seconds."
}

function Wait-ContainerHealthy {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Service,
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    do {
        $cid = (& docker compose ps -q $Service 2>$null | Out-String).Trim()

        if ($cid) {
            $status = (& docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $cid 2>$null | Out-String).Trim()
            if ($status -eq "healthy") {
                Write-Host "  [OK] $Service is healthy." -ForegroundColor Green
                return
            }
        }

        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)

    throw "$Service did not become HEALTHY within $TimeoutSeconds seconds."
}

function Wait-SCMReady {
    param([int]$TimeoutSeconds = 90)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    do {
        $probe = Invoke-ComposeProbe -ComposeArgs @("exec", "-T", "scm", "ozone", "admin", "datanode", "list")
        if ($probe.ExitCode -eq 0) {
            Write-Host "  [OK] SCM admin API is ready." -ForegroundColor Green
            return
        }

        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)

    throw "SCM admin API did not become ready within $TimeoutSeconds seconds."
}

function Wait-DataNodeHealthy {
    param(
        [Parameter(Mandatory = $true)]
        [string]$DataNode,
        [int]$TimeoutSeconds = 120
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    do {
        $probe = Invoke-ComposeProbe -ComposeArgs @("exec", "-T", "scm", "ozone", "admin", "datanode", "list")
        $text = $probe.Output

        if ($probe.ExitCode -eq 0 -and $text) {
            $blocks = $text -split "(?:\r?\n){2,}"

            foreach ($block in $blocks) {
                if (
                    $block -match [regex]::Escape("/$DataNode/") -and
                    $block -match "Operational State:\s*IN_SERVICE" -and
                    $block -match "Health State:\s*HEALTHY"
                ) {
                    Write-Host "  [OK] $DataNode is IN_SERVICE / HEALTHY." -ForegroundColor Green
                    return
                }
            }
        }

        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)

    throw "$DataNode did not become IN_SERVICE / HEALTHY within $TimeoutSeconds seconds."
}

function Wait-OMReady {
    param([int]$TimeoutSeconds = 90)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    do {
        $probe = Invoke-ComposeProbe -ComposeArgs @("exec", "-T", "om", "ozone", "sh", "volume", "list")
        if ($probe.ExitCode -eq 0) {
            Write-Host "  [OK] OM is ready." -ForegroundColor Green
            return
        }

        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)

    throw "OM did not become ready within $TimeoutSeconds seconds."
}

function Wait-S3GReady {
    param([int]$TimeoutSeconds = 120)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    do {
        $cid = (& docker compose ps -q s3g 2>$null | Out-String).Trim()

        if ($cid) {
            $status = (& docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $cid 2>$null | Out-String).Trim()

            if ($status -eq "healthy" -or $status -eq "running") {
                $probe = Invoke-ComposeProbe -ComposeArgs @("exec", "-T", "s3g", "ozone", "sh", "bucket", "info", "/s3v/warehouse")
                if ($probe.ExitCode -eq 0) {
                    Write-Host "  [OK] S3 Gateway and /s3v/warehouse are ready." -ForegroundColor Green
                    return
                }
            }
        }

        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)

    throw "S3 Gateway did not become ready within $TimeoutSeconds seconds."
}

function Ensure-OzoneBucket {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Bucket
    )

    $bucketPath = "/s3v/$Bucket"
    $probe = Invoke-ComposeProbe -ComposeArgs @("exec", "-T", "s3g", "ozone", "sh", "bucket", "info", $bucketPath)

    if ($probe.ExitCode -ne 0) {
        Invoke-Compose -ComposeArgs @("exec", "-T", "s3g", "ozone", "sh", "bucket", "create", "-t", "RATIS", "-r", "THREE", $bucketPath)
    }

    Invoke-Compose -ComposeArgs @("exec", "-T", "s3g", "ozone", "sh", "bucket", "set-replication-config", "-t", "RATIS", "-r", "THREE", $bucketPath)
    Write-Host "  [OK] Ozone bucket $bucketPath is ready." -ForegroundColor Green
}

function Wait-LakekeeperReady {
    param([int]$TimeoutSeconds = 90)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    do {
        $probe = Invoke-ComposeProbe -ComposeArgs @("exec", "-T", "flink-jobmanager", "curl", "-sS", "http://lakekeeper:8181/management/v1/info")
        if ($probe.ExitCode -eq 0 -and $probe.Output -match '"lakekeeper-version"') {
            Write-Host "  [OK] Lakekeeper API is ready." -ForegroundColor Green
            return
        }

        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)

    throw "Lakekeeper API did not become ready within $TimeoutSeconds seconds."
}

function Invoke-LakekeeperJsonPost {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Json,
        [Parameter(Mandatory = $true)]
        [int[]]$ExpectedStatusCodes
    )

    $jsonBase64 = ConvertTo-Base64Utf8 -Text $Json
    $cmd = "printf %s $jsonBase64 | base64 -d | curl -sS -o /tmp/lakekeeper-response.json -w '%{http_code}' -X POST http://lakekeeper:8181$Path -H 'Content-Type: application/json' --data-binary @-; printf '\n'; cat /tmp/lakekeeper-response.json"
    $probe = Invoke-ComposeProbe -ComposeArgs @("exec", "-T", "flink-jobmanager", "bash", "-lc", $cmd)

    if ($probe.ExitCode -ne 0) {
        throw "Lakekeeper POST $Path failed: $($probe.Output)"
    }

    $lines = @($probe.Output -split "\r?\n")
    $statusCode = [int]$lines[0]
    $body = ($lines | Select-Object -Skip 1) -join [Environment]::NewLine

    if ($statusCode -notin $ExpectedStatusCodes) {
        throw "Lakekeeper POST $Path returned HTTP $statusCode. Body: $body"
    }

    [pscustomobject]@{
        StatusCode = $statusCode
        Body = $body
    }
}

function Ensure-LakekeeperBootstrapped {
    $infoProbe = Invoke-ComposeProbe -ComposeArgs @("exec", "-T", "flink-jobmanager", "curl", "-sS", "http://lakekeeper:8181/management/v1/info")
    if ($infoProbe.ExitCode -ne 0) {
        throw "Could not query Lakekeeper info: $($infoProbe.Output)"
    }

    $info = $infoProbe.Output | ConvertFrom-Json
    if ($info.bootstrapped) {
        Write-Host "  [OK] Lakekeeper is bootstrapped." -ForegroundColor Green
        return
    }

    $bootstrapJson = @{
        "accept-terms-of-use" = $true
        "user-name" = "admin"
        "user-type" = "human"
    } | ConvertTo-Json -Compress

    $null = Invoke-LakekeeperJsonPost -Path "/management/v1/bootstrap" -Json $bootstrapJson -ExpectedStatusCodes @(204)
    Write-Host "  [OK] Lakekeeper bootstrap completed." -ForegroundColor Green
}

function Ensure-LakekeeperWarehouse {
    $warehouseName = "lakehouse"
    $configProbe = Invoke-ComposeProbe -ComposeArgs @("exec", "-T", "flink-jobmanager", "curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}", "http://lakekeeper:8181/catalog/v1/config?warehouse=$warehouseName")

    if ($configProbe.ExitCode -ne 0) {
        throw "Could not query Lakekeeper warehouse '$warehouseName': $($configProbe.Output)"
    }

    $statusCode = [int]$configProbe.Output.Trim()

    if ($statusCode -eq 200) {
        Write-Host "  [OK] Lakekeeper warehouse '$warehouseName' exists." -ForegroundColor Green
        return
    }

    if ($statusCode -ne 404) {
        throw "Lakekeeper warehouse '$warehouseName' check returned HTTP $statusCode."
    }

    $warehouseConfigPath = Join-Path $RepoRoot "platform\iceberg\config\create-warehouse.json"
    if (-not (Test-Path -LiteralPath $warehouseConfigPath)) {
        throw "Warehouse config was not found: $warehouseConfigPath"
    }

    $warehouseJson = Get-Content -Raw -LiteralPath $warehouseConfigPath
    $null = Invoke-LakekeeperJsonPost -Path "/management/v1/warehouse" -Json $warehouseJson -ExpectedStatusCodes @(201)
    Write-Host "  [OK] Lakekeeper warehouse '$warehouseName' created." -ForegroundColor Green
}

function Ensure-KafkaTopic {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Topic,
        [int]$Partitions = 3
    )

    $probe = Invoke-ComposeProbe -ComposeArgs @(
        "exec", "-T", "kafka",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server", "kafka:19092",
        "--create",
        "--if-not-exists",
        "--topic", $Topic,
        "--partitions", "$Partitions",
        "--replication-factor", "1"
    )

    if ($probe.ExitCode -ne 0) {
        throw "Could not create Kafka topic '$Topic': $($probe.Output)"
    }

    Write-Host "  [OK] Kafka topic '$Topic' is ready." -ForegroundColor Green
}

function Ensure-FlinkDatabase {
    param([Parameter(Mandatory = $true)][string]$Database)

    $sqlBase64 = ConvertTo-Base64Utf8 -Text "CREATE DATABASE IF NOT EXISTS $Database;`n"
    $cmd = "printf %s $sqlBase64 | base64 -d > /tmp/create-$Database.sql && /opt/flink/bin/sql-client.sh -i /opt/flink/conf/init.sql -f /tmp/create-$Database.sql"
    $probe = Invoke-ComposeProbe -ComposeArgs @("exec", "-T", "flink-jobmanager", "bash", "-lc", $cmd)

    if ($probe.ExitCode -ne 0) {
        throw "Could not create Flink/Iceberg database '$Database': $($probe.Output)"
    }

    Write-Host "  [OK] Flink/Iceberg database '$Database' is ready." -ForegroundColor Green
}

function Wait-TrinoReady {
    param([int]$TimeoutSeconds = 120)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    do {
        try {
            $info = Invoke-RestMethod `
                -Uri "http://localhost:8080/v1/info" `
                -TimeoutSec 5

            if ($info.nodeVersion.version) {
                Write-Host "  [OK] Trino $($info.nodeVersion.version) is ready." -ForegroundColor Green
                return
            }
        }
        catch {
            # Trino may still be starting; retry until timeout.
        }

        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)

    throw "Trino did not become ready within $TimeoutSeconds seconds."
}

function Test-RatisTcp {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FromService,
        [Parameter(Mandatory = $true)]
        [string]$ToHost
    )

    $cmd = "echo > /dev/tcp/$ToHost/9856 && echo CONNECT_OK || echo CONNECT_FAIL"
    $output = (& docker compose exec -T $FromService bash -lc $cmd 2>&1 | Out-String).Trim()

    if ($output -notmatch "CONNECT_OK") {
        throw "Ratis TCP test failed: $FromService -> ${ToHost}:9856. Output: $output"
    }

    Write-Host "  [OK] Ratis TCP: $FromService -> ${ToHost}:9856" -ForegroundColor Green
}

function Assert-OpenRatisThreePipeline {
    $text = (& docker compose exec -T scm ozone admin pipeline list 2>$null | Out-String)

    if ($LASTEXITCODE -ne 0) {
        throw "Could not query Ozone pipelines."
    }

    $openLines = @($text -split "\r?\n" | Where-Object {
        $_ -match "ReplicationConfig:\s*RATIS/THREE" -and
        $_ -match "State:OPEN" -and
        $_ -match "leaderId:[^,}\s]+"
    })

    if ($openLines.Count -lt 1) {
        Write-Host $text
        throw "No OPEN RATIS/THREE pipeline with a leader was found."
    }

    Write-Host "  [OK] Found $($openLines.Count) OPEN RATIS/THREE pipeline(s) with leaders." -ForegroundColor Green
}

function Assert-NoRecentRatisConnectivityErrors {
    param([int]$WindowSeconds = 60)

    $services = @(
        "scm",
        "datanode1",
        "datanode2",
        "datanode3",
        "datanode4",
        "datanode5"
    )

    $logs = (& docker compose logs --since "${WindowSeconds}s" @services 2>&1 | Out-String)

    $matches = @($logs -split "\r?\n" | Where-Object {
        $_ -match "Connection refused" -or
        $_ -match "UNAVAILABLE:\s*io exception" -or
        $_ -match "group-.*not found" -or
        $_ -match "no leader" -or
        $_ -match "NotLeaderException"
    })

    if ($matches.Count -gt 0) {
        Write-Host ""
        Write-Host "Recent Ratis/network errors detected:" -ForegroundColor Red
        $matches | Select-Object -First 40 | ForEach-Object {
            Write-Host "  $_" -ForegroundColor Red
        }

        throw "Ratis connectivity is not stable. Do not submit Flink jobs or run benchmarks yet."
    }

    Write-Host "  [OK] No recent Ratis connectivity errors in the last $WindowSeconds seconds." -ForegroundColor Green
}

function Show-FlinkJobStatus {
    try {
        $overview = Invoke-RestMethod -Uri "http://localhost:8081/jobs/overview" -TimeoutSec 5

        if (-not $overview.jobs -or $overview.jobs.Count -eq 0) {
            Write-Host "  [WARN] Flink cluster is up, but no jobs are currently submitted." -ForegroundColor Yellow
            Write-Host "         Resubmit the Bronze ingestion job manually after this script finishes." -ForegroundColor Yellow
            return
        }

        Write-Host "  Flink jobs:" -ForegroundColor Cyan
        $overview.jobs |
            Select-Object jid, name, state |
            Format-Table -AutoSize
    }
    catch {
        Write-Host "  [WARN] Flink REST API is not ready yet; check http://localhost:8081 manually." -ForegroundColor Yellow
    }
}

Write-Step "0/10 - Render private lab configs and validate Compose"
& python (Join-Path $RepoRoot "scripts\render-local-config.py")
if ($LASTEXITCODE -ne 0) { throw "Local S3 config rendering failed; check your .env" }
# Public repository stores only templates, not secrets.
Write-Host "  [OK] Private catalog files rendered locally." -ForegroundColor Green

Invoke-Compose -ComposeArgs @("config", "--quiet")
Write-Host "  [OK] docker compose config is valid." -ForegroundColor Green

$allServices = @(& docker compose config --services | Where-Object { $_ -and $_.Trim() })
$required = @(
    "scm",
    "om",
    "datanode1",
    "datanode2",
    "datanode3",
    "datanode4",
    "datanode5",
    "recon",
    "s3g",
    "kafka",
    "lakekeeper",
    "flink-jobmanager",
    "flink-taskmanager",
    "trino"
)

$missing = @($required | Where-Object { $_ -notin $allServices })
if ($missing.Count -gt 0) {
    throw "Required services are missing from Compose: $($missing -join ', ')"
}

Write-Step "1/10 - Apply/start SCM first"
Invoke-Compose -ComposeArgs @("up", "-d", "--no-deps", "scm")
Wait-ContainerRunning -Service "scm" -TimeoutSeconds $ServiceTimeoutSeconds
Wait-SCMReady -TimeoutSeconds $ServiceTimeoutSeconds

Write-Step "2/10 - Rolling apply/start DataNodes"
foreach ($dn in 1..5 | ForEach-Object { "datanode$_" }) {
    Write-Host ""
    Write-Host ">>> Applying/starting $dn ..." -ForegroundColor Yellow

    # No --force-recreate: Compose recreates only if this service's effective
    # configuration/image changed; otherwise it starts the existing container.
    Invoke-Compose -ComposeArgs @("up", "-d", "--no-deps", $dn)

    Wait-ContainerRunning -Service $dn -TimeoutSeconds $ServiceTimeoutSeconds
    Wait-DataNodeHealthy -DataNode $dn -TimeoutSeconds $DataNodeTimeoutSeconds

    $hostnameMode = (& docker compose exec -T $dn ozone getconf -confKey hdds.datanode.use.datanode.hostname 2>$null | Out-String).Trim()
    $advertisedHostname = (& docker compose exec -T $dn ozone getconf -confKey hdds.datanode.hostname 2>$null | Out-String).Trim()

    if ($hostnameMode -ne "true") {
        throw "$dn does not have hdds.datanode.use.datanode.hostname=true."
    }

    if ($advertisedHostname -ne $dn) {
        throw "$dn advertises '$advertisedHostname' instead of '$dn'."
    }

    Write-Host "  [OK] $dn hostname mode is correct." -ForegroundColor Green
    Start-Sleep -Seconds 5
}

Write-Step "3/10 - Apply/start OM"
Invoke-Compose -ComposeArgs @("up", "-d", "--no-deps", "om")
Wait-ContainerRunning -Service "om" -TimeoutSeconds $ServiceTimeoutSeconds
Wait-OMReady -TimeoutSeconds $ServiceTimeoutSeconds

Write-Step "4/10 - Apply/start Recon and S3 Gateway"
Invoke-Compose -ComposeArgs @("up", "-d", "--no-deps", "recon")
Wait-ContainerRunning -Service "recon" -TimeoutSeconds $ServiceTimeoutSeconds

Invoke-Compose -ComposeArgs @("up", "-d", "--no-deps", "s3g")
Wait-ContainerRunning -Service "s3g" -TimeoutSeconds $ServiceTimeoutSeconds
Wait-S3GReady -TimeoutSeconds $ServiceTimeoutSeconds

$s3gHostnameMode = (& docker compose exec -T s3g ozone getconf -confKey hdds.datanode.use.datanode.hostname 2>$null | Out-String).Trim()
if ($s3gHostnameMode -ne "true") {
    throw "s3g does not have hdds.datanode.use.datanode.hostname=true."
}
Write-Host "  [OK] S3G uses DataNode hostname mode." -ForegroundColor Green
Ensure-OzoneBucket -Bucket "flink-state"

Write-Step "5/10 - Start/reconcile the rest of the stack"
# All Ozone stateful services have already been reconciled in a safe order.
# Normal `up -d` now starts/reconciles Kafka, Lakekeeper, Flink, Trino, etc.
# Profile-only services such as the simulator stay off unless their profile is enabled.
Invoke-Compose -ComposeArgs @("up", "-d")

Write-Step "6/10 - Bootstrap lakehouse runtime resources"
Wait-ContainerHealthy -Service "kafka" -TimeoutSeconds $ServiceTimeoutSeconds
Wait-ContainerHealthy -Service "lakekeeper" -TimeoutSeconds $ServiceTimeoutSeconds
Wait-ContainerRunning -Service "flink-jobmanager" -TimeoutSeconds $ServiceTimeoutSeconds
Wait-ContainerRunning -Service "flink-taskmanager" -TimeoutSeconds $ServiceTimeoutSeconds
Wait-LakekeeperReady -TimeoutSeconds $ServiceTimeoutSeconds
Ensure-LakekeeperBootstrapped
Ensure-LakekeeperWarehouse
Ensure-KafkaTopic -Topic "ami.meter.events" -Partitions 3
Ensure-FlinkDatabase -Database "bronze"

Wait-ContainerRunning -Service "trino" -TimeoutSeconds $ServiceTimeoutSeconds
Wait-TrinoReady -TimeoutSeconds $ServiceTimeoutSeconds

Write-Step "7/10 - Validate DataNode connectivity"
Test-RatisTcp -FromService "datanode2" -ToHost "datanode1"
Test-RatisTcp -FromService "datanode2" -ToHost "datanode5"

Write-Step "8/10 - Wait for Ratis to stabilize"
Write-Host "  Waiting $StabilizeSeconds seconds without changing topology..."
Start-Sleep -Seconds $StabilizeSeconds

Assert-OpenRatisThreePipeline
Assert-NoRecentRatisConnectivityErrors -WindowSeconds $RecentLogWindowSeconds

Write-Step "9/10 - Show Ozone health summaries"
Write-Host ""
Write-Host "--- DataNodes ---" -ForegroundColor Cyan
& docker compose exec -T scm ozone admin datanode list

Write-Host ""
Write-Host "--- Pipelines ---" -ForegroundColor Cyan
& docker compose exec -T scm ozone admin pipeline list

Write-Host ""
Write-Host "--- Container report ---" -ForegroundColor Cyan
& docker compose exec -T scm ozone admin container report

Write-Step "10/10 - Final stack status"
& docker compose ps

Show-FlinkJobStatus

Write-Host ""
Write-Host "SAFE START COMPLETED." -ForegroundColor Green
Write-Host "Ozone networking/Ratis checks passed." -ForegroundColor Green
Write-Host "Lakehouse runtime checks passed, including Trino readiness." -ForegroundColor Green
Write-Host ""
Write-Host "Important:" -ForegroundColor Yellow
Write-Host "  - This script does NOT delete volumes or old Ozone containers." -ForegroundColor Yellow
Write-Host "  - This script does NOT start profile-only simulators." -ForegroundColor Yellow
Write-Host "  - This script does NOT automatically resubmit the Flink Bronze SQL job." -ForegroundColor Yellow
Write-Host "  - Trino is started as part of the normal Compose stack and checked via /v1/info." -ForegroundColor Yellow
Write-Host "  - Lakekeeper warehouse, Bronze namespace, checkpoint bucket, and AMI Kafka topic are bootstrapped." -ForegroundColor Yellow
Write-Host "  - Historical QUASI_CLOSED_STUCK containers are reported but not deleted." -ForegroundColor Yellow
