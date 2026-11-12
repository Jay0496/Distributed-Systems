#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Run the end-to-end demo: RS write/read, replication comparison, two-node
    failure, automatic repair, and delete.
.PARAMETER Mode
    Mode used for the main storage comparison (rs32 or replication).
.PARAMETER SkipFailure
    Skip the node-stop/repair section.
.PARAMETER Keep
    Leave a cluster that this script started running afterwards.
.PARAMETER ValueSize
    Number of bytes in the deterministic demo value (default 4096).
.PARAMETER RepairTimeout
    Seconds to wait for degraded state to clear after nodes restart (default 120).
.PARAMETER Help
    Show this help.
#>
param(
    [ValidateSet('rs32', 'replication')][string]$Mode = 'rs32',
    [switch]$SkipFailure,
    [switch]$Keep,
    [int]$ValueSize = 4096,
    [int]$RepairTimeout = 120,
    [int]$Timeout = 120,
    [switch]$Help
)

. "$PSScriptRoot/common.ps1"

if ($Help) {
    Show-ScriptHelp @'
Usage: scripts/demo.ps1 [-Mode rs32|replication] [-SkipFailure] [-Keep] [-ValueSize BYTES] [-RepairTimeout SECONDS] [-Timeout SECONDS] [-Help]

Runs a complete demonstration without host Go:
  1. start the cluster if needed and wait for health
  2. write and read a deterministic value in RS mode
  3. show per-node fragment counts and bytes with sqlite3
  4. write the same key in replication mode and compare storage amplification
  5. stop node4 and node5, prove reads/writes still work, show degraded status
  6. restart the nodes and wait for repair instead of grepping logs
  7. delete the key and confirm it is gone
  8. print a summary table and the auth state

Options:
  -Mode rs32|replication   Mode for the storage comparison (default rs32)
  -SkipFailure             Skip the two-node failure and repair section
  -Keep                    Leave a cluster started by this script running
  -ValueSize BYTES         Demo value size (default 4096)
  -RepairTimeout SECONDS   Repair wait after restart (default 120)
  -Timeout SECONDS         Initial health wait (default 120)
  -Help                    Show this help

Examples:
  ./scripts/demo.ps1
  ./scripts/demo.ps1 -Mode replication -SkipFailure

Exit codes:
  0  every demo check passed
  1  the cluster was unhealthy or a demo step failed
'@
    exit 0
}

Assert-DockerCompose

$key = 'demo-key'
$failureKey = 'demo-key-failure'
$degradedKey = 'demo-key-degraded'
$results = [System.Collections.Generic.List[object]]::new()
$failed = $false
$startedCluster = $false
$tempValueFile = $null

function Add-Check {
    param([string]$Step, [string]$Detail)
    $results.Add([pscustomobject]@{ Step = $Step; Result = 'PASS'; Detail = $Detail }) | Out-Null
    Write-Log ("PASS: {0} - {1}" -f $Step, $Detail)
}

function New-DemoValue {
    param([int]$Size)
    if ($Size -le 0) {
        throw '-ValueSize must be greater than zero'
    }
    $pattern = 'distributed-systems-demo-'
    $builder = [System.Text.StringBuilder]::new($Size + $pattern.Length)
    while ($builder.Length -lt $Size) {
        [void]$builder.Append($pattern)
    }
    return $builder.ToString(0, $Size)
}

function Get-FragmentStats {
    param([Parameter(Mandatory = $true)][string]$StripeID)
    $rows = @()
    foreach ($node in $script:NodeServices) {
        $sql = "SELECT COUNT(*), COALESCE(SUM(length(data)),0) FROM fragments WHERE stripe_id='$StripeID';"
        $raw = & docker compose exec -T $node sqlite3 /data/node.db $sql 2>$null
        if ($LASTEXITCODE -ne 0) {
            throw "sqlite3 query failed on $node"
        }
        $parts = (($raw | Out-String).Trim()).Split('|')
        $rows += [pscustomobject]@{
            Node      = $node
            Fragments = [int]$parts[0]
            Bytes     = [long]$parts[1]
        }
    }
    return $rows
}

function Show-FragmentTable {
    param($Rows)
    $Rows | Format-Table Node, Fragments, @{ Label = 'Bytes'; Expression = { Format-Bytes $_.Bytes } } -AutoSize |
        Out-String -Width 200 | Write-Host
}

$tempValueFile = Join-Path ([System.IO.Path]::GetTempPath()) ("kv-demo-{0}.txt" -f [guid]::NewGuid().ToString('N'))
$expectedValue = New-DemoValue -Size $ValueSize
[System.IO.File]::WriteAllText($tempValueFile, $expectedValue, [System.Text.Encoding]::ASCII)
$valueVolume = "$($tempValueFile):/demo/value:ro"

# With two nodes stopped, the leader may wait for the dead owners' read/upload
# timeouts before it can answer a forwarded call. Five retries cover every node
# in the ring so the demo reliably reaches the current leader; this is a demo
# setting, not a change to the gateway's production default.
$previousGatewayRetries = $env:GATEWAY_RETRIES
$previousGatewayTimeout = $env:GATEWAY_TIMEOUT
$env:GATEWAY_RETRIES = '5'
$env:GATEWAY_TIMEOUT = '45s'

try {
    if ((Get-HealthyNodeCount) -lt 5) {
        Write-Log 'cluster is not fully healthy; starting it'
        Invoke-Compose -Arguments @('up', '--build', '-d') | Out-Null
        $startedCluster = $true
    }
    Wait-NodeHealth -Timeout $Timeout
    Wait-ClusterHealthy -Timeout $Timeout | Out-Null
    Add-Check 'cluster health' 'five nodes healthy and not degraded'
    Write-Log (Get-AuthStatusText)

    # ---- storage comparison -------------------------------------------------
    Invoke-Gateway -Arguments @('mode', 'rs32') | Out-Null
    $putRaw = Invoke-GatewayCapture -Arguments @(
        'put', $key, '--value-file', '/demo/value', '--mode', 'rs32', '--json'
    ) -Volumes @($valueVolume)
    $put = $putRaw | ConvertFrom-Json
    if (-not $put.success) { throw "RS put was rejected: $($put.message)" }
    if ($put.mode -ne 'rs32') { throw "expected rs32, got $($put.mode)" }
    Add-Check 'rs32 write' ("version={0} shards={1}/5 stripe={2}" -f $put.version, $put.shards_stored, $put.stripe_id)

    $getRaw = Invoke-GatewayCapture -Arguments @('get', $key, '--json')
    $get = $getRaw | ConvertFrom-Json
    if ($get.value -ne $expectedValue) { throw 'RS read-back value did not match the write' }
    Add-Check 'rs32 read' ("verified {0} bytes with {1} shards" -f $ValueSize, $get.shards_used)

    $rsRows = @(Get-FragmentStats -StripeID $put.stripe_id)
    Write-Host ''
    Write-Host 'Per-node fragments for the rs32 stripe:'
    Show-FragmentTable -Rows $rsRows
    $rsTotal = ($rsRows | Measure-Object -Property Bytes -Sum).Sum
    $rsFragments = ($rsRows | Measure-Object -Property Fragments -Sum).Sum
    if ($rsFragments -ne 5) { throw "expected five RS shards, found $rsFragments" }
    Add-Check 'rs32 placement' ("{0} shards, {1} total" -f $rsFragments, (Format-Bytes $rsTotal))

    Invoke-Gateway -Arguments @('mode', 'replication') | Out-Null
    $repRaw = Invoke-GatewayCapture -Arguments @(
        'put', $key, '--value-file', '/demo/value', '--mode', 'replication', '--json'
    ) -Volumes @($valueVolume)
    $rep = $repRaw | ConvertFrom-Json
    if (-not $rep.success) { throw "replication put was rejected: $($rep.message)" }
    if ($rep.mode -ne 'replication') { throw "expected replication, got $($rep.mode)" }
    Add-Check 'replication write' ("version={0} copies={1}/5 stripe={2}" -f $rep.version, $rep.shards_stored, $rep.stripe_id)

    $repRows = @(Get-FragmentStats -StripeID $rep.stripe_id)
    Write-Host 'Per-node fragments for the replication stripe:'
    Show-FragmentTable -Rows $repRows
    $repTotal = ($repRows | Measure-Object -Property Bytes -Sum).Sum
    $repFragments = ($repRows | Measure-Object -Property Fragments -Sum).Sum
    if ($repFragments -ne 5) { throw "expected five replication copies, found $repFragments" }
    Add-Check 'replication placement' ("{0} copies, {1} total" -f $repFragments, (Format-Bytes $repTotal))

    $rsMultiplier = [math]::Round($rsTotal / $ValueSize, 2)
    $repMultiplier = [math]::Round($repTotal / $ValueSize, 2)
    $rsOverhead = [math]::Round(($rsMultiplier - 1) * 100, 0)
    $repOverhead = [math]::Round(($repMultiplier - 1) * 100, 0)
    $comparison = @(
        ('rs32:        {0} stored for {1} original ({2}x, about {3}% extra)' -f (Format-Bytes $rsTotal), (Format-Bytes $ValueSize), $rsMultiplier, $rsOverhead),
        ('replication: {0} stored for {1} original ({2}x, about {3}% extra)' -f (Format-Bytes $repTotal), (Format-Bytes $ValueSize), $repMultiplier, $repOverhead),
        ('replication / rs32 total ratio: {0}x' -f [math]::Round($repTotal / $rsTotal, 2))
    )
    Write-Host ''
    $comparison | ForEach-Object { Write-Host $_ }
    Add-Check 'storage comparison' 'rs32 stores about 1.67x, replication stores 5x'

    # ---- failure and repair -------------------------------------------------
    if (-not $SkipFailure) {
        Invoke-Gateway -Arguments @('mode', 'rs32') | Out-Null
        $failureRaw = Invoke-GatewayCapture -Arguments @(
            'put', $failureKey, '--value-file', '/demo/value', '--mode', 'rs32', '--json'
        ) -Volumes @($valueVolume)
        $failurePut = $failureRaw | ConvertFrom-Json
        if (-not $failurePut.success) { throw "failure-test put was rejected: $($failurePut.message)" }
        Add-Check 'failure baseline' ("wrote {0} with five shards" -f $failureKey)

        $nodesStopped = $false
        try {
            Invoke-Compose -Arguments @('stop', 'node4', 'node5') | Out-Null
            $nodesStopped = $true
            Write-Log 'waiting for the detector to report degraded mode'

            $deadline = (Get-Date).AddSeconds(45)
            $degraded = $false
            do {
                try {
                    $status = Get-ClusterStatus
                    if ($status.degraded) { $degraded = $true; break }
                }
                catch {
                    Write-Log 'status probe waiting for a leader'
                }
                Start-Sleep -Seconds 2
            } while ((Get-Date) -lt $deadline)

            if (-not $degraded) { throw 'cluster did not report degraded mode after stopping node4 and node5' }
            Add-Check 'degraded status' ("healthy={0}/{1}, degraded_stripes={2}" -f $status.healthy_nodes, $status.total_nodes, $status.degraded_stripes)

            $downGetRaw = Invoke-GatewayCapture -Arguments @('get', $failureKey, '--json')
            $downGet = $downGetRaw | ConvertFrom-Json
            if ($downGet.value -ne $expectedValue) { throw 'read with two nodes down did not match' }
            Add-Check 'read with two nodes down' ("reconstructed from {0} shards" -f $downGet.shards_used)

            $degradedRaw = Invoke-GatewayCapture -Arguments @(
                'put', $degradedKey, '--value-file', '/demo/value', '--mode', 'rs32', '--json'
            ) -Volumes @($valueVolume)
            $degradedPut = $degradedRaw | ConvertFrom-Json
            if (-not $degradedPut.success) { throw "degraded write failed: $($degradedPut.message)" }
            Add-Check 'write with two nodes down' ("committed {0}/5 shards, degraded={1}" -f $degradedPut.shards_stored, $degradedPut.degraded)
        }
        finally {
            if ($nodesStopped) {
                Invoke-Compose -Arguments @('start', 'node4', 'node5') | Out-Null
            }
        }

        Write-Log 'waiting for automatic repair (not a log-line grep)'
        Wait-ClusterHealthy -Timeout $RepairTimeout | Out-Null
        Add-Check 'repair' 'degraded mode cleared automatically'

        $restoredRows = @(Get-FragmentStats -StripeID $failurePut.stripe_id)
        Write-Host 'Per-node fragments after repair:'
        Show-FragmentTable -Rows $restoredRows
        $restoredFragments = ($restoredRows | Measure-Object -Property Fragments -Sum).Sum
        if ($restoredFragments -ne 5) { throw "expected five restored shards, found $restoredFragments" }
        Add-Check 'fragment refill' ("{0} shards present after restart" -f $restoredFragments)
    }
    else {
        Write-Log 'skipping the failure and repair section (-SkipFailure)'
    }

    # ---- delete -------------------------------------------------------------
    Invoke-Gateway -Arguments @('delete', $key) | Out-Null
    $deleted = $false
    try {
        Invoke-GatewayCapture -Arguments @('get', $key) -SuppressErrors | Out-Null
    }
    catch {
        $deleted = $true
    }
    if (-not $deleted) { throw 'get still returned a value after delete' }
    Add-Check 'delete' 'key is gone after delete'

    if (-not $SkipFailure) {
        Invoke-Gateway -Arguments @('delete', $failureKey) | Out-Null
        Invoke-Gateway -Arguments @('delete', $degradedKey) | Out-Null
    }
}
catch {
    $failed = $true
    Write-Error $_.Exception.Message
}
finally {
    if ($null -eq $previousGatewayRetries) {
        Remove-Item Env:GATEWAY_RETRIES -ErrorAction SilentlyContinue
    }
    else {
        $env:GATEWAY_RETRIES = $previousGatewayRetries
    }
    if ($null -eq $previousGatewayTimeout) {
        Remove-Item Env:GATEWAY_TIMEOUT -ErrorAction SilentlyContinue
    }
    else {
        $env:GATEWAY_TIMEOUT = $previousGatewayTimeout
    }

    if ($tempValueFile -and (Test-Path -LiteralPath $tempValueFile)) {
        Remove-Item -LiteralPath $tempValueFile -Force -ErrorAction SilentlyContinue
    }

    Write-Section 'Demo summary'
    $results | Format-Table Step, Result, Detail -AutoSize | Out-String -Width 220 | Write-Host
    Write-Host (Get-AuthStatusText)
    Write-Host ("value size: {0}; storage mode tested: {1}; failure section: {2}" -f `
            (Format-Bytes $ValueSize), $Mode, $(if ($SkipFailure) { 'skipped' } else { 'included' }))

    if ($startedCluster -and -not $Keep) {
        Write-Log 'stopping the cluster started by this script (use -Keep to leave it running)'
        & docker compose down --remove-orphans
    }
    elseif ($startedCluster -and $Keep) {
        Write-Log 'leaving the cluster running (-Keep)'
    }
}

if ($failed) {
    exit 1
}
exit 0
