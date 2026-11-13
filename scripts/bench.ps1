#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Run the benchmark harness: load, storage ratios, recovery timing, and CPU
    sampling inside Docker.
.PARAMETER Mode
    rs32, replication, or both.
.PARAMETER Runs
    Repetitions per scenario (default 3).
.PARAMETER Duration
    Measured duration passed to bench run (default 60s).
.PARAMETER Concurrency
    Load workers (default 32).
.PARAMETER Values
    Keys written by storage and recovery-seed scenarios.
.PARAMETER SkipLoad
    Skip the put/get load scenarios.
.PARAMETER SkipStorage
    Skip the destructive storage measurements.
.PARAMETER SkipRecovery
    Skip the stop/restart recovery measurements.
#>
param(
    [ValidateSet('rs32', 'replication', 'both')][string]$Mode = 'both',
    [int]$Runs = 3,
    [string]$Duration = '60s',
    [int]$Concurrency = 32,
    [int]$Keys = 10000,
    [int]$ValueSize = 4096,
    [double]$ReadRatio = 0.5,
    [string]$Warmup = '10s',
    [int]$Values = 1000,
    [string]$Output = 'bench-results',
    [int]$RepairTimeout = 300,
    [switch]$SkipLoad,
    [switch]$SkipStorage,
    [switch]$SkipRecovery,
    [switch]$Help
)

. "$PSScriptRoot/common.ps1"

if ($Help) {
    Show-ScriptHelp @'
Usage: scripts/bench.ps1 [-Mode rs32|replication|both] [-Runs 3] [-Duration 60s]
                         [-Concurrency 32] [-Keys 10000] [-ValueSize 4096]
                         [-ReadRatio 0.5] [-Warmup 10s] [-Values 1000]
                         [-Output bench-results] [-RepairTimeout 300]
                         [-SkipLoad] [-SkipStorage] [-SkipRecovery] [-Help]

Runs entirely in Docker; no host Go is required. Results are written under
bench-results/<run-id>/ as meta.json, run.json, latency.csv, throughput.csv,
storage.csv, recovery.csv, cpu.csv, and summary.md.

Scenarios:
  load      bench run in each mode: p50/p95/p99, throughput, error rate
  storage   bench storage in each mode: logical, fragment, disk bytes and ratios
  recovery  stop one node, then two nodes; measure degraded and repair time

Storage measurement is destructive: it resets the five data volumes before each
mode so the measurement starts clean. Recovery seeds keys before stopping nodes.

Options:
  -Mode rs32|replication|both   Modes to measure (default both)
  -Runs N                        Repetitions per scenario (default 3)
  -Duration 60s                  Measured load duration
  -Concurrency 32                Load workers
  -Keys 10000                    Load key space
  -ValueSize 4096                Payload size in bytes
  -ReadRatio 0.5                 Read fraction in the load mix
  -Warmup 10s                    Discarded warmup
  -Values 1000                   Keys for storage and recovery seed
  -Output bench-results          Host results directory
  -RepairTimeout 300             Seconds to wait for degraded to clear
  -SkipLoad                      Skip load scenarios
  -SkipStorage                   Skip storage measurements (non-destructive run)
  -SkipRecovery                  Skip stop/restart scenarios
  -Help                          Show this help

Examples:
  ./scripts/bench.ps1 -Runs 1 -Duration 15s
  ./scripts/bench.ps1 -Mode replication -SkipStorage -SkipRecovery
  ./scripts/bench.ps1 -Mode both -Runs 3 -SkipRecovery

Exit codes:
  0  every selected scenario completed
  1  Docker, the cluster, or a benchmark command failed
'@
    exit 0
}

Assert-DockerCompose

if ($SkipLoad -and $SkipStorage -and $SkipRecovery) {
    throw 'every scenario was skipped; omit at least one -Skip switch'
}

if ([System.IO.Path]::IsPathRooted($Output)) {
    $hostOut = [System.IO.Path]::GetFullPath($Output)
}
else {
    $hostOut = [System.IO.Path]::GetFullPath((Join-Path $script:RepoRoot $Output))
}
New-Item -ItemType Directory -Force -Path $hostOut | Out-Null

$modes = if ($Mode -eq 'both') { @('rs32', 'replication') } else { @($Mode) }
$runStamp = Get-Date -Format 'yyyyMMdd-HHmmss'

function Ensure-Cluster {
    if ((Get-HealthyNodeCount) -lt 5) {
        Write-Log 'cluster is not fully healthy; starting it'
        Invoke-Compose -Arguments @('up', '--build', '-d') | Out-Null
    }
    Wait-NodeHealth -Timeout 180
    Wait-ClusterHealthy -Timeout 180 | Out-Null
}

# Reset-Cluster is deliberately destructive: storage measurements must start
# from empty volumes, and the help text says so.
function Reset-Cluster {
    Write-Warn 'resetting the cluster and removing all five data volumes for a clean storage measurement'
    Invoke-Compose -Arguments @('down', '-v', '--remove-orphans') -AllowFailure | Out-Null
    Invoke-Compose -Arguments @('up', '-d') | Out-Null
    Wait-NodeHealth -Timeout 180
    Wait-ClusterHealthy -Timeout 180 | Out-Null
}

function Get-CpuSnapshot {
    $samples = @()
    $raw = & docker stats --no-stream --format '{{json .}}' 2>$null
    if ($LASTEXITCODE -ne 0) { return $samples }
    foreach ($line in @($raw)) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        try {
            $row = $line | ConvertFrom-Json
        }
        catch {
            continue
        }
        if ($row.Name -notmatch '^node[1-5]$') { continue }
        $percent = 0.0
        $rawPercent = [string]$row.CPUPerc
        [void][double]::TryParse($rawPercent.TrimEnd('%'), [ref]$percent)
        $samples += [pscustomobject]@{ Node = $row.Name; Percent = $percent }
    }
    return $samples
}

function Write-CpuCsv {
    param(
        [string]$Directory,
        [hashtable]$Samples
    )
    if ($Samples.Count -eq 0) { return }
    $path = Join-Path $Directory 'cpu.csv'
    $lines = @('node,avg_cpu_percent,samples')
    foreach ($node in ($Samples.Keys | Sort-Object)) {
        $values = $Samples[$node].ToArray()
        $average = ($values | Measure-Object -Average).Average
        $lines += ('{0},{1:N2},{2}' -f $node, $average, $values.Count)
    }
    Set-Content -Path $path -Value $lines -Encoding utf8
}

function Add-CpuSnapshot {
    param(
        [hashtable]$Samples
    )
    foreach ($sample in (Get-CpuSnapshot)) {
        if (-not $Samples.ContainsKey($sample.Node)) {
            $Samples[$sample.Node] = [System.Collections.Generic.List[double]]::new()
        }
        $Samples[$sample.Node].Add($sample.Percent)
    }
}

# Invoke-BenchWithCpu runs one bench command in a background job and samples
# `docker stats --no-stream` before, during, and after it.
function Invoke-BenchWithCpu {
    param(
        [string]$RunID,
        [string]$Scenario,
        [string[]]$Arguments
    )

    $runDir = Join-Path $hostOut $RunID
    New-Item -ItemType Directory -Force -Path $runDir | Out-Null
    $composeArgs = @('compose', 'run', '--rm', '-T')
    $composeArgs += Get-AuthContainerArgs
    $composeArgs += @('-v', "${hostOut}:/results", 'bench')
    $composeArgs += $Arguments
    $composeArgs += @('--output', '/results', '--run-id', $RunID)

    $samples = @{}
    Add-CpuSnapshot -Samples $samples
    Write-Log ("bench scenario {0} (run {1})" -f $Scenario, $RunID)
    $job = Start-Job -ScriptBlock {
        param($argsToRun, $workdir)
        Set-Location $workdir
        & docker @argsToRun 2>&1
    } -ArgumentList (,$composeArgs), $script:RepoRoot
    try {
        while ($job.State -eq 'Running') {
            Start-Sleep -Seconds 2
            Add-CpuSnapshot -Samples $samples
        }
        $output = Receive-Job -Job $job -Wait
        if ($job.State -eq 'Failed') {
            throw ("bench scenario {0} failed: {1}" -f $Scenario, ($job.ChildJobs[0].JobStateInfo.Reason))
        }
        $text = ($output | Out-String)
        if ([string]::IsNullOrWhiteSpace($text)) {
            $text = ''
        }
        Set-Content -Path (Join-Path $runDir 'console.txt') -Value $text -Encoding utf8
        Write-Host $text
        Add-CpuSnapshot -Samples $samples
        Write-CpuCsv -Directory $runDir -Samples $samples
        Update-BenchSummary -RunID $RunID
    }
    finally {
        if ($job.State -eq 'Running') {
            Stop-Job -Job $job | Out-Null
        }
        Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
    }
}

function Invoke-BenchDirect {
    param([string[]]$Arguments)
    $composeArgs = @('run', '--rm', '-T')
    $composeArgs += Get-AuthContainerArgs
    $composeArgs += @('-v', "${hostOut}:/results", 'bench')
    $composeArgs += $Arguments
    Invoke-Compose -Arguments $composeArgs | Out-Host
}

# Update-BenchSummary regenerates summary.md after the script writes cpu.csv.
function Update-BenchSummary {
    param([string]$RunID)
    $composeArgs = @('run', '--rm', '-T')
    $composeArgs += Get-AuthContainerArgs
    $composeArgs += @('-v', "${hostOut}:/results", 'bench', 'report', '--input', "/results/$RunID")
    Invoke-Compose -Arguments $composeArgs | Out-Null
}

function Get-BenchStatus {
    $raw = Invoke-GatewayCapture -Arguments @('status', '--json')
    return ($raw | ConvertFrom-Json)
}

function Wait-Degraded {
    param([int]$Timeout = 60)
    $deadline = (Get-Date).AddSeconds($Timeout)
    do {
        try {
            $status = Get-BenchStatus
            if ($status.degraded) { return (Get-Date).ToUniversalTime() }
        }
        catch {
            Write-Log ("status probe retrying: {0}" -f $_.Exception.Message)
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw ("timed out after {0}s waiting for degraded state" -f $Timeout)
}

function Wait-HealthyAgain {
    param([int]$Timeout = 180)
    $deadline = (Get-Date).AddSeconds($Timeout)
    do {
        try {
            $status = Get-BenchStatus
            if (-not $status.degraded -and $status.healthy_nodes -ge 5) { return }
        }
        catch {
            Write-Log ("status probe retrying: {0}" -f $_.Exception.Message)
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw ("timed out after {0}s waiting for the cluster to recover" -f $Timeout)
}

$failures = [System.Collections.Generic.List[string]]::new()

try {
    # The tools profile is not started by `up`, so build its image explicitly;
    # this keeps the bench binary in step with the checked-out source.
    Invoke-Compose -Arguments @('build', 'bench') | Out-Null
    Ensure-Cluster

    if (-not $SkipLoad) {
        foreach ($run in 1..$Runs) {
            foreach ($benchMode in $modes) {
                $id = "${runStamp}-load-${benchMode}-run${run}"
                Invoke-BenchWithCpu -RunID $id -Scenario "load-${benchMode}" -Arguments @(
                    'run',
                    '--mode', $benchMode,
                    '--concurrency', "$Concurrency",
                    '--duration', $Duration,
                    '--warmup', $Warmup,
                    '--keys', "$Keys",
                    '--value-size', "$ValueSize",
                    '--read-ratio', "$ReadRatio"
                )
            }
        }
    }

    if (-not $SkipStorage) {
        foreach ($run in 1..$Runs) {
            foreach ($benchMode in $modes) {
                Reset-Cluster
                $id = "${runStamp}-storage-${benchMode}-run${run}"
                Invoke-BenchWithCpu -RunID $id -Scenario "storage-${benchMode}" -Arguments @(
                    'storage',
                    '--mode', $benchMode,
                    '--values', "$Values",
                    '--value-size', "$ValueSize"
                )
            }
        }
    }

    if (-not $SkipRecovery) {
        Ensure-Cluster
        foreach ($run in 1..$Runs) {
            Invoke-BenchDirect -Arguments @(
                'recovery', '--seed-only', '--seed-keys', "$Values",
                '--value-size', "$ValueSize", '--key-prefix', "recovery-${runStamp}-run${run}"
            )

            $scenarios = @(
                @{ Name = 'one-node'; Nodes = @('node4') },
                @{ Name = 'two-node'; Nodes = @('node4', 'node5') }
            )
            foreach ($scenario in $scenarios) {
                Write-Section ("recovery scenario {0}" -f $scenario.Name)
                $stopAt = (Get-Date).ToUniversalTime()
                Invoke-Compose -Arguments (@('stop') + $scenario.Nodes) | Out-Null
                $degradedAt = Wait-Degraded -Timeout 60
                $restartAt = (Get-Date).ToUniversalTime()
                Invoke-Compose -Arguments (@('start') + $scenario.Nodes) | Out-Null

                $id = "${runStamp}-recovery-$($scenario.Name)-run${run}"
                Invoke-BenchWithCpu -RunID $id -Scenario "recovery-$($scenario.Name)" -Arguments @(
                    'recovery',
                    '--measure-only',
                    '--scenario', $scenario.Name,
                    '--start-time', $stopAt.ToString('o'),
                    '--degraded-time', $degradedAt.ToString('o'),
                    '--restart-time', $restartAt.ToString('o'),
                    '--timeout', ("{0}s" -f $RepairTimeout)
                )
                Wait-HealthyAgain -Timeout $RepairTimeout
            }
        }
    }

    Ensure-Cluster
    Write-Section 'benchmark summary'
    Write-Log ("results directory: {0}" -f $hostOut)
    Get-ChildItem -Path $hostOut -Directory | Sort-Object Name | ForEach-Object {
        Write-Log ("  {0}" -f $_.Name)
    }
}
catch {
    $failures.Add($_.Exception.Message)
    Write-Error $_.Exception.Message
}

if ($failures.Count -gt 0) {
    exit 1
}
