#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Run unit, integration, chaos, or benchmark suites in Docker only.
.PARAMETER Unit
    Run the default unit/component suite (chosen automatically when no suite is named).
.PARAMETER Integration
    Start the Compose cluster if needed and run the integration-tagged suites on
    the pinned cluster network.
.PARAMETER Chaos
    Run the chaos-tagged in-process failure tests.
.PARAMETER Bench
    Run the benchmark-tagged suites (placeholder until the benchmark task).
.PARAMETER Keep
    Leave a cluster that this script started running after the tests finish.
.PARAMETER Timeout
    Seconds to wait for cluster health before integration tests (default 120).
.PARAMETER Help
    Show this help.
#>
param(
    [switch]$Unit,
    [switch]$Integration,
    [switch]$Chaos,
    [switch]$Bench,
    [switch]$Keep,
    [int]$Timeout = 120,
    [switch]$Help
)

. "$PSScriptRoot/common.ps1"

if ($Help) {
    Show-ScriptHelp @'
Usage: scripts/test.ps1 [-Unit] [-Integration] [-Chaos] [-Bench] [-Keep] [-Timeout SECONDS] [-Help]

Runs Go tests inside golang:1.22 containers. No host Go installation is needed.

Suites (combine switches to run more than one; Unit is the default):
  -Unit          go test -race ./... (unit and component tests)
  -Integration   starts the cluster, waits for health, then runs the
                 integration-tagged suite on distributed-systems_cluster-net
  -Chaos         runs the chaos-tagged failure suite
  -Bench         runs the benchmark-tagged suite

Options:
  -Keep              Leave a cluster started by this script running
  -Timeout SECONDS   Health timeout for integration (default 120)
  -Help              Show this help

Examples:
  ./scripts/test.ps1
  ./scripts/test.ps1 -Integration -Keep
  ./scripts/test.ps1 -Unit -Chaos

Exit codes:
  0  every selected suite passed
  1  Docker failed or a test failed
'@
    exit 0
}

if (-not ($Unit -or $Integration -or $Chaos -or $Bench)) {
    $Unit = $true
}

function Invoke-GoTest {
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string[]]$TestArguments,
        [switch]$OnClusterNetwork
    )

    $dockerArgs = @('run', '--rm')
    if ($OnClusterNetwork) {
        $dockerArgs += @('--network', $script:ComposeNetwork)
    }
    $dockerArgs += Get-AuthContainerArgs
    $dockerArgs += @('-v', "$($script:RepoRoot):/app", '-w', '/app', 'golang:1.22', 'go', 'test')
    $dockerArgs += $TestArguments

    Write-Section $Label
    Write-Log ("docker {0}" -f ($dockerArgs -join ' '))
    & docker @dockerArgs
    if ($LASTEXITCODE -ne 0) {
        throw ("{0} failed with exit code {1}" -f $Label, $LASTEXITCODE)
    }
}

$startedCluster = $false
try {
    Assert-DockerCompose

    if ($Integration) {
        if ((Get-HealthyNodeCount) -lt 5) {
            Write-Log 'cluster is not fully healthy; starting it'
            Invoke-Compose -Arguments @('up', '--build', '-d') | Out-Null
            $startedCluster = $true
        }
        Wait-NodeHealth -Timeout $Timeout
        Wait-ClusterHealthy -Timeout $Timeout | Out-Null

        Invoke-GoTest -Label 'integration tests' -OnClusterNetwork `
            -TestArguments @('-tags=integration', '-count=1', '-v', './tests/...')
    }

    if ($Unit) {
        Invoke-GoTest -Label 'unit and component tests' `
            -TestArguments @('-race', '-count=1', './...')
    }

    if ($Chaos) {
        Invoke-GoTest -Label 'chaos tests' `
            -TestArguments @('-race', '-tags=chaos', '-count=1', '-v', './tests/chaos/...')
    }

    if ($Bench) {
        Invoke-GoTest -Label 'benchmark-tagged tests' `
            -TestArguments @('-race', '-tags=bench', '-count=1', './tests/bench/...')
    }

    Write-Log 'all selected test suites passed'
}
catch {
    Write-Error $_.Exception.Message
    exit 1
}
finally {
    if ($startedCluster -and -not $Keep) {
        Write-Log 'stopping the cluster started by this script (use -Keep to leave it running)'
        & docker compose down --remove-orphans
    }
    elseif ($startedCluster -and $Keep) {
        Write-Log 'leaving the cluster running (-Keep)'
    }
}
