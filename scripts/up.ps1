#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Build and start the five-node cluster, then wait until it is healthy.
.PARAMETER Timeout
    Seconds to wait for node health and a non-degraded cluster (default 120).
.PARAMETER Help
    Show this help.
#>
param(
    [int]$Timeout = 120,
    [switch]$Help
)

. "$PSScriptRoot/common.ps1"

if ($Help) {
    Show-ScriptHelp @'
Usage: scripts/up.ps1 [-Timeout SECONDS] [-Help]

Builds the deployment image, starts node1-node5 in the background, waits for all
five health checks, prints docker compose ps, and asks the gateway for cluster
status. No Go installation is required on the host.

Options:
  -Timeout SECONDS   Health and status timeout (default 120)
  -Help              Show this help

Examples:
  ./scripts/up.ps1
  ./scripts/up.ps1 -Timeout 180

Exit codes:
  0  all five nodes are healthy and the cluster is not degraded
  1  Docker/Compose is unavailable, startup failed, or the timeout expired
'@
    exit 0
}

try {
    Assert-DockerCompose
    Write-Log (Get-AuthStatusText)
    Invoke-Compose -Arguments @('up', '--build', '-d') | Out-Null
    Wait-NodeHealth -Timeout $Timeout
    Wait-ClusterHealthy -Timeout $Timeout | Out-Null
    Invoke-Compose -Arguments @('ps') | Out-Host
    Invoke-Gateway -Arguments @('status') | Out-Host
    Write-Log 'cluster is ready'
}
catch {
    Write-Error $_.Exception.Message
    exit 1
}
