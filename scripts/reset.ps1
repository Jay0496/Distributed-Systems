#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Destructively reset the cluster, including all data volumes, then optionally
    rebuild the image.
.PARAMETER Build
    Rebuild the deployment image after removing the cluster.
.PARAMETER Help
    Show this help.
#>
param(
    [switch]$Build,
    [switch]$Help
)

. "$PSScriptRoot/common.ps1"

if ($Help) {
    Show-ScriptHelp @'
Usage: scripts/reset.ps1 [-Build] [-Help]

PERMANENTLY deletes the cluster containers, the Compose network, and the five
named data volumes (node1-data ... node5-data). Use scripts/down.ps1 when you
only want to stop the cluster and keep its data.

Options:
  -Build   Rebuild the deployment image after reset (does not start the cluster)
  -Help    Show this help

Examples:
  ./scripts/reset.ps1
  ./scripts/reset.ps1 -Build

Exit codes:
  0  the reset completed
  1  Docker/Compose failed
'@
    exit 0
}

try {
    Assert-DockerCompose
    Write-Warn 'RESET IS DESTRUCTIVE: removing containers, the Compose network, and node1-data ... node5-data.'
    Invoke-Compose -Arguments @('down', '--volumes', '--remove-orphans') | Out-Null
    if ($Build) {
        Invoke-Compose -Arguments @('build') | Out-Null
    }
    Write-Log 'cluster reset complete; run ./scripts/up.ps1 to start again'
}
catch {
    Write-Error $_.Exception.Message
    exit 1
}
