#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Stop the cluster while keeping data volumes by default.
.PARAMETER Volumes
    Also remove the five named data volumes. This deletes all stored data.
.PARAMETER Help
    Show this help.
#>
param(
    [switch]$Volumes,
    [switch]$Help
)

. "$PSScriptRoot/common.ps1"

if ($Help) {
    Show-ScriptHelp @'
Usage: scripts/down.ps1 [-Volumes] [-Help]

Stops and removes the cluster containers. Named data volumes are preserved
unless -Volumes is supplied.

Options:
  -Volumes   Remove node1-data ... node5-data (destructive)
  -Help      Show this help

Examples:
  ./scripts/down.ps1
  ./scripts/down.ps1 -Volumes

Exit codes:
  0  the cluster stopped
  1  Docker/Compose failed
'@
    exit 0
}

try {
    Assert-DockerCompose
    if ($Volumes) {
        Write-Warn 'Removing containers AND the five named data volumes. All cluster data will be deleted.'
        Invoke-Compose -Arguments @('down', '--volumes', '--remove-orphans') | Out-Null
    }
    else {
        Write-Log 'Volumes are preserved; use -Volumes to delete them.'
        Invoke-Compose -Arguments @('down', '--remove-orphans') | Out-Null
    }
    Write-Log 'cluster stopped'
}
catch {
    Write-Error $_.Exception.Message
    exit 1
}
