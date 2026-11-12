#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Run the benchmark tool inside the deployment image.
.PARAMETER Help
    Show this script's help.
.PARAMETER BenchArgs
    Arguments forwarded to /app/bench. Task 13 fills in the real scenarios.
#>
param(
    [switch]$Help,
    [Parameter(ValueFromRemainingArguments = $true)][string[]]$BenchArgs = @()
)

. "$PSScriptRoot/common.ps1"

if ($Help -or ($BenchArgs.Count -gt 0 -and $BenchArgs[0] -in @('-h', '--help', 'help'))) {
    Show-ScriptHelp @'
Usage: scripts/bench.ps1 [bench arguments] [-Help]

Runs the one-shot bench tool from the deployment image with the tools profile
environment (GATEWAY_NODES, AUTH_TOKEN, MAX_MESSAGE_SIZE). This is a placeholder
runner; the benchmark task adds the real flags and reporting.

Examples:
  ./scripts/bench.ps1
  ./scripts/bench.ps1 --help
  ./scripts/bench.ps1 --scenario rs32

Exit codes:
  0  the bench tool exited successfully
  1  Docker/Compose failed or the bench tool failed
'@
    exit 0
}

try {
    Assert-DockerCompose
    $composeArgs = @('run', '--rm')
    $composeArgs += Get-AuthContainerArgs
    $composeArgs += 'bench'
    $composeArgs += $BenchArgs
    Invoke-Compose -Arguments $composeArgs | Out-Null
}
catch {
    Write-Error $_.Exception.Message
    exit 1
}
