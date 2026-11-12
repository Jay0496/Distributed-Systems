# Shared PowerShell helpers for the deployment scripts. Dot-source this file:
#   . "$PSScriptRoot/common.ps1"
# It only defines functions and constants; it does not change the caller's
# location and never requires Go on the host.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$script:RepoRoot = Split-Path -Parent $PSScriptRoot
$script:ComposeNetwork = 'distributed-systems_cluster-net'
$script:NodeServices = @('node1', 'node2', 'node3', 'node4', 'node5')
$script:AuthToken = [string]$env:AUTH_TOKEN
$script:GatewayNodes = 'node1=node1:50051,node2=node2:50052,node3=node3:50053,node4=node4:50054,node5=node5:50055'

function Write-Log {
    param([Parameter(Mandatory = $true)][string]$Message)
    Write-Host ("[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Message)
}

function Write-Section {
    param([Parameter(Mandatory = $true)][string]$Title)
    Write-Host ''
    Write-Host ("=== {0} ===" -f $Title)
}

function Write-Warn {
    param([Parameter(Mandatory = $true)][string]$Message)
    Write-Warning ("[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Message)
}

function Assert-DockerCompose {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw 'docker was not found on PATH. Install Docker Desktop or Docker Engine with the Compose plugin.'
    }
    & docker compose version *> $null
    if ($LASTEXITCODE -ne 0) {
        throw 'docker compose (v2) is unavailable. Install the Docker Compose plugin and retry.'
    }
}

function Invoke-Compose {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$AllowFailure
    )
    Write-Log ("docker compose {0}" -f ($Arguments -join ' '))
    & docker compose @Arguments
    $code = $LASTEXITCODE
    if ($code -ne 0 -and -not $AllowFailure) {
        throw ("docker compose {0} failed with exit code {1}" -f ($Arguments -join ' '), $code)
    }
}

function Get-ComposeRows {
    $raw = & docker compose ps --all --format json 2>$null
    if ($LASTEXITCODE -ne 0) {
        return @()
    }
    $text = (($raw | Out-String).Trim())
    if ([string]::IsNullOrWhiteSpace($text)) {
        return @()
    }

    $rows = @()
    if ($text.StartsWith('[')) {
        $rows = @($text | ConvertFrom-Json)
    }
    else {
        foreach ($line in ($text -split "`r?`n")) {
            if (-not [string]::IsNullOrWhiteSpace($line)) {
                $rows += ($line | ConvertFrom-Json)
            }
        }
    }
    return $rows
}

function Get-HealthyNodeCount {
    $rows = @(Get-ComposeRows | Where-Object { $script:NodeServices -contains $_.Service })
    return @($rows | Where-Object { $_.Health -eq 'healthy' }).Count
}

function Show-ClusterDiagnostics {
    Write-Warn 'Cluster did not become healthy in time. Current status:'
    & docker compose ps
    Write-Warn 'Recent node logs:'
    & docker compose logs --tail 40 node1 node2 node3 node4 node5
}

function Wait-NodeHealth {
    param([int]$Timeout = 120)
    $deadline = (Get-Date).AddSeconds($Timeout)
    $lastCount = -1

    do {
        $healthy = Get-HealthyNodeCount
        if ($healthy -ne $lastCount) {
            Write-Log ("healthy nodes: {0}/5" -f $healthy)
            $lastCount = $healthy
        }
        if ($healthy -ge 5) {
            Write-Log 'all five nodes report healthy'
            return
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)

    Show-ClusterDiagnostics
    throw ("timed out after {0}s waiting for five healthy nodes" -f $Timeout)
}

function Get-AuthStatusText {
    if ([string]::IsNullOrWhiteSpace($script:AuthToken)) {
        return 'auth: disabled'
    }
    return 'auth: enabled'
}

function Get-AuthContainerArgs {
    if ([string]::IsNullOrWhiteSpace($script:AuthToken)) {
        return @()
    }
    # Pass the variable by name so Docker copies it from the environment. The
    # token value never appears in a command line, log, or error message.
    return @('-e', 'AUTH_TOKEN')
}

function Invoke-Gateway {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [string[]]$Volumes = @()
    )
    $composeArgs = @('run', '--rm', '-T')
    $composeArgs += Get-AuthContainerArgs
    foreach ($volume in $Volumes) {
        $composeArgs += @('-v', $volume)
    }
    $composeArgs += 'gateway'
    $composeArgs += $Arguments
    return Invoke-Compose -Arguments $composeArgs
}

function Invoke-GatewayCapture {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [string[]]$Volumes = @(),
        [switch]$SuppressErrors
    )
    $composeArgs = @('run', '--rm', '-T')
    $composeArgs += Get-AuthContainerArgs
    foreach ($volume in $Volumes) {
        $composeArgs += @('-v', $volume)
    }
    $composeArgs += 'gateway'
    $composeArgs += $Arguments

    Write-Log ("docker compose {0}" -f ($composeArgs -join ' '))
    if ($SuppressErrors) {
        $output = (& docker compose @composeArgs 2>$null | Out-String)
    }
    else {
        $output = (& docker compose @composeArgs | Out-String)
    }
    $code = $LASTEXITCODE
    if ($code -ne 0) {
        throw ("gateway {0} failed with exit code {1}" -f ($Arguments -join ' '), $code)
    }
    return $output
}

function Get-ClusterStatus {
    $raw = Invoke-GatewayCapture -Arguments @('status', '--json')
    return ($raw | ConvertFrom-Json)
}

function Wait-ClusterHealthy {
    param([int]$Timeout = 120)
    $deadline = (Get-Date).AddSeconds($Timeout)
    $last = ''

    do {
        try {
            $status = Get-ClusterStatus
            $line = ("healthy_nodes={0}/{1} degraded={2} degraded_stripes={3} repair_queue={4}" -f `
                    $status.healthy_nodes, $status.total_nodes, $status.degraded,
                $status.degraded_stripes, $status.repair_queue_depth)
            if ($line -ne $last) {
                Write-Log $line
                $last = $line
            }
            if ($status.healthy_nodes -ge 5 -and $status.total_nodes -ge 5 -and -not $status.degraded) {
                return $status
            }
        }
        catch {
            Write-Log ("status probe retrying: {0}" -f $_.Exception.Message)
        }
        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)

    Show-ClusterDiagnostics
    throw ("timed out after {0}s waiting for a healthy, non-degraded cluster" -f $Timeout)
}

function Format-Bytes {
    param([double]$Bytes)
    if ($Bytes -lt 1KB) { return ("{0:N0} B" -f $Bytes) }
    if ($Bytes -lt 1MB) { return ("{0:N2} KiB" -f ($Bytes / 1KB)) }
    return ("{0:N2} MiB" -f ($Bytes / 1MB))
}

function Show-ScriptHelp {
    param([Parameter(Mandatory = $true)][string]$Text)
    Write-Host $Text
}
