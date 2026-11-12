#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Run the free local equivalent of the former GitHub Actions CI job.

.DESCRIPTION
    Runs build, vet (default, integration, chaos), the fast race suite, the chaos
    suite, and a gofmt check entirely in Docker. No host Go and no paid CI are
    required.

.PARAMETER SkipChaos
    Skip the chaos-tagged suite for a faster check.

.PARAMETER Help
    Show this help.
#>
param(
    [switch]$SkipChaos,
    [switch]$Help
)

. "$PSScriptRoot/common.ps1"

if ($Help) {
    Show-ScriptHelp @'
Usage: scripts/check.ps1 [-SkipChaos] [-Help]

Runs the local Docker check suite. No host Go installation is required.

Checks:
  build                 go build ./...
  vet                   go vet ./...
  vet integration       go vet -tags=integration ./...
  vet chaos             go vet -tags=chaos ./...
  fast tests            go test -race -count=1 ./...
  chaos tests           go test -race -tags=chaos -count=1 ./tests/chaos/...
  format check          gofmt over a CRLF-normalized copy of the working tree

Options:
  -SkipChaos            Skip the chaos suite for a faster check
  -Help                 Show this help

Examples:
  ./scripts/check.ps1
  ./scripts/check.ps1 -SkipChaos

Exit codes:
  0  every check passed
  1  Docker failed or a check failed
'@
    exit 0
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw 'docker was not found on PATH. Install Docker Desktop or Docker Engine.'
}

$script:DockerRunArgs = @(
    'run', '--rm',
    '-v', "$($script:RepoRoot):/app",
    '-w', '/app',
    'golang:1.22'
)

function Invoke-DockerCheck {
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    Write-Section $Label
    Write-Log ("docker run --rm -v {0}:/app -w /app golang:1.22 {1}" -f $script:RepoRoot, ($Arguments -join ' '))
    $base = $script:DockerRunArgs
    & docker @base @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw ("{0} failed with exit code {1}" -f $Label, $LASTEXITCODE)
    }
}

try {
    Invoke-DockerCheck -Label 'build' -Arguments @('go', 'build', './...')
    Invoke-DockerCheck -Label 'vet' -Arguments @('go', 'vet', './...')
    Invoke-DockerCheck -Label 'vet integration-tagged files' -Arguments @('go', 'vet', '-tags=integration', './...')
    Invoke-DockerCheck -Label 'vet chaos-tagged files' -Arguments @('go', 'vet', '-tags=chaos', './...')
    Invoke-DockerCheck -Label 'fast tests' -Arguments @('go', 'test', '-race', '-count=1', './...')
    if (-not $SkipChaos) {
        Invoke-DockerCheck -Label 'chaos tests' `
            -Arguments @('go', 'test', '-race', '-tags=chaos', '-count=1', './tests/chaos/...')
    }
    Invoke-DockerCheck -Label 'format check' -Arguments @('bash', '/app/scripts/format-check.sh')

    Write-Log 'all local checks passed'
}
catch {
    Write-Error $_.Exception.Message
    exit 1
}
