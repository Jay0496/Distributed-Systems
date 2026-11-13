#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Check the documentation set for broken relative links, stale strings, and
    runnable command blocks.

.DESCRIPTION
    Scans every Markdown file outside .git and .opencode:

      1. every relative Markdown link must resolve to a real file or directory;
      2. fenced powershell/bash blocks tagged <!-- runnable --> are printed so a
         reader can execute them manually;
      3. stale strings that describe removed code or unfinished work fail the
         check.

    This is a documentation guard, not a test runner. It never starts Docker.

.PARAMETER Help
    Show this help.

.EXAMPLE
    ./scripts/check-docs.ps1

.NOTES
    Exit codes:
      0  every check passed
      1  a broken link, stale string, or malformed runnable block was found
#>
param(
    [switch]$Help
)

$ErrorActionPreference = 'Stop'

if ($Help) {
    @'
Usage: scripts/check-docs.ps1 [-Help]

Checks every Markdown file outside .git and .opencode:
  - relative links resolve to a real file or directory
  - no stale strings remain in the documentation
  - <!-- runnable --> powershell/bash blocks are listed for manual execution

Options:
  -Help    Show this help

Examples:
  ./scripts/check-docs.ps1

Exit codes:
  0  every check passed
  1  a broken link, stale string, or malformed runnable block was found
'@ | Write-Host
    exit 0
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$staleStrings = @(
    'storage.NewMockStore',
    'go run main.go',
    'NewRouter(100)',
    'not yet implemented',
    'legacy_kv',
    'TODO'
)

$failures = [System.Collections.Generic.List[string]]::new()
$runnableCount = 0
$linkCount = 0

function Get-MarkdownFiles {
    param([string]$Root)
    Get-ChildItem -LiteralPath $Root -Recurse -Filter '*.md' -File | Where-Object {
        $relative = $_.FullName.Substring($Root.Length).TrimStart('\', '/')
        $relative -notmatch '(^|[\\/])\.git([\\/]|$)' -and
        $relative -notmatch '(^|[\\/])\.opencode([\\/]|$)'
    } | Sort-Object FullName
}

function Get-LinkTarget {
    param([string]$Raw)
    $target = $Raw.Trim()
    if ($target.StartsWith('<')) {
        $end = $target.IndexOf('>')
        if ($end -gt 0) {
            return $target.Substring(1, $end - 1)
        }
    }
    # Drop a Markdown title: [text](path "title").
    $target = ($target -split '\s+', 2)[0]
    return $target
}

foreach ($file in Get-MarkdownFiles -Root $repoRoot) {
    $relative = $file.FullName.Substring($repoRoot.Length).TrimStart('\', '/')
    $lines = Get-Content -LiteralPath $file.FullName

    # ---- links -------------------------------------------------------------
    $linkPattern = '!?\[[^\]]*\]\(([^)]+)\)'
    for ($i = 0; $i -lt $lines.Length; $i++) {
        foreach ($match in [regex]::Matches($lines[$i], $linkPattern)) {
            $linkCount++
            $target = Get-LinkTarget -Raw $match.Groups[1].Value
            if ([string]::IsNullOrWhiteSpace($target)) { continue }
            if ($target -match '^[a-zA-Z][a-zA-Z0-9+.-]*:') { continue }  # http, https, mailto, ...
            if ($target.StartsWith('#')) { continue }                     # in-page anchor

            $path = ($target -split '#', 2)[0]
            if ([string]::IsNullOrWhiteSpace($path)) { continue }
            $path = [System.Uri]::UnescapeDataString($path)

            $resolved = $path
            if (-not [System.IO.Path]::IsPathRooted($path)) {
                $resolved = Join-Path $file.DirectoryName $path
            }
            if (-not (Test-Path -LiteralPath $resolved)) {
                $failures.Add(("{0}:{1}: broken link -> {2}" -f $relative, ($i + 1), $target))
            }
        }
    }

    # ---- stale strings -----------------------------------------------------
    for ($i = 0; $i -lt $lines.Length; $i++) {
        foreach ($stale in $staleStrings) {
            if ($lines[$i].IndexOf($stale, [System.StringComparison]::Ordinal) -ge 0) {
                $failures.Add(("{0}:{1}: stale string -> {2}" -f $relative, ($i + 1), $stale))
            }
        }
    }

    # ---- runnable blocks ---------------------------------------------------
    for ($i = 0; $i -lt $lines.Length; $i++) {
        if ($lines[$i].Trim() -ne '<!-- runnable -->') { continue }

        $fence = -1
        for ($j = $i + 1; $j -lt $lines.Length; $j++) {
            if ([string]::IsNullOrWhiteSpace($lines[$j])) { continue }
            $fence = $j
            break
        }
        if ($fence -lt 0 -or $lines[$fence].Trim() -notmatch '^```(\S*)$') {
            $failures.Add(("{0}:{1}: runnable tag is not followed by a code fence" -f $relative, ($i + 1)))
            continue
        }

        if ($lines[$fence].Trim() -match '^```(\S*)$') {
            $language = $Matches[1].ToLowerInvariant()
        }
        else {
            $language = ''
        }
        if ($language -notin @('powershell', 'bash', 'sh')) {
            $failures.Add(("{0}:{1}: runnable block uses unsupported language '{2}'" -f $relative, ($fence + 1), $language))
            continue
        }

        $body = [System.Collections.Generic.List[string]]::new()
        $closed = $false
        for ($k = $fence + 1; $k -lt $lines.Length; $k++) {
            if ($lines[$k].Trim() -eq '```') {
                $closed = $true
                break
            }
            $body.Add($lines[$k])
        }
        if (-not $closed) {
            $failures.Add(("{0}:{1}: runnable block has no closing fence" -f $relative, ($fence + 1)))
            continue
        }

        $runnableCount++
        Write-Host ''
        Write-Host ("runnable: {0}:{1} ({2})" -f $relative, ($fence + 2), $language) -ForegroundColor Cyan
        $body | ForEach-Object { Write-Host ("    " + $_) }
    }
}

Write-Host ''
Write-Host ("checked {0} Markdown files, {1} relative links, {2} runnable blocks" -f `
        (Get-MarkdownFiles -Root $repoRoot).Count, $linkCount, $runnableCount)

if ($failures.Count -gt 0) {
    Write-Host ''
    $failures | ForEach-Object { Write-Host ("FAIL: " + $_) -ForegroundColor Red }
    Write-Host ("documentation check failed with {0} problem(s)" -f $failures.Count) -ForegroundColor Red
    exit 1
}

Write-Host 'documentation check passed' -ForegroundColor Green
exit 0
