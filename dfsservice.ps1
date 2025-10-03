# Script starting/stopping DFS Service (for Windows PowerShell)
#
# Input arguments:
#
# 1. Toggle switch to indicate starting or stopping the service (allowed values: start, stop)
# 2. Port where the service will start (e.g. 3001) or address where to connect and stop the service (e.g. 127.0.0.1:3001)
# 3. Address and port where to connect Extent Service (format is IP:port, e.g. 127.0.0.1:2001)
# 4. Address and port where to connect Lock Service (format is IP:port, e.g. 127.0.0.1:1001)
#
# Examples how to use the script:
#
# .\dfsservice.ps1 start 3001 127.0.0.1:2001 127.0.0.1:1001
# .\dfsservice.ps1 stop 127.0.0.1:3001

param (
    [Parameter(Mandatory=$true)]
    [ValidateSet('start','stop')]
    [string]$command,

    [Parameter(Mandatory=$false)]
    [string]$arg1,

    [Parameter(Mandatory=$false)]
    [string]$arg2,

    [Parameter(Mandatory=$false)]
    [string]$arg3
)

$scriptRootName = Split-Path $PSScriptRoot -Leaf
if ($scriptRootName -eq 'dfs-grading-0.10') {
    $repoRoot = Split-Path -Parent $PSScriptRoot
    $pidFileRoot = $PSScriptRoot
} else {
    $repoRoot = $PSScriptRoot
    $pidFileRoot = $PSScriptRoot
}

$serverSrc  = Join-Path $repoRoot 'servers\dfs\dfsserver.go'
$binaryPath = Join-Path $repoRoot 'dfsserver.exe'
$pidFile    = Join-Path $pidFileRoot 'dfsserver.pid'

$portRegex = '^\d{2,5}$'
$addrRegex = '^(?:\d{1,3}\.){3}\d{1,3}:\d+$'

switch ($command) {
    'start' {
        if (-not $arg1 -or -not $arg2 -or -not $arg3) {
            Write-Host "Usage: .\dfsservice.ps1 start <dfsPort> <extentAddr> <lockAddr>"
            exit 1
        }

         if ($arg1 -notmatch $portRegex) {
            Write-Host "Invalid DFS port: $arg1"
            exit 1
        }

        if ($arg2 -notmatch $addrRegex) {
            Write-Host "Invalid extent address: $arg2 (expected format: 127.0.0.1:2001)"
            exit 1
        }
        if ($arg3 -notmatch $addrRegex) {
            Write-Host "Invalid lock address: $arg3 (expected format: 127.0.0.1:1001)"
            exit 1
        }

        $dfsPort = $arg1
        $extentAddr = $arg2
        $lockAddr = $arg3

        Write-Host "Building DFS Service..."
        & go build -o dfsserver.exe .\servers\dfs\dfsserver.go

        Write-Host "Starting DFS Service on port $lockPort"
        & .\dfsserver.exe $dfsPort, $extentAddr, $lockAddr
    }

    'stop' {
        if (-not $arg1) {
            Write-Host "Usage: .\dfsservice.ps1 stop <ignored>"
            exit 1
        }

        if (-not (Test-Path $pidFile)) {
            Write-Host "DFS Service PID file not found; nothing to stop"
            exit 0
        }

        $pid = Get-Content -Path $pidFile | Select-Object -First 1
        if ($pid -and (Get-Process -Id $pid -ErrorAction SilentlyContinue)) {
            Write-Host "Stopping DFS Service (PID $pid)"
            Stop-Process -Id $pid -Force
        }
        Remove-Item -Path $pidFile -ErrorAction SilentlyContinue
    }
}
