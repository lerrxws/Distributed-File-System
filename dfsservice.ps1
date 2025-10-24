param(
    [Parameter(Mandatory=$true)]
    [string]$command,

    [Parameter(Mandatory=$false)]
    [string]$arg1,

    [Parameter(Mandatory=$false)]
    [string]$arg2,

    [Parameter(Mandatory=$false)]
    [string]$arg3
)

$pidFile = ".\exe-files\dfsserver.pid"
$exePath = ".\exe-files\dfsserver.exe"

switch ($command) {
    "start" {
        if (-not $arg1 -or -not $arg2 -or -not $arg3) {
            Write-Host "Usage: .\dfsservice.ps1 start <dfsPort> <extentAddr> <lockAddr>"
            exit 1
        }

        # TODO: add regex check

        $dfsPort = $arg1
        $extentAddr = $arg2
        $lockAddr = $arg3

        Write-Host "Building DFS Service..."
        & go build -o $exePath .\servers\dfs\dfsserver.go

        Write-Host "Starting DFS Service on port $lockPort"
        $proc = Start-Process $exePath -ArgumentList $dfsPort, $extentAddr, $lockAddr  -PassThru
        $pidNum = $proc.Id
        Set-Content -Path $pidFile -Value $pidNum
        Write-Host "Started Lock Service with PID $pidNum"
    }

    "stop" {
        if (-not $arg1) {
            Write-Host "Usage: .\dfsservice.ps1 stop <dfsAddr>"
            exit 1
        }

        # TODO: write regex check

        $dfsAddr = $arg1
        $pidNum = Get-Content -Path $pidFile
        
        Write-Host "Stopping DFS Service at $dfsAddr"
        Write-Host "Stopping Lock Service with PID $pidNum"
        Stop-Process -Id $pidNum -Force
        Remove-Item $pidFile -Force
        # Remove-Item $exePath -Force
        
    }

    Default {
        Write-Host "Unknown command. Use start or stop."
        exit 1
    }
}
