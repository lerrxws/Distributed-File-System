param (
    # command
    [Parameter(Mandatory = $true)]
    [string]$command,

    # port / addr
    [Parameter(Mandatory = $false)]
    [string]$arg1
)

if (-not $arg1) {
    Write-Host "Usage: .\lockservice.ps1 start <PORT> | stop <PORT>"
    exit 1
}

$pidFile = ".\exe-files\lockserver.pid"
$exePath = ".\exe-files\lockserver.exe"

switch ($command) {
    "start" {
        # TODO: add regex check for port
        $lockPort = $arg1

        Write-Host "Building Lock Service..."
        & go build -o $exePath .\servers\lock\lockserver.go

        Write-Host "Starting Lock Service on port $lockPort"
        $proc = Start-Process $exePath -ArgumentList $lockPort -PassThru
        $pidNum = $proc.Id
        Set-Content -Path $pidFile -Value $pidNum
        Write-Host "Started Lock Service with PID $pidNum"
    }

    "stop" {
        # TODO: optionally check if port matches
        $pidNum = Get-Content -Path $pidFile
        
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
