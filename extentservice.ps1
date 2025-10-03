param (
    # command
    [Parameter(Mandatory=$true)]
    [string]$command,

    # port / addr
    [Parameter(Mandatory=$false)]
    [string]$arg1,

    # root path
    [Parameter(Mandatory=$false)]
    [string]$arg2
)

$pidFile = ".\exe-files\extentserver.pid"
$exePath = ".\exe-files\extentserver.exe"

switch ($command) {
    "start" { 
        if (-not $arg1 -or -not $arg2) {
             Write-Host "Usage: .\extentservice.ps1 start <extentPort> <rootPath>"
            exit 1
        }

        # TODO: write regex check

        $extentPort = $arg1
        $rootDirectory = $arg2

        Write-Host "Building Extent Service..."
        & go build -o $exePath .\servers\extent\extentserver.go

        Write-Host "Starting Extent Service on port $extentPort"
        $proc = Start-Process $exePath -ArgumentList $extentPort, $rootDirectory -PassThru
        $pidNum = $proc.Id
        Set-Content -Path $pidFile -Value $pidNum
        Write-Host "Started Lock Service with PID $pidNum"
    }

    "stop" {
        if (-not $arg1) {
            Write-Host "Usage: .\extentservice.ps1 stop <dfsAddr>"
            exit 1
        }

        # TODO: write regex check

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