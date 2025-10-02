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
        & go build -o extentserver.exe .\servers\extent\extentserver.go

        Write-Host "Starting Extent Service on port $extentPort"
        & .\extentserver.exe $extentPort, $rootDirectory
    }

    "stop" {
        if (-not $arg1) {
            Write-Host "Usage: .\extentservice.ps1 stop <dfsAddr>"
            exit 1
        }

        # TODO: write regex check

        $extentAddr = $arg1
        Write-Host "Stopping Extent Service at $extentAddr"

        grpcurl -plaintext $extentAddr extent.ExtentServiceServer/Stop
    }
    Default {
        Write-Host "Unknown command. Use start or stop."
        exit 1
    }
}