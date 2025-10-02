param (
    # command
    [Parameter(Mandatory=$true)]
    [string]$command,

    # port / addr
    [Parameter(Mandatory=$false)]
    [string]$arg1
)

if (-not $arg1) {
    Write-Host "Usage: .\lockservice.ps1 stop <ARG1>"
    exit 1
}

switch ($command) {
    "start" {
        # TODO: add regex check for port
        $lockPort = $arg1

        Write-Host "Building Lock Service..."
        & go build -o lockserver.exe .\servers\lock\lockserver.go

        Write-Host "Starting Lock Service on port $lockPort"
        & .\lockserver.exe $lockPort
    }

    "stop" {
        # TODO: write regex check - addr:port

        $lockAddr = $arg1

        Write-Host "Stopping Lock Service at $lockAddr"

        #Start-Process -NoNewWindow -FilePath "evans" -ArgumentList "-r", "-p", $port, "-s", "lock.LockServiceServer", "-c", "Stop"
    }

    Default {
        Write-Host "Unknown command. Use start or stop."
        exit 1
    }
}