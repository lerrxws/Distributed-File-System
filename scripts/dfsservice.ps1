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

$portRegex = '^\d{2,5}$'
$addrRegex = '^(?:\d{1,3}\.){3}\d{1,3}:\d+$'

switch ($command) {
    "start" {
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

    "stop" {
        if (-not $arg1) {
            Write-Host "Usage: .\dfsservice.ps1 stop <dfsAddr>"
            exit 1
        }

        $dfsAddr = $arg1
        Write-Host "Stopping DFS Service at $dfsAddr"

        # Піди на адресу $dfsAddr, підключись без TLS, і виклич gRPC метод Stop з сервісу dfs.DfsService. 
        # grpcurl -plaintext $dfsAddr dfs.DfsService/Stop
    }

    Default {
        Write-Host "Unknown command. Use start or stop."
        exit 1
    }
}
