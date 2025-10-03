# Script starting/stopping Replica Lock Service (for Windows PowerShell)
#
# Input arguments:
#
# 1. Toggle switch to indicate starting or stopping the service (allowed values: start, stop)
# 2. Port where the service will start (e.g. 4001) or address where to connect and stop the service (e.g. 127.0.0.1:4001)
# 3. Address and port where to connect Lock Service (format is IP:port, e.g. 127.0.0.1:1001)
# 4. Either "noprimary" or address and port where to connect primary Replica Lock Service (format is IP:port, e.g. 127.0.0.1:4001)
#
# Examples how to use the script:
#
# .\replicalockservice.ps1 start 4001 127.0.0.1:1001 noprimary
# .\replicalockservice.ps1 start 4002 127.0.0.1:1002 127.0.0.1:4001
# .\replicalockservice.ps1 stop 127.0.0.1:4001
