# Script starting/stopping Proxy Lock Service (for Windows PowerShell)
#
# Input arguments:
#
# 1. Toggle switch to indicate starting or stopping the service (allowed values: start, stop)
# 2. Port where the service will start (e.g. 5001) or address where to connect and stop the service (e.g. 127.0.0.1:5001)
# 3. Address and port where to connect Replica Lock Service (format is IP:port, e.g. 127.0.0.1:4001)
#
# Examples how to use the script:
#
# .\proxylockservice.ps1 start 5001 127.0.0.1:4001
# .\proxylockservice.ps1 stop 127.0.0.1:5001
