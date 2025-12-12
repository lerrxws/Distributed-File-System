package main

import (
	"fmt"
	"net"
	"os"

	replica "dfs/services/replica"

	paxosApi "dfs/proto-gen/paxos"
	replicaApi "dfs/proto-gen/replica"

	seelog "github.com/cihub/seelog"
	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) < 2 {
		seelog.Critical("[Main] Usage: go run lockserver.go <port>")
		os.Exit(1)
	}

	port := os.Args[1]

	primaryAddr := ""
	if len(os.Args) == 3{
		primaryAddr = os.Args[2]
	}

	logger, err := createLoggerWithPortNumber(port)

	if err != nil {
		seelog.Criticalf("[Main] Failed to load seelog config: %v", err)
		os.Exit(1)
	}
	defer seelog.Flush()

	s := grpc.NewServer()

	addr := "127.0.0.1:" + port

	srv := replica.NewReplicaServiceServer(s, addr, logger, primaryAddr)
	replicaApi.RegisterReplicaServiceServer(s, srv)
	paxosApi.RegisterPaxosServiceServer(s, srv)
	

	portStr := ":" + port
	lis, err := net.Listen("tcp", portStr)
	if err != nil {
		logger.Criticalf("[Main] Failed to listen on %s: %v", portStr, err)
		os.Exit(1)
	}

	logger.Infof("[Main] LockProxyServer is running on port %s", portStr)

	if err := s.Serve(lis); err != nil {
		logger.Criticalf("[Main] Failed to serve gRPC: %v", err)
		os.Exit(1)
	}

	logger.Infof("[Main] LockProxyServer shutdown completed.")
}

func createLoggerWithPortNumber(port string) (seelog.LoggerInterface, error) {
	seelogConfig := fmt.Sprintf(`
	<seelog minlevel="info">
		<outputs formatid="main">
			<console/>
			<rollingfile type="size" filename="logs/replica/replica_%s.log" maxsize="1000000" maxrolls="5"/>
		</outputs>
		<formats>
			<format id="main" format="%%Date %%Time [%%LEVEL] %%Msg%%n"/>
		</formats>
	</seelog>
	`, port)

	configBytes := []byte(seelogConfig)

	return seelog.LoggerFromConfigAsBytes(configBytes)
}