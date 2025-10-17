package dfs

import (
	"context"
	api "dfs/proto-gen/dfs"
	extent "dfs/proto-gen/extent"
	lock "dfs/proto-gen/lock"
	"dfs/services/dfs/lockcache"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// todo : ось тут userid - це ip:port:userid
// todo : додати releaser в сервіс
type DfsServiceServer struct {
	clientID         string              
	localLockHandler *lockcache.LocalLockHandler
	localCache 		 *lockcache.LockCache
	releaser 	  	 *Releaser
	lockClient   	 lock.LockServiceClient
	extentClient     extent.ExtentServiceClient 
	grpc             *grpc.Server 

	api.UnimplementedDfsServiceServer
}

func NewDfsServiceServer(port, lockAddr, extentAddr string, grpcServer *grpc.Server) (*DfsServiceServer, error) {
	clientID := getClientID(port)

	lockConn, err := grpc.NewClient(lockAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil
	}

	lockClient := lock.NewLockServiceClient(lockConn)

	extentConn, err := grpc.NewClient(extentAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil
	}

	extentClient := extent.NewExtentServiceClient(extentConn)

	lockCache := lockcache.NewLockCache()
	localHandler := lockcache.NewLocalLockHandler(lockCache, lockClient)

	releaser := newReleaser(lockCache, lockClient)

	return &DfsServiceServer{
		clientID:         clientID,
		localLockHandler: localHandler,
		localCache:       lockCache,
		releaser: 		  releaser,
		lockClient:       lockClient,
		extentClient:     extentClient,
		grpc:             grpcServer,
	}, nil
}

func getClientID(port string) string {
	host, _ := os.Hostname()

	ip := "127.0.0.1" // fallback
	ifaces, err := net.Interfaces()
	if err == nil {
		for _, iface := range ifaces {
			if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
				continue
			}
			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				var ipAddr net.IP
				switch v := addr.(type) {
				case *net.IPNet:
					ipAddr = v.IP
				case *net.IPAddr:
					ipAddr = v.IP
				}

				if ipAddr == nil || ipAddr.IsLoopback() {
					continue
				}
				ipAddr = ipAddr.To4()
				if ipAddr == nil {
					continue
				}
				ip = ipAddr.String()
				break
			}
		}
	}


	return fmt.Sprintf("%s:%s:%s", ip, port, host)
}

func (s *DfsServiceServer) acquireLock(ctx context.Context, lockId string) error {
	lockResp := s.localLockHandler.AcquireLock(lockId, s.clientID)
	if !lockResp {
		log.Printf("Failed to acquire lock %s\n", lockId)
	}
	return nil
}

func (s *DfsServiceServer) releaseLock(ctx context.Context, lockId string) {
	s.localLockHandler.ReleaseLock(lockId, s.clientID)
}

// todo : ось тут треба не забути вбивати і releaser
func (s *DfsServiceServer) Stop(ctx context.Context, req *api.StopRequest) (*api.StopResponse, error) {
	go func() {
		s.grpc.GracefulStop()
	}()
	return &api.StopResponse{}, nil
}

func (s *DfsServiceServer) Dir(ctx context.Context, req *api.DirRequest) (*api.DirResponse, error) {
	if !strings.HasSuffix(req.DirectoryName, "/") {
		return &api.DirResponse{Success: proto.Bool(false)}, nil
	}

	if req.DirectoryName == "" {
		return &api.DirResponse{Success: proto.Bool(false)}, nil
	}

	err := s.acquireLock(ctx, req.DirectoryName)
	if err != nil {
		return &api.DirResponse{Success: proto.Bool(false)}, nil
	}
	defer s.releaseLock(ctx, req.DirectoryName)

	resp, err := s.extentClient.Get(ctx, &extent.GetRequest{
		FileName: req.DirectoryName,
	})
	if err != nil {
		return &api.DirResponse{Success: proto.Bool(false)}, nil
	}
	if resp == nil {
		return &api.DirResponse{Success: proto.Bool(false)}, nil
	}

	fileDataString := string(resp.FileData)
	fileDataList := strings.Split(fileDataString, "\n")
	return &api.DirResponse{Success: proto.Bool(true), DirList: fileDataList}, nil
}


func (s *DfsServiceServer) Mkdir(ctx context.Context, req *api.MkdirRequest) (*api.MkdirResponse, error) {
	if req.DirectoryName == "" {
		return &api.MkdirResponse{Success: proto.Bool(false)}, nil
	}

	if !strings.HasSuffix(req.DirectoryName, "/") {
		return &api.MkdirResponse{Success: proto.Bool(false)}, nil
	}

	err := s.acquireLock(ctx, req.DirectoryName)
	if err != nil {
		return &api.MkdirResponse{Success: proto.Bool(false)}, nil
	}
	defer s.releaseLock(ctx, req.DirectoryName)

	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{
		FileName: req.DirectoryName,
		FileData: []byte{},
	})
	if err != nil || !(*resp.Success) {
		return &api.MkdirResponse{Success: proto.Bool(false)}, nil
	}

	return &api.MkdirResponse{Success: proto.Bool(true)}, nil
}

func (s *DfsServiceServer) Rmdir(ctx context.Context, req *api.RmdirRequest) (*api.RmdirResponse, error) {
	if req.DirectoryName == "" {
		return &api.RmdirResponse{Success: proto.Bool(false)}, nil
	}

	if !strings.HasSuffix(req.DirectoryName, "/") {
		return &api.RmdirResponse{Success: proto.Bool(false)}, nil
	}

	err := s.acquireLock(ctx, req.DirectoryName)
	if err != nil {
		return &api.RmdirResponse{Success: proto.Bool(false)}, nil
	}
	defer s.releaseLock(ctx, req.DirectoryName)

	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{FileName: req.DirectoryName})
	if err != nil || !(*resp.Success) {
		return &api.RmdirResponse{Success: proto.Bool(false)}, nil
	}

	return &api.RmdirResponse{Success: proto.Bool(true)}, nil
}

func (s *DfsServiceServer) Put(ctx context.Context, req *api.PutRequest) (*api.PutResponse, error) {
	if req.FileName == "" {
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	if strings.HasSuffix(req.FileName, "/") {
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	err := s.acquireLock(ctx, req.FileName)
	if err != nil {
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}
	defer s.releaseLock(ctx, req.FileName)

	if req.FileData == nil {
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{FileName: req.FileName, FileData: req.FileData})
	if err != nil || !(*resp.Success) {
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	return &api.PutResponse{Success: proto.Bool(true)}, nil
}

func (s *DfsServiceServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	if req.FileName == "" {
		return &api.GetResponse{}, nil
	}

	if strings.HasSuffix(req.FileName, "/") {
		return &api.GetResponse{}, nil
	}

	err := s.acquireLock(ctx, req.FileName)
	if err != nil {
		return &api.GetResponse{}, nil
	}
	defer s.releaseLock(ctx, req.FileName)

	resp, err := s.extentClient.Get(ctx, &extent.GetRequest{FileName: req.FileName})
	if (resp == &extent.GetResponse{}) || err != nil {
		return &api.GetResponse{}, nil
	}

	return &api.GetResponse{FileData: resp.FileData}, nil

}

func (s *DfsServiceServer) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	if req.FileName == "" {
		return &api.DeleteResponse{Success: proto.Bool(false)}, nil
	}

	if strings.HasSuffix(req.FileName, "/") {
		return &api.DeleteResponse{Success: proto.Bool(false)}, nil
	}

	err := s.acquireLock(ctx,  req.FileName)
	if err != nil {
		return &api.DeleteResponse{Success: proto.Bool(false)}, nil
	}
	defer s.releaseLock(ctx, req.FileName)

	resp, err := s.extentClient.Put(ctx, &extent.PutRequest{FileName: req.FileName})
	if err != nil || !(*resp.Success) {
		return &api.DeleteResponse{Success: proto.Bool(false)}, nil
	}

	return &api.DeleteResponse{Success: proto.Bool(true)}, nil
}
