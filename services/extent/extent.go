package extent

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	api "dfs/proto-gen/extent"

	seelog "github.com/cihub/seelog"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type ExtentServiceServer struct {
	rootpath string
	grpc     *grpc.Server
	logger   seelog.LoggerInterface

	api.UnimplementedExtentServiceServer
}

func NewExtentServiceServer(rootpath string, grpcServer *grpc.Server, logger seelog.LoggerInterface) *ExtentServiceServer {
	s := &ExtentServiceServer{
		rootpath: rootpath,
		grpc:     grpcServer,
		logger:   logger,
	}

	return s
}

func (s *ExtentServiceServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	fullPath := s.rootpath + req.FileName
	s.logger.Infof("[ExtentServer] Get request for: %s", req.FileName)

	if strings.HasSuffix(fullPath, "/") {
		files, err := os.ReadDir(fullPath)
		if err != nil {
			s.logger.Errorf("[ExtentServer] failed to read directory %s: %v", fullPath, err)
			return &api.GetResponse{}, nil
		}

		var fileNames []string
		for _, file := range files {
			name := file.Name()
			if file.IsDir() {
				name += "\\"
			}

			fileNames = append(fileNames, name)
		}

		data := []byte(strings.Join(fileNames, "\n"))
		s.logger.Infof("[ExtentServer] returning directory listing for %s with %d entries", req.FileName, len(fileNames))

		return &api.GetResponse{FileData: data}, nil
	}

	data, err := os.ReadFile(fullPath)
	if err != nil {
		s.logger.Errorf("[ExtentServer] failed to read file %s: %v", fullPath, err)
		return &api.GetResponse{}, nil
	}

	s.logger.Infof("[ExtentServer] returning file data for %s (%d bytes)", req.FileName, len(data))
	return &api.GetResponse{FileData: data}, nil
}

func (s *ExtentServiceServer) Put(ctx context.Context, req *api.PutRequest) (*api.PutResponse, error) {
	fullPath := s.rootpath + req.FileName
	s.logger.Infof("[ExtentServer] Put request for: %s", req.FileName)

	// check whether it is directory
	if strings.HasSuffix(fullPath, "/") {
		files, _ := os.ReadDir(fullPath)

		// if no data -> delete
		if req.FileData == nil {
			s.logger.Infof("[ExtentServer] attempting to delete directory: %s", fullPath)

			// check if directory is empty
			if len(files) != 0 {
				s.logger.Warnf("[ExtentServer] directory %s is not empty, cannot delete", fullPath)
				return &api.PutResponse{Success: proto.Bool(false)}, nil
			}

			err := os.Remove(fullPath)
			if err != nil {
				s.logger.Errorf("[ExtentServer] failed to delete directory %s: %v", fullPath, err)
				return &api.PutResponse{Success: proto.Bool(false)}, nil
			}

			s.logger.Infof("[ExtentServer] successfully deleted directory: %s", fullPath)
			return &api.PutResponse{Success: proto.Bool(true)}, nil
		}

		// create new directory
		s.logger.Infof("[ExtentServer] creating directory: %s", fullPath)
		err := os.MkdirAll(fullPath, 0755)
		if err != nil {
			s.logger.Errorf("[ExtentServer] failed to create directory %s: %v", fullPath, err)
			return &api.PutResponse{Success: proto.Bool(false)}, err
		}

		s.logger.Infof("[ExtentServer] successfully created directory: %s", fullPath)
		return &api.PutResponse{Success: proto.Bool(true)}, nil
	}

	// if type is file
	// if no data -> remove
	if req.FileData == nil {
		s.logger.Infof("[ExtentServer] attempting to delete file: %s", fullPath)

		_, err := os.ReadFile(fullPath)
		if err != nil {
			s.logger.Errorf("[ExtentServer] file %s does not exist or cannot be read: %v", fullPath, err)
			return &api.PutResponse{Success: proto.Bool(false)}, nil
		}

		err = os.Remove(fullPath)
		if err != nil {
			s.logger.Errorf("[ExtentServer] failed to delete file %s: %v", fullPath, err)
			return &api.PutResponse{Success: proto.Bool(false)}, nil
		}

		s.logger.Infof("[ExtentServer] successfully deleted file: %s", fullPath)
		return &api.PutResponse{Success: proto.Bool(true)}, nil
	}

	// create new file
	s.logger.Infof("[ExtentServer] creating/updating file: %s (%d bytes)", fullPath, len(req.FileData))
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		s.logger.Errorf("[ExtentServer] failed to create parent directories for %s: %v", fullPath, err)
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	err := os.WriteFile(fullPath, req.FileData, 0644)
	if err != nil {
		s.logger.Errorf("[ExtentServer] failed to write file %s: %v", fullPath, err)
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	s.logger.Infof("[ExtentServer] successfully wrote file: %s", fullPath)
	return &api.PutResponse{Success: proto.Bool(true)}, nil
}

func (s *ExtentServiceServer) Stop(ctx context.Context, req *api.StopRequest) (*api.StopResponse, error) {
	s.logger.Infof("[ExtentServer] Stop request received, shutting down gracefully")
	go func() {
		// shut down server in a goroutine so we can return the response first
		s.grpc.GracefulStop()
		s.logger.Infof("[ExtentServer] Server stopped")
	}()
	return &api.StopResponse{}, nil
}