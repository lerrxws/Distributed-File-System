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

// region public methods

func (s *ExtentServiceServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	fullPath := s.rootpath + req.FileName
	s.logger.Infof("[ExtentServer] Received Get request for: %s", req.FileName)

	data, err := s.handleGet(fullPath)
	if err != nil {
		s.logger.Errorf("[ExtentServer] Failed to get %s: %v", req.FileName, err)
		return nil, err
	}

	s.logger.Infof("[ExtentServer] Returning file data for %s (%d bytes)", req.FileName, len(data))
	return &api.GetResponse{FileData: data}, nil
}

func (s *ExtentServiceServer) Put(ctx context.Context, req *api.PutRequest) (*api.PutResponse, error) {
	fullPath := s.rootpath + req.FileName
	s.logger.Infof("[ExtentServer] Put request for: %s", req.FileName)

	isSuccess, err := s.handlePut(fullPath, req.FileData)
	if err != nil {
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	if !isSuccess {
		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	s.logger.Infof("[ExtentServer] successfully deleted file: %s", fullPath)
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

// endregion

// region private methods

// region get help-methods
func (s *ExtentServiceServer) handleGet(path string) ([]byte, error) {
	if s.isDir(path) {
		return s.handleGetDir(path)
	}
	return s.handleGetFile(path)
}

func (s *ExtentServiceServer) isDir(fullPath string) bool {
	return strings.HasSuffix(fullPath, "/")
}

func (s *ExtentServiceServer) handleGetDir(fullpath string) ([]byte, error) {
	files, err := os.ReadDir(fullpath)
	if err != nil {
		s.logger.Errorf("[ExtentServer] failed to read directory %s: %v", fullpath, err)
		return nil, err
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
	return data, nil
}

func (s *ExtentServiceServer) handleGetFile(fullpath string) ([]byte, error) {
	data, err := os.ReadFile(fullpath)
	if err != nil {
		s.logger.Errorf("[ExtentServer] failed to read file %s: %v", fullpath, err)
		return nil, err
	}

	return data, nil
}

// endregion

// region put help-methods

// endregion
func (s *ExtentServiceServer) handlePut(fullpath string, data []byte) (bool, error) {
	if s.isDir(fullpath) {
		return s.handleDir(fullpath, data)
	}
	return s.handleFile(fullpath, data)
}

func (s *ExtentServiceServer) handleDir(fullpath string, data []byte) (bool, error) {
	if data == nil {
		return s.handleDeleteDir(fullpath)
	}

	return s.handlePutDir(fullpath)
}

func (s *ExtentServiceServer) handleFile(fullpath string, data []byte) (bool, error) {
	if data == nil {
		return s.handleDeleteFile(fullpath)
	}

	return s.handlePutFile(fullpath, data)
}

func (s *ExtentServiceServer) handleDeleteDir(fullpath string) (bool, error) {
	s.logger.Infof("[ExtentServer] attempting to delete directory: %s", fullpath)

	files, _ := os.ReadDir(fullpath)

	if len(files) != 0 {
		s.logger.Warnf("[ExtentServer] directory %s is not empty, cannot delete", fullpath)
		return false, nil
	}

	err := os.Remove(fullpath)
	if err != nil {
		s.logger.Errorf("[ExtentServer] failed to delete directory %s: %v", fullpath, err)
		return false, err
	}

	s.logger.Infof("[ExtentServer] successfully deleted directory: %s", fullpath)
	return true, nil
}

func (s *ExtentServiceServer) handlePutDir(fullpath string) (bool, error) {
	s.logger.Infof("[ExtentServer] creating directory: %s", fullpath)
	err := os.MkdirAll(fullpath, 0755)
	if err != nil {
		s.logger.Errorf("[ExtentServer] failed to create directory %s: %v", fullpath, err)
		return false, err
	}

	s.logger.Infof("[ExtentServer] successfully created directory: %s", fullpath)
	return true, nil
}

func (s *ExtentServiceServer) handleDeleteFile(fullpath string) (bool, error) {
	s.logger.Infof("[ExtentServer] attempting to delete file: %s", fullpath)

	_, err := os.ReadFile(fullpath)
	if err != nil {
		s.logger.Errorf("[ExtentServer] file %s does not exist or cannot be read: %v", fullpath, err)
		return false, err
	}

	err = os.Remove(fullpath)
	if err != nil {
		s.logger.Errorf("[ExtentServer] failed to delete file %s: %v", fullpath, err)
		return false, nil
	}

	s.logger.Infof("[ExtentServer] successfully deleted file: %s", fullpath)
	return true, nil
}

func (s *ExtentServiceServer) handlePutFile(fullpath string, data []byte) (bool, error) {
	s.logger.Infof("[ExtentServer] creating/updating file: %s (%d bytes)", fullpath, len(data))
	dir := filepath.Dir(fullpath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		s.logger.Errorf("[ExtentServer] failed to create parent directories for %s: %v", fullpath, err)
		return false, nil
	}

	err := os.WriteFile(fullpath, data, 0644)
	if err != nil {
		s.logger.Errorf("[ExtentServer] failed to write file %s: %v", fullpath, err)
		return false, nil
	}

	s.logger.Infof("[ExtentServer] successfully wrote file: %s", fullpath)
	return true, nil
}

// endregion
