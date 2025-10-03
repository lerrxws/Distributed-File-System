package extent

import (
	"context"
	api "dfs/proto-gen/extent"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)


type ExtentServiceServer struct {
	rootpath string
	grpc  *grpc.Server

	api.UnimplementedExtentServiceServer
}

func NewExtentServiceServer(rootpath string, grpcServer *grpc.Server) *ExtentServiceServer {
    s := &ExtentServiceServer{
        rootpath: rootpath,
        grpc:     grpcServer,
    }

	return s
}

func (s *ExtentServiceServer) Get(ctx context.Context, req *api.GetRequest) (* api.GetResponse, error){
	fullPath := s.rootpath + req.FileName

	if strings.HasSuffix(fullPath, "/") {
		files, err := os.ReadDir(fullPath)
		if err != nil {
			return &api.GetResponse{}, nil
		}

		var fileNames []string
		for _,file := range files {
			name := file.Name()
			if file.IsDir() {
				name += "/"
			}

			fileNames = append(fileNames, name)
		}

		data := []byte(strings.Join(fileNames, "\n"))

		return &api.GetResponse{FileData: data}, nil
	}

	data, err := os.ReadFile(fullPath)
	if err != nil {
		return &api.GetResponse{}, nil
	}

	return &api.GetResponse{FileData: data}, nil

}

func (s *ExtentServiceServer) Put(ctx context.Context, req *api.PutRequest) (* api.PutResponse, error) {
	fullPath := s.rootpath + req.FileName

	// check wether it is directory
	if strings.HasSuffix(fullPath, "/") {
		files, _ := os.ReadDir(fullPath)

		// if no data -> delete
		if req.FileData == nil {

			// check if directory is empty
			if len(files) != 0 {
				return &api.PutResponse{Success: proto.Bool(false)}, nil
			}

			err := os.Remove(fullPath)
			if err != nil {
				return &api.PutResponse{Success: proto.Bool(false)}, nil
			}

			return &api.PutResponse{Success: proto.Bool(true)}, nil
		}

		// create new directory
		err := os.MkdirAll(fullPath, 0755) // mkdir -> fails if parents of dir don`t exist so we use mkdirall that creates parents also 
		if err != nil {
			return &api.PutResponse{Success: proto.Bool(false)}, err
		}
		return &api.PutResponse{Success: proto.Bool(true)}, nil
	}

	// if type is file
	// if no data -> remove
	if req.FileData == nil {
		_, err := os.ReadFile(fullPath)
		if err != nil {
			return &api.PutResponse{Success: proto.Bool(false)}, nil
		}

		err = os.Remove(fullPath)
		if err != nil {
			return &api.PutResponse{Success: proto.Bool(false)}, nil
		}

		return &api.PutResponse{Success: proto.Bool(false)}, nil
	}

	// create new file
	err := os.WriteFile(fullPath, req.FileData, 0644)
    if err != nil {
        return &api.PutResponse{Success: proto.Bool(false)}, nil
    }

	return &api.PutResponse{Success: proto.Bool(true)}, nil

}


func (s *ExtentServiceServer) Stop(ctx context.Context, req *api.StopRequest) (*api.StopResponse, error) {
	go func() {
        // shut down server in a goroutine so we can return the response first
        s.grpc.GracefulStop()
    }()
    return &api.StopResponse{}, nil
}