package extent

import (
	"context"
	api "dfs/proto-gen/extent"
	"fmt"

	// "fmt"
	// "log"
	"os"
	"strings"

	"google.golang.org/grpc"
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

func (s *ExtentServer) get(ctx context.Context, req *api.GetRequest) (* api.GetResponse, error){
	fullPath := s.rootpath + req.FileName

	if strings.HasSuffix(fullPath, "/") {
		files, err := os.ReadDir(fullPath)
		if err != nil {
			return &api.GetResponse{FileData: nil}, nil
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
		return &api.GetResponse{FileData: nil}, nil
	}

	return &api.GetResponse{FileData: data}, nil

}

func (s *ExtentServer) put(ctx context.Context, req *api.PutRequest) (* api.PutResponse, error) {
	fullPath := s.rootpath + req.FileName

	// check wether it is directory
	if strings.HasSuffix(fullPath, "/") {
		files, _ := os.ReadDir(fullPath)
		// if err != nil {
		// 	return &api.PutReply{Result: false}, nil
		// }

		// if no data -> delete
		if req.FileData == nil {

			// check if directory is empty
			if len(files) != 0 {
				return &api.PutResponse{Success: false}, nil
			}

			err := os.Remove(fullPath)
			if err != nil {
				return &api.PutResponse{Success: false}, nil
			}

			return &api.PutResponse{Success: true}, nil
		}

		// create new directory
		err := os.MkdirAll(fullPath, 0755) // mkdir -> fails if parents of dir don`t exist so we use mkdirall that creates parents also 
		if err != nil {
			return &api.PutResponse{Success: false}, err
		}
		return &api.PutResponse{Success: true}, nil
	}

	// if type is file
	// if no data -> remove
	if req.FileData == nil {
		_, err := os.ReadFile(fullPath)
		if err != nil {
			return &api.PutResponse{Success: false}, nil
		}

		err = os.Remove(fullPath)
		if err != nil {
			return &api.PutResponse{Success: false}, nil
		}

		return &api.PutResponse{Success: false}, nil
	}

	// create new file
	err := os.WriteFile(fullPath, req.FileData, 0644)
    if err != nil {
        return &api.PutResponse{Success: false}, nil
    }

	return &api.PutResponse{Success: true}, nil

}


func (s *ExtentServer) stop(ctx context.Context, req *api.StopRequest) (*api.StopResponse, error) {
	go func() {
        // shut down server in a goroutine so we can return the response first
        s.grpc.GracefulStop()
    }()
    return &api.StopResponse{}, nil
}