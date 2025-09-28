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

type ExtentServer struct {
	rootpath string
	grpc  *grpc.Server

	api.UnimplementedExtentServer
}

func NewExtentServer(rootpath string, grpcServer *grpc.Server) *ExtentServer {
    s := &ExtentServer{
        rootpath: rootpath,
        grpc:     grpcServer,
    }

	return s
}

func (s *ExtentServer) Get(ctx context.Context, req *api.GetRequest) (* api.GetReply, error){
	fullPath := s.rootpath + req.FileName

	if strings.HasSuffix(fullPath, "/") {
		files, err := os.ReadDir(fullPath)
		if err != nil {
			return &api.GetReply{Data: nil}, nil
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

		return &api.GetReply{Data: data}, nil
	}

	data, err := os.ReadFile(fullPath)
	if err != nil {
		return &api.GetReply{Data: nil}, nil
	}

	return &api.GetReply{Data: data}, nil

}

func (s *ExtentServer) Put(ctx context.Context, req *api.PutRequest) (* api.PutReply, error) {
	fullPath := s.rootpath + req.FileName

	fmt.Println("here")
	fmt.Println(fullPath)

	// check wether it is directory
	if strings.HasSuffix(fullPath, "/") {
		files, _ := os.ReadDir(fullPath)
		// if err != nil {
		// 	return &api.PutReply{Result: false}, nil
		// }

		fmt.Println("here")

		// if no data -> delete
		if req.Data == nil {

			// check if directory is empty
			if len(files) != 0 {
				fmt.Println("have files")
				return &api.PutReply{Result: false}, nil
			}

			err := os.Remove(fullPath)
			if err != nil {
				return &api.PutReply{Result: false}, nil
			}

			return &api.PutReply{Result: true}, nil
		}

		// create new directory
		err := os.MkdirAll(fullPath, 0755) // mkdir -> fails if parents of dir don`t exist so we use mkdirall that creates parents also 
		if err != nil {
			return &api.PutReply{Result: false}, err
		}
		return &api.PutReply{Result: true}, nil
	}

	// if type is file
	// if no data -> remove
	if req.Data == nil {
		err := os.Remove(fullPath)
		if err != nil {
			return &api.PutReply{Result: false}, nil
		}

		return &api.PutReply{Result: true}, nil
	}

	// create new file
	err := os.WriteFile(fullPath, req.Data, 0644)
    if err != nil {
        return &api.PutReply{Result: false}, err
    }

	return &api.PutReply{Result: true}, err

}


func (s *ExtentServer) Stop(ctx context.Context, req *api.StopRequest) (*api.StopReply, error) {
	go func() {
        // shut down server in a goroutine so we can return the response first
        s.grpc.GracefulStop()
    }()
    return &api.StopReply{Result: true}, nil
}