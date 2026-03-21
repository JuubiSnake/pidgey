package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	healthv1 "com.pidgey.server/generated/protos/health/v1"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

type server struct {
	healthv1.PidgeyServiceServer
}

func (s *server) SayHello(_ context.Context, in *healthv1.SayHelloRequest) (*healthv1.SayHelloResponse, error) {
	return &healthv1.SayHelloResponse{Message: "Hello " + in.Name}, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	healthv1.RegisterPidgeyServiceServer(s, &server{})
	reflection.Register(s)
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
