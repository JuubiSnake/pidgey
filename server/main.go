package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	healthv1 "com.pidgey.server/generated/protos/health/v1"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

type server struct {
	healthv1.UnimplementedPidgeyServiceServer
	mu       sync.RWMutex
	watchers map[chan string]struct{}
}

func (s *server) SayHello(_ context.Context, in *healthv1.SayHelloRequest) (*healthv1.SayHelloResponse, error) {
	msg := "Hello " + in.Name

	s.mu.RLock()
	for ch := range s.watchers {
		select {
		case ch <- msg:
		default:
		}
	}
	s.mu.RUnlock()

	return &healthv1.SayHelloResponse{Message: msg}, nil
}

func (s *server) WatchHello(in *healthv1.WatchHelloRequest, stream healthv1.PidgeyService_WatchHelloServer) error {
	ch := make(chan string, 10)

	s.mu.Lock()
	s.watchers[ch] = struct{}{}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.watchers, ch)
		s.mu.Unlock()
	}()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case msg := <-ch:
			if err := stream.Send(&healthv1.WatchHelloResponse{Message: msg}); err != nil {
				return err
			}
		}
	}
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	healthv1.RegisterPidgeyServiceServer(s, &server{watchers: make(map[chan string]struct{}), mu: sync.RWMutex{}})
	reflection.Register(s)
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
