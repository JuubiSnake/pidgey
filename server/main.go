package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	healthv1 "com.pidgey.server/generated/protos/health/v1"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

type server struct {
	healthv1.UnimplementedPidgeyServiceServer
	mu       sync.RWMutex
	watchers map[chan string]struct{}
	buffer   chan string
	payload  string
}

func (s *server) watchBuffer() {
	for {
		payload := <-s.buffer
		s.mu.Lock()
		for ch := range s.watchers {
			ch <- payload
		}
		s.mu.Unlock()
	}
}

func (s *server) UpdateNote(_ context.Context, request *healthv1.UpdateNoteRequest) (*healthv1.UpdateNoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if request.Position > int64(len(s.payload)) {
		return nil, status.Errorf(codes.InvalidArgument, "position out of range")
	}
	s.payload = s.payload[:request.Position] + request.Payload + s.payload[request.Position:]

	s.buffer <- s.payload
	return &healthv1.UpdateNoteResponse{Message: s.payload}, nil
}

func (s *server) WatchNote(in *healthv1.WatchNoteRequest, stream healthv1.PidgeyService_WatchNoteServer) error {
	ch := make(chan string, 1)

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
			if err := stream.Send(&healthv1.WatchNoteResponse{Payload: msg}); err != nil {
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
	g := grpc.NewServer()
	s := &server{
		payload:  "",
		watchers: make(map[chan string]struct{}),
		mu:       sync.RWMutex{},
		buffer:   make(chan string),
	}
	go s.watchBuffer()
	healthv1.RegisterPidgeyServiceServer(g, s)
	reflection.Register(g)
	log.Printf("server listening at %v", lis.Addr())
	if err := g.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
