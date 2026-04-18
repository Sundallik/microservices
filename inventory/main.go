package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/brianvoe/gofakeit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	inventoryV1 "github.com/Sundallik/microservices/shared/pkg/proto/inventory/v1"
)

const grpcPort = 50051

type inventoryService struct {
	inventoryV1.UnimplementedInventoryServiceServer

	mu    sync.RWMutex
	parts map[string]*inventoryV1.Part
}

func (s *inventoryService) GetPart(_ context.Context, req *inventoryV1.GetPartRequest) (*inventoryV1.GetPartResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	part, ok := s.parts[req.GetUuid()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "part with UUID %s not found", req.GetUuid())
	}

	return &inventoryV1.GetPartResponse{
		Part: part,
	}, nil
}

func (s *inventoryService) ListParts(_ context.Context, req *inventoryV1.ListPartsRequest) (*inventoryV1.ListPartsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*inventoryV1.Part
	filters := req.GetFilter()

	for _, part := range s.parts {
		if s.matches(part, filters) {
			result = append(result, part)
		}
	}

	return &inventoryV1.ListPartsResponse{Parts: result}, nil
}

func (s *inventoryService) matches(p *inventoryV1.Part, req *inventoryV1.PartsFilter) bool {
	if !s.matchField(p.GetUuid(), req.GetUuids()) {
		return false
	}
	if !s.matchField(p.GetName(), req.GetNames()) {
		return false
	}

	if !s.matchCategory(p.GetCategory(), req.GetCategories()) {
		return false
	}

	if len(req.GetManufacturerCountries()) > 0 {
		if p.GetManufacturer() == nil || !s.matchField(p.GetManufacturer().GetCountry(), req.GetManufacturerCountries()) {
			return false
		}
	}

	if !s.matchTags(p.GetTags(), req.GetTags()) {
		return false
	}

	return true
}

func (s *inventoryService) matchField(val string, filter []string) bool {
	if len(filter) == 0 {
		return true
	}
	for _, f := range filter {
		if val == f {
			return true
		}
	}
	return false
}

func (s *inventoryService) matchCategory(val inventoryV1.Category, filter []inventoryV1.Category) bool {
	if len(filter) == 0 {
		return true
	}
	for _, f := range filter {
		if val == f {
			return true
		}
	}
	return false
}

func (s *inventoryService) matchTags(partTags, filterTags []string) bool {
	if len(filterTags) == 0 {
		return true
	}
	for _, ft := range filterTags {
		for _, pt := range partTags {
			if ft == pt {
				return true
			}
		}
	}
	return false
}

func generateParts() map[string]*inventoryV1.Part {
	result := make(map[string]*inventoryV1.Part)

	allCategories := getAllCategories()

	number := gofakeit.Number(5, 10)

	for i := 0; i < number; i++ {
		id := gofakeit.UUID()
		result[id] = &inventoryV1.Part{
			Uuid:          id,
			Name:          gofakeit.BeerName(),
			Description:   gofakeit.Sentence(10),
			Price:         gofakeit.Price(10.0, 1000.0),
			StockQuantity: int64(gofakeit.Number(0, 500)),
			Category:      allCategories[gofakeit.Number(0, len(allCategories)-1)],
			Dimensions: &inventoryV1.Dimensions{
				Length: gofakeit.Float64Range(1, 100),
				Width:  gofakeit.Float64Range(1, 100),
				Height: gofakeit.Float64Range(1, 100),
				Weight: gofakeit.Float64Range(1, 100),
			},
			Manufacturer: &inventoryV1.Manufacturer{
				Name:    gofakeit.Company(),
				Country: gofakeit.Country(),
				Website: gofakeit.Company() + ".net",
			},
			Tags: []string{gofakeit.Word(), gofakeit.Word(), gofakeit.Word()},
			Metadata: &inventoryV1.Part_StringValue{
				StringValue: gofakeit.Color(),
			},
			CreatedAt: timestamppb.New(gofakeit.Date()),
			UpdatedAt: timestamppb.Now(),
		}
	}

	return result
}

func getAllCategories() []inventoryV1.Category {
	values := make([]inventoryV1.Category, 0, len(inventoryV1.Category_value))
	for _, v := range inventoryV1.Category_value {
		if v != 0 {
			values = append(values, inventoryV1.Category(v))
		}
	}
	return values
}

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v\n", err)
		return
	}
	defer func() {
		if cerr := lis.Close(); cerr != nil {
			log.Fatalf("failed to close listener: %v\n", cerr)
		}
	}()

	s := grpc.NewServer()

	service := &inventoryService{
		parts: generateParts(),
	}

	inventoryV1.RegisterInventoryServiceServer(s, service)

	reflection.Register(s)

	go func() {
		log.Printf("Starting gRPC server on port %d\n", grpcPort)
		err := s.Serve(lis)
		if err != nil {
			log.Fatalf("failed to serve: %v\n", err)
			return
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Printf("Shutting down gRPC server on port %d\n", grpcPort)
	s.GracefulStop()
	log.Printf("gRPC server stopped")
}
