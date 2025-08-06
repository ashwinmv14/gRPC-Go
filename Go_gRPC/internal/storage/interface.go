package storage

import (
	"context"
	"time"

	pb "ad-event-processor/proto"
)

// Storage defines the interface for event storage backends
type Storage interface {
	// Store stores a batch of ad events
	Store(ctx context.Context, events []*pb.AdEvent) error
	
	// GetEvents retrieves events based on query parameters
	GetEvents(ctx context.Context, query *Query) ([]*pb.AdEvent, error)
	
	// GetEventCount returns the total count of events matching the query
	GetEventCount(ctx context.Context, query *Query) (int64, error)
	
	// Health checks the health of the storage backend
	Health(ctx context.Context) error
	
	// Close closes the storage connection
	Close() error
}

// Query represents query parameters for retrieving events
type Query struct {
	// Time range filters
	StartTime *time.Time
	EndTime   *time.Time
	
	// User and Ad filters
	UserID string
	AdID   string
	
	// Event type filter
	EventType pb.EventType
	
	// Pagination
	Limit  int
	Offset int
	
	// Sorting
	SortBy    string // timestamp, user_id, ad_id, event_type
	SortOrder string // asc, desc
}

// StorageMetrics contains metrics about storage operations
type StorageMetrics struct {
	TotalEvents      int64
	EventsPerSecond  float64
	ErrorRate        float64
	AverageLatency   time.Duration
	LastUpdated      time.Time
	StorageSize      int64 // in bytes
	ConnectionStatus string
}