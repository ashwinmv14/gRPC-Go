package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"ad-event-processor/internal/config"
	"ad-event-processor/internal/processor"
	"ad-event-processor/internal/storage"
	pb "ad-event-processor/proto"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GRPCServer represents the gRPC server for ad event processing
type GRPCServer struct {
	pb.UnimplementedAdEventServiceServer
	
	config          *config.Config
	logger          *logrus.Logger
	batchProcessor  *processor.BatchProcessor
	storage         storage.Storage
	server          *grpc.Server
	listener        net.Listener
}

// NewGRPCServer creates a new gRPC server instance
func NewGRPCServer(
	config *config.Config,
	logger *logrus.Logger,
	batchProcessor *processor.BatchProcessor,
	storage storage.Storage,
) *GRPCServer {
	return &GRPCServer{
		config:         config,
		logger:         logger,
		batchProcessor: batchProcessor,
		storage:        storage,
	}
}

// Start starts the gRPC server
func (s *GRPCServer) Start(ctx context.Context) error {
	// Create listener
	var err error
	s.listener, err = net.Listen("tcp", s.config.GetAddress())
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.config.GetAddress(), err)
	}

	// Configure gRPC server options
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(s.config.Server.MaxReceiveSize),
		grpc.MaxSendMsgSize(s.config.Server.MaxSendSize),
		grpc.ConnectionTimeout(s.config.Server.ConnectionTimeout),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    s.config.Server.KeepAliveTime,
			Timeout: s.config.Server.KeepAliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.UnaryInterceptor(s.loggingInterceptor),
	}

	// Create gRPC server
	s.server = grpc.NewServer(opts...)
	
	// Register service
	pb.RegisterAdEventServiceServer(s.server, s)

	s.logger.WithField("address", s.config.GetAddress()).Info("Starting gRPC server")

	// Start server in a goroutine
	go func() {
		if err := s.server.Serve(s.listener); err != nil {
			s.logger.WithError(err).Error("gRPC server error")
		}
	}()

	return nil
}

// Stop gracefully stops the gRPC server
func (s *GRPCServer) Stop(ctx context.Context) error {
	s.logger.Info("Stopping gRPC server...")

	// Create a channel to signal when graceful stop is complete
	done := make(chan struct{})
	
	go func() {
		s.server.GracefulStop()
		close(done)
	}()

	// Wait for graceful stop or timeout
	select {
	case <-done:
		s.logger.Info("gRPC server stopped gracefully")
	case <-time.After(s.config.Server.GracefulTimeout):
		s.logger.Warn("Graceful stop timeout, forcing shutdown")
		s.server.Stop()
	case <-ctx.Done():
		s.logger.Warn("Context cancelled, forcing shutdown")
		s.server.Stop()
	}

	return nil
}

// SendAdEvent handles single ad event submissions
func (s *GRPCServer) SendAdEvent(ctx context.Context, req *pb.SendAdEventRequest) (*pb.SendAdEventResponse, error) {
	if req.Event == nil {
		return nil, status.Errorf(codes.InvalidArgument, "event is required")
	}

	// Validate and enrich the event
	if err := s.validateAndEnrichEvent(req.Event); err != nil {
		s.logger.WithError(err).WithField("event_id", req.Event.EventId).Warn("Invalid event received")
		return &pb.SendAdEventResponse{
			Success: false,
			Message: fmt.Sprintf("validation failed: %v", err),
			EventId: req.Event.EventId,
		}, nil
	}

	// Process the event through batch processor
	if err := s.batchProcessor.ProcessEvent(ctx, req.Event); err != nil {
		s.logger.WithError(err).WithField("event_id", req.Event.EventId).Error("Failed to process event")
		return &pb.SendAdEventResponse{
			Success: false,
			Message: fmt.Sprintf("processing failed: %v", err),
			EventId: req.Event.EventId,
		}, nil
	}

	s.logger.WithFields(logrus.Fields{
		"event_id":   req.Event.EventId,
		"user_id":    req.Event.UserId,
		"ad_id":      req.Event.AdId,
		"event_type": req.Event.EventType.String(),
	}).Debug("Event accepted for processing")

	return &pb.SendAdEventResponse{
		Success: true,
		Message: "Event accepted for processing",
		EventId: req.Event.EventId,
	}, nil
}

// SendAdEventBatch handles batch ad event submissions
func (s *GRPCServer) SendAdEventBatch(ctx context.Context, req *pb.SendAdEventBatchRequest) (*pb.SendAdEventBatchResponse, error) {
	if len(req.Events) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "at least one event is required")
	}

	var processedCount int32
	var failedEventIds []string

	// Process each event in the batch
	for _, event := range req.Events {
		// Validate and enrich the event
		if err := s.validateAndEnrichEvent(event); err != nil {
			s.logger.WithError(err).WithField("event_id", event.EventId).Warn("Invalid event in batch")
			failedEventIds = append(failedEventIds, event.EventId)
			continue
		}

		// Process the event through batch processor
		if err := s.batchProcessor.ProcessEvent(ctx, event); err != nil {
			s.logger.WithError(err).WithField("event_id", event.EventId).Error("Failed to process event in batch")
			failedEventIds = append(failedEventIds, event.EventId)
			continue
		}

		processedCount++
	}

	success := len(failedEventIds) == 0
	message := fmt.Sprintf("Processed %d out of %d events", processedCount, len(req.Events))
	
	if !success {
		message += fmt.Sprintf(", %d failed", len(failedEventIds))
	}

	s.logger.WithFields(logrus.Fields{
		"total_events":     len(req.Events),
		"processed_count":  processedCount,
		"failed_count":     len(failedEventIds),
	}).Info("Batch processing completed")

	return &pb.SendAdEventBatchResponse{
		Success:        success,
		Message:        message,
		ProcessedCount: processedCount,
		FailedEventIds: failedEventIds,
	}, nil
}

// HealthCheck provides health check functionality
func (s *GRPCServer) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	details := make(map[string]string)
	healthy := true

	// Check batch processor health
	if !s.batchProcessor.IsRunning() {
		healthy = false
		details["batch_processor"] = "not running"
	} else {
		stats := s.batchProcessor.GetStats()
		details["batch_processor"] = "running"
		details["events_processed"] = fmt.Sprintf("%d", stats.EventsProcessed)
		details["current_batch_size"] = fmt.Sprintf("%d", s.batchProcessor.GetCurrentBatchSize())
	}

	// Check storage health
	if err := s.storage.Health(ctx); err != nil {
		healthy = false
		details["storage"] = fmt.Sprintf("unhealthy: %v", err)
	} else {
		details["storage"] = "healthy"
	}

	// Overall status
	status := "healthy"
	if !healthy {
		status = "unhealthy"
	}

	return &pb.HealthCheckResponse{
		Healthy: healthy,
		Status:  status,
		Details: details,
	}, nil
}

// validateAndEnrichEvent validates and enriches an ad event
func (s *GRPCServer) validateAndEnrichEvent(event *pb.AdEvent) error {
	// Generate event ID if not provided
	if event.EventId == "" {
		event.EventId = generateEventID()
	}

	// Set timestamp if not provided
	if event.Timestamp == nil {
		event.Timestamp = timestamppb.Now()
	}

	// Validate required fields
	if event.UserId == "" {
		return fmt.Errorf("user_id is required")
	}

	if event.AdId == "" {
		return fmt.Errorf("ad_id is required")
	}

	if event.EventType == pb.EventType_EVENT_TYPE_UNSPECIFIED {
		return fmt.Errorf("event_type is required")
	}

	// Validate timestamp is not too old or in the future
	now := time.Now()
	eventTime := event.Timestamp.AsTime()
	
	if eventTime.After(now.Add(5 * time.Minute)) {
		return fmt.Errorf("event timestamp is too far in the future")
	}
	
	if eventTime.Before(now.Add(-24 * time.Hour)) {
		return fmt.Errorf("event timestamp is too old")
	}

	// Initialize metadata if nil
	if event.Metadata == nil {
		event.Metadata = make(map[string]string)
	}

	// Add server-side metadata
	event.Metadata["server_received_at"] = now.Format(time.RFC3339)
	event.Metadata["server_version"] = "1.0.0"

	return nil
}

// generateEventID generates a unique event ID
func generateEventID() string {
	// In a production system, you might use UUID or a more sophisticated ID generation
	return fmt.Sprintf("evt_%d", time.Now().UnixNano())
}

// loggingInterceptor provides request logging for gRPC calls
func (s *GRPCServer) loggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()
	
	// Call the actual handler
	resp, err := handler(ctx, req)
	
	duration := time.Since(start)
	
	// Log the request
	fields := logrus.Fields{
		"method":      info.FullMethod,
		"duration_ms": duration.Milliseconds(),
	}
	
	if err != nil {
		fields["error"] = err.Error()
		s.logger.WithFields(fields).Error("gRPC request failed")
	} else {
		s.logger.WithFields(fields).Debug("gRPC request completed")
	}
	
	return resp, err
}