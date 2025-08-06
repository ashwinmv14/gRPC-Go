package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	pb "ad-event-processor/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
)

// FileStorage implements Storage interface using local file system
type FileStorage struct {
	basePath   string
	logger     *logrus.Logger
	mu         sync.RWMutex
	marshaler  *protojson.MarshalOptions
	fileIndex  map[string]*fileInfo
	totalEvents int64
}

type fileInfo struct {
	path      string
	eventCount int64
	lastWrite time.Time
}

// NewFileStorage creates a new file-based storage instance
func NewFileStorage(basePath string, logger *logrus.Logger) (*FileStorage, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	fs := &FileStorage{
		basePath:  basePath,
		logger:    logger,
		marshaler: &protojson.MarshalOptions{
			Multiline:       false,
			Indent:          "",
			AllowPartial:    false,
			UseProtoNames:   true,
			UseEnumNumbers:  false,
			EmitUnpopulated: false,
		},
		fileIndex: make(map[string]*fileInfo),
	}

	// Initialize file index
	if err := fs.buildFileIndex(); err != nil {
		return nil, fmt.Errorf("failed to build file index: %w", err)
	}

	return fs, nil
}

// Store stores a batch of ad events to files organized by date
func (fs *FileStorage) Store(ctx context.Context, events []*pb.AdEvent) error {
	if len(events) == 0 {
		return nil
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Group events by date for efficient file organization
	eventsByDate := make(map[string][]*pb.AdEvent)
	
	for _, event := range events {
		if event.Timestamp == nil {
			continue
		}
		
		timestamp := event.Timestamp.AsTime()
		dateKey := timestamp.Format("2006-01-02")
		eventsByDate[dateKey] = append(eventsByDate[dateKey], event)
	}

	// Write events to respective date files
	for dateKey, dateEvents := range eventsByDate {
		if err := fs.writeEventsToFile(ctx, dateKey, dateEvents); err != nil {
			fs.logger.WithError(err).Errorf("Failed to write events for date %s", dateKey)
			return err
		}
	}

	fs.totalEvents += int64(len(events))
	
	fs.logger.WithField("event_count", len(events)).Debug("Successfully stored events to file storage")
	return nil
}

// writeEventsToFile writes events to a specific date file
func (fs *FileStorage) writeEventsToFile(ctx context.Context, dateKey string, events []*pb.AdEvent) error {
	filename := fmt.Sprintf("events_%s.jsonl", dateKey)
	filePath := filepath.Join(fs.basePath, filename)

	// Open file for appending
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	// Write each event as a JSON line
	for _, event := range events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Convert protobuf to JSON
		jsonData, err := fs.marshaler.Marshal(event)
		if err != nil {
			fs.logger.WithError(err).WithField("event_id", event.EventId).Warn("Failed to marshal event")
			continue
		}

		// Write JSON line
		if _, err := file.WriteString(string(jsonData) + "\n"); err != nil {
			return fmt.Errorf("failed to write event to file: %w", err)
		}
	}

	// Update file index
	if info, exists := fs.fileIndex[dateKey]; exists {
		info.eventCount += int64(len(events))
		info.lastWrite = time.Now()
	} else {
		fs.fileIndex[dateKey] = &fileInfo{
			path:       filePath,
			eventCount: int64(len(events)),
			lastWrite:  time.Now(),
		}
	}

	return nil
}

// GetEvents retrieves events based on query parameters
func (fs *FileStorage) GetEvents(ctx context.Context, query *Query) ([]*pb.AdEvent, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	var allEvents []*pb.AdEvent
	
	// Determine which files to read based on time range
	filesToRead := fs.getFilesToRead(query)
	
	for _, fileName := range filesToRead {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		events, err := fs.readEventsFromFile(ctx, fileName, query)
		if err != nil {
			fs.logger.WithError(err).WithField("file", fileName).Warn("Failed to read events from file")
			continue
		}
		
		allEvents = append(allEvents, events...)
	}

	// Apply in-memory filtering, sorting, and pagination
	filteredEvents := fs.filterEvents(allEvents, query)
	sortedEvents := fs.sortEvents(filteredEvents, query)
	paginatedEvents := fs.paginateEvents(sortedEvents, query)

	return paginatedEvents, nil
}

// readEventsFromFile reads events from a specific file
func (fs *FileStorage) readEventsFromFile(ctx context.Context, fileName string, query *Query) ([]*pb.AdEvent, error) {
	filePath := filepath.Join(fs.basePath, fileName)
	
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // File doesn't exist, return empty
		}
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	var events []*pb.AdEvent
	decoder := json.NewDecoder(file)
	
	for decoder.More() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		var jsonData json.RawMessage
		if err := decoder.Decode(&jsonData); err != nil {
			fs.logger.WithError(err).Warn("Failed to decode JSON line")
			continue
		}

		// Convert JSON back to protobuf
		var event pb.AdEvent
		if err := protojson.Unmarshal(jsonData, &event); err != nil {
			fs.logger.WithError(err).Warn("Failed to unmarshal event")
			continue
		}

		events = append(events, &event)
	}

	return events, nil
}

// getFilesToRead determines which files to read based on query time range
func (fs *FileStorage) getFilesToRead(query *Query) []string {
	var files []string
	
	if query.StartTime == nil && query.EndTime == nil {
		// No time filter, read all files
		for dateKey := range fs.fileIndex {
			files = append(files, fmt.Sprintf("events_%s.jsonl", dateKey))
		}
		return files
	}

	// Generate date range
	start := time.Now().AddDate(0, 0, -30) // Default to last 30 days
	if query.StartTime != nil {
		start = *query.StartTime
	}
	
	end := time.Now()
	if query.EndTime != nil {
		end = *query.EndTime
	}

	// Add files for each date in range
	for d := start; d.Before(end) || d.Equal(end); d = d.AddDate(0, 0, 1) {
		dateKey := d.Format("2006-01-02")
		if _, exists := fs.fileIndex[dateKey]; exists {
			files = append(files, fmt.Sprintf("events_%s.jsonl", dateKey))
		}
	}

	return files
}

// filterEvents applies filters to events
func (fs *FileStorage) filterEvents(events []*pb.AdEvent, query *Query) []*pb.AdEvent {
	if query == nil {
		return events
	}

	var filtered []*pb.AdEvent
	
	for _, event := range events {
		// Time range filter
		if query.StartTime != nil && event.Timestamp != nil {
			if event.Timestamp.AsTime().Before(*query.StartTime) {
				continue
			}
		}
		
		if query.EndTime != nil && event.Timestamp != nil {
			if event.Timestamp.AsTime().After(*query.EndTime) {
				continue
			}
		}

		// User ID filter
		if query.UserID != "" && event.UserId != query.UserID {
			continue
		}

		// Ad ID filter
		if query.AdID != "" && event.AdId != query.AdID {
			continue
		}

		// Event type filter
		if query.EventType != pb.EventType_EVENT_TYPE_UNSPECIFIED && event.EventType != query.EventType {
			continue
		}

		filtered = append(filtered, event)
	}

	return filtered
}

// sortEvents sorts events based on query parameters
func (fs *FileStorage) sortEvents(events []*pb.AdEvent, query *Query) []*pb.AdEvent {
	if query == nil || query.SortBy == "" {
		return events
	}

	// Implementation of sorting logic would go here
	// For brevity, returning events as-is
	// In a production system, you'd implement proper sorting
	return events
}

// paginateEvents applies pagination to events
func (fs *FileStorage) paginateEvents(events []*pb.AdEvent, query *Query) []*pb.AdEvent {
	if query == nil {
		return events
	}

	start := query.Offset
	if start < 0 {
		start = 0
	}
	
	if start >= len(events) {
		return []*pb.AdEvent{}
	}

	end := start + query.Limit
	if query.Limit <= 0 || end > len(events) {
		end = len(events)
	}

	return events[start:end]
}

// GetEventCount returns the total count of events matching the query
func (fs *FileStorage) GetEventCount(ctx context.Context, query *Query) (int64, error) {
	events, err := fs.GetEvents(ctx, query)
	if err != nil {
		return 0, err
	}
	return int64(len(events)), nil
}

// Health checks the health of the file storage
func (fs *FileStorage) Health(ctx context.Context) error {
	// Check if base directory is accessible
	if _, err := os.Stat(fs.basePath); err != nil {
		return fmt.Errorf("base directory not accessible: %w", err)
	}

	// Check if we can write to the directory
	testFile := filepath.Join(fs.basePath, ".health_check")
	if err := os.WriteFile(testFile, []byte("health check"), 0644); err != nil {
		return fmt.Errorf("cannot write to storage directory: %w", err)
	}

	// Clean up test file
	if err := os.Remove(testFile); err != nil {
		fs.logger.WithError(err).Warn("Failed to clean up health check file")
	}

	return nil
}

// Close closes the file storage (no-op for file storage)
func (fs *FileStorage) Close() error {
	fs.logger.Info("File storage closed")
	return nil
}

// buildFileIndex builds an index of existing files
func (fs *FileStorage) buildFileIndex() error {
	entries, err := os.ReadDir(fs.basePath)
	if err != nil {
		return fmt.Errorf("failed to read base directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()
		if !strings.HasPrefix(fileName, "events_") || !strings.HasSuffix(fileName, ".jsonl") {
			continue
		}

		// Extract date from filename
		dateKey := strings.TrimPrefix(fileName, "events_")
		dateKey = strings.TrimSuffix(dateKey, ".jsonl")

		// Get file info
		filePath := filepath.Join(fs.basePath, fileName)
		osFileInfo, err := os.Stat(filePath)
		if err != nil {
			fs.logger.WithError(err).WithField("file", fileName).Warn("Failed to get file info")
			continue
		}

		// Count events in file (simplified - in production you might cache this)
		eventCount, err := fs.countEventsInFile(filePath)
		if err != nil {
			fs.logger.WithError(err).WithField("file", fileName).Warn("Failed to count events in file")
			eventCount = 0
		}

		fs.fileIndex[dateKey] = &fileInfo{
			path:       filePath,
			eventCount: eventCount,
			lastWrite:  osFileInfo.ModTime(),
		}

		fs.totalEvents += eventCount
	}

	fs.logger.WithFields(logrus.Fields{
		"total_files":  len(fs.fileIndex),
		"total_events": fs.totalEvents,
	}).Info("File index built successfully")

	return nil
}

// countEventsInFile counts the number of events in a file
func (fs *FileStorage) countEventsInFile(filePath string) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var count int64
	decoder := json.NewDecoder(file)
	
	for decoder.More() {
		var jsonData json.RawMessage
		if err := decoder.Decode(&jsonData); err != nil {
			continue // Skip invalid lines
		}
		count++
	}

	return count, nil
}