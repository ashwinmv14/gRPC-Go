package processor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"ad-event-processor/internal/config"
	"ad-event-processor/internal/storage"
	pb "ad-event-processor/proto"

	"github.com/sirupsen/logrus"
)

// BatchProcessor handles batching and processing of ad events using goroutines
type BatchProcessor struct {
	storage       storage.Storage
	config        *config.BatchConfig
	logger        *logrus.Logger
	
	// Channels for event processing pipeline
	eventChan     chan *pb.AdEvent
	batchChan     chan []*pb.AdEvent
	
	// Worker pool management
	workerWg      sync.WaitGroup
	processorWg   sync.WaitGroup
	
	// Control channels
	stopChan      chan struct{}
	done          chan struct{}
	
	// Current batch state
	currentBatch  []*pb.AdEvent
	batchMutex    sync.Mutex
	lastFlush     time.Time
	
	// Metrics
	stats         *ProcessorStats
	
	// Lifecycle management
	running       int32 // atomic
	started       int32 // atomic
}

// ProcessorStats holds processor statistics
type ProcessorStats struct {
	EventsReceived    int64 // atomic
	EventsProcessed   int64 // atomic
	BatchesProcessed  int64 // atomic
	EventsDropped     int64 // atomic
	ProcessingErrors  int64 // atomic
	AverageLatency    time.Duration
	LastProcessedTime time.Time
	mutex             sync.RWMutex
}

// NewBatchProcessor creates a new batch processor instance
func NewBatchProcessor(storage storage.Storage, config *config.BatchConfig, logger *logrus.Logger) *BatchProcessor {
	return &BatchProcessor{
		storage:       storage,
		config:        config,
		logger:        logger,
		eventChan:     make(chan *pb.AdEvent, config.ChannelBufferSize),
		batchChan:     make(chan []*pb.AdEvent, config.WorkerPoolSize*2),
		stopChan:      make(chan struct{}),
		done:          make(chan struct{}),
		currentBatch:  make([]*pb.AdEvent, 0, config.Size),
		lastFlush:     time.Now(),
		stats:         &ProcessorStats{},
	}
}

// Start starts the batch processor with all goroutines
func (bp *BatchProcessor) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&bp.started, 0, 1) {
		return fmt.Errorf("processor already started")
	}

	atomic.StoreInt32(&bp.running, 1)
	
	bp.logger.WithFields(logrus.Fields{
		"batch_size":         bp.config.Size,
		"flush_interval":     bp.config.FlushInterval,
		"worker_pool_size":   bp.config.WorkerPoolSize,
		"channel_buffer_size": bp.config.ChannelBufferSize,
	}).Info("Starting batch processor")

	// Start the batch aggregator goroutine
	bp.processorWg.Add(1)
	go bp.batchAggregator(ctx)

	// Start worker pool for batch processing
	for i := 0; i < bp.config.WorkerPoolSize; i++ {
		bp.workerWg.Add(1)
		go bp.batchWorker(ctx, i)
	}

	// Start periodic flush ticker
	bp.processorWg.Add(1)
	go bp.flushTicker(ctx)

	// Start metrics reporter
	bp.processorWg.Add(1)
	go bp.metricsReporter(ctx)

	bp.logger.Info("Batch processor started successfully")
	return nil
}

// Stop gracefully stops the batch processor
func (bp *BatchProcessor) Stop(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&bp.running, 1, 0) {
		return fmt.Errorf("processor not running")
	}

	bp.logger.Info("Stopping batch processor...")

	// Signal stop to all goroutines
	close(bp.stopChan)

	// Flush any remaining events
	if err := bp.flushCurrentBatch(ctx); err != nil {
		bp.logger.WithError(err).Warn("Failed to flush remaining events during shutdown")
	}

	// Close event channel to stop accepting new events
	close(bp.eventChan)

	// Wait for batch aggregator to finish processing remaining events
	bp.processorWg.Wait()

	// Close batch channel after aggregator is done
	close(bp.batchChan)

	// Wait for all workers to finish
	bp.workerWg.Wait()

	close(bp.done)
	bp.logger.Info("Batch processor stopped successfully")
	return nil
}

// ProcessEvent adds an event to the processing pipeline
func (bp *BatchProcessor) ProcessEvent(ctx context.Context, event *pb.AdEvent) error {
	if atomic.LoadInt32(&bp.running) == 0 {
		return fmt.Errorf("processor not running")
	}

	atomic.AddInt64(&bp.stats.EventsReceived, 1)

	select {
	case bp.eventChan <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-bp.stopChan:
		return fmt.Errorf("processor stopping")
	default:
		// Channel is full, drop the event
		atomic.AddInt64(&bp.stats.EventsDropped, 1)
		bp.logger.Warn("Event channel full, dropping event")
		return fmt.Errorf("event channel full")
	}
}

// batchAggregator aggregates events into batches
func (bp *BatchProcessor) batchAggregator(ctx context.Context) {
	defer bp.processorWg.Done()
	
	bp.logger.Debug("Batch aggregator started")

	for {
		select {
		case event, ok := <-bp.eventChan:
			if !ok {
				// Channel closed, flush remaining batch and exit
				if len(bp.currentBatch) > 0 {
					bp.sendBatch(ctx, bp.currentBatch)
				}
				return
			}
			
			bp.addEventToBatch(ctx, event)

		case <-bp.stopChan:
			// Flush remaining batch and exit
			if len(bp.currentBatch) > 0 {
				bp.sendBatch(ctx, bp.currentBatch)
			}
			return

		case <-ctx.Done():
			return
		}
	}
}

// addEventToBatch adds an event to the current batch and flushes if necessary
func (bp *BatchProcessor) addEventToBatch(ctx context.Context, event *pb.AdEvent) {
	bp.batchMutex.Lock()
	defer bp.batchMutex.Unlock()

	bp.currentBatch = append(bp.currentBatch, event)

	// Check if we should flush the batch
	shouldFlush := len(bp.currentBatch) >= bp.config.Size

	if shouldFlush {
		batch := make([]*pb.AdEvent, len(bp.currentBatch))
		copy(batch, bp.currentBatch)
		bp.currentBatch = bp.currentBatch[:0] // Reset slice
		bp.lastFlush = time.Now()
		
		// Send batch to workers (non-blocking)
		go bp.sendBatch(ctx, batch)
	}
}

// sendBatch sends a batch to the worker pool
func (bp *BatchProcessor) sendBatch(ctx context.Context, batch []*pb.AdEvent) {
	if len(batch) == 0 {
		return
	}

	select {
	case bp.batchChan <- batch:
		bp.logger.WithField("batch_size", len(batch)).Debug("Batch sent to workers")
	case <-ctx.Done():
		return
	case <-bp.stopChan:
		return
	default:
		// Batch channel is full, this shouldn't happen often with proper sizing
		atomic.AddInt64(&bp.stats.ProcessingErrors, 1)
		bp.logger.Warn("Batch channel full, dropping batch")
	}
}

// flushTicker periodically flushes batches based on time interval
func (bp *BatchProcessor) flushTicker(ctx context.Context) {
	defer bp.processorWg.Done()
	
	ticker := time.NewTicker(bp.config.FlushInterval)
	defer ticker.Stop()

	bp.logger.WithField("interval", bp.config.FlushInterval).Debug("Flush ticker started")

	for {
		select {
		case <-ticker.C:
			bp.flushIfNeeded(ctx)

		case <-bp.stopChan:
			return

		case <-ctx.Done():
			return
		}
	}
}

// flushIfNeeded flushes the current batch if it's time to do so
func (bp *BatchProcessor) flushIfNeeded(ctx context.Context) {
	bp.batchMutex.Lock()
	defer bp.batchMutex.Unlock()

	if len(bp.currentBatch) == 0 {
		return
	}

	timeSinceFlush := time.Since(bp.lastFlush)
	if timeSinceFlush >= bp.config.FlushInterval {
		batch := make([]*pb.AdEvent, len(bp.currentBatch))
		copy(batch, bp.currentBatch)
		bp.currentBatch = bp.currentBatch[:0] // Reset slice
		bp.lastFlush = time.Now()
		
		// Send batch to workers (non-blocking)
		go bp.sendBatch(ctx, batch)
		
		bp.logger.WithField("batch_size", len(batch)).Debug("Time-based batch flush")
	}
}

// flushCurrentBatch immediately flushes the current batch
func (bp *BatchProcessor) flushCurrentBatch(ctx context.Context) error {
	bp.batchMutex.Lock()
	defer bp.batchMutex.Unlock()

	if len(bp.currentBatch) == 0 {
		return nil
	}

	batch := make([]*pb.AdEvent, len(bp.currentBatch))
	copy(batch, bp.currentBatch)
	bp.currentBatch = bp.currentBatch[:0]
	bp.lastFlush = time.Now()

	// Process batch synchronously during shutdown
	return bp.processBatchWithRetry(ctx, batch)
}

// batchWorker processes batches from the batch channel
func (bp *BatchProcessor) batchWorker(ctx context.Context, workerID int) {
	defer bp.workerWg.Done()
	
	bp.logger.WithField("worker_id", workerID).Debug("Batch worker started")

	for {
		select {
		case batch, ok := <-bp.batchChan:
			if !ok {
				// Channel closed, exit
				bp.logger.WithField("worker_id", workerID).Debug("Batch worker stopped")
				return
			}
			
			startTime := time.Now()
			err := bp.processBatchWithRetry(ctx, batch)
			duration := time.Since(startTime)
			
			if err != nil {
				atomic.AddInt64(&bp.stats.ProcessingErrors, 1)
				bp.logger.WithError(err).WithField("worker_id", workerID).Error("Failed to process batch")
			} else {
				atomic.AddInt64(&bp.stats.BatchesProcessed, 1)
				atomic.AddInt64(&bp.stats.EventsProcessed, int64(len(batch)))
				bp.updateAverageLatency(duration)
				bp.logger.WithFields(logrus.Fields{
					"worker_id":   workerID,
					"batch_size":  len(batch),
					"duration_ms": duration.Milliseconds(),
				}).Debug("Batch processed successfully")
			}

		case <-bp.stopChan:
			bp.logger.WithField("worker_id", workerID).Debug("Batch worker stopped")
			return

		case <-ctx.Done():
			return
		}
	}
}

// processBatchWithRetry processes a batch with retry logic
func (bp *BatchProcessor) processBatchWithRetry(ctx context.Context, batch []*pb.AdEvent) error {
	var lastErr error
	
	for attempt := 0; attempt <= bp.config.MaxRetries; attempt++ {
		if attempt > 0 {
			// Calculate exponential backoff
			backoff := time.Duration(attempt) * bp.config.RetryBackoffBase
			bp.logger.WithFields(logrus.Fields{
				"attempt":      attempt,
				"backoff_ms":   backoff.Milliseconds(),
				"batch_size":   len(batch),
			}).Warn("Retrying batch processing")
			
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			case <-bp.stopChan:
				return fmt.Errorf("processor stopping")
			}
		}

		err := bp.storage.Store(ctx, batch)
		if err == nil {
			if attempt > 0 {
				bp.logger.WithFields(logrus.Fields{
					"attempt":    attempt,
					"batch_size": len(batch),
				}).Info("Batch processing succeeded after retry")
			}
			return nil
		}
		
		lastErr = err
		bp.logger.WithError(err).WithFields(logrus.Fields{
			"attempt":    attempt,
			"batch_size": len(batch),
		}).Warn("Batch processing failed")
	}

	return fmt.Errorf("batch processing failed after %d attempts: %w", bp.config.MaxRetries+1, lastErr)
}

// updateAverageLatency updates the average latency metric
func (bp *BatchProcessor) updateAverageLatency(duration time.Duration) {
	bp.stats.mutex.Lock()
	defer bp.stats.mutex.Unlock()
	
	// Simple moving average (could be improved with more sophisticated methods)
	if bp.stats.AverageLatency == 0 {
		bp.stats.AverageLatency = duration
	} else {
		bp.stats.AverageLatency = (bp.stats.AverageLatency + duration) / 2
	}
	bp.stats.LastProcessedTime = time.Now()
}

// metricsReporter periodically reports metrics
func (bp *BatchProcessor) metricsReporter(ctx context.Context) {
	defer bp.processorWg.Done()
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bp.reportMetrics()

		case <-bp.stopChan:
			bp.reportMetrics() // Final metrics report
			return

		case <-ctx.Done():
			return
		}
	}
}

// reportMetrics logs current processor metrics
func (bp *BatchProcessor) reportMetrics() {
	stats := bp.GetStats()
	
	bp.logger.WithFields(logrus.Fields{
		"events_received":     stats.EventsReceived,
		"events_processed":    stats.EventsProcessed,
		"batches_processed":   stats.BatchesProcessed,
		"events_dropped":      stats.EventsDropped,
		"processing_errors":   stats.ProcessingErrors,
		"average_latency_ms":  stats.AverageLatency.Milliseconds(),
		"last_processed":      stats.LastProcessedTime.Format(time.RFC3339),
	}).Info("Processor metrics")
}

// GetStats returns a copy of current processor statistics
func (bp *BatchProcessor) GetStats() ProcessorStats {
	bp.stats.mutex.RLock()
	defer bp.stats.mutex.RUnlock()
	
	return ProcessorStats{
		EventsReceived:    atomic.LoadInt64(&bp.stats.EventsReceived),
		EventsProcessed:   atomic.LoadInt64(&bp.stats.EventsProcessed),
		BatchesProcessed:  atomic.LoadInt64(&bp.stats.BatchesProcessed),
		EventsDropped:     atomic.LoadInt64(&bp.stats.EventsDropped),
		ProcessingErrors:  atomic.LoadInt64(&bp.stats.ProcessingErrors),
		AverageLatency:    bp.stats.AverageLatency,
		LastProcessedTime: bp.stats.LastProcessedTime,
	}
}

// IsRunning returns whether the processor is currently running
func (bp *BatchProcessor) IsRunning() bool {
	return atomic.LoadInt32(&bp.running) == 1
}

// GetCurrentBatchSize returns the current batch size
func (bp *BatchProcessor) GetCurrentBatchSize() int {
	bp.batchMutex.Lock()
	defer bp.batchMutex.Unlock()
	return len(bp.currentBatch)
}