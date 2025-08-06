package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"ad-event-processor/internal/config"
	"ad-event-processor/internal/processor"
	"ad-event-processor/internal/server"
	"ad-event-processor/internal/storage"

	"github.com/sirupsen/logrus"
)

const (
	applicationName = "ad-event-processor"
	version         = "1.0.0"
)

func main() {
	// Initialize logger
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	
	logger.WithFields(logrus.Fields{
		"application": applicationName,
		"version":     version,
	}).Info("Starting Ad Event Processor")

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		logger.WithError(err).Fatal("Failed to load configuration")
	}

	// Configure logger based on config
	if err := configureLogger(logger, &cfg.Logging); err != nil {
		logger.WithError(err).Fatal("Failed to configure logger")
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		logger.WithError(err).Fatal("Invalid configuration")
	}

	logger.WithFields(logrus.Fields{
		"database_type":      cfg.Database.Type,
		"batch_size":         cfg.Batch.Size,
		"flush_interval":     cfg.Batch.FlushInterval,
		"worker_pool_size":   cfg.Batch.WorkerPoolSize,
		"server_address":     cfg.GetAddress(),
	}).Info("Configuration loaded successfully")

	// Create application context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize storage
	storage, err := initializeStorage(cfg, logger)
	if err != nil {
		logger.WithError(err).Fatal("Failed to initialize storage")
	}
	defer func() {
		if err := storage.Close(); err != nil {
			logger.WithError(err).Error("Failed to close storage")
		}
	}()

	// Test storage connection
	if err := storage.Health(ctx); err != nil {
		logger.WithError(err).Fatal("Storage health check failed")
	}
	logger.Info("Storage initialized and healthy")

	// Initialize batch processor
	batchProcessor := processor.NewBatchProcessor(storage, &cfg.Batch, logger)
	
	// Start batch processor
	if err := batchProcessor.Start(ctx); err != nil {
		logger.WithError(err).Fatal("Failed to start batch processor")
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()
		
		if err := batchProcessor.Stop(shutdownCtx); err != nil {
			logger.WithError(err).Error("Failed to stop batch processor")
		}
	}()

	// Initialize gRPC server
	grpcServer := server.NewGRPCServer(cfg, logger, batchProcessor, storage)
	
	// Start gRPC server
	if err := grpcServer.Start(ctx); err != nil {
		logger.WithError(err).Fatal("Failed to start gRPC server")
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()
		
		if err := grpcServer.Stop(shutdownCtx); err != nil {
			logger.WithError(err).Error("Failed to stop gRPC server")
		}
	}()

	logger.Info("Ad Event Processor started successfully")

	// Wait for shutdown signal
	waitForShutdown(logger)

	logger.Info("Shutdown signal received, stopping application...")
	cancel()

	logger.Info("Ad Event Processor stopped")
}

// initializeStorage creates and initializes the appropriate storage backend
func initializeStorage(cfg *config.Config, logger *logrus.Logger) (storage.Storage, error) {
	switch cfg.Database.Type {
	case "file":
		// Create data directory if it doesn't exist
		dataDir := "data"
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create data directory: %w", err)
		}
		
		absPath, err := filepath.Abs(dataDir)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path: %w", err)
		}
		
		logger.WithField("storage_path", absPath).Info("Using file storage")
		return storage.NewFileStorage(absPath, logger)
		
	case "redis":
		logger.WithField("redis_address", cfg.GetRedisAddress()).Info("Using Redis storage")
		// Redis storage implementation would go here
		// return storage.NewRedisStorage(&cfg.Redis, logger)
		return nil, fmt.Errorf("Redis storage not implemented yet")
		
	case "mongodb":
		logger.WithField("mongodb_uri", cfg.MongoDB.URI).Info("Using MongoDB storage")
		// MongoDB storage implementation would go here
		// return storage.NewMongoDBStorage(&cfg.MongoDB, logger)
		return nil, fmt.Errorf("MongoDB storage not implemented yet")
		
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.Database.Type)
	}
}

// configureLogger configures the logger based on configuration
func configureLogger(logger *logrus.Logger, cfg *config.LoggingConfig) error {
	// Set log level
	level, err := logrus.ParseLevel(cfg.Level)
	if err != nil {
		return fmt.Errorf("invalid log level: %w", err)
	}
	logger.SetLevel(level)

	// Set formatter
	switch cfg.Format {
	case "json":
		logger.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: time.RFC3339,
		})
	case "text":
		logger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: time.RFC3339,
		})
	default:
		return fmt.Errorf("unsupported log format: %s", cfg.Format)
	}

	// Set output
	switch cfg.Output {
	case "stdout":
		logger.SetOutput(os.Stdout)
	case "file":
		if cfg.Filename == "" {
			return fmt.Errorf("filename is required for file output")
		}
		
		// Create log directory if it doesn't exist
		logDir := filepath.Dir(cfg.Filename)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return fmt.Errorf("failed to create log directory: %w", err)
		}
		
		file, err := os.OpenFile(cfg.Filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return fmt.Errorf("failed to open log file: %w", err)
		}
		logger.SetOutput(file)
	default:
		return fmt.Errorf("unsupported log output: %s", cfg.Output)
	}

	return nil
}

// waitForShutdown waits for a shutdown signal
func waitForShutdown(logger *logrus.Logger) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	logger.WithField("signal", sig.String()).Info("Received shutdown signal")
}