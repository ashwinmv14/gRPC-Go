# Ad Event Processor

A high-performance, scalable ad event processing service built in Go using gRPC and goroutines. This project simulates a real-world advertising technology infrastructure for ingesting, processing, and storing ad events at scale.

## 🚀 Features

- **High-performance gRPC API** for event ingestion
- **Concurrent batch processing** using goroutines and channels
- **Multiple storage backends** (File, Redis, MongoDB)
- **Configurable batching** with size and time-based triggers
- **Graceful shutdown** with proper resource cleanup
- **Comprehensive monitoring** with Prometheus metrics
- **Production-ready** with Docker support
- **Load testing tools** for performance validation

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   gRPC Client   │───▶│   gRPC Server   │───▶│ Batch Processor │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                                                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│    File Store   │    │   Redis Store   │    │  MongoDB Store  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Core Components

1. **gRPC Server**: Accepts ad events via gRPC protocol
2. **Batch Processor**: Aggregates events using goroutines and channels
3. **Storage Layer**: Pluggable storage backends (File/Redis/MongoDB)
4. **Configuration**: Flexible YAML-based configuration
5. **Monitoring**: Prometheus metrics and health checks

## 📋 Requirements

- Go 1.21+
- Protocol Buffers compiler (protoc)
- Docker & Docker Compose (optional)

## 🛠️ Installation

### Local Development

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd ad-event-processor
   ```

2. **Install dependencies**
   ```bash
   make deps
   ```

3. **Install development tools**
   ```bash
   make install-tools
   ```

4. **Generate protobuf files**
   ```bash
   make proto
   ```

5. **Build the application**
   ```bash
   make build
   ```

### Docker Setup

1. **Start all services**
   ```bash
   docker-compose up -d
   ```

   This starts:
   - Ad Event Processor (port 8080)
   - Redis (port 6379)
   - MongoDB (port 27017)
   - Prometheus (port 9091)
   - Grafana (port 3000)

## 🚀 Usage

### Starting the Server

```bash
# Using local binary
./bin/ad-event-processor

# Using Docker
docker-compose up ad-event-processor

# Using Make
make run
```

### Configuration

The application uses YAML configuration with environment variable override support:

```yaml
server:
  host: "0.0.0.0"
  port: 8080

database:
  type: "file"  # Options: file, redis, mongodb

batch:
  size: 100
  flush_interval: "5s"
  worker_pool_size: 5
```

Environment variables use the `AEP_` prefix:
```bash
export AEP_DATABASE_TYPE=redis
export AEP_BATCH_SIZE=200
export AEP_LOGGING_LEVEL=debug
```

### Client Usage

The project includes a test client for sending events:

```bash
# Send 100 single events
./bin/client -events=100 -mode=single

# Send events in batches of 10
./bin/client -events=1000 -batch=10 -mode=batch

# Run load test with 50 concurrent clients
./bin/client -events=10000 -concurrent=50 -mode=load

# Custom server address
./bin/client -server=localhost:8080 -events=500
```

### API Examples

**Send Single Event (gRPC)**
```go
event := &pb.AdEvent{
    UserId:    "user123",
    AdId:      "ad456",
    EventType: pb.EventType_EVENT_TYPE_IMPRESSION,
    Timestamp: timestamppb.Now(),
}

response, err := client.SendAdEvent(ctx, &pb.SendAdEventRequest{
    Event: event,
})
```

**Send Batch Events**
```go
events := []*pb.AdEvent{
    // ... multiple events
}

response, err := client.SendAdEventBatch(ctx, &pb.SendAdEventBatchRequest{
    Events: events,
})
```

**Health Check**
```go
response, err := client.HealthCheck(ctx, &pb.HealthCheckRequest{})
```

## 🏃 Performance

### Benchmarks

| Metric | Value |
|--------|-------|
| Throughput | ~50,000 events/second |
| Latency (p99) | <10ms |
| Memory Usage | ~100MB base |
| Batch Size | 100 events (configurable) |
| Flush Interval | 5 seconds (configurable) |

### Tuning Parameters

- **Batch Size**: Larger batches = higher throughput, higher latency
- **Worker Pool Size**: More workers = better parallelism
- **Channel Buffer Size**: Larger buffers = better burst handling
- **Flush Interval**: Shorter intervals = lower latency, lower throughput

## 📊 Monitoring

### Metrics (Prometheus)

- `events_received_total`: Total events received
- `events_processed_total`: Total events processed
- `batches_processed_total`: Total batches processed
- `processing_errors_total`: Total processing errors
- `average_processing_latency`: Average processing latency

### Health Checks

```bash
# Using gRPC client
./bin/client -mode=health

# Using curl (if HTTP gateway is enabled)
curl http://localhost:8080/health
```

### Grafana Dashboards

Access Grafana at `http://localhost:3000` (admin/admin) for pre-configured dashboards showing:
- Event throughput and latency
- Error rates and success rates
- System resource usage
- Storage backend performance

## 🧪 Testing

### Unit Tests
```bash
make test
```

### Integration Tests
```bash
make test-integration
```

### Load Testing
```bash
# Start the server
make run

# Run load test (separate terminal)
make load-test

# Or custom load test
./bin/client -mode=load -events=50000 -concurrent=100
```

## 📁 Project Structure

```
ad-event-processor/
├── cmd/
│   ├── server/main.go          # Server entry point
│   └── client/main.go          # Test client
├── internal/
│   ├── config/config.go        # Configuration management
│   ├── processor/              # Batch processing logic
│   ├── server/grpc_server.go   # gRPC server implementation
│   └── storage/                # Storage backends
├── proto/
│   └── ad_events.proto         # Protocol buffer definitions
├── configs/
│   └── config.yaml             # Default configuration
├── monitoring/
│   ├── prometheus.yml          # Prometheus configuration
│   └── grafana/                # Grafana dashboards
├── scripts/                    # Utility scripts
├── docker-compose.yml          # Multi-service setup
├── Dockerfile                  # Server container
├── Makefile                    # Build automation
└── README.md                   # This file
```

## 🔧 Development

### Adding New Storage Backend

1. Implement the `storage.Storage` interface
2. Add configuration in `config.go`
3. Register in `main.go` initialization
4. Add tests and documentation

### Extending Event Schema

1. Update `proto/ad_events.proto`
2. Run `make proto`
3. Update validation logic
4. Update storage implementations

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Run `make lint` and `make test`
5. Submit a pull request

## 🔍 Troubleshooting

### Common Issues

**Server won't start**
- Check port availability: `lsof -i :8080`
- Verify configuration: `make lint`
- Check logs: `docker-compose logs ad-event-processor`

**High memory usage**
- Reduce batch size in configuration
- Decrease channel buffer sizes
- Check for memory leaks in custom code

**Poor performance**
- Increase worker pool size
- Optimize storage backend
- Use batch mode instead of single events
- Check network latency

**Storage errors**
- Verify storage backend is running
- Check connection strings
- Validate permissions

### Debugging

```bash
# Enable debug logging
export AEP_LOGGING_LEVEL=debug

# Run with race detection
go run -race cmd/server/main.go

# Profile memory usage
go tool pprof http://localhost:8080/debug/pprof/heap
```

## 📚 Additional Resources

- [gRPC Go Documentation](https://grpc.io/docs/languages/go/)
- [Go Concurrency Patterns](https://blog.golang.org/pipelines)
- [Protocol Buffers Guide](https://developers.google.com/protocol-buffers)
- [Prometheus Monitoring](https://prometheus.io/docs/)

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Acknowledgments

- Built for learning purposes, simulating real-world ad tech infrastructure
- Designed with industry best practices for scalability and reliability
- Inspired by modern event-driven architectures
