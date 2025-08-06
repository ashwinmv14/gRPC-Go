package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Config holds the application configuration
type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Database DatabaseConfig `mapstructure:"database"`
	Redis    RedisConfig    `mapstructure:"redis"`
	MongoDB  MongoDBConfig  `mapstructure:"mongodb"`
	Batch    BatchConfig    `mapstructure:"batch"`
	Metrics  MetricsConfig  `mapstructure:"metrics"`
	Logging  LoggingConfig  `mapstructure:"logging"`
}

// ServerConfig contains gRPC server configuration
type ServerConfig struct {
	Host              string        `mapstructure:"host"`
	Port              int           `mapstructure:"port"`
	GracefulTimeout   time.Duration `mapstructure:"graceful_timeout"`
	MaxReceiveSize    int           `mapstructure:"max_receive_size"`
	MaxSendSize       int           `mapstructure:"max_send_size"`
	ConnectionTimeout time.Duration `mapstructure:"connection_timeout"`
	KeepAliveTime     time.Duration `mapstructure:"keep_alive_time"`
	KeepAliveTimeout  time.Duration `mapstructure:"keep_alive_timeout"`
}

// DatabaseConfig contains general database configuration
type DatabaseConfig struct {
	Type string `mapstructure:"type"` // file, redis, mongodb
}

// RedisConfig contains Redis-specific configuration
type RedisConfig struct {
	Host         string        `mapstructure:"host"`
	Port         int           `mapstructure:"port"`
	Password     string        `mapstructure:"password"`
	DB           int           `mapstructure:"db"`
	PoolSize     int           `mapstructure:"pool_size"`
	MinIdleConns int           `mapstructure:"min_idle_conns"`
	DialTimeout  time.Duration `mapstructure:"dial_timeout"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
}

// MongoDBConfig contains MongoDB-specific configuration
type MongoDBConfig struct {
	URI            string        `mapstructure:"uri"`
	Database       string        `mapstructure:"database"`
	Collection     string        `mapstructure:"collection"`
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`
	MaxPoolSize    uint64        `mapstructure:"max_pool_size"`
	MinPoolSize    uint64        `mapstructure:"min_pool_size"`
}

// BatchConfig contains batch processing configuration
type BatchConfig struct {
	Size               int           `mapstructure:"size"`
	FlushInterval      time.Duration `mapstructure:"flush_interval"`
	MaxBufferSize      int           `mapstructure:"max_buffer_size"`
	WorkerPoolSize     int           `mapstructure:"worker_pool_size"`
	MaxRetries         int           `mapstructure:"max_retries"`
	RetryBackoffBase   time.Duration `mapstructure:"retry_backoff_base"`
	ChannelBufferSize  int           `mapstructure:"channel_buffer_size"`
	EnableCompression  bool          `mapstructure:"enable_compression"`
}

// MetricsConfig contains metrics and monitoring configuration
type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Host    string `mapstructure:"host"`
	Port    int    `mapstructure:"port"`
	Path    string `mapstructure:"path"`
}

// LoggingConfig contains logging configuration
type LoggingConfig struct {
	Level      string `mapstructure:"level"`
	Format     string `mapstructure:"format"` // json, text
	Output     string `mapstructure:"output"` // stdout, file
	Filename   string `mapstructure:"filename"`
	MaxSize    int    `mapstructure:"max_size"`    // megabytes
	MaxBackups int    `mapstructure:"max_backups"` // number of files
	MaxAge     int    `mapstructure:"max_age"`     // days
	Compress   bool   `mapstructure:"compress"`
}

// Load loads configuration from various sources
func Load() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./configs")
	viper.AddConfigPath("/etc/ad-event-processor/")

	// Set default values
	setDefaults()

	// Enable reading from environment variables
	viper.AutomaticEnv()
	viper.SetEnvPrefix("AEP") // Ad Event Processor

	// Read configuration file
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; use defaults and environment variables
			fmt.Println("Config file not found, using defaults and environment variables")
		} else {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	return &config, nil
}

// setDefaults sets default configuration values
func setDefaults() {
	// Server defaults
	viper.SetDefault("server.host", "0.0.0.0")
	viper.SetDefault("server.port", 8080)
	viper.SetDefault("server.graceful_timeout", "30s")
	viper.SetDefault("server.max_receive_size", 4*1024*1024) // 4MB
	viper.SetDefault("server.max_send_size", 4*1024*1024)    // 4MB
	viper.SetDefault("server.connection_timeout", "5s")
	viper.SetDefault("server.keep_alive_time", "30s")
	viper.SetDefault("server.keep_alive_timeout", "5s")

	// Database defaults
	viper.SetDefault("database.type", "file")

	// Redis defaults
	viper.SetDefault("redis.host", "localhost")
	viper.SetDefault("redis.port", 6379)
	viper.SetDefault("redis.password", "")
	viper.SetDefault("redis.db", 0)
	viper.SetDefault("redis.pool_size", 10)
	viper.SetDefault("redis.min_idle_conns", 5)
	viper.SetDefault("redis.dial_timeout", "5s")
	viper.SetDefault("redis.read_timeout", "3s")
	viper.SetDefault("redis.write_timeout", "3s")

	// MongoDB defaults
	viper.SetDefault("mongodb.uri", "mongodb://localhost:27017")
	viper.SetDefault("mongodb.database", "ad_events")
	viper.SetDefault("mongodb.collection", "events")
	viper.SetDefault("mongodb.connect_timeout", "10s")
	viper.SetDefault("mongodb.max_pool_size", 100)
	viper.SetDefault("mongodb.min_pool_size", 10)

	// Batch processing defaults
	viper.SetDefault("batch.size", 100)
	viper.SetDefault("batch.flush_interval", "5s")
	viper.SetDefault("batch.max_buffer_size", 10000)
	viper.SetDefault("batch.worker_pool_size", 5)
	viper.SetDefault("batch.max_retries", 3)
	viper.SetDefault("batch.retry_backoff_base", "1s")
	viper.SetDefault("batch.channel_buffer_size", 1000)
	viper.SetDefault("batch.enable_compression", true)

	// Metrics defaults
	viper.SetDefault("metrics.enabled", true)
	viper.SetDefault("metrics.host", "0.0.0.0")
	viper.SetDefault("metrics.port", 9090)
	viper.SetDefault("metrics.path", "/metrics")

	// Logging defaults
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.format", "json")
	viper.SetDefault("logging.output", "stdout")
	viper.SetDefault("logging.filename", "ad-event-processor.log")
	viper.SetDefault("logging.max_size", 100)
	viper.SetDefault("logging.max_backups", 3)
	viper.SetDefault("logging.max_age", 28)
	viper.SetDefault("logging.compress", true)
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Server.Port)
	}

	if c.Batch.Size <= 0 {
		return fmt.Errorf("batch size must be greater than 0")
	}

	if c.Batch.FlushInterval <= 0 {
		return fmt.Errorf("flush interval must be greater than 0")
	}

	if c.Batch.WorkerPoolSize <= 0 {
		return fmt.Errorf("worker pool size must be greater than 0")
	}

	validDbTypes := map[string]bool{
		"file":    true,
		"redis":   true,
		"mongodb": true,
	}

	if !validDbTypes[c.Database.Type] {
		return fmt.Errorf("invalid database type: %s", c.Database.Type)
	}

	return nil
}

// GetAddress returns the server address as host:port
func (c *Config) GetAddress() string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port)
}

// GetRedisAddress returns the Redis address as host:port
func (c *Config) GetRedisAddress() string {
	return fmt.Sprintf("%s:%d", c.Redis.Host, c.Redis.Port)
}

// GetMetricsAddress returns the metrics server address as host:port
func (c *Config) GetMetricsAddress() string {
	return fmt.Sprintf("%s:%d", c.Metrics.Host, c.Metrics.Port)
}