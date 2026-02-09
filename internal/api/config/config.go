package config

import (
	"log"
	"os"
	"path/filepath"

	"github.com/anthanhphan/gosdk/conflux"
	"github.com/anthanhphan/gosdk/logger"
)

// Config holds API Service configuration
type Config struct {
	Server  ServerConfig  `json:"server" yaml:"server"`
	App     AppConfig     `json:"app" yaml:"app"`
	Storage StorageConfig `json:"storage" yaml:"storage"`
	Redis   RedisConfig   `json:"redis" yaml:"redis"`
	Logger  logger.Config `json:"logger" yaml:"logger"`
}

type ServerConfig struct {
	Addr string `json:"addr" yaml:"addr"`
}

type AppConfig struct {
	NodeID           int64  `json:"node_id" yaml:"node_id"`
	ChunkSize        int64  `json:"chunk_size" yaml:"chunk_size"`
	MaxFileSize      int64  `json:"max_file_size" yaml:"max_file_size"`
	ParallelChunks   int    `json:"parallel_chunks" yaml:"parallel_chunks"`
	MaxRetries       int    `json:"max_retries" yaml:"max_retries"`
	WriteTimeoutMS   int    `json:"write_timeout_ms" yaml:"write_timeout_ms"`
	ReadConsistency  string `json:"read_consistency" yaml:"read_consistency"`   // "one", "quorum", "all"
	WriteConsistency string `json:"write_consistency" yaml:"write_consistency"` // "one", "quorum", "all"
}

type StorageConfig struct {
	Seeds []string `json:"seeds" yaml:"seeds"`
}

type RedisConfig struct {
	Addr     string `json:"addr" yaml:"addr"`
	Password string `json:"password" yaml:"password"`
	DB       int    `json:"db" yaml:"db"`
}

// DefaultConfig returns configuration with default values
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Addr: ":8090",
		},
		App: AppConfig{
			NodeID:           1,
			ChunkSize:        8 * 1024 * 1024,        // 8MB
			MaxFileSize:      2 * 1024 * 1024 * 1024, // 2GB
			ParallelChunks:   4,
			MaxRetries:       3,
			WriteTimeoutMS:   15000,
			ReadConsistency:  "one",
			WriteConsistency: "quorum",
		},
		Storage: StorageConfig{
			Seeds: []string{"localhost:8081", "localhost:8082", "localhost:8083"},
		},
		Redis: RedisConfig{
			Addr: "localhost:6379",
		},
		Logger: logger.Config{
			LogLevel:    logger.LevelInfo,
			LogEncoding: logger.EncodingJSON,
		},
	}
}

// Load loads configuration from file
func Load(path string) (*Config, error) {
	configPath := path
	if configPath == "" {
		env := os.Getenv("ENV")
		if env == "" {
			env = "local"
		}
		configPath = filepath.Join("internal", "api", "config", env+".yaml")
	}

	cfg := DefaultConfig()

	parsedCfg, err := conflux.ParseConfig(configPath, cfg)
	if err != nil {
		// Log warning but return default config if specific path wasn't provided,
		// otherwise return error if user explicitly asked for a file.
		// NOTE: The user provided code uses logger.Warnw but logger might not be init yet.
		// We'll use log.Println for safety or assume logger init is separate.
		// Actually conflux doesn't init logger.
		log.Printf("Config file not found or failed to parse, using defaults if file not specified. Path: %s, Error: %v", configPath, err)
		if path != "" {
			return nil, err
		}
		return cfg, nil
	}

	return parsedCfg, nil
}

// MustLoad loads configuration or exits on error
func MustLoad(path string) *Config {
	cfg, err := Load(path)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	return cfg
}
