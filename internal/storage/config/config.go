package config

import (
	"log"
	"os"
	"path/filepath"

	"github.com/anthanhphan/gosdk/conflux"
	"github.com/anthanhphan/gosdk/logger"
)

// Config holds Storage Service configuration
type Config struct {
	Server ServerConfig  `json:"server" yaml:"server"`
	Gossip GossipConfig  `json:"gossip" yaml:"gossip"`
	LSM    LSMConfig     `json:"lsm" yaml:"lsm"`
	Logger logger.Config `json:"logger" yaml:"logger"`
}

type ServerConfig struct {
	NodeID    string `json:"node_id" yaml:"node_id"`
	Hostname  string `json:"hostname" yaml:"hostname"`
	Port      int    `json:"port" yaml:"port"`
	ShardID   string `json:"shard_id" yaml:"shard_id"`
	ReplicaID int    `json:"replica_id" yaml:"replica_id"`
}

type GossipConfig struct {
	Port  int      `json:"port" yaml:"port"`
	Seeds []string `json:"seeds" yaml:"seeds"`
}

type LSMConfig struct {
	DataDir             string `json:"data_dir" yaml:"data_dir"`
	FSync               bool   `json:"fsync" yaml:"fsync"`
	CompactionThreshold int    `json:"compaction_threshold" yaml:"compaction_threshold"`
}

// DefaultConfig returns configuration with default values
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Hostname: "127.0.0.1",
			Port:     8081,
		},
		Gossip: GossipConfig{
			Port: 7946,
		},
		LSM: LSMConfig{
			DataDir: "./data",
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
		// Note: Storage nodes might have different config files than API
		configPath = filepath.Join("internal", "storage", "config", env+".yaml")
	}

	cfg := DefaultConfig()

	parsedCfg, err := conflux.ParseConfig(configPath, cfg)
	if err != nil {
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
