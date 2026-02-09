package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	httpHandler "github.com/anthanhphan/go-distributed-file-storage/internal/api/adapter/inbound/http"
	"github.com/anthanhphan/go-distributed-file-storage/internal/api/adapter/outbound/cluster_client"
	storageNode "github.com/anthanhphan/go-distributed-file-storage/internal/api/adapter/outbound/storage_node"
	"github.com/anthanhphan/go-distributed-file-storage/internal/api/config"
	"github.com/anthanhphan/go-distributed-file-storage/internal/api/service"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/idgen"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	"github.com/anthanhphan/gosdk/logger"
	"github.com/redis/go-redis/v9"
)

type App struct {
	cfg           *config.Config
	server        *httpHandler.Server
	clusterClient *cluster_client.ClusterClient
	IDGen         *idgen.Snowflake
}

func New(configPath string) (*App, error) {
	// 1. Load Config
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// 2. Initialize Logger
	logger.InitLogger(&cfg.Logger)

	// 3. Initialize Redis and Snowflake IDGen
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	redisClock := idgen.NewRedisClock(redisClient)
	idGen, err := idgen.New(cfg.App.NodeID, redisClock)
	if err != nil {
		return nil, fmt.Errorf("failed to init snowflake: %w", err)
	}

	// 4. Initialize Ring
	ring := shard.NewRing(shard.DefaultVNodesPerNode)

	// 4. Cluster Client (for topology discovery)
	// Gateway doesn't join the gossip ring as a peer.
	// It only polls storage seeds for topology.
	seeds := cfg.Storage.Seeds
	clusterClient := cluster_client.NewClusterClient(ring, seeds, 5*time.Second)

	// 5. Adapters & Services
	storageNodeAdapter := storageNode.NewGrpcAdapter()
	svc := service.NewFileService(cfg, ring, storageNodeAdapter, idGen)

	// 6. HTTP Server
	httpServer := httpHandler.NewServer(cfg, svc)

	return &App{
		cfg:           cfg,
		server:        httpServer,
		clusterClient: clusterClient,
	}, nil
}

func (a *App) Run() error {
	// Start Cluster Client
	go a.clusterClient.Start(context.Background())

	// Start HTTP
	logger.Infow("API Gateway starting", "addr", a.cfg.Server.Addr)
	serverErrCh := make(chan error, 1)
	go func() {
		if err := a.server.Start(); err != nil {
			serverErrCh <- err
		}
	}()

	// Wait for shutdown signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stop)

	var runErr error
	select {
	case sig := <-stop:
		logger.Infow("Shutdown signal received", "signal", sig.String())
	case err := <-serverErrCh:
		runErr = fmt.Errorf("http server failed: %w", err)
		logger.Errorw("API server exited unexpectedly", "error", err.Error())
	}

	logger.Info("Shutting down API services")
	a.clusterClient.Stop()
	if err := a.server.Stop(context.Background()); err != nil {
		logger.Errorw("API shutdown error", "error", err.Error())
		if runErr == nil {
			runErr = err
		}
	}

	return runErr
}
