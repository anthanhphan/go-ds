package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	grpcHandler "github.com/anthanhphan/go-distributed-file-storage/internal/storage/adapter/inbound/grpc"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/adapter/outbound/lsm"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/config"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/port"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/service"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/gossip"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	storagev1 "github.com/anthanhphan/go-distributed-file-storage/proto/gen/storage/v1"
	"github.com/anthanhphan/gosdk/logger"
)

type App struct {
	cfg            *config.Config
	server         *grpc.Server
	gossip         *gossip.GossipAdapter
	lsm            *lsm.LSMAdapter
	client         *grpcHandler.ClientAdapter
	storageService port.StorageService
	backgroundStop context.CancelFunc
}

func New(configPath string) (*App, error) {
	// 1. Load Config
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// 2. Initialize Logger
	logger.InitLogger(&cfg.Logger)

	if err := os.MkdirAll(cfg.LSM.DataDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	// 3. Shard Ring
	ring := shard.NewRing(shard.DefaultVNodesPerNode)

	// 4. Gossip
	// If NodeID is empty, generate it based on hostname and port
	nodeID := cfg.Server.NodeID
	if nodeID == "" {
		host, _ := os.Hostname()
		nodeID = fmt.Sprintf("%s-%d", host, cfg.Server.Port)
	}

	gossipAdapter, err := gossip.NewGossipAdapter(nodeID, cfg.Server.Hostname, cfg.Gossip.Port, cfg.Server.Port, ring, cfg.Server.ShardID, cfg.Server.ReplicaID)
	if err != nil {
		return nil, fmt.Errorf("failed to init gossip: %w", err)
	}

	// 5. Storage Engine
	lsm, err := lsm.NewLSMAdapter(cfg.LSM)
	if err != nil {
		return nil, fmt.Errorf("failed to init storage: %w", err)
	}

	// 6. Replication Client
	client := grpcHandler.NewClientAdapter()

	// 7. gRPC Server
	// Default 4MB is too small for 8MB chunks. Set to 16MB.
	maxMsgSize := 16 * 1024 * 1024
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	)
	storageService := service.NewStorageService(lsm, client, ring, nodeID, cfg.Server.ShardID)
	grpcService := grpcHandler.NewServer(storageService)
	storagev1.RegisterStorageServiceServer(grpcServer, grpcService)

	return &App{
		cfg:            cfg,
		server:         grpcServer,
		gossip:         gossipAdapter,
		lsm:            lsm,
		client:         client,
		storageService: storageService,
	}, nil
}

func (a *App) Run() error {
	// Start Gossip
	seeds := make([]string, 0, len(a.cfg.Gossip.Seeds))
	selfSeedSuffix := fmt.Sprintf(":%d", a.cfg.Gossip.Port)
	for _, seed := range a.cfg.Gossip.Seeds {
		if seed == "" {
			continue
		}
		if strings.HasSuffix(seed, selfSeedSuffix) && strings.Contains(seed, a.cfg.Server.Hostname) {
			continue
		}
		seeds = append(seeds, seed)
	}

	if len(seeds) > 0 {
		var joinErr error
		for i := 0; i < 5; i++ {
			joinErr = a.gossip.Join(seeds)
			if joinErr == nil {
				break
			}
			logger.Warnw("Failed to join cluster, retrying...", "attempt", i+1, "error", joinErr.Error())
			time.Sleep(2 * time.Second)
		}
		if joinErr != nil {
			logger.Errorw("Failed to join cluster after retries", "error", joinErr.Error())
		}
	}

	// Start gRPC
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", a.cfg.Server.Port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", a.cfg.Server.Port, err)
	}

	logger.Infow("Storage node starting",
		"id", a.gossip.LocalNode().ID,
		"port", a.cfg.Server.Port,
		"gossip", a.cfg.Gossip.Port,
		"shard", a.cfg.Server.ShardID,
		"replica", a.cfg.Server.ReplicaID)

	serverErrCh := make(chan error, 1)
	go func() {
		if err := a.server.Serve(listener); err != nil {
			serverErrCh <- err
		}
	}()

	// Start Anti-Entropy Worker (every 1 minute for PoC)
	if svc, ok := a.storageService.(*service.StorageServiceImpl); ok {
		bgCtx, cancel := context.WithCancel(context.Background())
		a.backgroundStop = cancel
		go svc.StartAntiEntropyWorker(bgCtx, 1*time.Minute)
	}

	// Wait for shutdown signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stop)

	var runErr error
	select {
	case sig := <-stop:
		logger.Infow("Shutdown signal received", "signal", sig.String())
	case err := <-serverErrCh:
		// Ignore expected stop errors.
		errMsg := err.Error()
		if !strings.Contains(errMsg, "use of closed network connection") && !errors.Is(err, grpc.ErrServerStopped) {
			runErr = fmt.Errorf("gRPC server failed: %w", err)
			logger.Errorw("Storage gRPC server exited unexpectedly", "error", errMsg)
		}
	}

	logger.Info("Shutting down storage services")
	if a.backgroundStop != nil {
		a.backgroundStop()
	}
	if err := a.gossip.Leave(); err != nil {
		logger.Warnw("Gossip leave failed", "error", err.Error())
	}
	a.server.GracefulStop()
	if err := a.lsm.Close(); err != nil {
		logger.Warnw("LSM close failed", "error", err.Error())
	}
	if err := a.client.Close(); err != nil {
		logger.Warnw("Replication client close failed", "error", err.Error())
	}

	return runErr
}
